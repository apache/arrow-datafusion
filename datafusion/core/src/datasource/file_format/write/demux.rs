// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Module containing helper methods/traits related to enabling
//! dividing input stream into multiple output files at execution time

use std::collections::HashMap;

use std::sync::Arc;

use crate::datasource::listing::ListingTableUrl;

use crate::error::Result;
use crate::physical_plan::SendableRecordBatchStream;

use arrow_array::builder::UInt64Builder;
use arrow_array::cast::AsArray;
use arrow_array::{downcast_dictionary_array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Schema};
use datafusion_common::cast::as_string_array;
use datafusion_common::{exec_datafusion_err, DataFusionError};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;

use futures::StreamExt;
use object_store::path::Path;

use rand::distributions::DistString;

use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};

type RecordBatchReceiver = Receiver<RecordBatch>;
type DemuxedStreamReceiver = UnboundedReceiver<(Path, RecordBatchReceiver)>;

/// Splits a single [SendableRecordBatchStream] into a dynamically determined
/// number of partitions at execution time. The partitions are determined by
/// factors known only at execution time, such as total number of rows and
/// partition column values. The demuxer task communicates to the caller
/// by sending channels over a channel. The inner channels send RecordBatches
/// which should be contained within the same output file. The outer channel
/// is used to send a dynamic number of inner channels, representing a dynamic
/// number of total output files. The caller is also responsible to monitor
/// the demux task for errors and abort accordingly. The single_file_ouput parameter
/// overrides all other settings to force only a single file to be written.
/// partition_by parameter will additionally split the input based on the unique
/// values of a specific column `<https://github.com/apache/datafusion/issues/7744>``
///                                                                              ┌───────────┐               ┌────────────┐    ┌─────────────┐
///                                                                     ┌──────▶ │  batch 1  ├────▶...──────▶│   Batch a  │    │ Output File1│
///                                                                     │        └───────────┘               └────────────┘    └─────────────┘
///                                                                     │
///                                                 ┌──────────┐        │        ┌───────────┐               ┌────────────┐    ┌─────────────┐
/// ┌───────────┐               ┌────────────┐      │          │        ├──────▶ │  batch a+1├────▶...──────▶│   Batch b  │    │ Output File2│
/// │  batch 1  ├────▶...──────▶│   Batch N  ├─────▶│  Demux   ├────────┤ ...    └───────────┘               └────────────┘    └─────────────┘
/// └───────────┘               └────────────┘      │          │        │
///                                                 └──────────┘        │        ┌───────────┐               ┌────────────┐    ┌─────────────┐
///                                                                     └──────▶ │  batch d  ├────▶...──────▶│   Batch n  │    │ Output FileN│
///                                                                              └───────────┘               └────────────┘    └─────────────┘
pub(crate) fn start_demuxer_task(
    input: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    partition_by: Option<Vec<(String, DataType)>>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    keep_partition_by_columns: bool,
) -> (SpawnedTask<Result<()>>, DemuxedStreamReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    let context = Arc::clone(context);
    let single_file_output = !base_output_path.is_collection();
    let task = match partition_by {
        Some(parts) => {
            // There could be an arbitrarily large number of parallel hive style partitions being written to, so we cannot
            // bound this channel without risking a deadlock.
            SpawnedTask::spawn(async move {
                hive_style_partitions_demuxer(
                    tx,
                    input,
                    context,
                    parts,
                    base_output_path,
                    file_extension,
                    keep_partition_by_columns,
                )
                .await
            })
        }
        None => SpawnedTask::spawn(async move {
            row_count_demuxer(
                tx,
                input,
                context,
                base_output_path,
                file_extension,
                single_file_output,
            )
            .await
        }),
    };

    (task, rx)
}

/// Dynamically partitions input stream to achieve desired maximum rows per file
async fn row_count_demuxer(
    mut tx: UnboundedSender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    single_file_output: bool,
) -> Result<()> {
    let exec_options = &context.session_config().options().execution;

    let max_rows_per_file = exec_options.soft_max_rows_per_output_file;
    let max_buffered_batches = exec_options.max_buffered_batches_per_output_file;
    let minimum_parallel_files = exec_options.minimum_parallel_output_files;
    let mut part_idx = 0;
    let write_id =
        rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

    let mut open_file_streams = Vec::with_capacity(minimum_parallel_files);

    let mut next_send_steam = 0;
    let mut row_counts = Vec::with_capacity(minimum_parallel_files);

    // Overrides if single_file_output is set
    let minimum_parallel_files = if single_file_output {
        1
    } else {
        minimum_parallel_files
    };

    let max_rows_per_file = if single_file_output {
        usize::MAX
    } else {
        max_rows_per_file
    };

    while let Some(rb) = input.next().await.transpose()? {
        // ensure we have at least minimum_parallel_files open
        if open_file_streams.len() < minimum_parallel_files {
            open_file_streams.push(create_new_file_stream(
                &base_output_path,
                &write_id,
                part_idx,
                &file_extension,
                single_file_output,
                max_buffered_batches,
                &mut tx,
            )?);
            row_counts.push(0);
            part_idx += 1;
        } else if row_counts[next_send_steam] >= max_rows_per_file {
            row_counts[next_send_steam] = 0;
            open_file_streams[next_send_steam] = create_new_file_stream(
                &base_output_path,
                &write_id,
                part_idx,
                &file_extension,
                single_file_output,
                max_buffered_batches,
                &mut tx,
            )?;
            part_idx += 1;
        }
        row_counts[next_send_steam] += rb.num_rows();
        open_file_streams[next_send_steam]
            .send(rb)
            .await
            .map_err(|_| {
                DataFusionError::Execution(
                    "Error sending RecordBatch to file stream!".into(),
                )
            })?;

        next_send_steam = (next_send_steam + 1) % minimum_parallel_files;
    }
    Ok(())
}

/// Helper for row count demuxer
fn generate_file_path(
    base_output_path: &ListingTableUrl,
    write_id: &str,
    part_idx: usize,
    file_extension: &str,
    single_file_output: bool,
) -> Path {
    if !single_file_output {
        base_output_path
            .prefix()
            .child(format!("{}_{}.{}", write_id, part_idx, file_extension))
    } else {
        base_output_path.prefix().to_owned()
    }
}

/// Helper for row count demuxer
fn create_new_file_stream(
    base_output_path: &ListingTableUrl,
    write_id: &str,
    part_idx: usize,
    file_extension: &str,
    single_file_output: bool,
    max_buffered_batches: usize,
    tx: &mut UnboundedSender<(Path, Receiver<RecordBatch>)>,
) -> Result<Sender<RecordBatch>> {
    let file_path = generate_file_path(
        base_output_path,
        write_id,
        part_idx,
        file_extension,
        single_file_output,
    );
    let (tx_file, rx_file) = mpsc::channel(max_buffered_batches / 2);
    tx.send((file_path, rx_file)).map_err(|_| {
        DataFusionError::Execution("Error sending RecordBatch to file stream!".into())
    })?;
    Ok(tx_file)
}

/// Splits an input stream based on the distinct values of a set of columns
/// Assumes standard hive style partition paths such as
/// /col1=val1/col2=val2/outputfile.parquet
async fn hive_style_partitions_demuxer(
    tx: UnboundedSender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    partition_by: Vec<(String, DataType)>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    keep_partition_by_columns: bool,
) -> Result<()> {
    let write_id =
        rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

    let exec_options = &context.session_config().options().execution;
    let max_buffered_recordbatches = exec_options.max_buffered_batches_per_output_file;

    // To support non string partition col types, cast the type to &str first
    let mut value_map: HashMap<Vec<String>, Sender<RecordBatch>> = HashMap::new();

    while let Some(rb) = input.next().await.transpose()? {
        // First compute partition key for each row of batch, e.g. (col1=val1, col2=val2, ...)
        let all_partition_values = compute_partition_keys_by_row(&rb, &partition_by)?;

        // Next compute how the batch should be split up to take each distinct key to its own batch
        let take_map = compute_take_arrays(&rb, all_partition_values);

        // Divide up the batch into distinct partition key batches and send each batch
        for (part_key, mut builder) in take_map.into_iter() {
            // Take method adapted from https://github.com/lancedb/lance/pull/1337/files
            // TODO: upstream RecordBatch::take to arrow-rs
            let take_indices = builder.finish();
            let struct_array: StructArray = rb.clone().into();
            let parted_batch = RecordBatch::from(
                arrow::compute::take(&struct_array, &take_indices, None)?.as_struct(),
            );

            // Get or create channel for this batch
            let part_tx = match value_map.get_mut(&part_key) {
                Some(part_tx) => part_tx,
                None => {
                    // Create channel for previously unseen distinct partition key and notify consumer of new file
                    let (part_tx, part_rx) = tokio::sync::mpsc::channel::<RecordBatch>(
                        max_buffered_recordbatches,
                    );
                    let file_path = compute_hive_style_file_path(
                        &part_key,
                        &partition_by,
                        &write_id,
                        &file_extension,
                        &base_output_path,
                    );

                    tx.send((file_path, part_rx)).map_err(|_| {
                        DataFusionError::Execution(
                            "Error sending new file stream!".into(),
                        )
                    })?;

                    value_map.insert(part_key.clone(), part_tx);
                    value_map
                        .get_mut(&part_key)
                        .ok_or(DataFusionError::Internal(
                            "Key must exist since it was just inserted!".into(),
                        ))?
                }
            };

            let final_batch_to_send = if keep_partition_by_columns {
                parted_batch
            } else {
                remove_partition_by_columns(&parted_batch, &partition_by)?
            };

            // Finally send the partial batch partitioned by distinct value!
            part_tx.send(final_batch_to_send).await.map_err(|_| {
                DataFusionError::Internal("Unexpected error sending parted batch!".into())
            })?;
        }
    }

    Ok(())
}

fn compute_partition_keys_by_row<'a>(
    rb: &'a RecordBatch,
    partition_by: &'a [(String, DataType)],
) -> Result<Vec<Vec<&'a str>>> {
    let mut all_partition_values = vec![];

    // For the purposes of writing partitioned data, we can rely on schema inference
    // to determine the type of the partition cols in order to provide a more ergonomic
    // UI which does not require specifying DataTypes manually. So, we ignore the
    // DataType within the partition_by array and infer the correct type from the
    // batch schema instead.
    let schema = rb.schema();
    for (col, _) in partition_by.iter() {
        let mut partition_values = vec![];

        let dtype = schema.field_with_name(col)?.data_type();
        let col_array = rb.column_by_name(col).ok_or(exec_datafusion_err!(
            "PartitionBy Column {} does not exist in source data! Got schema {schema}.",
            col
        ))?;

        match dtype {
            DataType::Utf8 => {
                let array = as_string_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(array.value(i));
                }
            }
            DataType::Dictionary(_, _) => {
                downcast_dictionary_array!(
                    col_array =>  {
                        let array = col_array.downcast_dict::<StringArray>()
                            .ok_or(exec_datafusion_err!("it is not yet supported to write to hive partitions with datatype {}",
                            dtype))?;

                        for val in array.values() {
                            partition_values.push(
                                val.ok_or(exec_datafusion_err!("Cannot partition by null value for column {}", col))?
                            );
                        }
                    },
                    _ => unreachable!(),
                )
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                "it is not yet supported to write to hive partitions with datatype {}",
                dtype
            )))
            }
        }

        all_partition_values.push(partition_values);
    }

    Ok(all_partition_values)
}

fn compute_take_arrays(
    rb: &RecordBatch,
    all_partition_values: Vec<Vec<&str>>,
) -> HashMap<Vec<String>, UInt64Builder> {
    let mut take_map = HashMap::new();
    for i in 0..rb.num_rows() {
        let mut part_key = vec![];
        for vals in all_partition_values.iter() {
            part_key.push(vals[i].to_owned());
        }
        let builder = take_map.entry(part_key).or_insert(UInt64Builder::new());
        builder.append_value(i as u64);
    }
    take_map
}

fn remove_partition_by_columns(
    parted_batch: &RecordBatch,
    partition_by: &[(String, DataType)],
) -> Result<RecordBatch> {
    let partition_names: Vec<_> = partition_by.iter().map(|(s, _)| s).collect();
    let (non_part_cols, non_part_fields): (Vec<_>, Vec<_>) = parted_batch
        .columns()
        .iter()
        .zip(parted_batch.schema().fields())
        .filter_map(|(a, f)| {
            if !partition_names.contains(&f.name()) {
                Some((Arc::clone(a), (**f).clone()))
            } else {
                None
            }
        })
        .unzip();

    let non_part_schema = Schema::new(non_part_fields);
    let final_batch_to_send =
        RecordBatch::try_new(Arc::new(non_part_schema), non_part_cols)?;

    Ok(final_batch_to_send)
}

fn compute_hive_style_file_path(
    part_key: &[String],
    partition_by: &[(String, DataType)],
    write_id: &str,
    file_extension: &str,
    base_output_path: &ListingTableUrl,
) -> Path {
    let mut file_path = base_output_path.prefix().clone();
    for j in 0..part_key.len() {
        file_path = file_path.child(format!("{}={}", partition_by[j].0, part_key[j]));
    }

    file_path.child(format!("{}.{}", write_id, file_extension))
}
