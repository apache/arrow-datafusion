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

//! Defines common code used in execution plans

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::metrics::MemTrackingMetrics;
use crate::physical_plan::{ColumnStatistics, ExecutionPlan, Statistics};
use crate::record_batch::RecordBatch;
use arrow::compute::aggregate::estimated_bytes_size;
use arrow::compute::concatenate::concatenate;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Error as ArrowError;
use arrow::error::Result as ArrowResult;
use arrow::io::ipc::write::{FileWriter, WriteOptions};
use datafusion_common::field_util::SchemaExt;
use futures::channel::mpsc;
use futures::{Future, SinkExt, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;
use std::fs;
use std::fs::{metadata, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

/// Stream of record batches
pub struct SizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
    metrics: MemTrackingMetrics,
}

impl SizedRecordBatchStream {
    /// Create a new RecordBatchIterator
    pub fn new(
        schema: SchemaRef,
        batches: Vec<Arc<RecordBatch>>,
        metrics: MemTrackingMetrics,
    ) -> Self {
        let size = batches.iter().map(|b| batch_byte_size(b)).sum::<usize>();
        metrics.init_mem_used(size);
        SizedRecordBatchStream {
            schema,
            index: 0,
            batches,
            metrics,
        }
    }
}

impl Stream for SizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = Poll::Ready(if self.index < self.batches.len() {
            self.index += 1;
            Some(Ok(self.batches[self.index - 1].as_ref().clone()))
        } else {
            None
        });
        self.metrics.record_poll(poll)
    }
}

impl RecordBatchStream for SizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(DataFusionError::from)
}

/// Combine a slice of record batches into one, or returns None if the slice itself
/// is empty; all the record batches inside the slice must be of the same schema.
pub(crate) fn combine_batches(
    batches: &[RecordBatch],
    schema: SchemaRef,
) -> ArrowResult<Option<RecordBatch>> {
    if batches.is_empty() {
        Ok(None)
    } else {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _)| {
                concatenate(
                    &batches
                        .iter()
                        .map(|batch| batch.column(i).as_ref())
                        .collect::<Vec<_>>(),
                )
                .map(|x| x.into())
            })
            .collect::<ArrowResult<Vec<_>>>()?;
        Ok(Some(RecordBatch::try_new(schema.clone(), columns)?))
    }
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_checked_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    if filenames.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "No files found at {path} with file extension {file_extension}",
            path = dir,
            file_extension = ext
        )));
    }
    Ok(filenames)
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    Ok(filenames)
}

/// Recursively build a list of files in a directory with a given extension with an accumulator list
fn build_file_list_recurse(
    dir: &str,
    filenames: &mut Vec<String>,
    ext: &str,
) -> Result<()> {
    let metadata = metadata(dir)?;
    if metadata.is_file() {
        if dir.ends_with(ext) {
            filenames.push(dir.to_string());
        }
    } else {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    build_file_list_recurse(path_name, filenames, ext)?;
                } else if path_name.ends_with(ext) {
                    filenames.push(path_name.to_string());
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
    }
    Ok(())
}

/// Spawns a task to the tokio threadpool and writes its outputs to the provided mpsc sender
pub(crate) fn spawn_execution(
    input: Arc<dyn ExecutionPlan>,
    mut output: mpsc::Sender<ArrowResult<RecordBatch>>,
    partition: usize,
    runtime: Arc<RuntimeEnv>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = match input.execute(partition, runtime).await {
            Err(e) => {
                // If send fails, plan being torn
                // down, no place to send the error
                let arrow_error = ArrowError::External("".to_string(), Box::new(e));
                output.send(Err(arrow_error)).await.ok();
                return;
            }
            Ok(stream) => stream,
        };

        while let Some(item) = stream.next().await {
            // If send fails, plan being torn down,
            // there is no place to send the error
            output.send(item).await.ok();
        }
    })
}

/// Computes the statistics for an in-memory RecordBatch
///
/// Only computes statistics that are in arrows metadata (num rows, byte size and nulls)
/// and does not apply any kernel on the actual data.
pub fn compute_record_batch_statistics(
    batches: &[Vec<RecordBatch>],
    schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Statistics {
    let nb_rows = batches.iter().flatten().map(RecordBatch::num_rows).sum();

    let total_byte_size = batches.iter().flatten().map(batch_byte_size).sum();

    let projection = match projection {
        Some(p) => p,
        None => (0..schema.fields().len()).collect(),
    };

    let mut column_statistics = vec![ColumnStatistics::default(); projection.len()];

    for partition in batches.iter() {
        for batch in partition {
            for (stat_index, col_index) in projection.iter().enumerate() {
                *column_statistics[stat_index].null_count.get_or_insert(0) +=
                    batch.column(*col_index).null_count();
            }
        }
    }

    Statistics {
        num_rows: Some(nb_rows),
        total_byte_size: Some(total_byte_size),
        column_statistics: Some(column_statistics),
        is_exact: true,
    }
}

pin_project! {
    /// Helper that aborts the given join handle on drop.
    ///
    /// Useful to kill background tasks when the consumer is dropped.
    #[derive(Debug)]
    pub struct AbortOnDropSingle<T>{
        #[pin]
        join_handle: JoinHandle<T>,
    }

    impl<T> PinnedDrop for AbortOnDropSingle<T> {
        fn drop(this: Pin<&mut Self>) {
            this.join_handle.abort();
        }
    }
}

impl<T> AbortOnDropSingle<T> {
    /// Create new abort helper from join handle.
    pub fn new(join_handle: JoinHandle<T>) -> Self {
        Self { join_handle }
    }
}

impl<T> Future for AbortOnDropSingle<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.join_handle.poll(cx)
    }
}

/// Helper that aborts the given join handles on drop.
///
/// Useful to kill background tasks when the consumer is dropped.
#[derive(Debug)]
pub struct AbortOnDropMany<T>(pub Vec<JoinHandle<T>>);

impl<T> Drop for AbortOnDropMany<T> {
    fn drop(&mut self) {
        for join_handle in &self.0 {
            join_handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Float32Array, Float64Array},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_common::field_util::SchemaExt;

    #[test]
    fn test_combine_batches_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let result = combine_batches(&[], schema)?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_combine_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));

        let batch_count = 1000;
        let batch_size = 10;
        let batches = (0..batch_count)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Float32Array::from_slice(&vec![i as f32; batch_size])),
                        Arc::new(Float64Array::from_slice(&vec![i as f64; batch_size])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let result = combine_batches(&batches, schema)?;
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(batch_count * batch_size, result.num_rows());
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let stats = compute_record_batch_statistics(&[], &schema, Some(vec![0, 1]));

        assert_eq!(stats.num_rows, Some(0));
        assert!(stats.is_exact);
        assert_eq!(stats.total_byte_size, Some(0));
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from_slice(&[1., 2., 3.])),
                Arc::new(Float64Array::from_slice(&[9., 8., 7.])),
            ],
        )?;
        let result =
            compute_record_batch_statistics(&[vec![batch]], &schema, Some(vec![0, 1]));

        let expected = Statistics {
            is_exact: true,
            num_rows: Some(3),
            // TODO: fix this once we got https://github.com/jorgecarleitao/arrow2/issues/421
            total_byte_size: Some(36),
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    null_count: Some(0),
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    null_count: Some(0),
                },
            ]),
        };

        assert_eq!(result, expected);
        Ok(())
    }
}

/// Write in Arrow IPC format.
pub struct IPCWriter {
    /// path
    pub path: PathBuf,
    /// Inner writer
    pub writer: FileWriter<File>,
    /// bathes written
    pub num_batches: u64,
    /// rows written
    pub num_rows: u64,
    /// bytes written
    pub num_bytes: u64,
}

impl IPCWriter {
    /// Create new writer
    pub fn new(path: &Path, schema: &Schema) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create partition file at {:?}: {:?}",
                path, e
            ))
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.into(),
            writer: FileWriter::try_new(file, schema, None, WriteOptions::default())?,
        })
    }

    /// Write one single batch
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(&batch.into(), None)?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows() as u64;
        let num_bytes: usize = batch_byte_size(batch);
        self.num_bytes += num_bytes as u64;
        Ok(())
    }

    /// Finish the writer
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(DataFusionError::ArrowError)
    }

    /// Path write to
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Returns the total number of bytes of memory occupied physically by this batch.
pub fn batch_byte_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|a| estimated_bytes_size(a.as_ref()))
        .sum()
}
