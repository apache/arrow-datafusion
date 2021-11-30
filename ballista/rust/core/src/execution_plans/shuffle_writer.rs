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

//! ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
//! can be executed as one unit with each partition being executed in parallel. The output of each
//! partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use std::fs::File;
use std::iter::{FromIterator, Iterator};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{any::Any, pin::Pin};

use crate::error::BallistaError;
use crate::memory_stream::MemoryStream;
use crate::utils;

use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::{PartitionLocation, PartitionStats};
use arrow::io::ipc::write::WriteOptions;
use async_trait::async_trait;
use datafusion::arrow::array::*;
use datafusion::arrow::compute::aggregate::estimated_bytes_size;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::io::ipc::read::FileReader;
use datafusion::arrow::io::ipc::write::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::Partitioning::RoundRobinBatch;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Metric, Partitioning, RecordBatchStream, Statistics,
};
use futures::StreamExt;
use hashbrown::HashMap;
use log::{debug, info};
use std::cell::RefCell;
use std::io::BufWriter;
use uuid::Uuid;

/// ShuffleWriterExec represents a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone)]
pub struct ShuffleWriterExec {
    /// Unique ID for the job (query) that this stage is a part of
    job_id: String,
    /// Unique query stage ID within the job
    stage_id: usize,
    /// Physical execution plan for this query stage
    plan: Arc<dyn ExecutionPlan>,
    /// Path to write output streams to
    work_dir: String,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
struct ShuffleWriteMetrics {
    /// Time spend writing batches to shuffle files
    write_time: metrics::Time,
    input_rows: metrics::Count,
    output_rows: metrics::Count,
}

impl ShuffleWriteMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let write_time = MetricBuilder::new(metrics).subset_time("write_time", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            write_time,
            input_rows,
            output_rows,
        }
    }
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: String,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            plan,
            work_dir,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Get the Job ID for this query stage
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the Stage ID for this query stage
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the true output partitioning
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    pub async fn execute_shuffle_write(
        &self,
        input_partition: usize,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let now = Instant::now();

        let mut stream = self.plan.execute(input_partition).await?;

        let mut path = PathBuf::from(&self.work_dir);
        path.push(&self.job_id);
        path.push(&format!("{}", self.stage_id));

        let write_metrics = ShuffleWriteMetrics::new(input_partition, &self.metrics);

        match &self.shuffle_output_partitioning {
            None => {
                let timer = write_metrics.write_time.timer();
                path.push(&format!("{}", input_partition));
                std::fs::create_dir_all(&path)?;
                path.push("data.arrow");
                let path = path.to_str().unwrap();
                info!("Writing results to {}", path);

                // stream results to disk
                let stats = utils::write_stream_to_disk(
                    &mut stream,
                    path,
                    &write_metrics.write_time,
                )
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

                write_metrics
                    .input_rows
                    .add(stats.num_rows.unwrap_or(0) as usize);
                write_metrics
                    .output_rows
                    .add(stats.num_rows.unwrap_or(0) as usize);
                timer.done();

                info!(
                    "Executed partition {} in {} seconds. Statistics: {}",
                    input_partition,
                    now.elapsed().as_secs(),
                    stats
                );

                Ok(vec![ShuffleWritePartition {
                    partition_id: input_partition as u64,
                    path: path.to_owned(),
                    num_batches: stats.num_batches.unwrap_or(0),
                    num_rows: stats.num_rows.unwrap_or(0),
                    num_bytes: stats.num_bytes.unwrap_or(0),
                }])
            }

            Some(Partitioning::Hash(exprs, n)) => {
                let num_output_partitions = *n;

                // we won't necessary produce output for every possible partition, so we
                // create writers on demand
                let mut writers: Vec<Option<ShuffleWriter>> = vec![];
                for _ in 0..num_output_partitions {
                    writers.push(None);
                }

                let hashes_buf = &mut vec![];
                let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);

                while let Some(result) = stream.next().await {
                    let input_batch = result?;

                    write_metrics.input_rows.add(input_batch.num_rows());

                    let arrays = exprs
                        .iter()
                        .map(|expr| {
                            Ok(expr
                                .evaluate(&input_batch)?
                                .into_array(input_batch.num_rows()))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    hashes_buf.clear();
                    hashes_buf.resize(arrays[0].len(), 0);
                    // Hash arrays and compute buckets based on number of partitions
                    let hashes = create_hashes(&arrays, &random_state, hashes_buf)?;
                    let mut indices = vec![vec![]; num_output_partitions];
                    for (index, hash) in hashes.iter().enumerate() {
                        indices[(*hash % num_output_partitions as u64) as usize]
                            .push(index as u64)
                    }
                    for (output_partition, partition_indices) in
                        indices.into_iter().enumerate()
                    {
                        // Produce batches based on indices
                        let columns = input_batch
                            .columns()
                            .iter()
                            .map(|c| {
                                take::take(
                                    c.as_ref(),
                                    &PrimitiveArray::<u64>::from_slice(
                                        &partition_indices,
                                    ),
                                )
                                .map_err(|e| DataFusionError::Execution(e.to_string()))
                                .map(ArrayRef::from)
                            })
                            .collect::<Result<Vec<Arc<dyn Array>>>>()?;

                        let output_batch =
                            RecordBatch::try_new(input_batch.schema().clone(), columns)?;

                        // write non-empty batch out

                        //TODO optimize so we don't write or fetch empty partitions
                        //if output_batch.num_rows() > 0 {
                        let timer = write_metrics.write_time.timer();
                        match &mut writers[output_partition] {
                            Some(w) => {
                                w.write(&output_batch)?;
                            }
                            None => {
                                let mut path = path.clone();
                                path.push(&format!("{}", output_partition));
                                std::fs::create_dir_all(&path)?;

                                path.push(format!("data-{}.arrow", input_partition));
                                let path = path.to_str().unwrap();
                                info!("Writing results to {}", path);

                                let mut writer =
                                    ShuffleWriter::new(path, stream.schema().as_ref())?;

                                writer.write(&output_batch)?;
                                writers[output_partition] = Some(writer);
                            }
                        }
                        write_metrics.output_rows.add(output_batch.num_rows());
                        timer.done();
                    }
                }

                let mut part_locs = vec![];

                for (i, w) in writers.iter_mut().enumerate() {
                    match w {
                        Some(w) => {
                            w.finish()?;
                            info!(
                                "Finished writing shuffle partition {} at {}. Batches: {}. Rows: {}. Bytes: {}.",
                                i,
                                w.path(),
                                w.num_batches,
                                w.num_rows,
                                w.num_bytes
                            );

                            part_locs.push(ShuffleWritePartition {
                                partition_id: i as u64,
                                path: w.path().to_owned(),
                                num_batches: w.num_batches,
                                num_rows: w.num_rows,
                                num_bytes: w.num_bytes,
                            });
                        }
                        None => {}
                    }
                }
                Ok(part_locs)
            }

            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // This operator needs to be executed once for each *input* partition and there
        // isn't really a mechanism yet in DataFusion to support this use case so we report
        // the input partitioning as the output partitioning here. The executor reports
        // output partition meta data back to the scheduler.
        self.plan.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(ShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            children[0].clone(),
            self.work_dir.clone(),
            self.shuffle_output_partitioning.clone(),
        )?))
    }

    async fn execute(
        &self,
        input_partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let part_loc = self.execute_shuffle_write(input_partition).await?;

        // build metadata result batch
        let num_writers = part_loc.len();
        let mut partition_builder = UInt32Vec::with_capacity(num_writers);
        let mut path_builder = MutableUtf8Array::<i32>::with_capacity(num_writers);
        let mut num_rows_builder = UInt64Vec::with_capacity(num_writers);
        let mut num_batches_builder = UInt64Vec::with_capacity(num_writers);
        let mut num_bytes_builder = UInt64Vec::with_capacity(num_writers);

        for loc in &part_loc {
            path_builder.push(Some(loc.path.clone()));
            partition_builder.push(Some(loc.partition_id as u32));
            num_rows_builder.push(Some(loc.num_rows));
            num_batches_builder.push(Some(loc.num_batches));
            num_bytes_builder.push(Some(loc.num_bytes));
        }

        // build arrays
        let partition_num: ArrayRef = partition_builder.into_arc();
        let path: ArrayRef = path_builder.into_arc();
        let field_builders: Vec<Arc<dyn Array>> = vec![
            num_rows_builder.into_arc(),
            num_batches_builder.into_arc(),
            num_bytes_builder.into_arc(),
        ];
        let stats_builder = StructArray::from_data(
            DataType::Struct(PartitionStats::default().arrow_struct_fields()),
            field_builders,
            None,
        );
        let stats = Arc::new(stats_builder);

        // build result batch containing metadata
        let schema = result_schema();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![partition_num, path, stats])
                .map_err(DataFusionError::ArrowError)?;

        debug!("RESULTS METADATA:\n{:?}", batch);

        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ShuffleWriterExec: {:?}",
                    self.shuffle_output_partitioning
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.plan.statistics()
    }
}

fn result_schema() -> SchemaRef {
    let stats = PartitionStats::default();
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        stats.arrow_struct_repr(),
    ]))
}

struct ShuffleWriter {
    path: String,
    writer: FileWriter<BufWriter<File>>,
    num_batches: u64,
    num_rows: u64,
    num_bytes: u64,
}

impl ShuffleWriter {
    fn new(path: &str, schema: &Schema) -> Result<Self> {
        let file = File::create(path)
            .map_err(|e| {
                BallistaError::General(format!(
                    "Failed to create partition file at {}: {:?}",
                    path, e
                ))
            })
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        let buffer_writer = std::io::BufWriter::new(file);
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.to_owned(),
            writer: FileWriter::try_new(buffer_writer, schema, WriteOptions::default())?,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows() as u64;
        let num_bytes: usize = batch
            .columns()
            .iter()
            .map(|array| estimated_bytes_size(array.as_ref()))
            .sum();
        self.num_bytes += num_bytes as u64;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(DataFusionError::ArrowError)
    }

    fn path(&self) -> &str {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StructArray, UInt32Array, UInt64Array, Utf8Array};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use std::borrow::Borrow;
    use tempfile::TempDir;

    pub trait StructArrayExt {
        fn column_names(&self) -> Vec<&str>;
        fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef>;
    }

    impl StructArrayExt for StructArray {
        fn column_names(&self) -> Vec<&str> {
            self.fields().iter().map(|f| f.name.as_str()).collect()
        }

        fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef> {
            self.fields()
                .iter()
                .position(|c| c.name() == column_name)
                .map(|pos| self.values()[pos].borrow())
        }
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0).await?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let path = batch.columns()[1]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();

        let file0 = path.value(0);
        assert!(
            file0.ends_with("/jobOne/1/0/data-0.arrow")
                || file0.ends_with("\\jobOne\\1\\0\\data-0.arrow")
        );
        let file1 = path.value(1);
        assert!(
            file1.ends_with("/jobOne/1/1/data-0.arrow")
                || file1.ends_with("\\jobOne\\1\\1\\data-0.arrow")
        );

        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let num_rows = stats
            // see https://github.com/jorgecarleitao/arrow2/pull/416 for fix
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(4, num_rows.value(0));
        assert_eq!(4, num_rows.value(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned() -> Result<()> {
        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0).await?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let stats = batch.columns()[2]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let num_rows = stats
            // see https://github.com/jorgecarleitao/arrow2/pull/416 for fix
            .column_by_name("num_rows")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, num_rows.value(0));
        assert_eq!(2, num_rows.value(1));

        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(Utf8Array::<i32>::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        Ok(Arc::new(MemoryExec::try_new(&partitions, schema, None)?))
    }
}
