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
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use std::{any::Any, pin::Pin};

use crate::error::BallistaError;
use crate::utils;

use crate::client::BallistaClient;
use crate::serde::protobuf::ShuffleWritePartition;
use crate::serde::scheduler::{ExecutorMeta, PartitionLocation, PartitionStats};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, StringBuilder, StructBuilder, UInt32Builder,
    UInt64Builder,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::common::IPCWriter;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::Partitioning::RoundRobinBatch;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Metric, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::StreamExt;
use hashbrown::HashMap;
use log::{debug, info};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
    /// output location to write output streams to
    pub output_loc: OutputLocation,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
pub enum OutputLocation {
    LocalDir(String),
    Executors(Vec<ExecutorMeta>),
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
        output_loc: OutputLocation,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            plan,
            output_loc,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Create a new shuffle writer for pull based shuffle
    pub fn try_new_pull_shuffle(
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
            output_loc: OutputLocation::LocalDir(work_dir),
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Create a new shuffle writer for push based shuffle
    pub fn try_new_push_shuffle(
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        execs: Vec<ExecutorMeta>,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            stage_id,
            plan,
            output_loc: OutputLocation::Executors(execs),
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

    /// Is push based shuffle
    pub fn is_push_shuffle(&self) -> bool {
        match self.output_loc {
            OutputLocation::LocalDir(_) => false,
            OutputLocation::Executors(_) => true,
        }
    }

    pub async fn execute_shuffle_write(
        &self,
        input_partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        let now = Instant::now();

        let mut stream = self.plan.execute(input_partition, runtime).await?;
        let write_metrics = ShuffleWriteMetrics::new(input_partition, &self.metrics);
        match &self.shuffle_output_partitioning {
            None => {
                let timer = write_metrics.write_time.timer();
                let (stats, path) = match &self.output_loc {
                    OutputLocation::LocalDir(work_dir) => {
                        let mut path = PathBuf::from(work_dir);
                        path.push(&self.job_id);
                        path.push(&format!("{}", self.stage_id));

                        path.push(&format!("{}", input_partition));
                        std::fs::create_dir_all(&path)?;
                        path.push("data.arrow");
                        let path = path.to_str().unwrap();
                        info!("Writing results to local path {}", path);

                        // stream results to disk
                        let stats = utils::write_stream_to_disk(
                            &mut stream,
                            path,
                            &write_metrics.write_time,
                        )
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

                        (stats, path.to_string())
                    }

                    OutputLocation::Executors(execs) => {
                        assert_eq!(execs.len(), 1);

                        let executor = execs[0].to_owned();
                        info!(
                            "Writing results to host {}, port {}",
                            executor.host.as_str(),
                            executor.port
                        );

                        // stream results to network
                        let stats = utils::write_stream_to_flight(
                            stream,
                            executor.host.as_str(),
                            executor.port,
                            self.job_id.clone(),
                            self.stage_id,
                            0,
                            &write_metrics.write_time,
                        )
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
                        (stats, String::from(""))
                    }
                };

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
                    path,
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
                        let indices = partition_indices.into();

                        // Produce batches based on indices
                        let columns = input_batch
                            .columns()
                            .iter()
                            .map(|c| {
                                take(c.as_ref(), &indices, None).map_err(|e| {
                                    DataFusionError::Execution(e.to_string())
                                })
                            })
                            .collect::<Result<Vec<Arc<dyn Array>>>>()?;

                        let output_batch =
                            RecordBatch::try_new(input_batch.schema(), columns)?;

                        let num_rows = output_batch.num_rows();

                        // write non-empty batch out

                        //TODO optimize so we don't write or fetch empty partitions
                        //if output_batch.num_rows() > 0 {
                        let timer = write_metrics.write_time.timer();
                        match &mut writers[output_partition] {
                            Some(w) => {
                                w.write(output_batch).await?;
                            }
                            None => {
                                // create proper shuffle writer
                                match &self.output_loc {
                                    OutputLocation::LocalDir(work_dir) => {
                                        let mut path = PathBuf::from(work_dir);
                                        path.push(&self.job_id);
                                        path.push(&format!("{}", self.stage_id));

                                        path.push(&format!("{}", output_partition));
                                        std::fs::create_dir_all(&path)?;

                                        path.push(format!(
                                            "data-{}.arrow",
                                            input_partition
                                        ));
                                        info!("Writing results to {:?}", path);

                                        let mut writer = IPCWriter::new(
                                            &path,
                                            stream.schema().as_ref(),
                                        )?;
                                        writer.write(&output_batch)?;
                                        writers[output_partition] =
                                            Some(ShuffleWriter::File(Box::from(writer)));
                                    }
                                    OutputLocation::Executors(execs) => {
                                        assert_eq!(execs.len(), num_output_partitions);

                                        let exec = &execs[output_partition];
                                        let mut writer = FlightShuffleWriter::new(
                                            exec.host.clone(),
                                            exec.port,
                                            self.job_id.clone(),
                                            self.stage_id,
                                            output_partition,
                                            &stream.schema(),
                                        )?;
                                        writer.write(output_batch).await?;
                                        writers[output_partition] =
                                            Some(ShuffleWriter::Flight(writer));
                                    }
                                }
                            }
                        }
                        write_metrics.output_rows.add(num_rows);
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
                                    w.num_batches(),
                                    w.num_rows(),
                                    w.num_bytes()
                                );

                            part_locs.push(ShuffleWritePartition {
                                partition_id: i as u64,
                                path: w.path().to_owned(),
                                num_batches: w.num_batches(),
                                num_rows: w.num_rows(),
                                num_bytes: w.num_bytes(),
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

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(ShuffleWriterExec::try_new(
            self.job_id.clone(),
            self.stage_id,
            children[0].clone(),
            self.output_loc.clone(),
            self.shuffle_output_partitioning.clone(),
        )?))
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let part_loc = self.execute_shuffle_write(partition, runtime).await?;

        // build metadata result batch
        let num_writers = part_loc.len();
        let mut partition_builder = UInt32Builder::new(num_writers);
        let mut path_builder = StringBuilder::new(num_writers);
        let mut num_rows_builder = UInt64Builder::new(num_writers);
        let mut num_batches_builder = UInt64Builder::new(num_writers);
        let mut num_bytes_builder = UInt64Builder::new(num_writers);

        for loc in &part_loc {
            path_builder.append_value(loc.path.clone())?;
            partition_builder.append_value(loc.partition_id as u32)?;
            num_rows_builder.append_value(loc.num_rows)?;
            num_batches_builder.append_value(loc.num_batches)?;
            num_bytes_builder.append_value(loc.num_bytes)?;
        }

        // build arrays
        let partition_num: ArrayRef = Arc::new(partition_builder.finish());
        let path: ArrayRef = Arc::new(path_builder.finish());
        let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(num_rows_builder),
            Box::new(num_batches_builder),
            Box::new(num_bytes_builder),
        ];
        let mut stats_builder = StructBuilder::new(
            PartitionStats::default().arrow_struct_fields(),
            field_builders,
        );
        for _ in 0..num_writers {
            stats_builder.append(true)?;
        }
        let stats = Arc::new(stats_builder.finish());

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

/// Different Shuffle writers
enum ShuffleWriter {
    File(Box<IPCWriter>),
    Flight(FlightShuffleWriter),
}

impl ShuffleWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            ShuffleWriter::File(writer) => writer.write(&batch),
            ShuffleWriter::Flight(writer) => writer.write(batch).await,
        }
    }

    fn finish(&mut self) -> Result<()> {
        match self {
            ShuffleWriter::File(writer) => writer.finish(),
            ShuffleWriter::Flight(writer) => writer.finish(),
        }
    }

    fn path(&self) -> &str {
        match self {
            ShuffleWriter::File(writer) => writer.path().to_str().unwrap_or(""),
            ShuffleWriter::Flight(writer) => writer.path(),
        }
    }

    pub fn num_batches(&self) -> u64 {
        match self {
            ShuffleWriter::File(writer) => writer.num_batches,
            ShuffleWriter::Flight(writer) => writer.num_batches,
        }
    }

    pub fn num_rows(&self) -> u64 {
        match self {
            ShuffleWriter::File(writer) => writer.num_rows,
            ShuffleWriter::Flight(writer) => writer.num_rows,
        }
    }

    pub fn num_bytes(&self) -> u64 {
        match self {
            ShuffleWriter::File(writer) => writer.num_bytes,
            ShuffleWriter::Flight(writer) => writer.num_bytes,
        }
    }
}

struct FlightShuffleWriter {
    pub num_batches: u64,
    pub num_rows: u64,
    pub num_bytes: u64,
    pub sender: Sender<ArrowResult<RecordBatch>>,
}

impl FlightShuffleWriter {
    fn new(
        host: String,
        port: u16,
        job_id: String,
        stage_id: usize,
        partition_id: usize,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let (sender, receiver): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let stream = RecordBatchReceiverStream::create(schema, receiver, None);

        tokio::task::spawn(async move {
            let mut client = BallistaClient::try_new(host.as_str(), port)
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

            let stat = client
                .push_partition(job_id, stage_id, partition_id, stream)
                .await
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)));
            stat
        });

        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            sender,
        })
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        let num_bytes: usize = batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum();
        self.sender.send(ArrowResult::Ok(batch)).await.ok();
        self.num_batches += 1;
        self.num_rows += num_rows as u64;
        self.num_bytes += num_bytes as u64;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn path(&self) -> &str {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, StructArray, UInt32Array, UInt64Array};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());

        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new_pull_shuffle(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, runtime).await?;
        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let path = batch.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
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
        let runtime = Arc::new(RuntimeEnv::default());

        let input_plan = create_input_plan()?;
        let work_dir = TempDir::new()?;
        let query_stage = ShuffleWriterExec::try_new_pull_shuffle(
            "jobOne".to_owned(),
            1,
            input_plan,
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0, runtime).await?;
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
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        Ok(Arc::new(MemoryExec::try_new(&partitions, schema, None)?))
    }
}
