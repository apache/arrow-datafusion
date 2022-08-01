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

//! The repartition operator maps N input partitions to M output partitions based on a
//! partitioning scheme.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::hash_utils::create_hashes;
use crate::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, Statistics};
use arrow::array::{ArrayRef, UInt64Builder};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use log::debug;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::common::{AbortOnDropMany, AbortOnDropSingle};
use super::expressions::PhysicalSortExpr;
use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{RecordBatchStream, SendableRecordBatchStream};

use crate::execution::context::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use futures::stream::Stream;
use futures::StreamExt;
use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

type MaybeBatch = Option<ArrowResult<RecordBatch>>;

/// Inner state of [`RepartitionExec`].
#[derive(Debug)]
struct RepartitionExecState {
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number.
    channels:
        HashMap<usize, (UnboundedSender<MaybeBatch>, UnboundedReceiver<MaybeBatch>)>,

    /// Helper that ensures that that background job is killed once it is no longer needed.
    abort_helper: Arc<AbortOnDropMany<()>>,
}

/// A utility that can be used to partition batches based on [`Partitioning`]
pub struct BatchPartitioner {
    state: BatchPartitionerState,
    timer: metrics::Time,
}

enum BatchPartitionerState {
    Hash {
        random_state: ahash::RandomState,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        num_partitions: usize,
        hash_buffer: Vec<u64>,
    },
    RoundRobin {
        num_partitions: usize,
        next_idx: usize,
    },
}

impl BatchPartitioner {
    /// Create a new [`BatchPartitioner`] with the provided [`Partitioning`]
    ///
    /// The time spent repartitioning will be recorded to `timer`
    pub fn try_new(partitioning: Partitioning, timer: metrics::Time) -> Result<Self> {
        let state = match partitioning {
            Partitioning::RoundRobinBatch(num_partitions) => {
                BatchPartitionerState::RoundRobin {
                    num_partitions,
                    next_idx: 0,
                }
            }
            Partitioning::Hash(exprs, num_partitions) => BatchPartitionerState::Hash {
                exprs,
                num_partitions,
                // Use fixed random hash
                random_state: ahash::RandomState::with_seeds(0, 0, 0, 0),
                hash_buffer: vec![],
            },
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported repartitioning scheme {:?}",
                    other
                )))
            }
        };

        Ok(Self { state, timer })
    }

    /// Partition the provided [`RecordBatch`] into one or more partitioned [`RecordBatch`]
    /// based on the [`Partitioning`] specified on construction
    ///
    /// `f` will be called for each partitioned [`RecordBatch`] with the corresponding
    /// partition index. Any error returned by `f` will be immediately returned by this
    /// function without attempting to publish further [`RecordBatch`]
    ///
    /// The time spent repartitioning, not including time spent in `f` will be recorded
    /// to the [`metrics::Time`] provided on construction
    pub fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        match &mut self.state {
            BatchPartitionerState::RoundRobin {
                num_partitions,
                next_idx,
            } => {
                let idx = *next_idx;
                *next_idx = (*next_idx + 1) % *num_partitions;
                f(idx, batch)?;
            }
            BatchPartitionerState::Hash {
                random_state,
                exprs,
                num_partitions: partitions,
                hash_buffer,
            } => {
                let mut timer = self.timer.timer();

                let arrays = exprs
                    .iter()
                    .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
                    .collect::<Result<Vec<_>>>()?;

                hash_buffer.clear();
                hash_buffer.resize(batch.num_rows(), 0);

                create_hashes(&arrays, random_state, hash_buffer)?;

                let mut indices: Vec<_> = (0..*partitions)
                    .map(|_| UInt64Builder::new(batch.num_rows()))
                    .collect();

                for (index, hash) in hash_buffer.iter().enumerate() {
                    indices[(*hash % *partitions as u64) as usize]
                        .append_value(index as u64);
                }

                for (partition, mut indices) in indices.into_iter().enumerate() {
                    let indices = indices.finish();
                    if indices.is_empty() {
                        continue;
                    }

                    // Produce batches based on indices
                    let columns = batch
                        .columns()
                        .iter()
                        .map(|c| {
                            arrow::compute::take(c.as_ref(), &indices, None)
                                .map_err(DataFusionError::ArrowError)
                        })
                        .collect::<Result<Vec<ArrayRef>>>()?;

                    let batch = RecordBatch::try_new(batch.schema(), columns).unwrap();

                    timer.stop();
                    f(partition, batch)?;
                    timer.restart();
                }
            }
        }
        Ok(())
    }
}

/// The repartition operator maps N input partitions to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the resulting partitions.
#[derive(Debug)]
pub struct RepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,

    /// Partitioning scheme to use
    partitioning: Partitioning,

    /// Inner state that is initialized when the first output stream is created.
    state: Arc<Mutex<RepartitionExecState>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
struct RepartitionMetrics {
    /// Time in nanos to execute child operator and fetch batches
    fetch_time: metrics::Time,
    /// Time in nanos to perform repartitioning
    repart_time: metrics::Time,
    /// Time in nanos for sending resulting batches to channels
    send_time: metrics::Time,
}

impl RepartitionMetrics {
    pub fn new(
        output_partition: usize,
        input_partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let label = metrics::Label::new("inputPartition", input_partition.to_string());

        // Time in nanos to execute child operator and fetch batches
        let fetch_time = MetricBuilder::new(metrics)
            .with_label(label.clone())
            .subset_time("fetch_time", output_partition);

        // Time in nanos to perform repartitioning
        let repart_time = MetricBuilder::new(metrics)
            .with_label(label.clone())
            .subset_time("repart_time", output_partition);

        // Time in nanos for sending resulting batches to channels
        let send_time = MetricBuilder::new(metrics)
            .with_label(label)
            .subset_time("send_time", output_partition);

        Self {
            fetch_time,
            repart_time,
            send_time,
        }
    }
}

impl RepartitionExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }
}

impl ExecutionPlan for RepartitionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RepartitionExec::try_new(
            children[0].clone(),
            self.partitioning.clone(),
        )?))
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start RepartitionExec::execute for partition: {}",
            partition
        );
        // lock mutexes
        let mut state = self.state.lock();

        let num_input_partitions = self.input.output_partitioning().partition_count();
        let num_output_partitions = self.partitioning.partition_count();

        // if this is the first partition to be invoked then we need to set up initial state
        if state.channels.is_empty() {
            // create one channel per *output* partition
            for partition in 0..num_output_partitions {
                // Note that this operator uses unbounded channels to avoid deadlocks because
                // the output partitions can be read in any order and this could cause input
                // partitions to be blocked when sending data to output UnboundedReceivers that are not
                // being read yet. This may cause high memory usage if the next operator is
                // reading output partitions in order rather than concurrently. One workaround
                // for this would be to add spill-to-disk capabilities.
                let (sender, receiver) =
                    mpsc::unbounded_channel::<Option<ArrowResult<RecordBatch>>>();
                state.channels.insert(partition, (sender, receiver));
            }

            // launch one async task per *input* partition
            let mut join_handles = Vec::with_capacity(num_input_partitions);
            for i in 0..num_input_partitions {
                let txs: HashMap<_, _> = state
                    .channels
                    .iter()
                    .map(|(partition, (tx, _rx))| (*partition, tx.clone()))
                    .collect();

                let r_metrics = RepartitionMetrics::new(i, partition, &self.metrics);

                let input_task: JoinHandle<Result<()>> =
                    tokio::spawn(Self::pull_from_input(
                        self.input.clone(),
                        i,
                        txs.clone(),
                        self.partitioning.clone(),
                        r_metrics,
                        context.clone(),
                    ));

                // In a separate task, wait for each input to be done
                // (and pass along any errors, including panic!s)
                let join_handle = tokio::spawn(Self::wait_for_task(
                    AbortOnDropSingle::new(input_task),
                    txs,
                ));
                join_handles.push(join_handle);
            }

            state.abort_helper = Arc::new(AbortOnDropMany(join_handles))
        }

        debug!(
            "Before returning stream in RepartitionExec::execute for partition: {}",
            partition
        );

        // now return stream for the specified *output* partition which will
        // read from the channel
        Ok(Box::pin(RepartitionStream {
            num_input_partitions,
            num_input_partitions_processed: 0,
            schema: self.input.schema(),
            input: UnboundedReceiverStream::new(
                state.channels.remove(&partition).unwrap().1,
            ),
            drop_helper: Arc::clone(&state.abort_helper),
        }))
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
                write!(f, "RepartitionExec: partitioning={:?}", self.partitioning)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

impl RepartitionExec {
    /// Create a new RepartitionExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(RepartitionExec {
            input,
            partitioning,
            state: Arc::new(Mutex::new(RepartitionExecState {
                channels: HashMap::new(),
                abort_helper: Arc::new(AbortOnDropMany::<()>(vec![])),
            })),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning
    ///
    /// i is the input partition index
    ///
    /// txs hold the output sending channels for each output partition
    async fn pull_from_input(
        input: Arc<dyn ExecutionPlan>,
        i: usize,
        mut txs: HashMap<usize, UnboundedSender<Option<ArrowResult<RecordBatch>>>>,
        partitioning: Partitioning,
        r_metrics: RepartitionMetrics,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let mut partitioner =
            BatchPartitioner::try_new(partitioning, r_metrics.repart_time.clone())?;

        // execute the child operator
        let timer = r_metrics.fetch_time.timer();
        let mut stream = input.execute(i, context)?;
        timer.done();

        // While there are still outputs to send to, keep
        // pulling inputs
        while !txs.is_empty() {
            // fetch the next batch
            let timer = r_metrics.fetch_time.timer();
            let result = stream.next().await;
            timer.done();

            // Input is done
            let batch = match result {
                Some(result) => result?,
                None => break,
            };

            partitioner.partition(batch, |partition, partitioned| {
                let timer = r_metrics.send_time.timer();
                // if there is still a receiver, send to it
                if let Some(tx) = txs.get_mut(&partition) {
                    if tx.send(Some(Ok(partitioned))).is_err() {
                        // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                        txs.remove(&partition);
                    }
                }
                timer.done();
                Ok(())
            })?;
        }

        Ok(())
    }

    /// Waits for `input_task` which is consuming one of the inputs to
    /// complete. Upon each successful completion, sends a `None` to
    /// each of the output tx channels to signal one of the inputs is
    /// complete. Upon error, propagates the errors to all output tx
    /// channels.
    async fn wait_for_task(
        input_task: AbortOnDropSingle<Result<()>>,
        txs: HashMap<usize, UnboundedSender<Option<ArrowResult<RecordBatch>>>>,
    ) {
        // wait for completion, and propagate error
        // note we ignore errors on send (.ok) as that means the receiver has already shutdown.
        match input_task.await {
            // Error in joining task
            Err(e) => {
                for (_, tx) in txs {
                    let err = DataFusionError::Execution(format!("Join Error: {}", e));
                    let err = Err(err.into());
                    tx.send(Some(err)).ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                for (_, tx) in txs {
                    // wrap it because need to send error to all output partitions
                    let err = DataFusionError::Execution(e.to_string());
                    let err = Err(err.into());
                    tx.send(Some(err)).ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {
                // notify each output partition that this input partition has no more data
                for (_, tx) in txs {
                    tx.send(None).ok();
                }
            }
        }
    }
}

struct RepartitionStream {
    /// Number of input partitions that will be sending batches to this output channel
    num_input_partitions: usize,

    /// Number of input partitions that have finished sending batches to this output channel
    num_input_partitions_processed: usize,

    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    input: UnboundedReceiverStream<Option<ArrowResult<RecordBatch>>>,

    /// Handle to ensure background tasks are killed when no longer needed.
    #[allow(dead_code)]
    drop_helper: Arc<AbortOnDropMany<()>>,
}

impl Stream for RepartitionStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Some(v))) => Poll::Ready(Some(v)),
            Poll::Ready(Some(None)) => {
                self.num_input_partitions_processed += 1;
                if self.num_input_partitions == self.num_input_partitions_processed {
                    // all input partitions have finished sending batches
                    Poll::Ready(None)
                } else {
                    // other partitions still have data to send
                    self.poll_next(cx)
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_slice::FromSlice;
    use crate::prelude::SessionContext;
    use crate::test::create_vec_batches;
    use crate::{
        assert_batches_sorted_eq,
        physical_plan::{collect, expressions::col, memory::MemoryExec},
        test::{
            assert_is_pending,
            exec::{
                assert_strong_count_converges_to_zero, BarrierExec, BlockingExec,
                ErrorExec, MockExec,
            },
        },
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::{
        array::{ArrayRef, StringArray},
        error::ArrowError,
    };
    use futures::FutureExt;
    use std::collections::HashSet;

    #[tokio::test]
    async fn one_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition];

        // repartition from 1 input to 4 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(4)).await?;

        assert_eq!(4, output_partitions.len());
        assert_eq!(13, output_partitions[0].len());
        assert_eq!(13, output_partitions[1].len());
        assert_eq!(12, output_partitions[2].len());
        assert_eq!(12, output_partitions[3].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_one_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 1 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(1)).await?;

        assert_eq!(1, output_partitions.len());
        assert_eq!(150, output_partitions[0].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 5 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await?;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_hash_partition() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        let output_partitions = repartition(
            &schema,
            partitions,
            Partitioning::Hash(vec![col("c0", &schema)?], 8),
        )
        .await?;

        let total_rows: usize = output_partitions
            .iter()
            .map(|x| x.iter().map(|x| x.num_rows()).sum::<usize>())
            .sum();

        assert_eq!(8, output_partitions.len());
        assert_eq!(total_rows, 8 * 50 * 3);

        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    async fn repartition(
        schema: &SchemaRef,
        input_partitions: Vec<Vec<RecordBatch>>,
        partitioning: Partitioning,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, schema.clone(), None)?;
        let exec = RepartitionExec::try_new(Arc::new(exec), partitioning)?;

        // execute and collect results
        let mut output_partitions = vec![];
        for i in 0..exec.partitioning.partition_count() {
            // execute this *output* partition and collect all batches
            let mut stream = exec.execute(i, task_ctx.clone())?;
            let mut batches = vec![];
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            output_partitions.push(batches);
        }
        Ok(output_partitions)
    }

    #[tokio::test]
    async fn many_to_many_round_robin_within_tokio_task() -> Result<()> {
        let join_handle: JoinHandle<Result<Vec<Vec<RecordBatch>>>> =
            tokio::spawn(async move {
                // define input partitions
                let schema = test_schema();
                let partition = create_vec_batches(&schema, 50);
                let partitions =
                    vec![partition.clone(), partition.clone(), partition.clone()];

                // repartition from 3 input to 5 output
                repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await
            });

        let output_partitions = join_handle
            .await
            .map_err(|e| DataFusionError::Internal(e.to_string()))??;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }

    #[tokio::test]
    async fn unsupported_partitioning() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        // have to send at least one batch through to provoke error
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch)], schema);
        // This generates an error (partitioning type not supported)
        // but only after the plan is executed. The error should be
        // returned and no results produced
        let partitioning = Partitioning::UnknownPartitioning(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::physical_plan::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string
                .contains("Unsupported repartitioning scheme UnknownPartitioning(1)"),
            "actual: {}",
            result_string
        );
    }

    #[tokio::test]
    async fn error_for_input_exec() {
        // This generates an error on a call to execute. The error
        // should be returned and no results produced.

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let input = ErrorExec::new();
        let partitioning = Partitioning::RoundRobinBatch(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::physical_plan::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("ErrorExec, unsurprisingly, errored in partition 0"),
            "actual: {}",
            result_string
        );
    }

    #[tokio::test]
    async fn repartition_with_error_in_stream() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        // input stream returns one good batch and then one error. The
        // error should be returned.
        let err = Err(ArrowError::ComputeError("bad data error".to_string()));

        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch), err], schema);
        let partitioning = Partitioning::RoundRobinBatch(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::physical_plan::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("bad data error"),
            "actual: {}",
            result_string
        );
    }

    #[tokio::test]
    async fn repartition_with_delayed_stream() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        // The mock exec doesn't return immediately (instead it
        // requires the input to wait at least once)
        let schema = batch1.schema();
        let expected_batches = vec![batch1.clone(), batch2.clone()];
        let input = MockExec::new(vec![Ok(batch1), Ok(batch2)], schema);
        let partitioning = Partitioning::RoundRobinBatch(1);

        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        let expected = vec![
            "+------------------+",
            "| my_awesome_field |",
            "+------------------+",
            "| foo              |",
            "| bar              |",
            "| frob             |",
            "| baz              |",
            "+------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &expected_batches);

        let output_stream = exec.execute(0, task_ctx).unwrap();
        let batches = crate::physical_plan::common::collect(output_stream)
            .await
            .unwrap();

        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn robin_repartition_with_dropping_output_stream() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let partitioning = Partitioning::RoundRobinBatch(2);
        // The barrier exec waits to be pinged
        // requires the input to wait at least once)
        let input = Arc::new(make_barrier_exec());

        // partition into two output streams
        let exec = RepartitionExec::try_new(input.clone(), partitioning).unwrap();

        let output_stream0 = exec.execute(0, task_ctx.clone()).unwrap();
        let output_stream1 = exec.execute(1, task_ctx.clone()).unwrap();

        // now, purposely drop output stream 0
        // *before* any outputs are produced
        std::mem::drop(output_stream0);

        // Now, start sending input
        input.wait().await;

        // output stream 1 should *not* error and have one of the input batches
        let batches = crate::physical_plan::common::collect(output_stream1)
            .await
            .unwrap();

        let expected = vec![
            "+------------------+",
            "| my_awesome_field |",
            "+------------------+",
            "| baz              |",
            "| frob             |",
            "| gaz              |",
            "| grob             |",
            "+------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    // As the hash results might be different on different platforms or
    // wiht different compilers, we will compare the same execution with
    // and without droping the output stream.
    async fn hash_repartition_with_dropping_output_stream() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let partitioning = Partitioning::Hash(
            vec![Arc::new(crate::physical_plan::expressions::Column::new(
                "my_awesome_field",
                0,
            ))],
            2,
        );

        // We first collect the results without droping the output stream.
        let input = Arc::new(make_barrier_exec());
        let exec = RepartitionExec::try_new(input.clone(), partitioning.clone()).unwrap();
        let output_stream1 = exec.execute(1, task_ctx.clone()).unwrap();
        input.wait().await;
        let batches_without_drop = crate::physical_plan::common::collect(output_stream1)
            .await
            .unwrap();

        // run some checks on the result
        let items_vec = str_batches_to_vec(&batches_without_drop);
        let items_set: HashSet<&str> = items_vec.iter().copied().collect();
        assert_eq!(items_vec.len(), items_set.len());
        let source_str_set: HashSet<&str> =
            ["foo", "bar", "frob", "baz", "goo", "gar", "grob", "gaz"]
                .iter()
                .copied()
                .collect();
        assert_eq!(items_set.difference(&source_str_set).count(), 0);

        // Now do the same but dropping the stream before waiting for the barrier
        let input = Arc::new(make_barrier_exec());
        let exec = RepartitionExec::try_new(input.clone(), partitioning).unwrap();
        let output_stream0 = exec.execute(0, task_ctx.clone()).unwrap();
        let output_stream1 = exec.execute(1, task_ctx.clone()).unwrap();
        // now, purposely drop output stream 0
        // *before* any outputs are produced
        std::mem::drop(output_stream0);
        input.wait().await;
        let batches_with_drop = crate::physical_plan::common::collect(output_stream1)
            .await
            .unwrap();

        assert_eq!(batches_without_drop, batches_with_drop);
    }

    fn str_batches_to_vec(batches: &[RecordBatch]) -> Vec<&str> {
        batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.columns().len(), 1);
                let string_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Unexpected type for repartitoned batch");

                string_array
                    .iter()
                    .map(|v| v.expect("Unexpected null"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    /// Create a BarrierExec that returns two partitions of two batches each
    fn make_barrier_exec() -> BarrierExec {
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        let batch3 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["goo", "gar"])) as ArrayRef,
        )])
        .unwrap();

        let batch4 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from_slice(&["grob", "gaz"])) as ArrayRef,
        )])
        .unwrap();

        // The barrier exec waits to be pinged
        // requires the input to wait at least once)
        let schema = batch1.schema();
        BarrierExec::new(vec![vec![batch1, batch2], vec![batch3, batch4]], schema)
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let repartition_exec = Arc::new(RepartitionExec::try_new(
            blocking_exec,
            Partitioning::UnknownPartitioning(1),
        )?);

        let fut = collect(repartition_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn hash_repartition_avoid_empty_batch() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let batch = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(StringArray::from_slice(&["foo"])) as ArrayRef,
        )])
        .unwrap();
        let partitioning = Partitioning::Hash(
            vec![Arc::new(crate::physical_plan::expressions::Column::new(
                "a", 0,
            ))],
            2,
        );
        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch)], schema);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();
        let output_stream0 = exec.execute(0, task_ctx.clone()).unwrap();
        let batch0 = crate::physical_plan::common::collect(output_stream0)
            .await
            .unwrap();
        let output_stream1 = exec.execute(1, task_ctx.clone()).unwrap();
        let batch1 = crate::physical_plan::common::collect(output_stream1)
            .await
            .unwrap();
        assert!(batch0.is_empty() || batch1.is_empty());
        Ok(())
    }
}
