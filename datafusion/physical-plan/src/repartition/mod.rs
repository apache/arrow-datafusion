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

//! This file implements the [`RepartitionExec`]  operator, which maps N input
//! partitions to M output partitions based on a partitioning scheme, optionally
//! maintaining the order of the input rows in the output.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use super::common::SharedMemoryReservation;
use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{
    DisplayAs, ExecutionPlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use crate::hash_utils::create_hashes;
use crate::metrics::BaselineMetrics;
use crate::repartition::distributor_channels::{
    channels, partition_aware_channels, DistributionReceiver, DistributionSender,
};
use crate::sorts::streaming_merge;
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, Statistics};

use arrow::array::{ArrayRef, UInt64Builder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::transpose;
use datafusion_common::{arrow_datafusion_err, not_impl_err, DataFusionError, Result};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};

use futures::stream::Stream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use hashbrown::HashMap;
use log::trace;
use parking_lot::Mutex;

mod distributor_channels;

type MaybeBatch = Option<Result<RecordBatch>>;
type InputPartitionsToCurrentPartitionSender = Vec<DistributionSender<MaybeBatch>>;
type InputPartitionsToCurrentPartitionReceiver = Vec<DistributionReceiver<MaybeBatch>>;

/// Inner state of [`RepartitionExec`].
#[derive(Debug)]
struct RepartitionExecState {
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number.
    channels: HashMap<
        usize,
        (
            InputPartitionsToCurrentPartitionSender,
            InputPartitionsToCurrentPartitionReceiver,
            SharedMemoryReservation,
        ),
    >,

    /// Helper that ensures that that background job is killed once it is no longer needed.
    abort_helper: Arc<Vec<SpawnedTask<()>>>,
}

impl RepartitionExecState {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        metrics: ExecutionPlanMetricsSet,
        preserve_order: bool,
        name: String,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_input_partitions = input.output_partitioning().partition_count();
        let num_output_partitions = partitioning.partition_count();

        let (txs, rxs) = if preserve_order {
            let (txs, rxs) =
                partition_aware_channels(num_input_partitions, num_output_partitions);
            // Take transpose of senders and receivers. `state.channels` keeps track of entries per output partition
            let txs = transpose(txs);
            let rxs = transpose(rxs);
            (txs, rxs)
        } else {
            // create one channel per *output* partition
            // note we use a custom channel that ensures there is always data for each receiver
            // but limits the amount of buffering if required.
            let (txs, rxs) = channels(num_output_partitions);
            // Clone sender for each input partitions
            let txs = txs
                .into_iter()
                .map(|item| vec![item; num_input_partitions])
                .collect::<Vec<_>>();
            let rxs = rxs.into_iter().map(|item| vec![item]).collect::<Vec<_>>();
            (txs, rxs)
        };

        let mut channels = HashMap::with_capacity(txs.len());
        for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
            let reservation = Arc::new(Mutex::new(
                MemoryConsumer::new(format!("{}[{partition}]", name))
                    .register(context.memory_pool()),
            ));
            channels.insert(partition, (tx, rx, reservation));
        }

        // launch one async task per *input* partition
        let mut spawned_tasks = Vec::with_capacity(num_input_partitions);
        for i in 0..num_input_partitions {
            let txs: HashMap<_, _> = channels
                .iter()
                .map(|(partition, (tx, _rx, reservation))| {
                    (*partition, (tx[i].clone(), Arc::clone(reservation)))
                })
                .collect();

            let r_metrics = RepartitionMetrics::new(i, num_output_partitions, &metrics);

            let input_task = SpawnedTask::spawn(RepartitionExec::pull_from_input(
                input.clone(),
                i,
                txs.clone(),
                partitioning.clone(),
                r_metrics,
                context.clone(),
            ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            let wait_for_task = SpawnedTask::spawn(RepartitionExec::wait_for_task(
                input_task,
                txs.into_iter()
                    .map(|(partition, (tx, _reservation))| (partition, tx))
                    .collect(),
            ));
            spawned_tasks.push(wait_for_task);
        }

        Self {
            channels,
            abort_helper: Arc::new(spawned_tasks),
        }
    }
}

/// Lazily initialized state
///
/// Note that the state is initialized ONCE for all partitions by a single task(thread).
/// This may take a short while.  It is also like that multiple threads
/// call execute at the same time, because we have just started "target partitions" tasks
/// which is commonly set to the number of CPU cores and all call execute at the same time.
///
/// Thus, use a **tokio** `OnceCell` for this initialization so as not to waste CPU cycles
/// in a futex lock but instead allow other threads to do something useful.
///
/// Uses a parking_lot `Mutex` to control other accesses as they are very short duration
///  (e.g. removing channels on completion) where the overhead of `await` is not warranted.
type LazyState = Arc<tokio::sync::OnceCell<Mutex<RepartitionExecState>>>;

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
            other => return not_impl_err!("Unsupported repartitioning scheme {other:?}"),
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
        self.partition_iter(batch)?.try_for_each(|res| match res {
            Ok((partition, batch)) => f(partition, batch),
            Err(e) => Err(e),
        })
    }

    /// Actual implementation of [`partition`](Self::partition).
    ///
    /// The reason this was pulled out is that we need to have a variant of `partition` that works w/ sync functions,
    /// and one that works w/ async. Using an iterator as an intermediate representation was the best way to achieve
    /// this (so we don't need to clone the entire implementation).
    fn partition_iter(
        &mut self,
        batch: RecordBatch,
    ) -> Result<impl Iterator<Item = Result<(usize, RecordBatch)>> + Send + '_> {
        let it: Box<dyn Iterator<Item = Result<(usize, RecordBatch)>> + Send> =
            match &mut self.state {
                BatchPartitionerState::RoundRobin {
                    num_partitions,
                    next_idx,
                } => {
                    let idx = *next_idx;
                    *next_idx = (*next_idx + 1) % *num_partitions;
                    Box::new(std::iter::once(Ok((idx, batch))))
                }
                BatchPartitionerState::Hash {
                    random_state,
                    exprs,
                    num_partitions: partitions,
                    hash_buffer,
                } => {
                    let timer = self.timer.timer();

                    let arrays = exprs
                        .iter()
                        .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                        .collect::<Result<Vec<_>>>()?;

                    hash_buffer.clear();
                    hash_buffer.resize(batch.num_rows(), 0);

                    create_hashes(&arrays, random_state, hash_buffer)?;

                    let mut indices: Vec<_> = (0..*partitions)
                        .map(|_| UInt64Builder::with_capacity(batch.num_rows()))
                        .collect();

                    for (index, hash) in hash_buffer.iter().enumerate() {
                        indices[(*hash % *partitions as u64) as usize]
                            .append_value(index as u64);
                    }

                    let it = indices
                        .into_iter()
                        .enumerate()
                        .filter_map(|(partition, mut indices)| {
                            let indices = indices.finish();
                            (!indices.is_empty()).then_some((partition, indices))
                        })
                        .map(move |(partition, indices)| {
                            // Produce batches based on indices
                            let columns = batch
                                .columns()
                                .iter()
                                .map(|c| {
                                    arrow::compute::take(c.as_ref(), &indices, None)
                                        .map_err(|e| arrow_datafusion_err!(e))
                                })
                                .collect::<Result<Vec<ArrayRef>>>()?;

                            let batch =
                                RecordBatch::try_new(batch.schema(), columns).unwrap();

                            // bind timer so it drops w/ this iterator
                            let _ = &timer;

                            Ok((partition, batch))
                        });

                    Box::new(it)
                }
            };

        Ok(it)
    }

    // return the number of output partitions
    fn num_partitions(&self) -> usize {
        match self.state {
            BatchPartitionerState::RoundRobin { num_partitions, .. } => num_partitions,
            BatchPartitionerState::Hash { num_partitions, .. } => num_partitions,
        }
    }
}

/// Maps `N` input partitions to `M` output partitions based on a
/// [`Partitioning`] scheme.
///
/// # Background
///
/// DataFusion, like most other commercial systems, with the
/// notable exception of DuckDB, uses the "Exchange Operator" based
/// approach to parallelism which works well in practice given
/// sufficient care in implementation.
///
/// DataFusion's planner picks the target number of partitions and
/// then `RepartionExec` redistributes [`RecordBatch`]es to that number
/// of output partitions.
///
/// For example, given `target_partitions=3` (trying to use 3 cores)
/// but scanning an input with 2 partitions, `RepartitionExec` can be
/// used to get 3 even streams of `RecordBatch`es
///
///
///```text
///        ▲                  ▲                  ▲
///        │                  │                  │
///        │                  │                  │
///        │                  │                  │
///┌───────────────┐  ┌───────────────┐  ┌───────────────┐
///│    GroupBy    │  │    GroupBy    │  │    GroupBy    │
///│   (Partial)   │  │   (Partial)   │  │   (Partial)   │
///└───────────────┘  └───────────────┘  └───────────────┘
///        ▲                  ▲                  ▲
///        └──────────────────┼──────────────────┘
///                           │
///              ┌─────────────────────────┐
///              │     RepartitionExec     │
///              │   (hash/round robin)    │
///              └─────────────────────────┘
///                         ▲   ▲
///             ┌───────────┘   └───────────┐
///             │                           │
///             │                           │
///        .─────────.                 .─────────.
///     ,─'           '─.           ,─'           '─.
///    ;      Input      :         ;      Input      :
///    :   Partition 0   ;         :   Partition 1   ;
///     ╲               ╱           ╲               ╱
///      '─.         ,─'             '─.         ,─'
///         `───────'                   `───────'
///```
///
/// # Output Ordering
///
/// If more than one stream is being repartitioned, the output will be some
/// arbitrary interleaving (and thus unordered) unless
/// [`Self::with_preserve_order`] specifies otherwise.
///
/// # Footnote
///
/// The "Exchange Operator" was first described in the 1989 paper
/// [Encapsulation of parallelism in the Volcano query processing
/// system
/// Paper](https://w6113.github.io/files/papers/volcanoparallelism-89.pdf)
/// which uses the term "Exchange" for the concept of repartitioning
/// data across threads.
#[derive(Debug)]
pub struct RepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    /// Inner state that is initialized when the first output stream is created.
    state: LazyState,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Boolean flag to decide whether to preserve ordering. If true means
    /// `SortPreservingRepartitionExec`, false means `RepartitionExec`.
    preserve_order: bool,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

#[derive(Debug, Clone)]
struct RepartitionMetrics {
    /// Time in nanos to execute child operator and fetch batches
    fetch_time: metrics::Time,
    /// Time in nanos to perform repartitioning
    repartition_time: metrics::Time,
    /// Time in nanos for sending resulting batches to channels.
    ///
    /// One metric per output partition.
    send_time: Vec<metrics::Time>,
}

impl RepartitionMetrics {
    pub fn new(
        input_partition: usize,
        num_output_partitions: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        // Time in nanos to execute child operator and fetch batches
        let fetch_time =
            MetricBuilder::new(metrics).subset_time("fetch_time", input_partition);

        // Time in nanos to perform repartitioning
        let repart_time =
            MetricBuilder::new(metrics).subset_time("repart_time", input_partition);

        // Time in nanos for sending resulting batches to channels
        let send_time = (0..num_output_partitions)
            .map(|output_partition| {
                let label =
                    metrics::Label::new("outputPartition", output_partition.to_string());
                MetricBuilder::new(metrics)
                    .with_label(label)
                    .subset_time("send_time", input_partition)
            })
            .collect();

        Self {
            fetch_time,
            repartition_time: repart_time,
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

    /// Get preserve_order flag of the RepartitionExecutor
    /// `true` means `SortPreservingRepartitionExec`, `false` means `RepartitionExec`
    pub fn preserve_order(&self) -> bool {
        self.preserve_order
    }

    /// Get name used to display this Exec
    pub fn name(&self) -> &str {
        "RepartitionExec"
    }
}

impl DisplayAs for RepartitionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "{}: partitioning={}, input_partitions={}",
                    self.name(),
                    self.partitioning,
                    self.input.output_partitioning().partition_count()
                )?;

                if self.preserve_order {
                    write!(f, ", preserve_order=true")?;
                }

                if let Some(sort_exprs) = self.sort_exprs() {
                    write!(
                        f,
                        ", sort_exprs={}",
                        PhysicalSortExpr::format_list(sort_exprs)
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for RepartitionExec {
    fn name(&self) -> &'static str {
        "RepartitionExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut repartition =
            RepartitionExec::try_new(children.swap_remove(0), self.partitioning.clone())?;
        if self.preserve_order {
            repartition = repartition.with_preserve_order();
        }
        Ok(Arc::new(repartition))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![matches!(self.partitioning, Partitioning::Hash(_, _))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order_helper(self.input(), self.preserve_order)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start {}::execute for partition: {}",
            self.name(),
            partition
        );

        let lazy_state = Arc::clone(&self.state);
        let input = Arc::clone(&self.input);
        let partitioning = self.partitioning.clone();
        let metrics = self.metrics.clone();
        let preserve_order = self.preserve_order;
        let name = self.name().to_owned();
        let schema = self.schema();
        let schema_captured = Arc::clone(&schema);

        // Get existing ordering to use for merging
        let sort_exprs = self.sort_exprs().unwrap_or(&[]).to_owned();

        let stream = futures::stream::once(async move {
            let num_input_partitions = input.output_partitioning().partition_count();

            let input_captured = Arc::clone(&input);
            let metrics_captured = metrics.clone();
            let name_captured = name.clone();
            let context_captured = Arc::clone(&context);
            let state = lazy_state
                .get_or_init(|| async move {
                    Mutex::new(RepartitionExecState::new(
                        input_captured,
                        partitioning,
                        metrics_captured,
                        preserve_order,
                        name_captured,
                        context_captured,
                    ))
                })
                .await;

            // lock scope
            let (mut rx, reservation, abort_helper) = {
                // lock mutexes
                let mut state = state.lock();

                // now return stream for the specified *output* partition which will
                // read from the channel
                let (_tx, rx, reservation) = state
                    .channels
                    .remove(&partition)
                    .expect("partition not used yet");

                (rx, reservation, Arc::clone(&state.abort_helper))
            };

            trace!(
                "Before returning stream in {}::execute for partition: {}",
                name,
                partition
            );

            if preserve_order {
                // Store streams from all the input partitions:
                let input_streams = rx
                    .into_iter()
                    .map(|receiver| {
                        Box::pin(PerPartitionStream {
                            schema: Arc::clone(&schema_captured),
                            receiver,
                            drop_helper: Arc::clone(&abort_helper),
                            reservation: reservation.clone(),
                        }) as SendableRecordBatchStream
                    })
                    .collect::<Vec<_>>();
                // Note that receiver size (`rx.len()`) and `num_input_partitions` are same.

                // Merge streams (while preserving ordering) coming from
                // input partitions to this partition:
                let fetch = None;
                let merge_reservation =
                    MemoryConsumer::new(format!("{}[Merge {partition}]", name))
                        .register(context.memory_pool());
                streaming_merge(
                    input_streams,
                    schema_captured,
                    &sort_exprs,
                    BaselineMetrics::new(&metrics, partition),
                    context.session_config().batch_size(),
                    fetch,
                    merge_reservation,
                )
            } else {
                Ok(Box::pin(RepartitionStream {
                    num_input_partitions,
                    num_input_partitions_processed: 0,
                    schema: input.schema(),
                    input: rx.swap_remove(0),
                    drop_helper: abort_helper,
                    reservation,
                }) as SendableRecordBatchStream)
            }
        })
        .try_flatten();
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

impl RepartitionExec {
    /// Create a new RepartitionExec, that produces output `partitioning`, and
    /// does not preserve the order of the input (see [`Self::with_preserve_order`]
    /// for more details)
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        let preserve_order = false;
        let cache =
            Self::compute_properties(&input, partitioning.clone(), preserve_order);
        Ok(RepartitionExec {
            input,
            partitioning,
            state: Default::default(),
            metrics: ExecutionPlanMetricsSet::new(),
            preserve_order,
            cache,
        })
    }

    fn maintains_input_order_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_order: bool,
    ) -> Vec<bool> {
        // We preserve ordering when repartition is order preserving variant or input partitioning is 1
        vec![preserve_order || input.output_partitioning().partition_count() <= 1]
    }

    fn eq_properties_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_order: bool,
    ) -> EquivalenceProperties {
        // Equivalence Properties
        let mut eq_properties = input.equivalence_properties().clone();
        // If the ordering is lost, reset the ordering equivalence class:
        if !Self::maintains_input_order_helper(input, preserve_order)[0] {
            eq_properties.clear_orderings();
        }
        // When there are more than one input partitions, they will be fused at the output.
        // Therefore, remove per partition constants.
        if input.output_partitioning().partition_count() > 1 {
            eq_properties.clear_per_partition_constants();
        }
        eq_properties
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        preserve_order: bool,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = Self::eq_properties_helper(input, preserve_order);

        PlanProperties::new(
            eq_properties,          // Equivalence Properties
            partitioning,           // Output Partitioning
            input.execution_mode(), // Execution Mode
        )
    }

    /// Specify if this reparititoning operation should preserve the order of
    /// rows from its input when producing output. Preserving order is more
    /// expensive at runtime, so should only be set if the output of this
    /// operator can take advantage of it.
    ///
    /// If the input is not ordered, or has only one partition, this is a no op,
    /// and the node remains a `RepartitionExec`.
    pub fn with_preserve_order(mut self) -> Self {
        self.preserve_order =
                // If the input isn't ordered, there is no ordering to preserve
                self.input.output_ordering().is_some() &&
                // if there is only one input partition, merging is not required
                // to maintain order
                self.input.output_partitioning().partition_count() > 1;
        let eq_properties = Self::eq_properties_helper(&self.input, self.preserve_order);
        self.cache = self.cache.with_eq_properties(eq_properties);
        self
    }

    /// Return the sort expressions that are used to merge
    fn sort_exprs(&self) -> Option<&[PhysicalSortExpr]> {
        if self.preserve_order {
            self.input.output_ordering()
        } else {
            None
        }
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning
    ///
    /// txs hold the output sending channels for each output partition
    async fn pull_from_input(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        mut output_channels: HashMap<
            usize,
            (DistributionSender<MaybeBatch>, SharedMemoryReservation),
        >,
        partitioning: Partitioning,
        metrics: RepartitionMetrics,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let mut partitioner =
            BatchPartitioner::try_new(partitioning, metrics.repartition_time.clone())?;

        // execute the child operator
        let timer = metrics.fetch_time.timer();
        let mut stream = input.execute(partition, context)?;
        timer.done();

        // While there are still outputs to send to, keep pulling inputs
        let mut batches_until_yield = partitioner.num_partitions();
        while !output_channels.is_empty() {
            // fetch the next batch
            let timer = metrics.fetch_time.timer();
            let result = stream.next().await;
            timer.done();

            // Input is done
            let batch = match result {
                Some(result) => result?,
                None => break,
            };

            for res in partitioner.partition_iter(batch)? {
                let (partition, batch) = res?;
                let size = batch.get_array_memory_size();

                let timer = metrics.send_time[partition].timer();
                // if there is still a receiver, send to it
                if let Some((tx, reservation)) = output_channels.get_mut(&partition) {
                    reservation.lock().try_grow(size)?;

                    if tx.send(Some(Ok(batch))).await.is_err() {
                        // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                        reservation.lock().shrink(size);
                        output_channels.remove(&partition);
                    }
                }
                timer.done();
            }

            // If the input stream is endless, we may spin forever and
            // never yield back to tokio.  See
            // https://github.com/apache/datafusion/issues/5278.
            //
            // However, yielding on every batch causes a bottleneck
            // when running with multiple cores. See
            // https://github.com/apache/datafusion/issues/6290
            //
            // Thus, heuristically yield after producing num_partition
            // batches
            //
            // In round robin this is ideal as each input will get a
            // new batch. In hash partitioning it may yield too often
            // on uneven distributions even if some partition can not
            // make progress, but parallelism is going to be limited
            // in that case anyways
            if batches_until_yield == 0 {
                tokio::task::yield_now().await;
                batches_until_yield = partitioner.num_partitions();
            } else {
                batches_until_yield -= 1;
            }
        }

        Ok(())
    }

    /// Waits for `input_task` which is consuming one of the inputs to
    /// complete. Upon each successful completion, sends a `None` to
    /// each of the output tx channels to signal one of the inputs is
    /// complete. Upon error, propagates the errors to all output tx
    /// channels.
    async fn wait_for_task(
        input_task: SpawnedTask<Result<()>>,
        txs: HashMap<usize, DistributionSender<MaybeBatch>>,
    ) {
        // wait for completion, and propagate error
        // note we ignore errors on send (.ok) as that means the receiver has already shutdown.

        match input_task.join().await {
            // Error in joining task
            Err(e) => {
                let e = Arc::new(e);

                for (_, tx) in txs {
                    let err = Err(DataFusionError::Context(
                        "Join Error".to_string(),
                        Box::new(DataFusionError::External(Box::new(Arc::clone(&e)))),
                    ));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                let e = Arc::new(e);

                for (_, tx) in txs {
                    // wrap it because need to send error to all output partitions
                    let err = Err(DataFusionError::External(Box::new(e.clone())));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {
                // notify each output partition that this input partition has no more data
                for (_, tx) in txs {
                    tx.send(None).await.ok();
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
    input: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    #[allow(dead_code)]
    drop_helper: Arc<Vec<SpawnedTask<()>>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,
}

impl Stream for RepartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.recv().poll_unpin(cx) {
                Poll::Ready(Some(Some(v))) => {
                    if let Ok(batch) = &v {
                        self.reservation
                            .lock()
                            .shrink(batch.get_array_memory_size());
                    }

                    return Poll::Ready(Some(v));
                }
                Poll::Ready(Some(None)) => {
                    self.num_input_partitions_processed += 1;

                    if self.num_input_partitions == self.num_input_partitions_processed {
                        // all input partitions have finished sending batches
                        return Poll::Ready(None);
                    } else {
                        // other partitions still have data to send
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl RecordBatchStream for RepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// This struct converts a receiver to a stream.
/// Receiver receives data on an SPSC channel.
struct PerPartitionStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    receiver: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    #[allow(dead_code)]
    drop_helper: Arc<Vec<SpawnedTask<()>>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,
}

impl Stream for PerPartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.recv().poll_unpin(cx) {
            Poll::Ready(Some(Some(v))) => {
                if let Ok(batch) = &v {
                    self.reservation
                        .lock()
                        .shrink(batch.get_array_memory_size());
                }
                Poll::Ready(Some(v))
            }
            Poll::Ready(Some(None)) => {
                // Input partition has finished sending batches
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for PerPartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{
        test::{
            assert_is_pending,
            exec::{
                assert_strong_count_converges_to_zero, BarrierExec, BlockingExec,
                ErrorExec, MockExec,
            },
        },
        {collect, expressions::col, memory::MemoryExec},
    };

    use arrow::array::{StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::cast::as_string_array;
    use datafusion_common::{assert_batches_sorted_eq, exec_err};
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};

    use tokio::task::JoinSet;

    #[tokio::test]
    async fn one_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
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
        let partition = create_vec_batches(50);
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
        let partition = create_vec_batches(50);
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
        let partition = create_vec_batches(50);
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
        let task_ctx = Arc::new(TaskContext::default());
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
        let handle: SpawnedTask<Result<Vec<Vec<RecordBatch>>>> =
            SpawnedTask::spawn(async move {
                // define input partitions
                let schema = test_schema();
                let partition = create_vec_batches(50);
                let partitions =
                    vec![partition.clone(), partition.clone(), partition.clone()];

                // repartition from 3 input to 5 output
                repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await
            });

        let output_partitions = handle.join().await.unwrap().unwrap();

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
        let task_ctx = Arc::new(TaskContext::default());
        // have to send at least one batch through to provoke error
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
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
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string
                .contains("Unsupported repartitioning scheme UnknownPartitioning(1)"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn error_for_input_exec() {
        // This generates an error on a call to execute. The error
        // should be returned and no results produced.

        let task_ctx = Arc::new(TaskContext::default());
        let input = ErrorExec::new();
        let partitioning = Partitioning::RoundRobinBatch(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("ErrorExec, unsurprisingly, errored in partition 0"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn repartition_with_error_in_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        // input stream returns one good batch and then one error. The
        // error should be returned.
        let err = exec_err!("bad data error");

        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch), err], schema);
        let partitioning = Partitioning::RoundRobinBatch(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("bad data error"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn repartition_with_delayed_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["frob", "baz"])) as ArrayRef,
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
        let batches = crate::common::collect(output_stream).await.unwrap();

        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn robin_repartition_with_dropping_output_stream() {
        let task_ctx = Arc::new(TaskContext::default());
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
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });

        // output stream 1 should *not* error and have one of the input batches
        let batches = crate::common::collect(output_stream1).await.unwrap();

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
        let task_ctx = Arc::new(TaskContext::default());
        let partitioning = Partitioning::Hash(
            vec![Arc::new(crate::expressions::Column::new(
                "my_awesome_field",
                0,
            ))],
            2,
        );

        // We first collect the results without droping the output stream.
        let input = Arc::new(make_barrier_exec());
        let exec = RepartitionExec::try_new(input.clone(), partitioning.clone()).unwrap();
        let output_stream1 = exec.execute(1, task_ctx.clone()).unwrap();
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });
        let batches_without_drop = crate::common::collect(output_stream1).await.unwrap();

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
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });
        let batches_with_drop = crate::common::collect(output_stream1).await.unwrap();

        assert_eq!(batches_without_drop, batches_with_drop);
    }

    fn str_batches_to_vec(batches: &[RecordBatch]) -> Vec<&str> {
        batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.columns().len(), 1);
                let string_array = as_string_array(batch.column(0))
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
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        let batch3 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["goo", "gar"])) as ArrayRef,
        )])
        .unwrap();

        let batch4 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["grob", "gaz"])) as ArrayRef,
        )])
        .unwrap();

        // The barrier exec waits to be pinged
        // requires the input to wait at least once)
        let schema = batch1.schema();
        BarrierExec::new(vec![vec![batch1, batch2], vec![batch3, batch4]], schema)
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
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
        let task_ctx = Arc::new(TaskContext::default());
        let batch = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(StringArray::from(vec!["foo"])) as ArrayRef,
        )])
        .unwrap();
        let partitioning = Partitioning::Hash(
            vec![Arc::new(crate::expressions::Column::new("a", 0))],
            2,
        );
        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch)], schema);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();
        let output_stream0 = exec.execute(0, task_ctx.clone()).unwrap();
        let batch0 = crate::common::collect(output_stream0).await.unwrap();
        let output_stream1 = exec.execute(1, task_ctx.clone()).unwrap();
        let batch1 = crate::common::collect(output_stream1).await.unwrap();
        assert!(batch0.is_empty() || batch1.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn oom() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let input_partitions = vec![partition];
        let partitioning = Partitioning::RoundRobinBatch(4);

        // setup up context
        let runtime = Arc::new(
            RuntimeEnv::new(RuntimeConfig::default().with_memory_limit(1, 1.0)).unwrap(),
        );

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, schema.clone(), None)?;
        let exec = RepartitionExec::try_new(Arc::new(exec), partitioning)?;

        // pull partitions
        for i in 0..exec.partitioning.partition_count() {
            let mut stream = exec.execute(i, task_ctx.clone())?;
            let err =
                arrow_datafusion_err!(stream.next().await.unwrap().unwrap_err().into());
            let err = err.find_root();
            assert!(
                matches!(err, DataFusionError::ResourcesExhausted(_)),
                "Wrong error type: {err}",
            );
        }

        Ok(())
    }

    /// Create vector batches
    fn create_vec_batches(n: usize) -> Vec<RecordBatch> {
        let batch = create_batch();
        (0..n).map(|_| batch.clone()).collect()
    }

    /// Create batch
    fn create_batch() -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use arrow_schema::{DataType, Field, Schema, SortOptions};

    use datafusion_physical_expr::expressions::col;

    use crate::memory::MemoryExec;
    use crate::union::UnionExec;

    use super::*;

    /// Asserts that the plan is as expected
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$PLAN`: the plan to optimized
    ///
    macro_rules! assert_plan {
        ($EXPECTED_PLAN_LINES: expr,  $PLAN: expr) => {
            let physical_plan = $PLAN;
            let formatted = crate::displayable(&physical_plan).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );
        };
    }

    #[tokio::test]
    async fn test_preserve_order() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source1 = sorted_memory_exec(&schema, sort_exprs.clone());
        let source2 = sorted_memory_exec(&schema, sort_exprs);
        // output has multiple partitions, and is sorted
        let union = UnionExec::new(vec![source1, source2]);
        let exec =
            RepartitionExec::try_new(Arc::new(union), Partitioning::RoundRobinBatch(10))
                .unwrap()
                .with_preserve_order();

        // Repartition should preserve order
        let expected_plan = [
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true, sort_exprs=c0@0 ASC",
            "  UnionExec",
            "    MemoryExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC",
            "    MemoryExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC",
        ];
        assert_plan!(expected_plan, exec);
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_one_partition() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source = sorted_memory_exec(&schema, sort_exprs);
        // output is sorted, but has only a single partition, so no need to sort
        let exec = RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(10))
            .unwrap()
            .with_preserve_order();

        // Repartition should not preserve order
        let expected_plan = [
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "  MemoryExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC",
        ];
        assert_plan!(expected_plan, exec);
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_input_not_sorted() -> Result<()> {
        let schema = test_schema();
        let source1 = memory_exec(&schema);
        let source2 = memory_exec(&schema);
        // output has multiple partitions, but is not sorted
        let union = UnionExec::new(vec![source1, source2]);
        let exec =
            RepartitionExec::try_new(Arc::new(union), Partitioning::RoundRobinBatch(10))
                .unwrap()
                .with_preserve_order();

        // Repartition should not preserve order, as there is no order to preserve
        let expected_plan = [
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "  UnionExec",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_plan!(expected_plan, exec);
        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn sort_exprs(schema: &Schema) -> Vec<PhysicalSortExpr> {
        let options = SortOptions::default();
        vec![PhysicalSortExpr {
            expr: col("c0", schema).unwrap(),
            options,
        }]
    }

    fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(MemoryExec::try_new(&[vec![]], schema.clone(), None).unwrap())
    }

    fn sorted_memory_exec(
        schema: &SchemaRef,
        sort_exprs: Vec<PhysicalSortExpr>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            MemoryExec::try_new(&[vec![]], schema.clone(), None)
                .unwrap()
                .with_sort_information(vec![sort_exprs]),
        )
    }
}
