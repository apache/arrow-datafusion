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

//! Defines the Sort-Merge join execution plan.
//! A Sort-Merge join plan consumes two sorted children plan and produces
//! joined output by given join type and other options.
//! Sort-Merge join feature is currently experimental.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::expressions::PhysicalSortExpr;
use crate::joins::utils::{
    build_join_schema, check_join_is_valid, estimate_join_statistics,
    symmetric_join_output_partitioning, JoinFilter, JoinOn, JoinOnRef,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use crate::{
    execution_mode_from_children, metrics, DisplayAs, DisplayFormatType, Distribution,
    ExecutionPlan, ExecutionPlanProperties, PhysicalExpr, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::array::*;
use arrow::compute::{self, concat_batches, take, SortOptions};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow_array::types::UInt64Type;

use datafusion_common::{
    internal_err, not_impl_err, plan_err, DataFusionError, JoinSide, JoinType, Result,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{PhysicalExprRef, PhysicalSortRequirement};

use futures::{Stream, StreamExt};
use hashbrown::HashSet;

/// join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
#[derive(Debug)]
pub struct SortMergeJoinExec {
    /// Left sorted joining execution plan
    pub left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub on: JoinOn,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed
    pub join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The left SortExpr
    left_sort_exprs: Vec<PhysicalSortExpr>,
    /// The right SortExpr
    right_sort_exprs: Vec<PhysicalSortExpr>,
    /// Sort options of join columns used in sorting left and right execution plans
    pub sort_options: Vec<SortOptions>,
    /// If null_equals_null is true, null == null else null != null
    pub null_equals_null: bool,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl SortMergeJoinExec {
    /// Tries to create a new [SortMergeJoinExec].
    /// The inputs are sorted using `sort_options` are applied to the columns in the `on`
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        if join_type == JoinType::RightSemi {
            return not_impl_err!(
                "SortMergeJoinExec does not support JoinType::RightSemi"
            );
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        if sort_options.len() != on.len() {
            return plan_err!(
                "Expected number of sort options: {}, actual: {}",
                on.len(),
                sort_options.len()
            );
        }

        let (left_sort_exprs, right_sort_exprs): (Vec<_>, Vec<_>) = on
            .iter()
            .zip(sort_options.iter())
            .map(|((l, r), sort_op)| {
                let left = PhysicalSortExpr {
                    expr: Arc::clone(l),
                    options: *sort_op,
                };
                let right = PhysicalSortExpr {
                    expr: Arc::clone(r),
                    options: *sort_op,
                };
                (left, right)
            })
            .unzip();

        let schema =
            Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);
        let cache =
            Self::compute_properties(&left, &right, Arc::clone(&schema), join_type, &on);
        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            left_sort_exprs,
            right_sort_exprs,
            sort_options,
            null_equals_null,
            cache,
        })
    }

    /// Get probe side (e.g streaming side) information for this sort merge join.
    /// In current implementation, probe side is determined according to join type.
    pub fn probe_side(join_type: &JoinType) -> JoinSide {
        // When output schema contains only the right side, probe side is right.
        // Otherwise probe side is the left side.
        match join_type {
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                JoinSide::Right
            }
            JoinType::Inner
            | JoinType::Left
            | JoinType::Full
            | JoinType::LeftAnti
            | JoinType::LeftSemi => JoinSide::Left,
        }
    }

    /// Calculate order preservation flags for this sort merge join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        match join_type {
            JoinType::Inner => vec![true, false],
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => vec![true, false],
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                vec![false, true]
            }
            _ => vec![false, false],
        }
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        join_on: JoinOnRef,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side(&join_type)),
            join_on,
        );

        let output_partitioning =
            symmetric_join_output_partitioning(left, right, &join_type);

        // Determine execution mode:
        let mode = execution_mode_from_children([left, right]);

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl DisplayAs for SortMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let display_filter = self.filter.as_ref().map_or_else(
            || "".to_string(),
            |f| format!(", filter={}", f.expression()),
        );

        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "SortMergeJoin: join_type={:?}, on=[{}]{}",
                    self.join_type, on, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn name(&self) -> &'static str {
        "SortMergeJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();
        vec![
            Distribution::HashPartitioned(left_expr),
            Distribution::HashPartitioned(right_expr),
        ]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![
            Some(PhysicalSortRequirement::from_sort_exprs(
                &self.left_sort_exprs,
            )),
            Some(PhysicalSortRequirement::from_sort_exprs(
                &self.right_sort_exprs,
            )),
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SortMergeJoinExec::try_new(
                Arc::clone(left),
                Arc::clone(right),
                self.on.clone(),
                self.filter.clone(),
                self.join_type,
                self.sort_options.clone(),
                self.null_equals_null,
            )?)),
            _ => internal_err!("SortMergeJoin wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        if left_partitions != right_partitions {
            return internal_err!(
                "Invalid SortMergeJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }
        let (on_left, on_right) = self.on.iter().cloned().unzip();
        let (streamed, buffered, on_streamed, on_buffered) =
            if SortMergeJoinExec::probe_side(&self.join_type) == JoinSide::Left {
                (
                    Arc::clone(&self.left),
                    Arc::clone(&self.right),
                    on_left,
                    on_right,
                )
            } else {
                (
                    Arc::clone(&self.right),
                    Arc::clone(&self.left),
                    on_right,
                    on_left,
                )
            };

        // execute children plans
        let streamed = streamed.execute(partition, Arc::clone(&context))?;
        let buffered = buffered.execute(partition, Arc::clone(&context))?;

        // create output buffer
        let batch_size = context.session_config().batch_size();

        // create memory reservation
        let reservation = MemoryConsumer::new(format!("SMJStream[{partition}]"))
            .register(context.memory_pool());

        // create join stream
        Ok(Box::pin(SMJStream::try_new(
            Arc::clone(&self.schema),
            self.sort_options.clone(),
            self.null_equals_null,
            streamed,
            buffered,
            on_streamed,
            on_buffered,
            self.filter.clone(),
            self.join_type,
            batch_size,
            SortMergeJoinMetrics::new(partition, &self.metrics),
            reservation,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        estimate_join_statistics(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.on.clone(),
            &self.join_type,
            &self.schema,
        )
    }
}

/// Metrics for SortMergeJoinExec
#[allow(dead_code)]
struct SortMergeJoinMetrics {
    /// Total time for joining probe-side batches to the build-side batches
    join_time: metrics::Time,
    /// Number of batches consumed by this operator
    input_batches: metrics::Count,
    /// Number of rows consumed by this operator
    input_rows: metrics::Count,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
    /// Peak memory used for buffered data.
    /// Calculated as sum of peak memory values across partitions
    peak_mem_used: metrics::Gauge,
}

impl SortMergeJoinMetrics {
    #[allow(dead_code)]
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);
        let output_rows = MetricBuilder::new(metrics).output_rows(partition);
        let peak_mem_used = MetricBuilder::new(metrics).gauge("peak_mem_used", partition);

        Self {
            join_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
            peak_mem_used,
        }
    }
}

/// State of SMJ stream
#[derive(Debug, PartialEq, Eq)]
enum SMJState {
    /// Init joining with a new streamed row or a new buffered batches
    Init,
    /// Polling one streamed row or one buffered batch, or both
    Polling,
    /// Joining polled data and making output
    JoinOutput,
    /// No more output
    Exhausted,
}

/// State of streamed data stream
#[derive(Debug, PartialEq, Eq)]
enum StreamedState {
    /// Init polling
    Init,
    /// Polling one streamed row
    Polling,
    /// Ready to produce one streamed row
    Ready,
    /// No more streamed row
    Exhausted,
}

/// State of buffered data stream
#[derive(Debug, PartialEq, Eq)]
enum BufferedState {
    /// Init polling
    Init,
    /// Polling first row in the next batch
    PollingFirst,
    /// Polling rest rows in the next batch
    PollingRest,
    /// Ready to produce one batch
    Ready,
    /// No more buffered batches
    Exhausted,
}

/// Represents a chunk of joined data from streamed and buffered side
struct StreamedJoinedChunk {
    /// Index of batch in buffered_data
    buffered_batch_idx: Option<usize>,
    /// Array builder for streamed indices
    streamed_indices: UInt64Builder,
    /// Array builder for buffered indices
    /// This could contain nulls if the join is null-joined
    buffered_indices: UInt64Builder,
}

struct StreamedBatch {
    /// The streamed record batch
    pub batch: RecordBatch,
    /// The index of row in the streamed batch to compare with buffered batches
    pub idx: usize,
    /// The join key arrays of streamed batch which are used to compare with buffered batches
    /// and to produce output. They are produced by evaluating `on` expressions.
    pub join_arrays: Vec<ArrayRef>,
    /// Chunks of indices from buffered side (may be nulls) joined to streamed
    pub output_indices: Vec<StreamedJoinedChunk>,
    /// Index of currently scanned batch from buffered data
    pub buffered_batch_idx: Option<usize>,
    /// Indices that found a match for the given join filter
    /// Used for semi joins to keep track the streaming index which got a join filter match
    /// and already emitted to the output.
    pub join_filter_matched_idxs: HashSet<u64>,
}

impl StreamedBatch {
    fn new(batch: RecordBatch, on_column: &[Arc<dyn PhysicalExpr>]) -> Self {
        let join_arrays = join_arrays(&batch, on_column);
        StreamedBatch {
            batch,
            idx: 0,
            join_arrays,
            output_indices: vec![],
            buffered_batch_idx: None,
            join_filter_matched_idxs: HashSet::new(),
        }
    }

    fn new_empty(schema: SchemaRef) -> Self {
        StreamedBatch {
            batch: RecordBatch::new_empty(schema),
            idx: 0,
            join_arrays: vec![],
            output_indices: vec![],
            buffered_batch_idx: None,
            join_filter_matched_idxs: HashSet::new(),
        }
    }

    /// Appends new pair consisting of current streamed index and `buffered_idx`
    /// index of buffered batch with `buffered_batch_idx` index.
    fn append_output_pair(
        &mut self,
        buffered_batch_idx: Option<usize>,
        buffered_idx: Option<usize>,
    ) {
        // If no current chunk exists or current chunk is not for current buffered batch,
        // create a new chunk
        if self.output_indices.is_empty() || self.buffered_batch_idx != buffered_batch_idx
        {
            self.output_indices.push(StreamedJoinedChunk {
                buffered_batch_idx,
                streamed_indices: UInt64Builder::with_capacity(1),
                buffered_indices: UInt64Builder::with_capacity(1),
            });
            self.buffered_batch_idx = buffered_batch_idx;
        };
        let current_chunk = self.output_indices.last_mut().unwrap();

        // Append index of streamed batch and index of buffered batch into current chunk
        current_chunk.streamed_indices.append_value(self.idx as u64);
        if let Some(idx) = buffered_idx {
            current_chunk.buffered_indices.append_value(idx as u64);
        } else {
            current_chunk.buffered_indices.append_null();
        }
    }
}

/// A buffered batch that contains contiguous rows with same join key
#[derive(Debug)]
struct BufferedBatch {
    /// The buffered record batch
    pub batch: RecordBatch,
    /// The range in which the rows share the same join key
    pub range: Range<usize>,
    /// Array refs of the join key
    pub join_arrays: Vec<ArrayRef>,
    /// Buffered joined index (null joining buffered)
    pub null_joined: Vec<usize>,
    /// Size estimation used for reserving / releasing memory
    pub size_estimation: usize,
    /// The indices of buffered batch that failed the join filter.
    /// When dequeuing the buffered batch, we need to produce null joined rows for these indices.
    pub join_filter_failed_idxs: HashSet<u64>,
}

impl BufferedBatch {
    fn new(
        batch: RecordBatch,
        range: Range<usize>,
        on_column: &[PhysicalExprRef],
    ) -> Self {
        let join_arrays = join_arrays(&batch, on_column);

        // Estimation is calculated as
        //   inner batch size
        // + join keys size
        // + worst case null_joined (as vector capacity * element size)
        // + Range size
        // + size of this estimation
        let size_estimation = batch.get_array_memory_size()
            + join_arrays
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>()
            + batch.num_rows().next_power_of_two() * mem::size_of::<usize>()
            + mem::size_of::<Range<usize>>()
            + mem::size_of::<usize>();

        BufferedBatch {
            batch,
            range,
            join_arrays,
            null_joined: vec![],
            size_estimation,
            join_filter_failed_idxs: HashSet::new(),
        }
    }
}

/// Sort-merge join stream that consumes streamed and buffered data stream
/// and produces joined output
struct SMJStream {
    /// Current state of the stream
    pub state: SMJState,
    /// Output schema
    pub schema: SchemaRef,
    /// Sort options of join columns used to sort streamed and buffered data stream
    pub sort_options: Vec<SortOptions>,
    /// null == null?
    pub null_equals_null: bool,
    /// Input schema of streamed
    pub streamed_schema: SchemaRef,
    /// Input schema of buffered
    pub buffered_schema: SchemaRef,
    /// Streamed data stream
    pub streamed: SendableRecordBatchStream,
    /// Buffered data stream
    pub buffered: SendableRecordBatchStream,
    /// Current processing record batch of streamed
    pub streamed_batch: StreamedBatch,
    /// Currrent buffered data
    pub buffered_data: BufferedData,
    /// (used in outer join) Is current streamed row joined at least once?
    pub streamed_joined: bool,
    /// (used in outer join) Is current buffered batches joined at least once?
    pub buffered_joined: bool,
    /// State of streamed
    pub streamed_state: StreamedState,
    /// State of buffered
    pub buffered_state: BufferedState,
    /// The comparison result of current streamed row and buffered batches
    pub current_ordering: Ordering,
    /// Join key columns of streamed
    pub on_streamed: Vec<PhysicalExprRef>,
    /// Join key columns of buffered
    pub on_buffered: Vec<PhysicalExprRef>,
    /// optional join filter
    pub filter: Option<JoinFilter>,
    /// Staging output array builders
    pub output_record_batches: Vec<RecordBatch>,
    /// Staging output size, including output batches and staging joined results.
    /// Increased when we put rows into buffer and decreased after we actually output batches.
    /// Used to trigger output when sufficient rows are ready
    pub output_size: usize,
    /// Target output batch size
    pub batch_size: usize,
    /// How the join is performed
    pub join_type: JoinType,
    /// Metrics
    pub join_metrics: SortMergeJoinMetrics,
    /// Memory reservation
    pub reservation: MemoryReservation,
}

impl RecordBatchStream for SMJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for SMJStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let join_time = self.join_metrics.join_time.clone();
        let _timer = join_time.timer();

        loop {
            match &self.state {
                SMJState::Init => {
                    let streamed_exhausted =
                        self.streamed_state == StreamedState::Exhausted;
                    let buffered_exhausted =
                        self.buffered_state == BufferedState::Exhausted;
                    self.state = if streamed_exhausted && buffered_exhausted {
                        SMJState::Exhausted
                    } else {
                        match self.current_ordering {
                            Ordering::Less | Ordering::Equal => {
                                if !streamed_exhausted {
                                    self.streamed_joined = false;
                                    self.streamed_state = StreamedState::Init;
                                }
                            }
                            Ordering::Greater => {
                                if !buffered_exhausted {
                                    self.buffered_joined = false;
                                    self.buffered_state = BufferedState::Init;
                                }
                            }
                        }
                        SMJState::Polling
                    };
                }
                SMJState::Polling => {
                    if ![StreamedState::Exhausted, StreamedState::Ready]
                        .contains(&self.streamed_state)
                    {
                        match self.poll_streamed_row(cx)? {
                            Poll::Ready(_) => {}
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    if ![BufferedState::Exhausted, BufferedState::Ready]
                        .contains(&self.buffered_state)
                    {
                        match self.poll_buffered_batches(cx)? {
                            Poll::Ready(_) => {}
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                    let streamed_exhausted =
                        self.streamed_state == StreamedState::Exhausted;
                    let buffered_exhausted =
                        self.buffered_state == BufferedState::Exhausted;
                    if streamed_exhausted && buffered_exhausted {
                        self.state = SMJState::Exhausted;
                        continue;
                    }
                    self.current_ordering = self.compare_streamed_buffered()?;
                    self.state = SMJState::JoinOutput;
                }
                SMJState::JoinOutput => {
                    self.join_partial()?;

                    if self.output_size < self.batch_size {
                        if self.buffered_data.scanning_finished() {
                            self.buffered_data.scanning_reset();
                            self.state = SMJState::Init;
                        }
                    } else {
                        self.freeze_all()?;
                        if !self.output_record_batches.is_empty() {
                            let record_batch = self.output_record_batch_and_reset()?;
                            return Poll::Ready(Some(Ok(record_batch)));
                        }
                        return Poll::Pending;
                    }
                }
                SMJState::Exhausted => {
                    self.freeze_all()?;
                    if !self.output_record_batches.is_empty() {
                        let record_batch = self.output_record_batch_and_reset()?;
                        return Poll::Ready(Some(Ok(record_batch)));
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl SMJStream {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
        streamed: SendableRecordBatchStream,
        buffered: SendableRecordBatchStream,
        on_streamed: Vec<Arc<dyn PhysicalExpr>>,
        on_buffered: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        batch_size: usize,
        join_metrics: SortMergeJoinMetrics,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let streamed_schema = streamed.schema();
        let buffered_schema = buffered.schema();
        Ok(Self {
            state: SMJState::Init,
            sort_options,
            null_equals_null,
            schema,
            streamed_schema: Arc::clone(&streamed_schema),
            buffered_schema,
            streamed,
            buffered,
            streamed_batch: StreamedBatch::new_empty(streamed_schema),
            buffered_data: BufferedData::default(),
            streamed_joined: false,
            buffered_joined: false,
            streamed_state: StreamedState::Init,
            buffered_state: BufferedState::Init,
            current_ordering: Ordering::Equal,
            on_streamed,
            on_buffered,
            filter,
            output_record_batches: vec![],
            output_size: 0,
            batch_size,
            join_type,
            join_metrics,
            reservation,
        })
    }

    /// Poll next streamed row
    fn poll_streamed_row(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>> {
        loop {
            match &self.streamed_state {
                StreamedState::Init => {
                    if self.streamed_batch.idx + 1 < self.streamed_batch.batch.num_rows()
                    {
                        self.streamed_batch.idx += 1;
                        self.streamed_state = StreamedState::Ready;
                        return Poll::Ready(Some(Ok(())));
                    } else {
                        self.streamed_state = StreamedState::Polling;
                    }
                }
                StreamedState::Polling => match self.streamed.poll_next_unpin(cx)? {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        self.streamed_state = StreamedState::Exhausted;
                    }
                    Poll::Ready(Some(batch)) => {
                        if batch.num_rows() > 0 {
                            self.freeze_streamed()?;
                            self.join_metrics.input_batches.add(1);
                            self.join_metrics.input_rows.add(batch.num_rows());
                            self.streamed_batch =
                                StreamedBatch::new(batch, &self.on_streamed);
                            self.streamed_state = StreamedState::Ready;
                        }
                    }
                },
                StreamedState::Ready => {
                    return Poll::Ready(Some(Ok(())));
                }
                StreamedState::Exhausted => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    /// Poll next buffered batches
    fn poll_buffered_batches(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>> {
        loop {
            match &self.buffered_state {
                BufferedState::Init => {
                    // pop previous buffered batches
                    while !self.buffered_data.batches.is_empty() {
                        let head_batch = self.buffered_data.head_batch();
                        // If the head batch is fully processed, dequeue it and produce output of it.
                        if head_batch.range.end == head_batch.batch.num_rows() {
                            self.freeze_dequeuing_buffered()?;
                            if let Some(buffered_batch) =
                                self.buffered_data.batches.pop_front()
                            {
                                self.reservation.shrink(buffered_batch.size_estimation);
                            }
                        } else {
                            // If the head batch is not fully processed, break the loop.
                            // Streamed batch will be joined with the head batch in the next step.
                            break;
                        }
                    }
                    if self.buffered_data.batches.is_empty() {
                        self.buffered_state = BufferedState::PollingFirst;
                    } else {
                        let tail_batch = self.buffered_data.tail_batch_mut();
                        tail_batch.range.start = tail_batch.range.end;
                        tail_batch.range.end += 1;
                        self.buffered_state = BufferedState::PollingRest;
                    }
                }
                BufferedState::PollingFirst => match self.buffered.poll_next_unpin(cx)? {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        self.buffered_state = BufferedState::Exhausted;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Some(batch)) => {
                        self.join_metrics.input_batches.add(1);
                        self.join_metrics.input_rows.add(batch.num_rows());
                        if batch.num_rows() > 0 {
                            let buffered_batch =
                                BufferedBatch::new(batch, 0..1, &self.on_buffered);
                            self.reservation.try_grow(buffered_batch.size_estimation)?;
                            self.join_metrics
                                .peak_mem_used
                                .set_max(self.reservation.size());

                            self.buffered_data.batches.push_back(buffered_batch);
                            self.buffered_state = BufferedState::PollingRest;
                        }
                    }
                },
                BufferedState::PollingRest => {
                    if self.buffered_data.tail_batch().range.end
                        < self.buffered_data.tail_batch().batch.num_rows()
                    {
                        while self.buffered_data.tail_batch().range.end
                            < self.buffered_data.tail_batch().batch.num_rows()
                        {
                            if is_join_arrays_equal(
                                &self.buffered_data.head_batch().join_arrays,
                                self.buffered_data.head_batch().range.start,
                                &self.buffered_data.tail_batch().join_arrays,
                                self.buffered_data.tail_batch().range.end,
                            )? {
                                self.buffered_data.tail_batch_mut().range.end += 1;
                            } else {
                                self.buffered_state = BufferedState::Ready;
                                return Poll::Ready(Some(Ok(())));
                            }
                        }
                    } else {
                        match self.buffered.poll_next_unpin(cx)? {
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                            Poll::Ready(None) => {
                                self.buffered_state = BufferedState::Ready;
                            }
                            Poll::Ready(Some(batch)) => {
                                self.join_metrics.input_batches.add(1);
                                self.join_metrics.input_rows.add(batch.num_rows());
                                if batch.num_rows() > 0 {
                                    let buffered_batch = BufferedBatch::new(
                                        batch,
                                        0..0,
                                        &self.on_buffered,
                                    );
                                    self.reservation
                                        .try_grow(buffered_batch.size_estimation)?;
                                    self.join_metrics
                                        .peak_mem_used
                                        .set_max(self.reservation.size());
                                    self.buffered_data.batches.push_back(buffered_batch);
                                }
                            }
                        }
                    }
                }
                BufferedState::Ready => {
                    return Poll::Ready(Some(Ok(())));
                }
                BufferedState::Exhausted => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    /// Get comparison result of streamed row and buffered batches
    fn compare_streamed_buffered(&self) -> Result<Ordering> {
        if self.streamed_state == StreamedState::Exhausted {
            return Ok(Ordering::Greater);
        }
        if !self.buffered_data.has_buffered_rows() {
            return Ok(Ordering::Less);
        }

        return compare_join_arrays(
            &self.streamed_batch.join_arrays,
            self.streamed_batch.idx,
            &self.buffered_data.head_batch().join_arrays,
            self.buffered_data.head_batch().range.start,
            &self.sort_options,
            self.null_equals_null,
        );
    }

    /// Produce join and fill output buffer until reaching target batch size
    /// or the join is finished
    fn join_partial(&mut self) -> Result<()> {
        // Whether to join streamed rows
        let mut join_streamed = false;
        // Whether to join buffered rows
        let mut join_buffered = false;

        // determine whether we need to join streamed/buffered rows
        match self.current_ordering {
            Ordering::Less => {
                if matches!(
                    self.join_type,
                    JoinType::Left
                        | JoinType::Right
                        | JoinType::RightSemi
                        | JoinType::Full
                        | JoinType::LeftAnti
                ) {
                    join_streamed = !self.streamed_joined;
                }
            }
            Ordering::Equal => {
                if matches!(self.join_type, JoinType::LeftSemi) {
                    // if the join filter is specified then its needed to output the streamed index
                    // only if it has not been emitted before
                    // the `join_filter_matched_idxs` keeps track on if streamed index has a successful
                    // filter match and prevents the same index to go into output more than once
                    if self.filter.is_some() {
                        join_streamed = !self
                            .streamed_batch
                            .join_filter_matched_idxs
                            .contains(&(self.streamed_batch.idx as u64))
                            && !self.streamed_joined;
                        // if the join filter specified there can be references to buffered columns
                        // so buffered columns are needed to access them
                        join_buffered = join_streamed;
                    } else {
                        join_streamed = !self.streamed_joined;
                    }
                }
                if matches!(
                    self.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) {
                    join_streamed = true;
                    join_buffered = true;
                };

                if matches!(self.join_type, JoinType::LeftAnti) && self.filter.is_some() {
                    join_streamed = !self
                        .streamed_batch
                        .join_filter_matched_idxs
                        .contains(&(self.streamed_batch.idx as u64))
                        && !self.streamed_joined;
                    join_buffered = join_streamed;
                }
            }
            Ordering::Greater => {
                if matches!(self.join_type, JoinType::Full) {
                    join_buffered = !self.buffered_joined;
                };
            }
        }
        if !join_streamed && !join_buffered {
            // no joined data
            self.buffered_data.scanning_finish();
            return Ok(());
        }

        if join_buffered {
            // joining streamed/nulls and buffered
            while !self.buffered_data.scanning_finished()
                && self.output_size < self.batch_size
            {
                let scanning_idx = self.buffered_data.scanning_idx();
                if join_streamed {
                    // Join streamed row and buffered row
                    self.streamed_batch.append_output_pair(
                        Some(self.buffered_data.scanning_batch_idx),
                        Some(scanning_idx),
                    );
                } else {
                    // Join nulls and buffered row for FULL join
                    self.buffered_data
                        .scanning_batch_mut()
                        .null_joined
                        .push(scanning_idx);
                }
                self.output_size += 1;
                self.buffered_data.scanning_advance();

                if self.buffered_data.scanning_finished() {
                    self.streamed_joined = join_streamed;
                    self.buffered_joined = true;
                }
            }
        } else {
            // joining streamed and nulls
            let scanning_batch_idx = if self.buffered_data.scanning_finished() {
                None
            } else {
                Some(self.buffered_data.scanning_batch_idx)
            };

            self.streamed_batch
                .append_output_pair(scanning_batch_idx, None);
            self.output_size += 1;
            self.buffered_data.scanning_finish();
            self.streamed_joined = true;
        }
        Ok(())
    }

    fn freeze_all(&mut self) -> Result<()> {
        self.freeze_streamed()?;
        self.freeze_buffered(self.buffered_data.batches.len(), false)?;
        Ok(())
    }

    // Produces and stages record batches to ensure dequeued buffered batch
    // no longer needed:
    //   1. freezes all indices joined to streamed side
    //   2. freezes NULLs joined to dequeued buffered batch to "release" it
    fn freeze_dequeuing_buffered(&mut self) -> Result<()> {
        self.freeze_streamed()?;
        // Only freeze and produce the first batch in buffered_data as the batch is fully processed
        self.freeze_buffered(1, true)?;
        Ok(())
    }

    // Produces and stages record batch from buffered indices with corresponding
    // NULLs on streamed side.
    //
    // Applicable only in case of Full join.
    //
    // If `output_not_matched_filter` is true, this will also produce record batches
    // for buffered rows which are joined with streamed side but don't match join filter.
    fn freeze_buffered(
        &mut self,
        batch_count: usize,
        output_not_matched_filter: bool,
    ) -> Result<()> {
        if !matches!(self.join_type, JoinType::Full) {
            return Ok(());
        }
        for buffered_batch in self.buffered_data.batches.range_mut(..batch_count) {
            let buffered_indices = UInt64Array::from_iter_values(
                buffered_batch.null_joined.iter().map(|&index| index as u64),
            );
            if let Some(record_batch) = produce_buffered_null_batch(
                &self.schema,
                &self.streamed_schema,
                &buffered_indices,
                buffered_batch,
            )? {
                self.output_record_batches.push(record_batch);
            }
            buffered_batch.null_joined.clear();

            // For buffered rows which are joined with streamed side but doesn't satisfy the join filter
            if output_not_matched_filter {
                let buffered_indices = UInt64Array::from_iter_values(
                    buffered_batch.join_filter_failed_idxs.iter().copied(),
                );
                if let Some(record_batch) = produce_buffered_null_batch(
                    &self.schema,
                    &self.streamed_schema,
                    &buffered_indices,
                    buffered_batch,
                )? {
                    self.output_record_batches.push(record_batch);
                }
                buffered_batch.join_filter_failed_idxs.clear();
            }
        }
        Ok(())
    }

    // Produces and stages record batch for all output indices found
    // for current streamed batch and clears staged output indices.
    fn freeze_streamed(&mut self) -> Result<()> {
        for chunk in self.streamed_batch.output_indices.iter_mut() {
            // The row indices of joined streamed batch
            let streamed_indices = chunk.streamed_indices.finish();

            if streamed_indices.is_empty() {
                continue;
            }

            let mut streamed_columns = self
                .streamed_batch
                .batch
                .columns()
                .iter()
                .map(|column| take(column, &streamed_indices, None))
                .collect::<Result<Vec<_>, ArrowError>>()?;

            // The row indices of joined buffered batch
            let buffered_indices: UInt64Array = chunk.buffered_indices.finish();
            let mut buffered_columns =
                if matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    vec![]
                } else if let Some(buffered_idx) = chunk.buffered_batch_idx {
                    get_buffered_columns(
                        &self.buffered_data,
                        buffered_idx,
                        &buffered_indices,
                    )?
                } else {
                    // If buffered batch none, meaning it is null joined batch.
                    // We need to create null arrays for buffered columns to join with streamed rows.
                    self.buffered_schema
                        .fields()
                        .iter()
                        .map(|f| new_null_array(f.data_type(), buffered_indices.len()))
                        .collect::<Vec<_>>()
                };

            let streamed_columns_length = streamed_columns.len();
            let buffered_columns_length = buffered_columns.len();

            // Prepare the columns we apply join filter on later.
            // Only for joined rows between streamed and buffered.
            let filter_columns = if chunk.buffered_batch_idx.is_some() {
                if matches!(self.join_type, JoinType::Right) {
                    get_filter_column(&self.filter, &buffered_columns, &streamed_columns)
                } else if matches!(
                    self.join_type,
                    JoinType::LeftSemi | JoinType::LeftAnti
                ) {
                    // unwrap is safe here as we check is_some on top of if statement
                    let buffered_columns = get_buffered_columns(
                        &self.buffered_data,
                        chunk.buffered_batch_idx.unwrap(),
                        &buffered_indices,
                    )?;

                    get_filter_column(&self.filter, &streamed_columns, &buffered_columns)
                } else {
                    get_filter_column(&self.filter, &streamed_columns, &buffered_columns)
                }
            } else {
                // This chunk is totally for null joined rows (outer join), we don't need to apply join filter.
                // Any join filter applied only on either streamed or buffered side will be pushed already.
                vec![]
            };

            let columns = if matches!(self.join_type, JoinType::Right) {
                buffered_columns.extend(streamed_columns.clone());
                buffered_columns
            } else {
                streamed_columns.extend(buffered_columns);
                streamed_columns
            };

            let output_batch =
                RecordBatch::try_new(Arc::clone(&self.schema), columns.clone())?;

            // Apply join filter if any
            if !filter_columns.is_empty() {
                if let Some(f) = &self.filter {
                    // Construct batch with only filter columns
                    let filter_batch = RecordBatch::try_new(
                        Arc::new(f.schema().clone()),
                        filter_columns,
                    )?;

                    let filter_result = f
                        .expression()
                        .evaluate(&filter_batch)?
                        .into_array(filter_batch.num_rows())?;

                    // The boolean selection mask of the join filter result
                    let pre_mask =
                        datafusion_common::cast::as_boolean_array(&filter_result)?;

                    // If there are nulls in join filter result, exclude them from selecting
                    // the rows to output.
                    let mask = if pre_mask.null_count() > 0 {
                        compute::prep_null_mask_filter(
                            datafusion_common::cast::as_boolean_array(&filter_result)?,
                        )
                    } else {
                        pre_mask.clone()
                    };

                    // For certain join types, we need to adjust the initial mask to handle the join filter.
                    let maybe_filtered_join_mask: Option<(BooleanArray, Vec<u64>)> =
                        get_filtered_join_mask(
                            self.join_type,
                            &streamed_indices,
                            &mask,
                            &self.streamed_batch.join_filter_matched_idxs,
                            &self.buffered_data.scanning_offset,
                        );

                    let mask =
                        if let Some(ref filtered_join_mask) = maybe_filtered_join_mask {
                            self.streamed_batch
                                .join_filter_matched_idxs
                                .extend(&filtered_join_mask.1);
                            &filtered_join_mask.0
                        } else {
                            &mask
                        };

                    // Push the filtered batch which contains rows passing join filter to the output
                    let filtered_batch =
                        compute::filter_record_batch(&output_batch, mask)?;
                    self.output_record_batches.push(filtered_batch);

                    // For outer joins, we need to push the null joined rows to the output if
                    // all joined rows are failed on the join filter.
                    // I.e., if all rows joined from a streamed row are failed with the join filter,
                    // we need to join it with nulls as buffered side.
                    if matches!(
                        self.join_type,
                        JoinType::Left | JoinType::Right | JoinType::Full
                    ) {
                        // We need to get the mask for row indices that the joined rows are failed
                        // on the join filter. I.e., for a row in streamed side, if all joined rows
                        // between it and all buffered rows are failed on the join filter, we need to
                        // output it with null columns from buffered side. For the mask here, it
                        // behaves like LeftAnti join.
                        let null_mask: BooleanArray = get_filtered_join_mask(
                            // Set a mask slot as true only if all joined rows of same streamed index
                            // are failed on the join filter.
                            // The masking behavior is like LeftAnti join.
                            JoinType::LeftAnti,
                            &streamed_indices,
                            mask,
                            &self.streamed_batch.join_filter_matched_idxs,
                            &self.buffered_data.scanning_offset,
                        )
                        .unwrap()
                        .0;

                        let null_joined_batch =
                            compute::filter_record_batch(&output_batch, &null_mask)?;

                        let mut buffered_columns = self
                            .buffered_schema
                            .fields()
                            .iter()
                            .map(|f| {
                                new_null_array(
                                    f.data_type(),
                                    null_joined_batch.num_rows(),
                                )
                            })
                            .collect::<Vec<_>>();

                        let columns = if matches!(self.join_type, JoinType::Right) {
                            let streamed_columns = null_joined_batch
                                .columns()
                                .iter()
                                .skip(buffered_columns_length)
                                .cloned()
                                .collect::<Vec<_>>();

                            buffered_columns.extend(streamed_columns);
                            buffered_columns
                        } else {
                            // Left join or full outer join
                            let mut streamed_columns = null_joined_batch
                                .columns()
                                .iter()
                                .take(streamed_columns_length)
                                .cloned()
                                .collect::<Vec<_>>();

                            streamed_columns.extend(buffered_columns);
                            streamed_columns
                        };

                        // Push the streamed/buffered batch joined nulls to the output
                        let null_joined_streamed_batch = RecordBatch::try_new(
                            Arc::clone(&self.schema),
                            columns.clone(),
                        )?;
                        self.output_record_batches.push(null_joined_streamed_batch);

                        // For full join, we also need to output the null joined rows from the buffered side.
                        // Usually this is done by `freeze_buffered`. However, if a buffered row is joined with
                        // streamed side, it won't be outputted by `freeze_buffered`.
                        // We need to check if a buffered row is joined with streamed side and output.
                        // If it is joined with streamed side, but doesn't match the join filter,
                        // we need to output it with nulls as streamed side.
                        if matches!(self.join_type, JoinType::Full) {
                            for i in 0..pre_mask.len() {
                                let buffered_batch = &mut self.buffered_data.batches
                                    [chunk.buffered_batch_idx.unwrap()];
                                let buffered_index = buffered_indices.value(i);

                                if !pre_mask.value(i) {
                                    // For a buffered row that is joined with streamed side but doesn't satisfy the join filter,
                                    buffered_batch
                                        .join_filter_failed_idxs
                                        .insert(buffered_index);
                                } else if buffered_batch
                                    .join_filter_failed_idxs
                                    .contains(&buffered_index)
                                {
                                    buffered_batch
                                        .join_filter_failed_idxs
                                        .remove(&buffered_index);
                                }
                            }
                        }
                    }
                } else {
                    self.output_record_batches.push(output_batch);
                }
            } else {
                self.output_record_batches.push(output_batch);
            }
        }

        self.streamed_batch.output_indices.clear();

        Ok(())
    }

    fn output_record_batch_and_reset(&mut self) -> Result<RecordBatch> {
        let record_batch = concat_batches(&self.schema, &self.output_record_batches)?;
        self.join_metrics.output_batches.add(1);
        self.join_metrics.output_rows.add(record_batch.num_rows());
        // If join filter exists, `self.output_size` is not accurate as we don't know the exact
        // number of rows in the output record batch. If streamed row joined with buffered rows,
        // once join filter is applied, the number of output rows may be more than 1.
        // If `record_batch` is empty, we should reset `self.output_size` to 0. It could be happened
        // when the join filter is applied and all rows are filtered out.
        if record_batch.num_rows() == 0 || record_batch.num_rows() > self.output_size {
            self.output_size = 0;
        } else {
            self.output_size -= record_batch.num_rows();
        }
        self.output_record_batches.clear();
        Ok(record_batch)
    }
}

/// Gets the arrays which join filters are applied on.
fn get_filter_column(
    join_filter: &Option<JoinFilter>,
    streamed_columns: &[ArrayRef],
    buffered_columns: &[ArrayRef],
) -> Vec<ArrayRef> {
    let mut filter_columns = vec![];

    if let Some(f) = join_filter {
        let left_columns = f
            .column_indices()
            .iter()
            .filter(|col_index| col_index.side == JoinSide::Left)
            .map(|i| Arc::clone(&streamed_columns[i.index]))
            .collect::<Vec<_>>();

        let right_columns = f
            .column_indices()
            .iter()
            .filter(|col_index| col_index.side == JoinSide::Right)
            .map(|i| Arc::clone(&buffered_columns[i.index]))
            .collect::<Vec<_>>();

        filter_columns.extend(left_columns);
        filter_columns.extend(right_columns);
    }

    filter_columns
}

fn produce_buffered_null_batch(
    schema: &SchemaRef,
    streamed_schema: &SchemaRef,
    buffered_indices: &PrimitiveArray<UInt64Type>,
    buffered_batch: &BufferedBatch,
) -> Result<Option<RecordBatch>> {
    if buffered_indices.is_empty() {
        return Ok(None);
    }

    // Take buffered (right) columns
    let buffered_columns = buffered_batch
        .batch
        .columns()
        .iter()
        .map(|column| take(column, &buffered_indices, None))
        .collect::<Result<Vec<_>, ArrowError>>()
        .map_err(Into::<DataFusionError>::into)?;

    // Create null streamed (left) columns
    let mut streamed_columns = streamed_schema
        .fields()
        .iter()
        .map(|f| new_null_array(f.data_type(), buffered_indices.len()))
        .collect::<Vec<_>>();

    streamed_columns.extend(buffered_columns);

    Ok(Some(RecordBatch::try_new(
        Arc::clone(schema),
        streamed_columns,
    )?))
}

/// Get `buffered_indices` rows for `buffered_data[buffered_batch_idx]`
#[inline(always)]
fn get_buffered_columns(
    buffered_data: &BufferedData,
    buffered_batch_idx: usize,
    buffered_indices: &UInt64Array,
) -> Result<Vec<ArrayRef>, ArrowError> {
    buffered_data.batches[buffered_batch_idx]
        .batch
        .columns()
        .iter()
        .map(|column| take(column, &buffered_indices, None))
        .collect::<Result<Vec<_>, ArrowError>>()
}

/// Calculate join filter bit mask considering join type specifics
/// `streamed_indices` - array of streamed datasource JOINED row indices
/// `mask` - array booleans representing computed join filter expression eval result:
///      true = the row index matches the join filter
///      false = the row index doesn't match the join filter
/// `streamed_indices` have the same length as `mask`
/// `matched_indices` array of streaming indices that already has a join filter match
/// `scanning_buffered_offset` current buffered offset across batches
///
/// This return a tuple of:
/// - corrected mask with respect to the join type
/// - indices of rows in streamed batch that have a join filter match
fn get_filtered_join_mask(
    join_type: JoinType,
    streamed_indices: &UInt64Array,
    mask: &BooleanArray,
    matched_indices: &HashSet<u64>,
    scanning_buffered_offset: &usize,
) -> Option<(BooleanArray, Vec<u64>)> {
    let mut seen_as_true: bool = false;
    let streamed_indices_length = streamed_indices.len();
    let mut corrected_mask: BooleanBuilder =
        BooleanBuilder::with_capacity(streamed_indices_length);

    let mut filter_matched_indices: Vec<u64> = vec![];

    #[allow(clippy::needless_range_loop)]
    match join_type {
        // for LeftSemi Join the filter mask should be calculated in its own way:
        // if we find at least one matching row for specific streaming index
        // we don't need to check any others for the same index
        JoinType::LeftSemi => {
            // have we seen a filter match for a streaming index before
            for i in 0..streamed_indices_length {
                // LeftSemi respects only first true values for specific streaming index,
                // others true values for the same index must be false
                let streamed_idx = streamed_indices.value(i);
                if mask.value(i)
                    && !seen_as_true
                    && !matched_indices.contains(&streamed_idx)
                {
                    seen_as_true = true;
                    corrected_mask.append_value(true);
                    filter_matched_indices.push(streamed_idx);
                } else {
                    corrected_mask.append_value(false);
                }

                // if switched to next streaming index(e.g. from 0 to 1, or from 1 to 2), we reset seen_as_true flag
                if i < streamed_indices_length - 1
                    && streamed_idx != streamed_indices.value(i + 1)
                {
                    seen_as_true = false;
                }
            }
            Some((corrected_mask.finish(), filter_matched_indices))
        }
        // LeftAnti semantics: return true if for every x in the collection the join matching filter is false.
        // `filter_matched_indices` needs to be set once per streaming index
        // to prevent duplicates in the output
        JoinType::LeftAnti => {
            // have we seen a filter match for a streaming index before
            for i in 0..streamed_indices_length {
                if mask.value(i) && !seen_as_true {
                    seen_as_true = true;
                    filter_matched_indices.push(streamed_indices.value(i));
                }

                // Reset `seen_as_true` flag and calculate mask for the current streaming index
                // - if within the batch it switched to next streaming index(e.g. from 0 to 1, or from 1 to 2)
                // - if it is at the end of the all buffered batches for the given streaming index, 0 index comes last
                if (i < streamed_indices_length - 1
                    && streamed_indices.value(i) != streamed_indices.value(i + 1))
                    || (i == streamed_indices_length - 1
                        && *scanning_buffered_offset == 0)
                {
                    corrected_mask.append_value(
                        !matched_indices.contains(&streamed_indices.value(i))
                            && !seen_as_true,
                    );
                    seen_as_true = false;
                } else {
                    corrected_mask.append_value(false);
                }
            }

            Some((corrected_mask.finish(), filter_matched_indices))
        }
        _ => None,
    }
}

/// Buffered data contains all buffered batches with one unique join key
#[derive(Debug, Default)]
struct BufferedData {
    /// Buffered batches with the same key
    pub batches: VecDeque<BufferedBatch>,
    /// current scanning batch index used in join_partial()
    pub scanning_batch_idx: usize,
    /// current scanning offset used in join_partial()
    pub scanning_offset: usize,
}

impl BufferedData {
    pub fn head_batch(&self) -> &BufferedBatch {
        self.batches.front().unwrap()
    }

    pub fn tail_batch(&self) -> &BufferedBatch {
        self.batches.back().unwrap()
    }

    pub fn tail_batch_mut(&mut self) -> &mut BufferedBatch {
        self.batches.back_mut().unwrap()
    }

    pub fn has_buffered_rows(&self) -> bool {
        self.batches.iter().any(|batch| !batch.range.is_empty())
    }

    pub fn scanning_reset(&mut self) {
        self.scanning_batch_idx = 0;
        self.scanning_offset = 0;
    }

    pub fn scanning_advance(&mut self) {
        self.scanning_offset += 1;
        while !self.scanning_finished() && self.scanning_batch_finished() {
            self.scanning_batch_idx += 1;
            self.scanning_offset = 0;
        }
    }

    pub fn scanning_batch(&self) -> &BufferedBatch {
        &self.batches[self.scanning_batch_idx]
    }

    pub fn scanning_batch_mut(&mut self) -> &mut BufferedBatch {
        &mut self.batches[self.scanning_batch_idx]
    }

    pub fn scanning_idx(&self) -> usize {
        self.scanning_batch().range.start + self.scanning_offset
    }

    pub fn scanning_batch_finished(&self) -> bool {
        self.scanning_offset == self.scanning_batch().range.len()
    }

    pub fn scanning_finished(&self) -> bool {
        self.scanning_batch_idx == self.batches.len()
    }

    pub fn scanning_finish(&mut self) {
        self.scanning_batch_idx = self.batches.len();
        self.scanning_offset = 0;
    }
}

/// Get join array refs of given batch and join columns
fn join_arrays(batch: &RecordBatch, on_column: &[PhysicalExprRef]) -> Vec<ArrayRef> {
    on_column
        .iter()
        .map(|c| {
            let num_rows = batch.num_rows();
            let c = c.evaluate(batch).unwrap();
            c.into_array(num_rows).unwrap()
        })
        .collect()
}

/// Get comparison result of two rows of join arrays
fn compare_join_arrays(
    left_arrays: &[ArrayRef],
    left: usize,
    right_arrays: &[ArrayRef],
    right: usize,
    sort_options: &[SortOptions],
    null_equals_null: bool,
) -> Result<Ordering> {
    let mut res = Ordering::Equal;
    for ((left_array, right_array), sort_options) in
        left_arrays.iter().zip(right_arrays).zip(sort_options)
    {
        macro_rules! compare_value {
            ($T:ty) => {{
                let left_array = left_array.as_any().downcast_ref::<$T>().unwrap();
                let right_array = right_array.as_any().downcast_ref::<$T>().unwrap();
                match (left_array.is_null(left), right_array.is_null(right)) {
                    (false, false) => {
                        let left_value = &left_array.value(left);
                        let right_value = &right_array.value(right);
                        res = left_value.partial_cmp(right_value).unwrap();
                        if sort_options.descending {
                            res = res.reverse();
                        }
                    }
                    (true, false) => {
                        res = if sort_options.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        };
                    }
                    (false, true) => {
                        res = if sort_options.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        };
                    }
                    _ => {
                        res = if null_equals_null {
                            Ordering::Equal
                        } else {
                            Ordering::Less
                        };
                    }
                }
            }};
        }

        match left_array.data_type() {
            DataType::Null => {}
            DataType::Boolean => compare_value!(BooleanArray),
            DataType::Int8 => compare_value!(Int8Array),
            DataType::Int16 => compare_value!(Int16Array),
            DataType::Int32 => compare_value!(Int32Array),
            DataType::Int64 => compare_value!(Int64Array),
            DataType::UInt8 => compare_value!(UInt8Array),
            DataType::UInt16 => compare_value!(UInt16Array),
            DataType::UInt32 => compare_value!(UInt32Array),
            DataType::UInt64 => compare_value!(UInt64Array),
            DataType::Float32 => compare_value!(Float32Array),
            DataType::Float64 => compare_value!(Float64Array),
            DataType::Utf8 => compare_value!(StringArray),
            DataType::LargeUtf8 => compare_value!(LargeStringArray),
            DataType::Decimal128(..) => compare_value!(Decimal128Array),
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => compare_value!(TimestampSecondArray),
                TimeUnit::Millisecond => compare_value!(TimestampMillisecondArray),
                TimeUnit::Microsecond => compare_value!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => compare_value!(TimestampNanosecondArray),
            },
            DataType::Date32 => compare_value!(Date32Array),
            DataType::Date64 => compare_value!(Date64Array),
            dt => {
                return not_impl_err!(
                    "Unsupported data type in sort merge join comparator: {}",
                    dt
                );
            }
        }
        if !res.is_eq() {
            break;
        }
    }
    Ok(res)
}

/// A faster version of compare_join_arrays() that only output whether
/// the given two rows are equal
fn is_join_arrays_equal(
    left_arrays: &[ArrayRef],
    left: usize,
    right_arrays: &[ArrayRef],
    right: usize,
) -> Result<bool> {
    let mut is_equal = true;
    for (left_array, right_array) in left_arrays.iter().zip(right_arrays) {
        macro_rules! compare_value {
            ($T:ty) => {{
                match (left_array.is_null(left), right_array.is_null(right)) {
                    (false, false) => {
                        let left_array =
                            left_array.as_any().downcast_ref::<$T>().unwrap();
                        let right_array =
                            right_array.as_any().downcast_ref::<$T>().unwrap();
                        if left_array.value(left) != right_array.value(right) {
                            is_equal = false;
                        }
                    }
                    (true, false) => is_equal = false,
                    (false, true) => is_equal = false,
                    _ => {}
                }
            }};
        }

        match left_array.data_type() {
            DataType::Null => {}
            DataType::Boolean => compare_value!(BooleanArray),
            DataType::Int8 => compare_value!(Int8Array),
            DataType::Int16 => compare_value!(Int16Array),
            DataType::Int32 => compare_value!(Int32Array),
            DataType::Int64 => compare_value!(Int64Array),
            DataType::UInt8 => compare_value!(UInt8Array),
            DataType::UInt16 => compare_value!(UInt16Array),
            DataType::UInt32 => compare_value!(UInt32Array),
            DataType::UInt64 => compare_value!(UInt64Array),
            DataType::Float32 => compare_value!(Float32Array),
            DataType::Float64 => compare_value!(Float64Array),
            DataType::Utf8 => compare_value!(StringArray),
            DataType::LargeUtf8 => compare_value!(LargeStringArray),
            DataType::Decimal128(..) => compare_value!(Decimal128Array),
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => compare_value!(TimestampSecondArray),
                TimeUnit::Millisecond => compare_value!(TimestampMillisecondArray),
                TimeUnit::Microsecond => compare_value!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => compare_value!(TimestampNanosecondArray),
            },
            DataType::Date32 => compare_value!(Date32Array),
            DataType::Date64 => compare_value!(Date64Array),
            dt => {
                return not_impl_err!(
                    "Unsupported data type in sort merge join comparator: {}",
                    dt
                );
            }
        }
        if !is_equal {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Date32Array, Date64Array, Int32Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::{BooleanArray, UInt64Array};
    use hashbrown::HashSet;

    use datafusion_common::JoinType::{LeftAnti, LeftSemi};
    use datafusion_common::{
        assert_batches_eq, assert_batches_sorted_eq, assert_contains, JoinType, Result,
    };
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion_execution::TaskContext;

    use crate::expressions::Column;
    use crate::joins::sort_merge_join::get_filtered_join_mask;
    use crate::joins::utils::JoinOn;
    use crate::joins::SortMergeJoinExec;
    use crate::memory::MemoryExec;
    use crate::test::build_table_i32;
    use crate::{common, ExecutionPlan};

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_table_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches.first().unwrap().schema();
        Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
    }

    fn build_date_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date32, false),
            Field::new(b.0, DataType::Date32, false),
            Field::new(c.0, DataType::Date32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(a.1.clone())),
                Arc::new(Date32Array::from(b.1.clone())),
                Arc::new(Date32Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_date64_table(
        a: (&str, &Vec<i64>),
        b: (&str, &Vec<i64>),
        c: (&str, &Vec<i64>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date64, false),
            Field::new(b.0, DataType::Date64, false),
            Field::new(c.0, DataType::Date64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date64Array::from(a.1.clone())),
                Arc::new(Date64Array::from(b.1.clone())),
                Arc::new(Date64Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    /// returns a table with 3 columns of i32 in memory
    pub fn build_table_i32_nullable(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a.0, DataType::Int32, true),
            Field::new(b.0, DataType::Int32, true),
            Field::new(c.0, DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<SortMergeJoinExec> {
        let sort_options = vec![SortOptions::default(); on.len()];
        SortMergeJoinExec::try_new(left, right, on, None, join_type, sort_options, false)
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            None,
            join_type,
            sort_options,
            null_equals_null,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let sort_options = vec![SortOptions::default(); on.len()];
        join_collect_with_options(left, right, on, join_type, sort_options, false).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = Arc::new(TaskContext::default());
        let join = join_with_options(
            left,
            right,
            on,
            join_type,
            sort_options,
            null_equals_null,
        )?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    async fn join_collect_batch_size_equals_two(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(2));
        let task_ctx = Arc::new(task_ctx);
        let join = join(left, right, on, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 1, 2]),
            ("b2", &vec![1, 1, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 1, 3]),
            ("b2", &vec![1, 1, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 1  | 1  | 7  | 1  | 1  | 80 |",
            "| 1  | 1  | 8  | 1  | 1  | 70 |",
            "| 1  | 1  | 8  | 1  | 1  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(2)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]), // null in key field
            ("c1", &vec![Some(1), None, Some(8), Some(9)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(3)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]),
            ("c2", &vec![Some(10), Some(70), Some(80), Some(90)]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(2), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]), // null in key field
            ("c1", &vec![Some(9), Some(8), None, Some(1)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]),
            ("c2", &vec![Some(90), Some(80), Some(70), Some(10)]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];
        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            JoinType::Inner,
            vec![
                SortOptions {
                    descending: true,
                    nulls_first: false,
                };
                2
            ],
            true,
        )
        .await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 1  |    | 1  | 1  |    | 10 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_output_two_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, JoinType::Inner).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Full).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_anti() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3, 5]),
            ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9, 11]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::LeftAnti).await?;
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "| 5  | 7  | 11 |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_semi() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 5 is double on the right
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::LeftSemi).await?;
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Arc::new(Column::new_with_schema("a", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = [
            "+---+---+---+----+---+----+",
            "| a | b | c | a  | b | c  |",
            "+---+---+---+----+---+----+",
            "| 1 | 4 | 7 | 10 | 1 | 70 |",
            "| 2 | 5 | 8 | 20 | 2 | 80 |",
            "+---+---+---+----+---+----+",
        ];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19107, 19108, 19108]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![19107, 19108, 19109]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = ["+------------+------------+------------+------------+------------+------------+",
            "| a1         | b1         | c1         | a2         | b1         | c2         |",
            "+------------+------------+------------+------------+------------+------------+",
            "| 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |",
            "| 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "| 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "+------------+------------+------------+------------+------------+------------+"];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64() -> Result<()> {
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650703441000, 1650903441000, 1650903441000]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_date64_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = ["+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
            "| a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |",
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+",
            "| 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.070 |",
            "| 1970-01-01T00:00:00.002 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |",
            "| 1970-01-01T00:00:00.003 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |",
            "+-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+"];
        // The output order is important as SMJ preserves sortedness
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![3, 4, 5, 6, 6, 7]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![2, 4, 6, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_sort_order() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3]),
            ("b1", &vec![3, 4, 5, 7]),
            ("c1", &vec![6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30]),
            ("b2", &vec![2, 4, 5, 6]),
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 2  | 60 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "| 6  | 9  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_multiple_batches() -> Result<()> {
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![3, 4, 5, 6]),
            ("b2", &vec![6, 6, 7, 9]),
            ("c2", &vec![7, 8, 9, 9]),
        );
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 10, 20]),
            ("b1", &vec![2, 4, 6]),
            ("c1", &vec![50, 60, 70]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![30, 40]),
            ("b1", &vec![6, 8]),
            ("c1", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 3  | 4  |",
            "| 10 | 4  | 60 | 1  | 4  | 5  |",
            "|    |    |    | 2  | 5  | 6  |",
            "| 20 | 6  | 70 | 3  | 6  | 7  |",
            "| 30 | 6  | 80 | 3  | 6  | 7  |",
            "| 20 | 6  | 70 | 4  | 6  | 8  |",
            "| 30 | 6  | 80 | 4  | 6  | 8  |",
            "|    |    |    | 5  | 7  | 9  |",
            "|    |    |    | 6  | 9  | 9  |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_multiple_batches() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1, 2]),
            ("b1", &vec![3, 4, 5]),
            ("c1", &vec![4, 5, 6]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![3, 4, 5, 6]),
            ("b1", &vec![6, 6, 7, 9]),
            ("c1", &vec![7, 8, 9, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10, 20]),
            ("b2", &vec![2, 4, 6]),
            ("c2", &vec![50, 60, 70]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![30, 40]),
            ("b2", &vec![6, 8]),
            ("c2", &vec![80, 90]),
        );
        let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
        let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Full).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 0  | 2  | 50 |",
            "|    |    |    | 40 | 8  | 90 |",
            "| 0  | 3  | 4  |    |    |    |",
            "| 1  | 4  | 5  | 10 | 4  | 60 |",
            "| 2  | 5  | 6  |    |    |    |",
            "| 3  | 6  | 7  | 20 | 6  | 70 |",
            "| 3  | 6  | 7  | 30 | 6  | 80 |",
            "| 4  | 6  | 8  | 20 | 6  | 70 |",
            "| 4  | 6  | 8  | 30 | 6  | 80 |",
            "| 5  | 7  | 9  |    |    |    |",
            "| 6  | 9  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn overallocation_single_batch() -> Result<()> {
        let left = build_table(
            ("a1", &vec![0, 1, 2, 3, 4, 5]),
            ("b1", &vec![1, 2, 3, 4, 5, 6]),
            ("c1", &vec![4, 5, 6, 7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![0, 10, 20, 30, 40]),
            ("b2", &vec![1, 3, 4, 6, 8]),
            ("c2", &vec![50, 60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let sort_options = vec![SortOptions::default(); on.len()];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];

        for join_type in join_types {
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let session_config = SessionConfig::default().with_batch_size(50);

            let task_ctx = TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                join_type,
                sort_options.clone(),
                false,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(
                err.to_string(),
                "Resources exhausted: Failed to allocate additional"
            );
            assert_contains!(err.to_string(), "SMJStream[0]");
        }

        Ok(())
    }

    #[tokio::test]
    async fn overallocation_multi_batch() -> Result<()> {
        let left_batch_1 = build_table_i32(
            ("a1", &vec![0, 1]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![4, 5]),
        );
        let left_batch_2 = build_table_i32(
            ("a1", &vec![2, 3]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![6, 7]),
        );
        let left_batch_3 = build_table_i32(
            ("a1", &vec![4, 5]),
            ("b1", &vec![1, 1]),
            ("c1", &vec![8, 9]),
        );
        let right_batch_1 = build_table_i32(
            ("a2", &vec![0, 10]),
            ("b2", &vec![1, 1]),
            ("c2", &vec![50, 60]),
        );
        let right_batch_2 = build_table_i32(
            ("a2", &vec![20, 30]),
            ("b2", &vec![1, 1]),
            ("c2", &vec![70, 80]),
        );
        let right_batch_3 =
            build_table_i32(("a2", &vec![40]), ("b2", &vec![1]), ("c2", &vec![90]));
        let left =
            build_table_from_batches(vec![left_batch_1, left_batch_2, left_batch_3]);
        let right =
            build_table_from_batches(vec![right_batch_1, right_batch_2, right_batch_3]);
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let sort_options = vec![SortOptions::default(); on.len()];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];

        for join_type in join_types {
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let session_config = SessionConfig::default().with_batch_size(50);
            let task_ctx = TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);
            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                join_type,
                sort_options.clone(),
                false,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(
                err.to_string(),
                "Resources exhausted: Failed to allocate additional"
            );
            assert_contains!(err.to_string(), "SMJStream[0]");
        }

        Ok(())
    }

    #[tokio::test]
    async fn left_semi_join_filtered_mask() -> Result<()> {
        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 0, 1, 1]),
                &BooleanArray::from(vec![true, true, false, false]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![true, false, false, false]), vec![0]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 1]),
                &BooleanArray::from(vec![true, true]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![true, true]), vec![0, 1]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 1]),
                &BooleanArray::from(vec![false, true]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![false, true]), vec![1]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 1]),
                &BooleanArray::from(vec![true, false]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![true, false]), vec![0]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 0, 0, 1, 1, 1]),
                &BooleanArray::from(vec![false, true, true, true, true, true]),
                &HashSet::new(),
                &0,
            ),
            Some((
                BooleanArray::from(vec![false, true, false, true, false, false]),
                vec![0, 1]
            ))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 0, 0, 1, 1, 1]),
                &BooleanArray::from(vec![false, false, false, false, false, true]),
                &HashSet::new(),
                &0,
            ),
            Some((
                BooleanArray::from(vec![false, false, false, false, false, true]),
                vec![1]
            ))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftSemi,
                &UInt64Array::from(vec![0, 0, 0, 1, 1, 1]),
                &BooleanArray::from(vec![true, false, false, false, false, true]),
                &HashSet::from_iter(vec![1]),
                &0,
            ),
            Some((
                BooleanArray::from(vec![true, false, false, false, false, false]),
                vec![0]
            ))
        );

        Ok(())
    }

    #[tokio::test]
    async fn left_anti_join_filtered_mask() -> Result<()> {
        assert_eq!(
            get_filtered_join_mask(
                LeftAnti,
                &UInt64Array::from(vec![0, 0, 1, 1]),
                &BooleanArray::from(vec![true, true, false, false]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![false, false, false, true]), vec![0]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftAnti,
                &UInt64Array::from(vec![0, 1]),
                &BooleanArray::from(vec![true, true]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![false, false]), vec![0, 1]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftAnti,
                &UInt64Array::from(vec![0, 1]),
                &BooleanArray::from(vec![false, true]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![true, false]), vec![1]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftAnti,
                &UInt64Array::from(vec![0, 1]),
                &BooleanArray::from(vec![true, false]),
                &HashSet::new(),
                &0,
            ),
            Some((BooleanArray::from(vec![false, true]), vec![0]))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftAnti,
                &UInt64Array::from(vec![0, 0, 0, 1, 1, 1]),
                &BooleanArray::from(vec![false, true, true, true, true, true]),
                &HashSet::new(),
                &0,
            ),
            Some((
                BooleanArray::from(vec![false, false, false, false, false, false]),
                vec![0, 1]
            ))
        );

        assert_eq!(
            get_filtered_join_mask(
                LeftAnti,
                &UInt64Array::from(vec![0, 0, 0, 1, 1, 1]),
                &BooleanArray::from(vec![false, false, false, false, false, true]),
                &HashSet::new(),
                &0,
            ),
            Some((
                BooleanArray::from(vec![false, false, true, false, false, false]),
                vec![1]
            ))
        );

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
