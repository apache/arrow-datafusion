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
//! A sort-merge join plan consumes two sorted children plan and produces
//! joined output by given join type and other options.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::*;
use arrow::compute::{take, SortOptions};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};

use crate::error::DataFusionError;
use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::logical_plan::JoinType;
use crate::physical_plan::common::combine_batches;
use crate::physical_plan::expressions::Column;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::join_utils::{build_join_schema, check_join_is_valid, JoinOn};
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use crate::physical_plan::{
    metrics, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

/// join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
#[derive(Debug)]
pub struct SortMergeJoinExec {
    /// Left sorted joining execution plan
    left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: JoinOn,
    /// How the join is performed
    join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Sort options of join columns used in sorting left and right execution plans
    sort_options: Vec<SortOptions>,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
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
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equals_null: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        if sort_options.len() != on.len() {
            return Err(DataFusionError::Plan(format!(
                "Expected number of sort options: {}, actual: {}",
                on.len(),
                sort_options.len()
            )));
        }

        let schema =
            Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);

        Ok(Self {
            left,
            right,
            on,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            sort_options,
            null_equals_null,
        })
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        match self.join_type {
            JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti => {
                self.left.output_ordering()
            }
            JoinType::Right => self.right.output_ordering(),
            JoinType::Full => None,
        }
    }

    fn relies_on_input_order(&self) -> bool {
        true
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SortMergeJoinExec::try_new(
                left.clone(),
                right.clone(),
                self.on.clone(),
                self.join_type,
                self.sort_options.clone(),
                self.null_equals_null,
            )?)),
            _ => Err(DataFusionError::Internal(
                "SortMergeJoin wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (streamed, buffered, on_streamed, on_buffered) = match self.join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::Full
            | JoinType::Anti
            | JoinType::Semi => (
                self.left.clone(),
                self.right.clone(),
                self.on.iter().map(|on| on.0.clone()).collect(),
                self.on.iter().map(|on| on.1.clone()).collect(),
            ),
            JoinType::Right => (
                self.right.clone(),
                self.left.clone(),
                self.on.iter().map(|on| on.1.clone()).collect(),
                self.on.iter().map(|on| on.0.clone()).collect(),
            ),
        };

        // execute children plans
        let streamed = streamed.execute(partition, context.clone())?;
        let buffered = buffered.execute(partition, context.clone())?;

        // create output buffer
        let batch_size = context.session_config().batch_size();

        // create join stream
        Ok(Box::pin(SMJStream::try_new(
            self.schema.clone(),
            self.sort_options.clone(),
            self.null_equals_null,
            streamed,
            buffered,
            on_streamed,
            on_buffered,
            self.join_type,
            batch_size,
            SortMergeJoinMetrics::new(partition, &self.metrics),
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "SortMergeJoin: join_type={:?}, on={:?}, schema={:?}",
                    self.join_type, self.on, &self.schema
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        todo!()
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

        Self {
            join_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
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

struct StreamedJoinedChunk {
    /// Index of batch buffered_data
    buffered_batch_idx: Option<usize>,
    /// Array builder for streamed indices
    streamed_indices: UInt64Builder,
    /// Array builder for buffered indices
    buffered_indices: UInt64Builder,
}

struct StreamedBatch {
    pub batch: RecordBatch,
    pub idx: usize,
    pub join_arrays: Vec<ArrayRef>,

    // Chunks of indices from buffered side (may be nulls) joined to streamed
    pub output_indices: Vec<StreamedJoinedChunk>,
    // Index of currently scanned batch from buffered data
    pub buffered_batch_idx: Option<usize>,
}
impl StreamedBatch {
    fn new(batch: RecordBatch, on_column: &[Column]) -> Self {
        let join_arrays = join_arrays(&batch, on_column);
        StreamedBatch {
            batch,
            idx: 0,
            join_arrays,
            output_indices: vec![],
            buffered_batch_idx: None,
        }
    }

    fn new_empty(schema: SchemaRef) -> Self {
        StreamedBatch {
            batch: RecordBatch::new_empty(schema),
            idx: 0,
            join_arrays: vec![],
            output_indices: vec![],
            buffered_batch_idx: None,
        }
    }

    /// Appends new pair consisting of current streamed index and `buffered_idx`
    /// index of buffered batch with `buffered_batch_idx` index.
    fn append_output_pair(
        &mut self,
        buffered_batch_idx: Option<usize>,
        buffered_idx: Option<usize>,
    ) {
        if self.output_indices.is_empty() || self.buffered_batch_idx != buffered_batch_idx
        {
            self.output_indices.push(StreamedJoinedChunk {
                buffered_batch_idx,
                streamed_indices: UInt64Builder::new(1),
                buffered_indices: UInt64Builder::new(1),
            });
            self.buffered_batch_idx = buffered_batch_idx;
        };
        let current_chunk = self.output_indices.last_mut().unwrap();

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
}
impl BufferedBatch {
    fn new(batch: RecordBatch, range: Range<usize>, on_column: &[Column]) -> Self {
        let join_arrays = join_arrays(&batch, on_column);
        BufferedBatch {
            batch,
            range,
            join_arrays,
            null_joined: vec![],
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
    pub on_streamed: Vec<Column>,
    /// Join key columns of buffered
    pub on_buffered: Vec<Column>,
    /// Staging output array builders
    pub output_record_batches: Vec<RecordBatch>,
    /// Staging output size, including output batches and staging joined results
    pub output_size: usize,
    /// Target output batch size
    pub batch_size: usize,
    /// How the join is performed
    pub join_type: JoinType,
    /// Metrics
    pub join_metrics: SortMergeJoinMetrics,
}

impl RecordBatchStream for SMJStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SMJStream {
    type Item = ArrowResult<RecordBatch>;

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
        on_streamed: Vec<Column>,
        on_buffered: Vec<Column>,
        join_type: JoinType,
        batch_size: usize,
        join_metrics: SortMergeJoinMetrics,
    ) -> Result<Self> {
        let streamed_schema = streamed.schema();
        let buffered_schema = buffered.schema();
        Ok(Self {
            state: SMJState::Init,
            sort_options,
            null_equals_null,
            schema,
            streamed_schema: streamed_schema.clone(),
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
            output_record_batches: vec![],
            output_size: 0,
            batch_size,
            join_type,
            join_metrics,
        })
    }

    /// Poll next streamed row
    fn poll_streamed_row(&mut self, cx: &mut Context) -> Poll<Option<ArrowResult<()>>> {
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
                    continue;
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
    fn poll_buffered_batches(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Option<ArrowResult<()>>> {
        loop {
            match &self.buffered_state {
                BufferedState::Init => {
                    // pop previous buffered batches
                    while !self.buffered_data.batches.is_empty() {
                        let head_batch = self.buffered_data.head_batch();
                        if head_batch.range.end == head_batch.batch.num_rows() {
                            self.freeze_dequeuing_buffered()?;
                            self.buffered_data.batches.pop_front();
                        } else {
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
                            self.buffered_data.batches.push_back(BufferedBatch::new(
                                batch,
                                0..1,
                                &self.on_buffered,
                            ));
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
                                if batch.num_rows() > 0 {
                                    self.join_metrics.input_rows.add(batch.num_rows());
                                    self.buffered_data.batches.push_back(
                                        BufferedBatch::new(
                                            batch,
                                            0..0,
                                            &self.on_buffered,
                                        ),
                                    );
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
    fn compare_streamed_buffered(&self) -> ArrowResult<Ordering> {
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
    fn join_partial(&mut self) -> ArrowResult<()> {
        let mut join_streamed = false;
        let mut join_buffered = false;

        // determine whether we need to join streamed/buffered rows
        match self.current_ordering {
            Ordering::Less => {
                if matches!(
                    self.join_type,
                    JoinType::Left | JoinType::Right | JoinType::Full | JoinType::Anti
                ) {
                    join_streamed = !self.streamed_joined;
                }
            }
            Ordering::Equal => {
                if matches!(self.join_type, JoinType::Semi) {
                    join_streamed = !self.streamed_joined;
                }
                if matches!(
                    self.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) {
                    join_streamed = true;
                    join_buffered = true;
                };
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
                    self.streamed_batch.append_output_pair(
                        Some(self.buffered_data.scanning_batch_idx),
                        Some(scanning_idx),
                    );
                } else {
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

    fn freeze_all(&mut self) -> ArrowResult<()> {
        self.freeze_streamed()?;
        self.freeze_buffered(self.buffered_data.batches.len())?;
        Ok(())
    }

    // Produces and stages record batches to ensure dequeued buffered batch
    // no longer needed:
    //   1. freezes all indices joined to streamed side
    //   2. freezes NULLs joined to dequeued buffered batch to "release" it
    fn freeze_dequeuing_buffered(&mut self) -> ArrowResult<()> {
        self.freeze_streamed()?;
        self.freeze_buffered(1)?;
        Ok(())
    }

    // Produces and stages record batch from buffered indices with corresponding
    // NULLs on streamed side.
    //
    // Applicable only in case of Full join.
    fn freeze_buffered(&mut self, batch_count: usize) -> ArrowResult<()> {
        if !matches!(self.join_type, JoinType::Full) {
            return Ok(());
        }
        for buffered_batch in self.buffered_data.batches.range_mut(..batch_count) {
            let buffered_indices = UInt64Array::from_iter_values(
                buffered_batch.null_joined.iter().map(|&index| index as u64),
            );
            if buffered_indices.is_empty() {
                continue;
            }
            buffered_batch.null_joined.clear();

            let buffered_columns = buffered_batch
                .batch
                .columns()
                .iter()
                .map(|column| take(column, &buffered_indices, None))
                .collect::<ArrowResult<Vec<_>>>()?;

            let mut streamed_columns = self
                .streamed_schema
                .fields()
                .iter()
                .map(|f| new_null_array(f.data_type(), buffered_indices.len()))
                .collect::<Vec<_>>();

            streamed_columns.extend(buffered_columns);
            let columns = streamed_columns;

            self.output_record_batches
                .push(RecordBatch::try_new(self.schema.clone(), columns)?);
        }
        Ok(())
    }

    // Produces and stages record batch for all output indices found
    // for current streamed batch and clears staged output indices.
    fn freeze_streamed(&mut self) -> ArrowResult<()> {
        for chunk in self.streamed_batch.output_indices.iter_mut() {
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
                .collect::<ArrowResult<Vec<_>>>()?;

            let buffered_indices: UInt64Array = chunk.buffered_indices.finish();

            let mut buffered_columns =
                if matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
                    vec![]
                } else if let Some(buffered_idx) = chunk.buffered_batch_idx {
                    self.buffered_data.batches[buffered_idx]
                        .batch
                        .columns()
                        .iter()
                        .map(|column| take(column, &buffered_indices, None))
                        .collect::<ArrowResult<Vec<_>>>()?
                } else {
                    self.buffered_schema
                        .fields()
                        .iter()
                        .map(|f| new_null_array(f.data_type(), buffered_indices.len()))
                        .collect::<Vec<_>>()
                };

            let columns = if matches!(self.join_type, JoinType::Right) {
                buffered_columns.extend(streamed_columns);
                buffered_columns
            } else {
                streamed_columns.extend(buffered_columns);
                streamed_columns
            };

            self.output_record_batches
                .push(RecordBatch::try_new(self.schema.clone(), columns)?);
        }

        self.streamed_batch.output_indices.clear();

        Ok(())
    }

    fn output_record_batch_and_reset(&mut self) -> ArrowResult<RecordBatch> {
        let record_batch =
            combine_batches(&self.output_record_batches, self.schema.clone())?.unwrap();
        self.join_metrics.output_batches.add(1);
        self.join_metrics.output_rows.add(record_batch.num_rows());
        self.output_size -= record_batch.num_rows();
        self.output_record_batches.clear();
        Ok(record_batch)
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
fn join_arrays(batch: &RecordBatch, on_column: &[Column]) -> Vec<ArrayRef> {
    on_column
        .iter()
        .map(|c| batch.column(c.index()).clone())
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
) -> ArrowResult<Ordering> {
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
            _ => {
                return Err(ArrowError::NotYetImplemented(
                    "Unsupported data type in sort merge join comparator".to_owned(),
                ));
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
) -> ArrowResult<bool> {
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
            _ => {
                return Err(ArrowError::NotYetImplemented(
                    "Unsupported data type in sort merge join comparator".to_owned(),
                ));
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

    use crate::error::Result;
    use crate::logical_plan::JoinType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::join_utils::JoinOn;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::sort_merge_join::SortMergeJoinExec;
    use crate::physical_plan::{common, ExecutionPlan};
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::{build_table_i32, columns};
    use crate::{assert_batches_eq, assert_batches_sorted_eq};

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
            schema.clone(),
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
        SortMergeJoinExec::try_new(left, right, on, join_type, sort_options, false)
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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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
        let session_ctx =
            SessionContext::with_config(SessionConfig::new().with_batch_size(2));
        let task_ctx = session_ctx.task_ctx();
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = vec![
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
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
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
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
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
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
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
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
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
                    nulls_first: false
                };
                2
            ],
            true,
        )
        .await?;
        let expected = vec![
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
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, JoinType::Inner).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Full).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Anti).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Semi).await?;
        let expected = vec![
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
            Column::new_with_schema("a", &left.schema())?,
            Column::new_with_schema("b", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = vec![
            "+------------+------------+------------+------------+------------+------------+",
            "| a1         | b1         | c1         | a2         | b1         | c2         |",
            "+------------+------------+------------+------------+------------+------------+",
            "| 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |",
            "| 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "| 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |",
            "+------------+------------+------------+------------+------------+------------+",
        ];
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = vec![
            "+------------+------------+------------+------------+------------+------------+",
            "| a1         | b1         | c1         | a2         | b1         | c2         |",
            "+------------+------------+------------+------------+------------+------------+",
            "| 1970-01-01 | 2022-04-23 | 1970-01-01 | 1970-01-01 | 2022-04-23 | 1970-01-01 |",
            "| 1970-01-01 | 2022-04-25 | 1970-01-01 | 1970-01-01 | 2022-04-25 | 1970-01-01 |",
            "| 1970-01-01 | 2022-04-25 | 1970-01-01 | 1970-01-01 | 2022-04-25 | 1970-01-01 |",
            "+------------+------------+------------+------------+------------+------------+",
        ];
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
        let expected = vec![
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
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
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
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
}
