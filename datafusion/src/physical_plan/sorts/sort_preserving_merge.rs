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

//! Defines the sort preserving merge plan

use crate::physical_plan::common::AbortOnDropMany;
use crate::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MemTrackingMetrics, MetricsSet,
};
use parking_lot::Mutex;
use std::any::Any;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::record_batch::RecordBatch;
use arrow::array::growable::make_growable;
use arrow::{
    compute::sort::SortOptions,
    datatypes::SchemaRef,
    error::{Error as ArrowError, Result as ArrowResult},
};
use async_trait::async_trait;
use datafusion_common::field_util::SchemaExt;
use futures::channel::mpsc;
use futures::stream::FusedStream;
use futures::{Stream, StreamExt};

use crate::error::{DataFusionError, Result};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::sorts::{RowIndex, SortKeyCursor, SortedStream, StreamWrapper};
use crate::physical_plan::{
    common::spawn_execution, expressions::PhysicalSortExpr, DisplayFormatType,
    Distribution, ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

/// Sort preserving merge execution plan
///
/// This takes an input execution plan and a list of sort expressions, and
/// provided each partition of the input plan is sorted with respect to
/// these sort expressions, this operator will yield a single partition
/// that is also sorted with respect to them
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ A │ B │ C │ D │ ...   │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────────────────┐
///   Stream 1                   │  │                   │    │ ┌───┬───╦═══╦───┬───╦═══╗     │
///                              ├─▶│SortPreservingMerge│───▶│ │ A │ B ║ B ║ C │ D ║ E ║ ... │
///                              │  │                   │    │ └───┴─▲─╩═══╩───┴───╩═══╝     │
/// ┌─────────────────────────┐  │  └───────────────────┘    └─┬─────┴───────────────────────┘
/// │ ╔═══╦═══╗               │  │
/// │ ║ B ║ E ║     ...       │──┘                             │
/// │ ╚═══╩═══╝               │              Note Stable Sort: the merged stream
/// └─────────────────────────┘                places equal rows from stream 1
///   Stream 2
///
///
///  Input Streams                                             Output stream
///    (sorted)                                                  (sorted)
/// ```
#[derive(Debug)]
pub struct SortPreservingMergeExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl SortPreservingMergeExec {
    /// Create a new sort execution plan
    pub fn new(expr: Vec<PhysicalSortExpr>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            expr,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] {
        &self.expr
    }
}

#[async_trait]
impl ExecutionPlan for SortPreservingMergeExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn relies_on_input_order(&self) -> bool {
        true
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.expr)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(SortPreservingMergeExec::new(
                self.expr.clone(),
                children[0].clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "SortPreservingMergeExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "SortPreservingMergeExec invalid partition {}",
                partition
            )));
        }

        let tracking_metrics = MemTrackingMetrics::new(&self.metrics, partition);

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => Err(DataFusionError::Internal(
                "SortPreservingMergeExec requires at least one input partition"
                    .to_owned(),
            )),
            1 => {
                // bypass if there is only one partition to merge (no metrics in this case either)
                self.input.execute(0, runtime).await
            }
            _ => {
                let (receivers, join_handles) = (0..input_partitions)
                    .into_iter()
                    .map(|part_i| {
                        let (sender, receiver) = mpsc::channel(1);
                        let join_handle = spawn_execution(
                            self.input.clone(),
                            sender,
                            part_i,
                            runtime.clone(),
                        );
                        (receiver, join_handle)
                    })
                    .unzip();

                Ok(Box::pin(SortPreservingMergeStream::new_from_receivers(
                    receivers,
                    AbortOnDropMany(join_handles),
                    self.schema(),
                    &self.expr,
                    tracking_metrics,
                    runtime,
                )))
            }
        }
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                write!(f, "SortPreservingMergeExec: [{}]", expr.join(","))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

#[derive(Debug)]
struct MergingStreams {
    /// The sorted input streams to merge together
    streams: Mutex<Vec<StreamWrapper>>,
    /// number of streams
    num_streams: usize,
}

impl MergingStreams {
    fn new(input_streams: Vec<StreamWrapper>) -> Self {
        Self {
            num_streams: input_streams.len(),
            streams: Mutex::new(input_streams),
        }
    }

    fn num_streams(&self) -> usize {
        self.num_streams
    }
}

#[derive(Debug)]
pub(crate) struct SortPreservingMergeStream {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    streams: MergingStreams,

    /// Drop helper for tasks feeding the input [`streams`](Self::streams)
    _drop_helper: AbortOnDropMany<()>,

    /// For each input stream maintain a dequeue of RecordBatches
    ///
    /// Exhausted batches will be popped off the front once all
    /// their rows have been yielded to the output
    batches: Vec<VecDeque<RecordBatch>>,

    /// Maintain a flag for each stream denoting if the current cursor
    /// has finished and needs to poll from the stream
    cursor_finished: Vec<bool>,

    /// The accumulated row indexes for the next record batch
    in_progress: Vec<RowIndex>,

    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,

    /// The sort options for each expression
    sort_options: Arc<Vec<SortOptions>>,

    /// used to record execution metrics
    tracking_metrics: MemTrackingMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// An id to uniquely identify the input stream batch
    next_batch_id: usize,

    /// min heap for record comparison
    min_heap: BinaryHeap<SortKeyCursor>,

    /// target batch size
    batch_size: usize,
}

impl SortPreservingMergeStream {
    pub(crate) fn new_from_receivers(
        receivers: Vec<mpsc::Receiver<ArrowResult<RecordBatch>>>,
        _drop_helper: AbortOnDropMany<()>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        tracking_metrics: MemTrackingMetrics,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let stream_count = receivers.len();
        let batches = (0..stream_count)
            .into_iter()
            .map(|_| VecDeque::new())
            .collect();
        let wrappers = receivers.into_iter().map(StreamWrapper::Receiver).collect();

        SortPreservingMergeStream {
            schema,
            batches,
            cursor_finished: vec![true; stream_count],
            streams: MergingStreams::new(wrappers),
            _drop_helper,
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            sort_options: Arc::new(expressions.iter().map(|x| x.options).collect()),
            tracking_metrics,
            aborted: false,
            in_progress: vec![],
            next_batch_id: 0,
            min_heap: BinaryHeap::with_capacity(stream_count),
            batch_size: runtime.batch_size(),
        }
    }

    pub(crate) fn new_from_streams(
        streams: Vec<SortedStream>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        tracking_metrics: MemTrackingMetrics,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let stream_count = streams.len();
        let batches = (0..stream_count)
            .into_iter()
            .map(|_| VecDeque::new())
            .collect();
        tracking_metrics.init_mem_used(streams.iter().map(|s| s.mem_used).sum());
        let wrappers = streams
            .into_iter()
            .map(|s| StreamWrapper::Stream(Some(s)))
            .collect();

        Self {
            schema,
            batches,
            cursor_finished: vec![true; stream_count],
            streams: MergingStreams::new(wrappers),
            _drop_helper: AbortOnDropMany(vec![]),
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            sort_options: Arc::new(expressions.iter().map(|x| x.options).collect()),
            tracking_metrics,
            aborted: false,
            in_progress: vec![],
            next_batch_id: 0,
            min_heap: BinaryHeap::with_capacity(stream_count),
            batch_size: runtime.batch_size(),
        }
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<ArrowResult<()>> {
        if !self.cursor_finished[idx] {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }
        let mut empty_batch = false;
        {
            let mut streams = self.streams.streams.lock();

            let stream = &mut streams[idx];
            if stream.is_terminated() {
                return Poll::Ready(Ok(()));
            }

            // Fetch a new input record and create a cursor from it
            match futures::ready!(stream.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(())),
                Some(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let cursor = match SortKeyCursor::new(
                            idx,
                            self.next_batch_id, // assign this batch an ID
                            &batch,
                            &self.column_expressions,
                            self.sort_options.clone(),
                        ) {
                            Ok(cursor) => cursor,
                            Err(e) => {
                                return Poll::Ready(Err(ArrowError::External(
                                    "datafusion".to_string(),
                                    Box::new(e),
                                )));
                            }
                        };
                        self.next_batch_id += 1;
                        self.min_heap.push(cursor);
                        self.cursor_finished[idx] = false;
                        self.batches[idx].push_back(batch)
                    } else {
                        empty_batch = true;
                    }
                }
            }
        }

        if empty_batch {
            self.maybe_poll_stream(cx, idx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        // Mapping from stream index to the index of the first buffer from that stream
        let mut buffer_idx = 0;
        let mut stream_to_buffer_idx = Vec::with_capacity(self.batches.len());

        for batches in &self.batches {
            stream_to_buffer_idx.push(buffer_idx);
            buffer_idx += batches.len();
        }

        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, _)| {
                let arrays = self
                    .batches
                    .iter()
                    .flat_map(|batch| {
                        batch.iter().map(|batch| batch.column(column_idx).as_ref())
                    })
                    .collect::<Vec<_>>();

                let mut array_data =
                    make_growable(&arrays, false, self.in_progress.len());

                if self.in_progress.is_empty() {
                    return array_data.as_arc();
                }

                let first = &self.in_progress[0];
                let mut buffer_idx =
                    stream_to_buffer_idx[first.stream_idx] + first.batch_idx;
                let mut start_row_idx = first.row_idx;
                let mut end_row_idx = start_row_idx + 1;

                for row_index in self.in_progress.iter().skip(1) {
                    let next_buffer_idx =
                        stream_to_buffer_idx[row_index.stream_idx] + row_index.batch_idx;

                    if next_buffer_idx == buffer_idx && row_index.row_idx == end_row_idx {
                        // subsequent row in same batch
                        end_row_idx += 1;
                        continue;
                    }

                    // emit current batch of rows for current buffer
                    array_data.extend(
                        buffer_idx,
                        start_row_idx,
                        end_row_idx - start_row_idx,
                    );

                    // start new batch of rows
                    buffer_idx = next_buffer_idx;
                    start_row_idx = row_index.row_idx;
                    end_row_idx = start_row_idx + 1;
                }

                // emit final batch of rows
                array_data.extend(buffer_idx, start_row_idx, end_row_idx - start_row_idx);
                array_data.as_arc()
            })
            .collect();

        self.in_progress.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch
        //
        // Additionally as `in_progress` has been drained, there are no longer
        // any RowIndex's reliant on the batch indexes
        //
        // We can therefore drop all but the last batch for each stream
        for batches in &mut self.batches {
            if batches.len() > 1 {
                // Drain all but the last batch
                batches.drain(0..(batches.len() - 1));
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

impl Stream for SortPreservingMergeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.tracking_metrics.record_poll(poll)
    }
}

impl SortPreservingMergeStream {
    #[inline]
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        // Ensure all non-exhausted streams have a cursor from which
        // rows can be pulled
        for i in 0..self.streams.num_streams() {
            match futures::ready!(self.maybe_poll_stream(cx, i)) {
                Ok(_) => {}
                Err(e) => {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        loop {
            // NB timer records time taken on drop, so there are no
            // calls to `timer.done()` below.
            let elapsed_compute = self.tracking_metrics.elapsed_compute().clone();
            let _timer = elapsed_compute.timer();

            match self.min_heap.pop() {
                Some(mut cursor) => {
                    let stream_idx = cursor.stream_idx;
                    let batch_idx = self.batches[stream_idx].len() - 1;
                    let row_idx = cursor.advance();

                    let mut cursor_finished = false;
                    // insert the cursor back to min_heap if the record batch is not exhausted
                    if !cursor.is_finished() {
                        self.min_heap.push(cursor);
                    } else {
                        cursor_finished = true;
                        self.cursor_finished[stream_idx] = true;
                    }

                    self.in_progress.push(RowIndex {
                        stream_idx,
                        batch_idx,
                        row_idx,
                    });

                    if self.in_progress.len() == self.batch_size {
                        return Poll::Ready(Some(self.build_record_batch()));
                    }

                    // If removed the last row from the cursor, need to fetch a new record
                    // batch if possible, before looping round again
                    if cursor_finished {
                        match futures::ready!(self.maybe_poll_stream(cx, stream_idx)) {
                            Ok(_) => {}
                            Err(e) => {
                                self.aborted = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }
                None if self.in_progress.is_empty() => return Poll::Ready(None),
                None => return Poll::Ready(Some(self.build_record_batch())),
            }
        }
    }
}

impl RecordBatchStream for SortPreservingMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::object_store::local::LocalFileSystem;

    use crate::physical_plan::metrics::MetricValue;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use arrow::array::ArrayRef;
    use std::iter::FromIterator;

    use crate::arrow::array::*;
    use crate::arrow::datatypes::*;
    use crate::arrow_print;
    use crate::assert_batches_eq;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::{CsvExec, FileScanConfig};
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::{collect, common};
    use crate::test::{self, assert_is_pending};
    use crate::test_util;

    use super::*;
    use crate::execution::runtime_env::RuntimeConfig;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{FutureExt, SinkExt};
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_merge_interleave() {
        let runtime = Arc::new(RuntimeEnv::default());
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from(&[
            Some("a"),
            Some("c"),
            Some("e"),
            Some("g"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[8, 7, 6, 5, 8])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("b"),
            Some("d"),
            Some("f"),
            Some("h"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[4, 6, 2, 2, 6])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );

        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01 00:00:00.000000008 |",
                "| 10 | b | 1970-01-01 00:00:00.000000004 |",
                "| 2  | c | 1970-01-01 00:00:00.000000007 |",
                "| 20 | d | 1970-01-01 00:00:00.000000006 |",
                "| 7  | e | 1970-01-01 00:00:00.000000006 |",
                "| 70 | f | 1970-01-01 00:00:00.000000002 |",
                "| 9  | g | 1970-01-01 00:00:00.000000005 |",
                "| 90 | h | 1970-01-01 00:00:00.000000002 |",
                "| 30 | j | 1970-01-01 00:00:00.000000006 |", // input b2 before b1
                "| 3  | j | 1970-01-01 00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            runtime,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_some_overlap() {
        let runtime = Arc::new(RuntimeEnv::default());
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[8, 7, 6, 5, 8])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[70, 90, 30, 100, 110]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from(&[
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[4, 6, 2, 2, 6])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01 00:00:00.000000008 |",
                "| 2   | b | 1970-01-01 00:00:00.000000007 |",
                "| 70  | c | 1970-01-01 00:00:00.000000004 |",
                "| 7   | c | 1970-01-01 00:00:00.000000006 |",
                "| 9   | d | 1970-01-01 00:00:00.000000005 |",
                "| 90  | d | 1970-01-01 00:00:00.000000006 |",
                "| 30  | e | 1970-01-01 00:00:00.000000002 |",
                "| 3   | e | 1970-01-01 00:00:00.000000008 |",
                "| 100 | f | 1970-01-01 00:00:00.000000002 |",
                "| 110 | g | 1970-01-01 00:00:00.000000006 |",
                "+-----+---+-------------------------------+",
            ],
            runtime,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_no_overlap() {
        let runtime = Arc::new(RuntimeEnv::default());
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from(&[
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[8, 7, 6, 5, 8])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[4, 6, 2, 2, 6])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01 00:00:00.000000008 |",
                "| 2  | b | 1970-01-01 00:00:00.000000007 |",
                "| 7  | c | 1970-01-01 00:00:00.000000006 |",
                "| 9  | d | 1970-01-01 00:00:00.000000005 |",
                "| 3  | e | 1970-01-01 00:00:00.000000008 |",
                "| 10 | f | 1970-01-01 00:00:00.000000004 |",
                "| 20 | g | 1970-01-01 00:00:00.000000006 |",
                "| 70 | h | 1970-01-01 00:00:00.000000002 |",
                "| 90 | i | 1970-01-01 00:00:00.000000002 |",
                "| 30 | j | 1970-01-01 00:00:00.000000006 |",
                "+----+---+-------------------------------+",
            ],
            runtime,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_three_partitions() {
        let runtime = Arc::new(RuntimeEnv::default());
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from(&[
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[8, 7, 6, 5, 8])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[40, 60, 20, 20, 60])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[100, 200, 700, 900, 300]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from_slice(&[4, 6, 2, 2, 6])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2], vec![b3]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01 00:00:00.000000008 |",
                "| 2   | b | 1970-01-01 00:00:00.000000007 |",
                "| 7   | c | 1970-01-01 00:00:00.000000006 |",
                "| 9   | d | 1970-01-01 00:00:00.000000005 |",
                "| 10  | e | 1970-01-01 00:00:00.000000040 |",
                "| 100 | f | 1970-01-01 00:00:00.000000004 |",
                "| 3   | f | 1970-01-01 00:00:00.000000008 |",
                "| 200 | g | 1970-01-01 00:00:00.000000006 |",
                "| 20  | g | 1970-01-01 00:00:00.000000060 |",
                "| 700 | h | 1970-01-01 00:00:00.000000002 |",
                "| 70  | h | 1970-01-01 00:00:00.000000020 |",
                "| 900 | i | 1970-01-01 00:00:00.000000002 |",
                "| 90  | i | 1970-01-01 00:00:00.000000020 |",
                "| 300 | j | 1970-01-01 00:00:00.000000006 |",
                "| 30  | j | 1970-01-01 00:00:00.000000060 |",
                "+-----+---+-------------------------------+",
            ],
            runtime,
        )
        .await;
    }

    async fn _test_merge(
        partitions: &[Vec<RecordBatch>],
        exp: &[&str],
        runtime: Arc<RuntimeEnv>,
    ) {
        let schema = partitions[0][0].schema();
        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", schema).unwrap(),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c", schema).unwrap(),
                options: Default::default(),
            },
        ];
        let exec = MemoryExec::try_new(partitions, schema.clone(), None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, runtime).await.unwrap();
        assert_batches_eq!(exp, collected.as_slice());
    }

    async fn sorted_merge(
        input: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
        runtime: Arc<RuntimeEnv>,
    ) -> RecordBatch {
        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let mut result = collect(merge, runtime).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    async fn partition_sort(
        input: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
        runtime: Arc<RuntimeEnv>,
    ) -> RecordBatch {
        let sort_exec =
            Arc::new(SortExec::new_with_partitioning(sort.clone(), input, true));
        sorted_merge(sort_exec, sort, runtime).await
    }

    async fn basic_sort(
        src: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
        runtime: Arc<RuntimeEnv>,
    ) -> RecordBatch {
        let merge = Arc::new(CoalescePartitionsExec::new(src));
        let sort_exec = Arc::new(SortExec::try_new(sort, merge).unwrap());
        let mut result = collect(sort_exec, runtime).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    #[tokio::test]
    async fn test_partition_sort() {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions).unwrap();

        let csv = Arc::new(CsvExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: Arc::clone(&schema),
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        ));

        let sort = vec![
            PhysicalSortExpr {
                expr: col("c1", &schema).unwrap(),
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: col("c2", &schema).unwrap(),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c12", &schema).unwrap(),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("c12", &schema).unwrap(),
                options: SortOptions::default(),
            },
        ];

        let basic = basic_sort(csv.clone(), sort.clone(), runtime.clone()).await;
        let partition = partition_sort(csv, sort, runtime.clone()).await;

        let basic = arrow_print::write(&[basic]);
        let partition = arrow_print::write(&[partition]);

        assert_eq!(
            basic, partition,
            "basic:\n\n{}\n\npartition:\n\n{}\n\n",
            basic, partition
        );
    }

    // Split the provided record batch into multiple batch_size record batches
    fn split_batch(sorted: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
        let batches = (sorted.num_rows() + batch_size - 1) / batch_size;

        // Split the sorted RecordBatch into multiple
        (0..batches)
            .into_iter()
            .map(|batch_idx| {
                let columns = (0..sorted.num_columns())
                    .map(|column_idx| {
                        let length =
                            batch_size.min(sorted.num_rows() - batch_idx * batch_size);

                        sorted
                            .column(column_idx)
                            .slice(batch_idx * batch_size, length)
                            .into()
                    })
                    .collect();

                RecordBatch::try_new(sorted.schema().clone(), columns).unwrap()
            })
            .collect()
    }

    async fn sorted_partitioned_input(
        sort: Vec<PhysicalSortExpr>,
        sizes: &[usize],
        runtime: Arc<RuntimeEnv>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions).unwrap();

        let csv = Arc::new(CsvExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: schema,
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        ));

        let sorted = basic_sort(csv, sort, runtime).await;
        let split: Vec<_> = sizes.iter().map(|x| split_batch(&sorted, *x)).collect();

        Arc::new(MemoryExec::try_new(&split, sorted.schema().clone(), None).unwrap())
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input() {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema = test_util::aggr_test_schema();
        let sort = vec![
            // uint8
            PhysicalSortExpr {
                expr: col("c7", &schema).unwrap(),
                options: Default::default(),
            },
            // int16
            PhysicalSortExpr {
                expr: col("c4", &schema).unwrap(),
                options: Default::default(),
            },
            // utf-8
            PhysicalSortExpr {
                expr: col("c1", &schema).unwrap(),
                options: SortOptions::default(),
            },
            // utf-8
            PhysicalSortExpr {
                expr: col("c13", &schema).unwrap(),
                options: SortOptions::default(),
            },
        ];

        let input =
            sorted_partitioned_input(sort.clone(), &[10, 3, 11], runtime.clone()).await;
        let basic = basic_sort(input.clone(), sort.clone(), runtime.clone()).await;
        let partition = sorted_merge(input, sort, runtime.clone()).await;

        assert_eq!(basic.num_rows(), 300);
        assert_eq!(partition.num_rows(), 300);

        let basic = arrow_print::write(&[basic]);
        let partition = arrow_print::write(&[partition]);

        assert_eq!(basic, partition);
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input_output() {
        let schema = test_util::aggr_test_schema();

        let sort = vec![
            // float64
            PhysicalSortExpr {
                expr: col("c12", &schema).unwrap(),
                options: Default::default(),
            },
            // utf-8
            PhysicalSortExpr {
                expr: col("c13", &schema).unwrap(),
                options: Default::default(),
            },
        ];

        let runtime = Arc::new(RuntimeEnv::default());
        let input =
            sorted_partitioned_input(sort.clone(), &[10, 5, 13], runtime.clone()).await;
        let basic = basic_sort(input.clone(), sort.clone(), runtime).await;

        let runtime_bs_23 =
            Arc::new(RuntimeEnv::new(RuntimeConfig::new().with_batch_size(23)).unwrap());
        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let merged = collect(merge, runtime_bs_23).await.unwrap();

        assert_eq!(merged.len(), 14);

        assert_eq!(basic.num_rows(), 300);
        assert_eq!(merged.iter().map(|x| x.num_rows()).sum::<usize>(), 300);

        let basic = arrow_print::write(&[basic]);
        let partition = arrow_print::write(merged.as_slice());

        assert_eq!(basic, partition);
    }

    #[tokio::test]
    async fn test_nulls() {
        let runtime = Arc::new(RuntimeEnv::default());
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from(&[
            None,
            Some("a"),
            Some("b"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from(&[Some(8), None, Some(6), None, Some(4)])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(Utf8Array::<i32>::from(&[
            None,
            Some("b"),
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(
            Int64Array::from(&[Some(8), None, Some(5), None, Some(4)])
                .to(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
        let schema = b1.schema().clone();

        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: col("c", &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ];
        let exec = MemoryExec::try_new(&[vec![b1], vec![b2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, runtime).await.unwrap();
        assert_eq!(collected.len(), 1);

        assert_batches_eq!(
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 |   | 1970-01-01 00:00:00.000000008 |",
                "| 1 |   | 1970-01-01 00:00:00.000000008 |",
                "| 2 | a |                               |",
                "| 7 | b | 1970-01-01 00:00:00.000000006 |",
                "| 2 | b |                               |",
                "| 9 | d |                               |",
                "| 3 | e | 1970-01-01 00:00:00.000000004 |",
                "| 3 | g | 1970-01-01 00:00:00.000000005 |",
                "| 4 | h |                               |",
                "| 5 | i | 1970-01-01 00:00:00.000000004 |",
                "+---+---+-------------------------------+",
            ],
            collected.as_slice()
        );
    }

    #[tokio::test]
    async fn test_async() {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema = test_util::aggr_test_schema();
        let sort = vec![PhysicalSortExpr {
            expr: col("c12", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let batches =
            sorted_partitioned_input(sort.clone(), &[5, 7, 3], runtime.clone()).await;

        let partition_count = batches.output_partitioning().partition_count();
        let mut join_handles = Vec::with_capacity(partition_count);
        let mut receivers = Vec::with_capacity(partition_count);

        for partition in 0..partition_count {
            let (mut sender, receiver) = mpsc::channel(1);
            let mut stream = batches.execute(partition, runtime.clone()).await.unwrap();
            let join_handle = tokio::spawn(async move {
                while let Some(batch) = stream.next().await {
                    sender.send(batch).await.unwrap();
                    // This causes the MergeStream to wait for more input
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            });
            join_handles.push(join_handle);
            receivers.push(receiver);
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let tracking_metrics = MemTrackingMetrics::new(&metrics, 0);

        let merge_stream = SortPreservingMergeStream::new_from_receivers(
            receivers,
            // Use empty vector since we want to use the join handles ourselves
            AbortOnDropMany(vec![]),
            batches.schema(),
            sort.as_slice(),
            tracking_metrics,
            runtime.clone(),
        );

        let mut merged = common::collect(Box::pin(merge_stream)).await.unwrap();

        // Propagate any errors
        for join_handle in join_handles {
            join_handle.await.unwrap();
        }

        assert_eq!(merged.len(), 1);
        let merged = merged.remove(0);
        let basic = basic_sort(batches, sort.clone(), runtime.clone()).await;

        let basic = arrow_print::write(&[basic]);
        let partition = arrow_print::write(&[merged]);

        assert_eq!(
            basic, partition,
            "basic:\n\n{}\n\npartition:\n\n{}\n\n",
            basic, partition
        );
    }

    #[tokio::test]
    async fn test_merge_metrics() {
        let runtime = Arc::new(RuntimeEnv::default());
        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[1, 2]));
        let b: ArrayRef =
            Arc::new(Utf8Array::<i32>::from_iter(vec![Some("a"), Some("c")]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from_slice(&[10, 20]));
        let b: ArrayRef =
            Arc::new(Utf8Array::<i32>::from_iter(vec![Some("b"), Some("d")]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let schema = b1.schema().clone();
        let sort = vec![PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: Default::default(),
        }];
        let exec =
            MemoryExec::try_new(&[vec![b1], vec![b2]], schema.clone(), None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge.clone(), runtime).await.unwrap();
        let expected = vec![
            "+----+---+",
            "| a  | b |",
            "+----+---+",
            "| 1  | a |",
            "| 10 | b |",
            "| 2  | c |",
            "| 20 | d |",
            "+----+---+",
        ];
        assert_batches_eq!(expected, collected.as_slice());

        // Now, validate metrics
        let metrics = merge.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let mut saw_start = false;
        let mut saw_end = false;
        metrics.iter().for_each(|m| match m.value() {
            MetricValue::StartTimestamp(ts) => {
                saw_start = true;
                assert!(ts.value().unwrap().timestamp_nanos() > 0);
            }
            MetricValue::EndTimestamp(ts) => {
                saw_end = true;
                assert!(ts.value().unwrap().timestamp_nanos() > 0);
            }
            _ => {}
        });

        assert!(saw_start);
        assert!(saw_end);
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let sort_preserving_merge_exec = Arc::new(SortPreservingMergeExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
        ));

        let fut = collect(sort_preserving_merge_exec, runtime);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_stable_sort() {
        let runtime = Arc::new(RuntimeEnv::default());

        // Create record batches like:
        // batch_number |value
        // -------------+------
        //    1         | A
        //    1         | B
        //
        // Ensure that the output is in the same order the batches were fed
        let partitions: Vec<Vec<RecordBatch>> = (0..10)
            .map(|batch_number| {
                let batch_number: Int32Array =
                    vec![Some(batch_number), Some(batch_number)]
                        .into_iter()
                        .collect();
                let value: Utf8Array<i32> =
                    vec![Some("A"), Some("B")].into_iter().collect();

                let batch = RecordBatch::try_from_iter(vec![
                    ("batch_number", Arc::new(batch_number) as ArrayRef),
                    ("value", Arc::new(value) as ArrayRef),
                ])
                .unwrap();

                vec![batch]
            })
            .collect();

        let schema = partitions[0][0].schema();

        let sort = vec![PhysicalSortExpr {
            expr: col("value", schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];

        let exec = MemoryExec::try_new(&partitions, schema.clone(), None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, runtime).await.unwrap();
        assert_eq!(collected.len(), 1);

        // Expect the data to be sorted first by "batch_number" (because
        // that was the order it was fed in, even though only "value"
        // is in the sort key)
        assert_batches_eq!(
            &[
                "+--------------+-------+",
                "| batch_number | value |",
                "+--------------+-------+",
                "| 0            | A     |",
                "| 1            | A     |",
                "| 2            | A     |",
                "| 3            | A     |",
                "| 4            | A     |",
                "| 5            | A     |",
                "| 6            | A     |",
                "| 7            | A     |",
                "| 8            | A     |",
                "| 9            | A     |",
                "| 0            | B     |",
                "| 1            | B     |",
                "| 2            | B     |",
                "| 3            | B     |",
                "| 4            | B     |",
                "| 5            | B     |",
                "| 6            | B     |",
                "| 7            | B     |",
                "| 8            | B     |",
                "| 9            | B     |",
                "+--------------+-------+",
            ],
            collected.as_slice()
        );
    }
}
