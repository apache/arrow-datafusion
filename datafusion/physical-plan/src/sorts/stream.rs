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

use crate::sorts::builder::SortOrder;
use crate::sorts::cursor::{Cursor, FieldArray, FieldCursor, RowCursor};
use crate::SendableRecordBatchStream;
use crate::{PhysicalExpr, PhysicalSortExpr};
use ahash::RandomState;
use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::stream::{Fuse, StreamExt};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use super::batch_cursor::{BatchCursor, BatchId, SlicedBatchCursorIdentifier};
use super::builder::YieldedSortOrder;

/// A fallible [`PartitionedStream`] of record batches.
///
/// Each [`Cursor`] and [`RecordBatch`] represents a single record batch.
pub(crate) trait BatchCursorStream<C: Cursor>:
    PartitionedStream + Send + 'static
{
    /// Acquire ownership over a subset of the partitioned streams.
    ///
    /// Like `Vec::take()`, this removes the indexed positions.
    fn take_partitions(&mut self, range: Range<usize>) -> Box<Self>
    where
        Self: Sized;
}

/// A [`PartitionedStream`] representing partial record batches (a.k.a. [`BatchCursor`]).
///
/// Each merge node (a `SortPreservingMergeStream` loser tree) will consume a [`CursorStream`].
pub(crate) type CursorStream<C> =
    Box<dyn PartitionedStream<Output = Result<BatchCursor<C>>> + Send>;

/// A fallible stream of yielded [`SortOrder`]s is a [`MergeStream`].
///
/// Within a cascade of merge nodes, (each node being a `SortPreservingMergeStream` loser tree),
/// the merge node will yield a SortOrder as well as any partial record batches from the SortOrder.
///
/// [`YieldedCursorStream`] then converts an output [`MergeStream`]
/// into an input [`CursorStream`] for the next merge.
pub(crate) type MergeStream<C> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<YieldedSortOrder<C>>> + Send>>;

/// A [`Stream`](futures::Stream) that has multiple partitions that can
/// be polled separately but not concurrently
///
/// Used by sort preserving merge to decouple the cursor merging logic from
/// the source of the cursors, the intention being to allow preserving
/// any row encoding performed for intermediate sorts
pub trait PartitionedStream: std::fmt::Debug + Send {
    type Output;

    /// Returns the number of partitions
    fn partitions(&self) -> usize;

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>>;
}

/// A newtype wrapper around a set of fused [`SendableRecordBatchStream`]
/// that implements debug, and skips over empty [`RecordBatch`]
struct FusedStreams(Vec<Fuse<SendableRecordBatchStream>>);

impl std::fmt::Debug for FusedStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedStreams")
            .field("num_streams", &self.0.len())
            .finish()
    }
}

impl FusedStreams {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok(b)) if b.num_rows() == 0 => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}

/// A [`PartitionedStream`] that wraps a set of [`SendableRecordBatchStream`]
/// and computes [`RowCursor`] based on the provided [`PhysicalSortExpr`]
#[derive(Debug)]
pub struct RowCursorStream {
    /// Converter to convert output of physical expressions
    converter: Arc<RowConverter>,
    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// Input streams
    streams: FusedStreams,
    /// Tracks the memory used by `converter`
    reservation: MemoryReservation,
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expressions: &[PhysicalSortExpr],
        streams: Vec<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;

        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        let converter = Arc::new(RowConverter::new(sort_fields)?);
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            streams: FusedStreams(streams),
        })
    }

    fn new_from_streams(&self, streams: FusedStreams) -> Self {
        Self {
            converter: self.converter.clone(),
            column_expressions: self.column_expressions.clone(),
            reservation: self.reservation.new_empty(),
            streams,
        }
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<RowCursor> {
        let cols = self
            .column_expressions
            .iter()
            .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        let rows = self.converter.convert_columns(&cols)?;
        self.reservation.try_resize(self.converter.size())?;

        // track the memory in the newly created Rows.
        let mut rows_reservation = self.reservation.new_empty();
        rows_reservation.try_grow(rows.size())?;
        Ok(RowCursor::new(rows, rows_reservation))
    }
}

impl PartitionedStream for RowCursorStream {
    type Output = Result<(RowCursor, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}

impl<C: Cursor> BatchCursorStream<C> for RowCursorStream {
    fn take_partitions(&mut self, range: Range<usize>) -> Box<Self> {
        let streams_slice = self.streams.0.drain(range).collect::<Vec<_>>();
        Box::new(self.new_from_streams(FusedStreams(streams_slice)))
    }
}

/// Specialized stream for sorts on single primitive columns
pub struct FieldCursorStream<T: FieldArray> {
    /// The physical expressions to sort by
    sort: PhysicalSortExpr,
    /// Input streams
    streams: FusedStreams,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: FieldArray> std::fmt::Debug for FieldCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream")
            .field("num_streams", &self.streams)
            .finish()
    }
}

impl<T: FieldArray> FieldCursorStream<T> {
    pub fn new(sort: PhysicalSortExpr, streams: Vec<SendableRecordBatchStream>) -> Self {
        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        Self {
            sort,
            streams: FusedStreams(streams),
            phantom: Default::default(),
        }
    }

    fn new_from_streams(&self, streams: FusedStreams) -> Self {
        Self {
            sort: self.sort.clone(),
            phantom: self.phantom,
            streams,
        }
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<FieldCursor<T::Values>> {
        let value = self.sort.expr.evaluate(batch)?;
        let array = value.into_array(batch.num_rows());
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        Ok(FieldCursor::new(self.sort.options, array))
    }
}

impl<T: FieldArray> PartitionedStream for FieldCursorStream<T> {
    type Output = Result<(FieldCursor<T::Values>, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}

impl<T: FieldArray, C: Cursor> BatchCursorStream<C> for FieldCursorStream<T> {
    fn take_partitions(&mut self, range: Range<usize>) -> Box<Self> {
        let streams_slice = self.streams.0.drain(range).collect::<Vec<_>>();
        Box::new(self.new_from_streams(FusedStreams(streams_slice)))
    }
}

/// A wrapper around [`CursorStream`] that collects the [`RecordBatch`] per poll,
/// and only passes along the [`BatchCursor`].
///
/// This is used in the leaf nodes of the cascading merge tree.
pub(crate) struct BatchTrackerStream<
    C: Cursor,
    S: BatchCursorStream<C, Output = Result<(C, RecordBatch)>>,
> {
    // Partitioned Input stream.
    streams: Box<S>,
    record_batch_holder: Arc<BatchTracker>,
}

impl<C: Cursor, S: BatchCursorStream<C, Output = Result<(C, RecordBatch)>>>
    BatchTrackerStream<C, S>
{
    pub fn new(streams: Box<S>, record_batch_holder: Arc<BatchTracker>) -> Self {
        Self {
            streams,
            record_batch_holder,
        }
    }
}

impl<C: Cursor + Sync, S: BatchCursorStream<C, Output = Result<(C, RecordBatch)>>>
    PartitionedStream for BatchTrackerStream<C, S>
{
    type Output = Result<BatchCursor<C>>;

    fn partitions(&self) -> usize {
        self.streams.partitions()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|(cursor, batch)| {
                let batch_id = self.record_batch_holder.add_batch(batch)?;
                Ok(BatchCursor::new(batch_id, cursor))
            })
        }))
    }
}

impl<C: Cursor, S: BatchCursorStream<C, Output = Result<(C, RecordBatch)>>>
    std::fmt::Debug for BatchTrackerStream<C, S>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetCursorStream").finish()
    }
}

/// Converts a [`BatchCursorStream`] into a [`CursorStream`].
///
/// While storing the record batches outside of the cascading merge tree.
/// Should be used with a Mutex.
pub struct BatchTracker {
    /// Monotonically increasing batch id
    monotonic_counter: AtomicU64,
    /// Write once, read many [`RecordBatch`]s
    batches: Mutex<HashMap<BatchId, Arc<RecordBatch>, RandomState>>,
    /// Accounts for memory used by buffered batches
    reservation: Mutex<MemoryReservation>,
}

impl BatchTracker {
    pub fn new(reservation: MemoryReservation) -> Self {
        Self {
            monotonic_counter: AtomicU64::new(0),
            batches: Mutex::new(HashMap::with_hasher(RandomState::new())),
            reservation: Mutex::new(reservation),
        }
    }

    pub fn add_batch(&self, batch: RecordBatch) -> Result<BatchId> {
        self.reservation
            .lock()
            .try_grow(batch.get_array_memory_size())?;
        let batch_id = self.monotonic_counter.fetch_add(1, Ordering::Relaxed);
        self.batches.lock().insert(batch_id, Arc::new(batch));
        Ok(batch_id)
    }

    pub fn get_batches(&self, batch_ids: &[BatchId]) -> Vec<Arc<RecordBatch>> {
        let batches = self.batches.lock();
        batch_ids.iter().map(|id| batches[id].clone()).collect()
    }

    pub fn remove_batches(&self, batch_ids: &[BatchId]) {
        let mut batches = self.batches.lock();
        for id in batch_ids {
            batches.remove(id);
        }
    }
}

impl std::fmt::Debug for BatchTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchTracker").finish()
    }
}

/// A newtype wrapper around a set of fused [`MergeStream`]
/// that implements debug, and skips over empty inner poll results
struct FusedMergeStreams<C: Cursor>(Vec<Fuse<MergeStream<C>>>);

impl<C: Cursor> FusedMergeStreams<C> {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<YieldedSortOrder<C>>>> {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok((_, sort_order))) if sort_order.is_empty() => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}

impl<C: Cursor> std::fmt::Debug for FusedMergeStreams<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedMergeStreams").finish()
    }
}

/// [`YieldedCursorStream`] converts an output [`MergeStream`]
/// into an input [`CursorStream`] for the next merge.
pub struct YieldedCursorStream<C: Cursor> {
    // Inner polled batch cursors, per stream_idx, which represents partially yielded batches.
    cursors: Vec<Option<VecDeque<BatchCursor<C>>>>,
    /// Streams being polled
    streams: FusedMergeStreams<C>,
}

impl<C: Cursor + std::marker::Send> YieldedCursorStream<C> {
    pub fn new(streams: Vec<MergeStream<C>>) -> Self {
        let stream_cnt = streams.len();
        Self {
            cursors: (0..stream_cnt).map(|_| None).collect(),
            streams: FusedMergeStreams(streams.into_iter().map(|s| s.fuse()).collect()),
        }
    }

    fn incr_next_batch(&mut self, stream_idx: usize) -> Option<BatchCursor<C>> {
        self.cursors[stream_idx]
            .as_mut()
            .and_then(|queue| queue.pop_front())
    }

    // The input [`SortOrder`] is across batches.
    // We need to further parse the cursors into smaller batches.
    //
    // Input:
    // - sort_order: Vec<(SlicedBatchCursorIdentifier, row_idx)> = [(0,0), (0,1), (1,0), (0,2), (0,3)]
    // - cursors: Vec<BatchCursor> = [cursor_0, cursor_1]
    //
    // Output stream:
    // Needs to be yielded to the next merge in three partial batches:
    // [(0,0),(0,1)] with cursor => then [(1,0)] with cursor => then [(0,2),(0,3)] with cursor
    //
    // This additional parsing is only required when streaming into another merge node,
    // and not required when yielding to the final interleave step.
    // (Performance slightly decreases when doing this additional parsing for all SortOrderBuilder yields.)
    fn try_parse_batches(
        &mut self,
        stream_idx: usize,
        cursors: Vec<BatchCursor<C>>,
        sort_order: Vec<SortOrder>,
    ) -> Result<()> {
        let mut cursors_per_batch: HashMap<
            SlicedBatchCursorIdentifier,
            BatchCursor<C>,
            RandomState,
        > = HashMap::with_capacity_and_hasher(cursors.len(), RandomState::new());
        for cursor in cursors {
            cursors_per_batch.insert(cursor.identifier(), cursor);
        }

        let mut parsed_cursors: Vec<BatchCursor<C>> =
            Vec::with_capacity(sort_order.len());
        let ((mut prev_batch_id, mut prev_batch_offset), mut prev_row_idx) =
            sort_order[0];
        let mut len = 0;

        for ((batch_id, batch_offset), row_idx) in sort_order.iter() {
            if prev_batch_id == *batch_id && batch_offset.0 == prev_batch_offset.0 {
                len += 1;
                continue;
            } else {
                // parse cursor
                if let Some(cursor) =
                    cursors_per_batch.get(&(prev_batch_id, prev_batch_offset))
                {
                    let parsed_cursor = cursor.slice(prev_row_idx, len)?;
                    parsed_cursors.push(parsed_cursor);
                } else {
                    unreachable!("cursor not found");
                }

                prev_batch_id = *batch_id;
                prev_row_idx = *row_idx;
                prev_batch_offset = *batch_offset;
                len = 1;
            }
        }
        if let Some(cursor) = cursors_per_batch.get(&(prev_batch_id, prev_batch_offset)) {
            let parsed_cursor = cursor.slice(prev_row_idx, len)?;
            parsed_cursors.push(parsed_cursor);
        }

        self.cursors[stream_idx] = Some(VecDeque::from(parsed_cursors));
        Ok(())
    }
}

impl<C: Cursor + std::marker::Send> PartitionedStream for YieldedCursorStream<C> {
    type Output = Result<BatchCursor<C>>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        match self.incr_next_batch(stream_idx) {
            None => match ready!(self.streams.poll_next(cx, stream_idx)) {
                None => Poll::Ready(None),
                Some(Err(e)) => Poll::Ready(Some(Err(e))),
                Some(Ok((cursors, sort_order))) => {
                    self.try_parse_batches(stream_idx, cursors, sort_order)?;
                    Poll::Ready((Ok(self.incr_next_batch(stream_idx))).transpose())
                }
            },
            Some(r) => Poll::Ready(Some(Ok(r))),
        }
    }
}

impl<C: Cursor + std::marker::Send> std::fmt::Debug for YieldedCursorStream<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YieldedCursorStream")
            .field("num_partitions", &self.partitions())
            .finish()
    }
}
