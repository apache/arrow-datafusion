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

use datafusion_common::Result;
use std::collections::VecDeque;
use std::mem::take;

use super::batch_cursor::{BatchCursor, SlicedBatchCursorIdentifier};
use super::cursor::Cursor;

pub type SortOrder = (SlicedBatchCursorIdentifier, usize); // batch_id, row_idx (without offset)
pub type YieldedSortOrder<C> = (Vec<BatchCursor<C>>, Vec<SortOrder>);

/// Provides an API to incrementally build a [`SortOrder`] from partitioned [`RecordBatch`](arrow::record_batch::RecordBatch)es
#[derive(Debug)]
pub struct SortOrderBuilder<C: Cursor> {
    /// Maintain a list of cursors for each finished (sorted) batch
    /// The number of total batches can be larger than the number of total streams
    batch_cursors: VecDeque<BatchCursor<C>>,

    /// The current [`BatchCursor`] for each stream_idx
    cursors: Vec<Option<BatchCursor<C>>>,

    /// The accumulated stream indexes from which to pull rows
    indices: Vec<SortOrder>,

    stream_count: usize,
}

impl<C: Cursor> SortOrderBuilder<C> {
    /// Create a new [`SortOrderBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(stream_count: usize, batch_size: usize) -> Self {
        Self {
            batch_cursors: VecDeque::with_capacity(stream_count * 2),
            cursors: (0..stream_count).map(|_| None).collect(),
            indices: Vec::with_capacity(batch_size),
            stream_count,
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(
        &mut self,
        stream_idx: usize,
        batch_cursor: BatchCursor<C>,
    ) -> Result<()> {
        self.cursors[stream_idx] = Some(batch_cursor);
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        if let Some(batch_cursor) = &mut self.cursors[stream_idx] {
            // This is tightly coupled to the loser tree implementation.
            // The winner (top of the tree) is from the stream_idx
            // and gets pushed after the cursor already advanced (to fill a leaf node).
            // Therefore, we need to subtract 1 from the current_idx.
            let row_idx = batch_cursor.cursor.current_idx() - 1;
            self.indices.push((batch_cursor.identifier(), row_idx));

            if batch_cursor.cursor.is_finished() {
                self.cursor_finished(stream_idx);
            }
        } else {
            unreachable!("row pushed for non-existant cursor");
        };
    }

    /// Returns the number of in-progress rows in this [`SortOrderBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`SortOrderBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// For a finished cursor, remove from BatchCursor (per stream_idx), and track in batch_cursors (per batch_idx)
    fn cursor_finished(&mut self, stream_idx: usize) {
        if let Some(cursor) = self.cursors[stream_idx].take() {
            self.batch_cursors.push_back(cursor);
        }
    }

    /// Advance the cursor for `stream_idx`
    /// Returns `true` if the cursor was advanced
    pub fn advance(&mut self, stream_idx: usize) -> bool {
        match &mut self.cursors[stream_idx] {
            Some(batch_cursor) => {
                let cursor = &mut batch_cursor.cursor;
                if cursor.is_finished() {
                    return false;
                }
                cursor.advance();
                true
            }
            None => false,
        }
    }

    /// Returns true if there is an in-progress cursor for a given stream
    pub fn cursor_in_progress(&mut self, stream_idx: usize) -> bool {
        self.cursors[stream_idx]
            .as_mut()
            .map_or(false, |batch_cursor| !batch_cursor.cursor.is_finished())
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`
    #[inline]
    pub fn is_gt(&mut self, stream_idx_a: usize, stream_idx_b: usize) -> bool {
        match (
            self.cursor_in_progress(stream_idx_a),
            self.cursor_in_progress(stream_idx_b),
        ) {
            (false, _) => true,
            (_, false) => false,
            _ => match (&self.cursors[stream_idx_a], &self.cursors[stream_idx_b]) {
                (Some(a), Some(b)) => a
                    .cursor
                    .cmp(&b.cursor)
                    .then_with(|| stream_idx_a.cmp(&stream_idx_b))
                    .is_gt(),
                _ => unreachable!(),
            },
        }
    }

    /// Takes the batches which already are sorted, and returns them with the corresponding cursors and sort order.
    ///
    /// This will drain the internal state of the builder, and return `None` if there are no pending.
    ///
    /// This slices cursors for each record batch, as follows:
    /// 1. input was N record_batchs of up to max M size
    /// 2. yielded ordered rows can only equal up to M size
    /// 3. of the N record_batches, each will be:
    ///         a. fully yielded (all rows)
    ///         b. partially yielded (some rows) => slice the batch_cursor
    ///         c. not yielded (no rows) => retain cursor
    /// 4. output will be:
    ///        - SortOrder
    ///        - corresponding cursors, each up to total yielded rows [cursor_batch_0, cursor_batch_1, ..]
    pub fn yield_sort_order(&mut self) -> Result<Option<YieldedSortOrder<C>>> {
        if self.is_empty() {
            return Ok(None);
        }

        let sort_order = take(&mut self.indices);
        let mut cursors_to_yield: Vec<BatchCursor<C>> =
            Vec::with_capacity(self.stream_count * 2);

        // drain already complete cursors
        for mut batch_cursor in take(&mut self.batch_cursors) {
            batch_cursor.cursor.reset();
            cursors_to_yield.push(batch_cursor);
        }

        // split any in_progress cursor
        for stream_idx in 0..self.cursors.len() {
            let mut batch_cursor = match self.cursors[stream_idx].take() {
                Some(c) => c,
                None => continue,
            };

            if batch_cursor.cursor.is_finished() {
                batch_cursor.cursor.reset();
                cursors_to_yield.push(batch_cursor);
            } else if batch_cursor.cursor.in_progress() {
                let row_idx = batch_cursor.cursor.current_idx();
                let num_rows = batch_cursor.cursor.num_rows();

                let batch_cursor_to_yield = batch_cursor.slice(0, row_idx)?;
                let batch_cursor_to_retain =
                    batch_cursor.slice(row_idx, num_rows - row_idx)?;
                assert_eq!(
                    batch_cursor_to_yield.cursor.num_rows()
                        + batch_cursor_to_retain.cursor.num_rows(),
                    num_rows
                );
                drop(batch_cursor.cursor); // drop the original cursor

                self.cursors[stream_idx] = Some(batch_cursor_to_retain);
                cursors_to_yield.push(batch_cursor_to_yield);
            } else {
                // retained all (nothing yielded)
                self.cursors[stream_idx] = Some(batch_cursor);
            }
        }

        Ok(Some((cursors_to_yield, sort_order)))
    }
}
