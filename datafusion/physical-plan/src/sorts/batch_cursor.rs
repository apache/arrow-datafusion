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

use super::cursor::Cursor;

pub type BatchId = u64;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BatchOffset(pub usize);

pub type SlicedBatchCursorIdentifier = (BatchId, BatchOffset);

/// The [`BatchCursor`] represents a complete, or partial, [`Cursor`] for a given record batch ([`BatchId`]).
///
/// A record batch (represented by its [`Cursor`]) can be sliced due to the following reason:
///   1. a merge node takes in 10 streams
///   2 at any given time, this means up to 10 cursors (record batches) are being merged (e.g. in the loser tree)
///   3. merge nodes will yield once it hits a size limit
///   4. at the moment of yielding, there may be some cursors which are partially yielded
///
/// Unique representation of sliced cursor is denoted by the [`SlicedBatchCursorIdentifier`].
#[derive(Debug)]
pub struct BatchCursor<C: Cursor> {
    /// The index into BatchTrackingStream::batches
    batch: BatchId,
    /// The offset of the row within the given batch, based on the idea of a sliced cursor.
    /// When a batch is partially yielded, then the offset->end will determine how much was yielded.
    row_offset: BatchOffset,

    /// The cursor for the given batch.
    pub cursor: C,
}

impl<C: Cursor> BatchCursor<C> {
    /// Create a new [`BatchCursor`] from a [`Cursor`] and a [`BatchId`].
    ///
    /// New [`BatchCursor`]s will have a [`BatchOffset`] of 0.
    /// Subsequent batch_cursors can be created by slicing.
    pub fn new(batch: BatchId, cursor: C) -> Self {
        Self {
            batch,
            row_offset: BatchOffset(0),
            cursor,
        }
    }

    /// A unique identifier used to identify a [`BatchCursor`]
    pub fn identifier(&self) -> SlicedBatchCursorIdentifier {
        (self.batch, self.row_offset)
    }

    /// Slicing of a batch cursor is done by slicing the underlying cursor,
    /// and adjust the BatchOffset
    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        Ok(Self {
            batch: self.batch,
            row_offset: BatchOffset(self.row_offset.0 + offset),
            cursor: self.cursor.slice(offset, length)?,
        })
    }
}

impl<C: Cursor> std::fmt::Display for BatchCursor<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BatchCursor(batch: {}, offset: {}, num_rows: {})",
            self.batch,
            self.row_offset.0,
            self.cursor.num_rows()
        )
    }
}
