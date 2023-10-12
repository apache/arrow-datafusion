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

use crate::sorts::sort::SortOptions;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::row::{Row, Rows};
use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrowPrimitiveType, GenericByteArray, PrimitiveArray};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use std::{cmp::Ordering, sync::Arc};

/// A [`Cursor`] for [`Rows`]
pub struct RowCursor {
    cur_row: usize,
    row_offset: usize,
    row_limit: usize, // exclusive [offset..limit]

    rows: Arc<Rows>,

    /// Tracks for the memory used by in the `Rows` of this
    /// cursor. Freed on drop
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl std::fmt::Debug for RowCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyCursor")
            .field("cur_row", &self.cur_row)
            .field("num_rows", &self.num_rows())
            .finish()
    }
}

impl RowCursor {
    /// Create a new SortKeyCursor from `rows` and a `reservation`
    /// that tracks its memory.
    ///
    /// Panic's if the reservation is not for exactly `rows.size()`
    /// bytes
    pub fn new(rows: Rows, reservation: MemoryReservation) -> Self {
        assert_eq!(
            rows.size(),
            reservation.size(),
            "memory reservation mismatch"
        );
        Self {
            cur_row: 0,
            row_offset: 0,
            row_limit: rows.num_rows(),
            rows: Arc::new(rows),
            reservation,
        }
    }

    /// Returns the current row
    fn current(&self) -> Row<'_> {
        self.rows.row(self.cur_row)
    }
}

impl PartialEq for RowCursor {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for RowCursor {}

impl PartialOrd for RowCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current().cmp(&other.current())
    }
}

/// A cursor into a sorted batch of rows
pub trait Cursor: Ord + Send + Sync {
    /// Returns true if there are no more rows in this cursor
    fn is_finished(&self) -> bool;

    /// Returns true this cursor is midway completed
    fn in_progress(&self) -> bool;

    /// Advance the cursor, returning the previous row index
    fn advance(&mut self) -> usize;

    /// Current row index
    fn current_idx(&self) -> usize;

    /// Go to start row
    fn reset(&mut self);

    /// Slice the cursor at a given row index, returning a new cursor
    ///
    /// Returns an error if the slice is out of bounds, or memory is insufficient
    fn slice(&self, offset: usize, length: usize) -> Result<Self>
    where
        Self: Sized;

    /// Returns the number of rows in this cursor
    fn num_rows(&self) -> usize;
}

impl Cursor for RowCursor {
    #[inline]
    fn is_finished(&self) -> bool {
        self.cur_row >= self.row_limit
    }

    #[inline]
    fn in_progress(&self) -> bool {
        !self.is_finished() && self.cur_row > self.row_offset
    }

    #[inline]
    fn advance(&mut self) -> usize {
        let t = self.cur_row;
        self.cur_row += 1;
        t - self.row_offset
    }

    #[inline]
    fn current_idx(&self) -> usize {
        self.cur_row - self.row_offset
    }

    #[inline]
    fn reset(&mut self) {
        self.cur_row = self.row_offset;
    }

    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        Ok(Self {
            cur_row: self.row_offset + offset,
            row_offset: self.row_offset + offset,
            row_limit: self.row_offset + offset + length,
            rows: self.rows.clone(),
            reservation: self.reservation.new_empty(), // Arc cloning of Rows is cheap
        })
    }

    #[inline]
    fn num_rows(&self) -> usize {
        self.row_limit - self.row_offset
    }
}

/// An [`Array`] that can be converted into [`FieldValues`]
pub trait FieldArray: Array + 'static {
    type Values: FieldValues;

    fn values(&self) -> Self::Values;
}

/// A comparable set of non-nullable values
pub trait FieldValues {
    type Value: ?Sized;

    fn len(&self) -> usize;

    fn compare(a: &Self::Value, b: &Self::Value) -> Ordering;

    fn value(&self, idx: usize) -> &Self::Value;

    fn slice(&self, offset: usize, length: usize) -> Result<Self>
    where
        Self: Sized;
}

impl<T: ArrowPrimitiveType> FieldArray for PrimitiveArray<T> {
    type Values = PrimitiveValues<T::Native>;

    fn values(&self) -> Self::Values {
        PrimitiveValues(self.values().clone())
    }
}

#[derive(Debug)]
pub struct PrimitiveValues<T: ArrowNativeTypeOp>(ScalarBuffer<T>);

impl<T: ArrowNativeTypeOp> FieldValues for PrimitiveValues<T> {
    type Value = T;

    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn compare(a: &Self::Value, b: &Self::Value) -> Ordering {
        T::compare(*a, *b)
    }

    #[inline]
    fn value(&self, idx: usize) -> &Self::Value {
        &self.0[idx]
    }

    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        if offset + length > self.len() {
            return Err(DataFusionError::Internal("slice out of bounds".into()));
        }
        Ok(Self(self.0.slice(offset, length)))
    }
}

impl<T: ByteArrayType> FieldArray for GenericByteArray<T> {
    type Values = Self;

    fn values(&self) -> Self::Values {
        // Once https://github.com/apache/arrow-rs/pull/4048 is released
        // Could potentially destructure array into buffers to reduce codegen,
        // in a similar vein to what is done for PrimitiveArray
        self.clone()
    }
}

impl<T: ByteArrayType> FieldValues for GenericByteArray<T> {
    type Value = T::Native;

    fn len(&self) -> usize {
        Array::len(self)
    }

    #[inline]
    fn compare(a: &Self::Value, b: &Self::Value) -> Ordering {
        let a: &[u8] = a.as_ref();
        let b: &[u8] = b.as_ref();
        a.cmp(b)
    }

    #[inline]
    fn value(&self, idx: usize) -> &Self::Value {
        self.value(idx)
    }

    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        if offset + length > Array::len(self) {
            return Err(DataFusionError::Internal("slice out of bounds".into()));
        }
        Ok(self.slice(offset, length))
    }
}

/// A cursor over sorted, nullable [`FieldValues`]
///
/// Note: comparing cursors with different `SortOptions` will yield an arbitrary ordering
#[derive(Debug)]
pub struct FieldCursor<T: FieldValues> {
    values: T,
    offset: usize,
    // If nulls first, the first non-null index
    // Otherwise, the first null index
    null_threshold: usize,
    options: SortOptions,
}

impl<T: FieldValues> FieldCursor<T> {
    /// Create a new [`FieldCursor`] from the provided `values` sorted according to `options`
    pub fn new<A: FieldArray<Values = T>>(options: SortOptions, array: &A) -> Self {
        let null_threshold = match options.nulls_first {
            true => array.null_count(),
            false => array.len() - array.null_count(),
        };

        Self {
            values: array.values(),
            offset: 0,
            null_threshold,
            options,
        }
    }

    fn is_null(&self) -> bool {
        (self.offset < self.null_threshold) == self.options.nulls_first
    }
}

impl<T: FieldValues> PartialEq for FieldCursor<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T: FieldValues> Eq for FieldCursor<T> {}
impl<T: FieldValues> PartialOrd for FieldCursor<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: FieldValues> Ord for FieldCursor<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_null(), other.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => match self.options.nulls_first {
                true => Ordering::Less,
                false => Ordering::Greater,
            },
            (false, true) => match self.options.nulls_first {
                true => Ordering::Greater,
                false => Ordering::Less,
            },
            (false, false) => {
                let s_v = self.values.value(self.offset);
                let o_v = other.values.value(other.offset);

                match self.options.descending {
                    true => T::compare(o_v, s_v),
                    false => T::compare(s_v, o_v),
                }
            }
        }
    }
}

impl<T: FieldValues + Send + Sync> Cursor for FieldCursor<T> {
    fn is_finished(&self) -> bool {
        self.offset == self.values.len()
    }

    fn in_progress(&self) -> bool {
        !self.is_finished() && self.offset > 0
    }

    fn advance(&mut self) -> usize {
        let t = self.offset;
        self.offset += 1;
        t
    }

    fn current_idx(&self) -> usize {
        self.offset
    }

    fn reset(&mut self) {
        self.offset = 0;
    }

    fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        let FieldCursor {
            values,
            offset: _,
            null_threshold,
            options,
        } = self;

        Ok(Self {
            values: values.slice(offset, length)?,
            offset: 0,
            null_threshold: *null_threshold,
            options: *options,
        })
    }

    fn num_rows(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_primitive(
        options: SortOptions,
        values: ScalarBuffer<i32>,
        null_count: usize,
    ) -> FieldCursor<PrimitiveValues<i32>> {
        let null_threshold = match options.nulls_first {
            true => null_count,
            false => values.len() - null_count,
        };

        FieldCursor {
            offset: 0,
            values: PrimitiveValues(values),
            null_threshold,
            options,
        }
    }

    #[test]
    fn test_primitive_nulls_first() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MAX, 1, 2, 3]);
        let mut a = new_primitive(options, buffer, 1);
        let buffer = ScalarBuffer::from(vec![1, 2, -2, -1, 1, 9]);
        let mut b = new_primitive(options, buffer, 2);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < -2
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 > -2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 > -1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 == 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // 9 > 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 9 > 2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![0, 1, i32::MIN, i32::MAX]);
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![-1, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

        // 0 > -1
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 0 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 < NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL = NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![6, 1, i32::MIN, i32::MAX]);
        let mut a = new_primitive(options, buffer, 3);
        let buffer = ScalarBuffer::from(vec![67, -3, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

        // 6 > 67
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MIN, i32::MAX, 6, 3]);
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![i32::MAX, 4546, -3]);
        let mut b = new_primitive(options, buffer, 1);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < 4546
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 > 4546
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);
    }
}
