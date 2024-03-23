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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::buffer::ScalarBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::row::{Row, Rows};
use arrow_array::types::ByteArrayType;
use arrow_array::{
    Array, ArrowPrimitiveType, GenericByteArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use datafusion_execution::memory_pool::MemoryReservation;

/// A comparable collection of values for use with [`Cursor`]
///
/// This is a trait as there are several specialized implementations, such as for
/// single columns or for normalized multi column keys ([`Rows`])
pub trait CursorValues {
    fn len(&self) -> usize;

    /// Returns true if `l[l_idx] == r[r_idx]`
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool;

    /// Returns comparison of `l[l_idx]` and `r[r_idx]`
    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering;

    /// Returns a zero-copy slice of this [`CursorValues`] with the indicated offset and length.
    ///
    /// # Panics
    ///
    /// Panics if the slice is out of bounds
    fn slice(&self, offset: usize, length: usize) -> Self;
}

/// A comparable cursor, used by sort operations
///
/// A `Cursor` is a pointer into a collection of rows, stored in
/// [`CursorValues`]
///
/// ```text
///
/// ┌───────────────────────┐
/// │                       │           ┌──────────────────────┐
/// │ ┌─────────┐ ┌─────┐   │    ─ ─ ─ ─│      Cursor<T>       │
/// │ │    1    │ │  A  │   │   │       └──────────────────────┘
/// │ ├─────────┤ ├─────┤   │
/// │ │    2    │ │  A  │◀─ ┼ ─ ┘          Cursor<T> tracks an
/// │ └─────────┘ └─────┘   │                offset within a
/// │     ...       ...     │                  CursorValues
/// │                       │
/// │ ┌─────────┐ ┌─────┐   │
/// │ │    3    │ │  E  │   │
/// │ └─────────┘ └─────┘   │
/// │                       │
/// │     CursorValues      │
/// └───────────────────────┘
///
///
/// Store logical rows using
/// one of several  formats,
/// with specialized
/// implementations
/// depending on the column
/// types
#[derive(Debug)]
pub struct Cursor<T: CursorValues> {
    offset: usize,
    values: T,
}

impl<T: CursorValues> Cursor<T> {
    /// Create a [`Cursor`] from the given [`CursorValues`]
    pub fn new(values: T) -> Self {
        Self { offset: 0, values }
    }

    /// Returns true if there are no more rows in this cursor
    pub fn is_finished(&self) -> bool {
        self.offset == self.values.len()
    }

    /// Advance the cursor, returning the previous row index
    pub fn advance(&mut self) -> usize {
        let t = self.offset;
        self.offset += 1;
        t
    }

    /// Ref to underlying [`CursorValues`]
    #[allow(dead_code)]
    pub fn cursor_values(&self) -> &T {
        &self.values
    }
}

impl<T: CursorValues> PartialEq for Cursor<T> {
    fn eq(&self, other: &Self) -> bool {
        T::eq(&self.values, self.offset, &other.values, other.offset)
    }
}

impl<T: CursorValues> Eq for Cursor<T> {}

impl<T: CursorValues> PartialOrd for Cursor<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CursorValues> Ord for Cursor<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        T::compare(&self.values, self.offset, &other.values, other.offset)
    }
}

/// Implements [`CursorValues`] for [`Rows`]
///
/// Used for sorting when there are multiple columns in the sort key
#[derive(Debug)]
pub struct RowValues {
    rows: Arc<Rows>,

    /// Lower bound within `rows`.
    offset: usize,
    /// Upper bound within `rows` (not inclusive).
    limit: usize,

    /// Tracks for the memory used by in the `Rows` of this
    /// cursor. Freed on drop
    #[allow(dead_code)]
    reservation: Arc<MemoryReservation>,
}

impl RowValues {
    /// Create a new [`RowValues`] from [`Rows`].
    ///
    /// Panics if `rows` is empty.
    pub fn new(rows: Rows, reservation: MemoryReservation) -> Self {
        assert_eq!(
            rows.size(),
            reservation.size(),
            "memory reservation mismatch"
        );
        assert!(rows.num_rows() > 0);
        Self {
            offset: 0,
            limit: rows.num_rows(),
            rows: Arc::new(rows),
            reservation: Arc::new(reservation),
        }
    }

    /// Return value for idx.
    fn get(&self, idx: usize) -> Row<'_> {
        self.rows.row(idx + self.offset)
    }
}

impl CursorValues for RowValues {
    fn len(&self) -> usize {
        self.limit - self.offset
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        l.get(l_idx) == r.get(r_idx)
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        l.get(l_idx).cmp(&r.get(r_idx))
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            self.offset + offset < self.limit,
            "slice offset is out of bounds"
        );
        assert!(
            self.offset + offset + length <= self.limit,
            "slice length is out of bounds"
        );

        Self {
            rows: self.rows.clone(),
            offset: self.offset + offset,
            limit: self.offset + offset + length,
            reservation: self.reservation.clone(),
        }
    }
}

/// An [`Array`] that can be converted into [`CursorValues`]
pub trait CursorArray: Array + 'static {
    type Values: CursorValues;

    fn values(&self) -> Self::Values;
}

impl<T: ArrowPrimitiveType> CursorArray for PrimitiveArray<T> {
    type Values = PrimitiveValues<T::Native>;

    fn values(&self) -> Self::Values {
        PrimitiveValues(self.values().clone())
    }
}

#[derive(Debug)]
pub struct PrimitiveValues<T: ArrowNativeTypeOp>(ScalarBuffer<T>);

impl<T: ArrowNativeTypeOp> CursorValues for PrimitiveValues<T> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        l.0[l_idx].is_eq(r.0[r_idx])
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        l.0[l_idx].compare(r.0[r_idx])
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(offset < self.0.len(), "slice offset is out of bounds");
        assert!(
            offset + length - 1 < self.0.len(),
            "slice length is out of bounds"
        );

        Self(self.0.slice(offset, length))
    }
}

pub struct ByteArrayValues<T: OffsetSizeTrait> {
    offsets: OffsetBuffer<T>,
    values: Buffer,
}

impl<T: OffsetSizeTrait> ByteArrayValues<T> {
    fn value(&self, idx: usize) -> &[u8] {
        assert!(idx < self.len());
        // Safety: offsets are valid and checked bounds above
        unsafe {
            let start = self.offsets.get_unchecked(idx).as_usize();
            let end = self.offsets.get_unchecked(idx + 1).as_usize();
            self.values.get_unchecked(start..end)
        }
    }
}

impl<T: OffsetSizeTrait> CursorValues for ByteArrayValues<T> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        l.value(l_idx) == r.value(r_idx)
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        l.value(l_idx).cmp(r.value(r_idx))
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        let start = self
            .offsets
            .get(offset)
            .expect("slice offset is out of bounds")
            .as_usize();
        let end = self
            .offsets
            .get(offset + length)
            .expect("slice length is out of bounds")
            .as_usize();

        let offsets = self
            .offsets
            .slice(offset, length)
            .iter()
            .map(|o| T::usize_as(o.as_usize().wrapping_sub(start)))
            .collect::<Vec<T>>();

        Self {
            offsets: OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values: self.values.slice_with_length(start, end - start),
        }
    }
}

impl<T: ByteArrayType> CursorArray for GenericByteArray<T> {
    type Values = ByteArrayValues<T::Offset>;

    fn values(&self) -> Self::Values {
        ByteArrayValues {
            offsets: self.offsets().clone(),
            values: self.values().clone(),
        }
    }
}

/// A collection of sorted, nullable [`CursorValues`]
///
/// Note: comparing cursors with different `SortOptions` will yield an arbitrary ordering
#[derive(Debug)]
pub struct ArrayValues<T: CursorValues> {
    values: T,
    // If nulls first, the first non-null index
    // Otherwise, the first null index
    null_threshold: usize,
    options: SortOptions,
}

impl<T: CursorValues> ArrayValues<T> {
    /// Create a new [`ArrayValues`] from the provided `values` sorted according
    /// to `options`.
    ///
    /// Panics if the array is empty
    pub fn new<A: CursorArray<Values = T>>(options: SortOptions, array: &A) -> Self {
        assert!(array.len() > 0, "Empty array passed to FieldCursor");
        let null_threshold = match options.nulls_first {
            true => array.null_count(),
            false => array.len() - array.null_count(),
        };

        Self {
            values: array.values(),
            null_threshold,
            options,
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        (idx < self.null_threshold) == self.options.nulls_first
    }
}

impl<T: CursorValues> CursorValues for ArrayValues<T> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        match (l.is_null(l_idx), r.is_null(r_idx)) {
            (true, true) => true,
            (false, false) => T::eq(&l.values, l_idx, &r.values, r_idx),
            _ => false,
        }
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        match (l.is_null(l_idx), r.is_null(r_idx)) {
            (true, true) => Ordering::Equal,
            (true, false) => match l.options.nulls_first {
                true => Ordering::Less,
                false => Ordering::Greater,
            },
            (false, true) => match l.options.nulls_first {
                true => Ordering::Greater,
                false => Ordering::Less,
            },
            (false, false) => match l.options.descending {
                true => T::compare(&r.values, r_idx, &l.values, l_idx),
                false => T::compare(&l.values, l_idx, &r.values, r_idx),
            },
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(offset < self.values.len(), "slice offset is out of bounds");
        assert!(
            offset + length - 1 < self.values.len(),
            "slice length is out of bounds"
        );

        Self {
            values: self.values.slice(offset, length),
            null_threshold: self.null_threshold.saturating_sub(offset),
            options: self.options,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::row::{RowConverter, SortField};
    use arrow_array::{ArrayRef, Float32Array, Int16Array};
    use arrow_schema::DataType;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    use super::*;

    fn new_primitive_cursor(
        options: SortOptions,
        values: ScalarBuffer<i32>,
        null_count: usize,
    ) -> Cursor<ArrayValues<PrimitiveValues<i32>>> {
        let null_threshold = match options.nulls_first {
            true => null_count,
            false => values.len() - null_count,
        };

        let values = ArrayValues {
            values: PrimitiveValues(values),
            null_threshold,
            options,
        };

        Cursor::new(values)
    }

    #[test]
    fn test_ord_nulls() {
        // nulls first
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let is_min = new_primitive_cursor(options, ScalarBuffer::from(vec![i32::MIN]), 0);
        let is_null =
            new_primitive_cursor(options, ScalarBuffer::from(vec![i32::MIN]), 1);
        assert_eq!(
            is_min.cmp(&is_null),
            Ordering::Greater,
            "should have MIN > NULL, when nulls first"
        );
        assert_eq!(
            is_null.cmp(&is_min),
            Ordering::Less,
            "should have NULL < MIN, when nulls first"
        );

        // nulls last
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let is_min = new_primitive_cursor(options, ScalarBuffer::from(vec![i32::MIN]), 0);
        let is_null =
            new_primitive_cursor(options, ScalarBuffer::from(vec![i32::MIN]), 1);
        assert_eq!(
            is_min.cmp(&is_null),
            Ordering::Less,
            "should have MIN < NULL, when nulls last"
        );
        assert_eq!(
            is_null.cmp(&is_min),
            Ordering::Greater,
            "should have NULL > MIN, when nulls last"
        );
    }

    #[test]
    fn test_primitive_nulls_first() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MAX, 1, 2, 3]);
        let mut a = new_primitive_cursor(options, buffer, 1);
        let buffer = ScalarBuffer::from(vec![1, 2, -2, -1, 1, 9]);
        let mut b = new_primitive_cursor(options, buffer, 2);

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
        let mut a = new_primitive_cursor(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![-1, i32::MAX, i32::MIN]);
        let mut b = new_primitive_cursor(options, buffer, 2);

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
        let mut a = new_primitive_cursor(options, buffer, 3);
        let buffer = ScalarBuffer::from(vec![67, -3, i32::MAX, i32::MIN]);
        let mut b = new_primitive_cursor(options, buffer, 2);

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
        let mut a = new_primitive_cursor(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![i32::MAX, 4546, -3]);
        let mut b = new_primitive_cursor(options, buffer, 1);

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

    #[test]
    fn test_slice_primitive() {
        fn run_test(options: SortOptions, num_nulls: usize, scenario: &str) {
            let is_min = Arc::new(new_primitive_cursor(
                options,
                ScalarBuffer::from(vec![i32::MIN]),
                0,
            ));
            let is_null = Arc::new(new_primitive_cursor(
                options,
                ScalarBuffer::from(vec![i32::MIN]),
                1,
            ));

            let buffer = ScalarBuffer::from(vec![i32::MIN, 79, 2, i32::MIN]);
            let mut a = new_primitive_cursor(options, buffer, num_nulls);
            let buffer = ScalarBuffer::from(vec![i32::MIN, -284, 3, i32::MIN, 2]);
            let mut b = new_primitive_cursor(options, buffer.clone(), num_nulls);

            // start
            // NULL == NULL, or i32::MIN == i32::MIN
            assert_eq!(a.cmp(&b), Ordering::Equal);
            let expected_val = match (options.nulls_first, num_nulls > 0) {
                (true, true) => is_null.clone(), // nulls_first
                (false, true) => is_min.clone(), // nulls_last
                (_, false) => is_min.clone(),    // no_nulls
            };
            assert_eq!(
                a.cmp(&expected_val),
                Ordering::Equal,
                "{scenario}: should have null mask applied properly, at start of values"
            );

            // start
            // NULL == NULL, or i32::MIN == i32::MIN
            a.advance();
            a.advance();
            a = Cursor::new(a.cursor_values().slice(0, 4));
            assert_eq!(
                a.cmp(&b),
                Ordering::Equal,
                "{scenario}: should ignore cursor position when sliced"
            );
            assert_eq!(
                a.cursor_values().len(),
                4,
                "{scenario}: should be able to slice to the full length"
            );

            // slice a
            // i32::MIN > NULL (nulls first), or NULL > i32::MIN (nulls last), or i32::MIN == i32::MIN (no nulls)
            a = Cursor::new(a.cursor_values().slice(3, 1));
            let (expected_val, expected_order) =
                match (options.nulls_first, num_nulls > 0) {
                    (true, true) => (is_min.clone(), Ordering::Greater), // nulls_first
                    (false, true) => (is_null, Ordering::Greater), // nulls_last, so nulls are considered greater
                    (_, false) => (is_min, Ordering::Equal),       // no_nulls
                };
            assert_eq!(
                a.cmp(&expected_val),
                Ordering::Equal,
                "{scenario}: should have null mask applied properly, at end of values"
            );
            assert_eq!(
                a.cmp(&b),
                expected_order,
                "{scenario}: should be able to slice with offset, with null mask"
            );
            assert_eq!(
                a.cursor_values().len(),
                1,
                "{scenario}: should be able to slice to shorten length"
            );

            // slice b, compare sliced_a to sliced_b
            // i32::MIN == i32::MIN, or NULL == NULL
            b = Cursor::new(b.cursor_values().slice(3, 2));
            assert_eq!(
                a.cmp(&b),
                Ordering::Equal,
                "{scenario}: should be able to slice with offset"
            );
            assert_eq!(b.cursor_values().len(), 2, "{scenario}: should have a smaller apparent length for the underlying cursor values");

            // re-slice b
            // i32::MIN < 2 (nulls first), or NULL == NULL (nulls last), or i32::MIN < 2 (no nulls)
            b = Cursor::new(b.cursor_values().slice(1, 1));
            let expected_order = match (options.nulls_first, num_nulls > 0) {
                (true, true) => Ordering::Less,   // nulls_first
                (false, true) => Ordering::Equal, // nulls_last
                (_, false) => Ordering::Less,     // no_nulls
            };
            assert_eq!(a.cmp(&b), expected_order, "{scenario}: should respect previous slice/windowed boundaries, when re-slicing");

            // length change: on slice vs advance
            let mut cursor = new_primitive_cursor(options, buffer, num_nulls);
            assert_eq!(
                cursor.cursor_values().len(),
                5,
                "{scenario}: expect initial length"
            );
            cursor.advance();
            cursor.advance();
            assert_eq!(cursor.cursor_values().len(), 5, "{scenario}: expect advancing cursor does not impact cursor_values length");
            cursor = Cursor::new(cursor.cursor_values().slice(2, 2));
            assert_eq!(
                cursor.cursor_values().len(),
                2,
                "{scenario}: expect cursor_values slicing to impact length"
            );
        }

        run_test(
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            2,
            "nulls_first",
        );

        run_test(
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            2,
            "nulls_last",
        );

        run_test(
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            0,
            "no_nulls",
        );
    }

    #[test]
    #[should_panic(expected = "slice offset is out of bounds")]
    fn test_slice_primitive_can_panic() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![0, 1, 2]);
        let cursor = new_primitive_cursor(options, buffer, 0);

        cursor
            .cursor_values()
            .slice(cursor.cursor_values().len(), 1);
    }

    fn new_row_cursor(cols: &[Arc<dyn Array>; 2]) -> Cursor<RowValues> {
        let converter = RowConverter::new(vec![
            SortField::new(DataType::Int16),
            SortField::new(DataType::Float32),
        ])
        .unwrap();

        let rows = converter.convert_columns(cols).unwrap();

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(rows.size()));
        let mut reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(rows.size()).unwrap();

        Cursor::new(RowValues::new(rows, reservation))
    }

    #[test]
    fn test_slice_rows() {
        // rows
        let cols = [
            Arc::new(Int16Array::from_iter([Some(1), Some(2), Some(3), Some(4)]))
                as ArrayRef,
            Arc::new(Float32Array::from_iter([
                Some(1.3),
                Some(2.5),
                Some(4.),
                Some(4.2),
            ])) as ArrayRef,
        ];

        let mut a = new_row_cursor(&cols);
        let mut b = new_row_cursor(&cols);
        assert_eq!(a.cursor_values().len(), 4);

        // 1,1.3 == 1,1.3
        assert_eq!(a.cmp(&b), Ordering::Equal);

        // advance A. slice B full length.
        // 2,2.5 > 1,1.3
        a.advance();
        b = Cursor::new(b.cursor_values().slice(0, 3));
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // slice B ahead by 2.
        // 2,2.5 < 3,4.0
        b = Cursor::new(b.cursor_values().slice(2, 1));
        assert_eq!(a.cmp(&b), Ordering::Less);

        // advanced cursor vs sliced cursor
        assert_eq!(a.cursor_values().len(), 4);
        assert_eq!(b.cursor_values().len(), 1);

        // slicing on a slice (a.k.a. combining offsets)
        let cursor = new_row_cursor(&cols);
        let sliced = Cursor::new(cursor.cursor_values().slice(1, 3));
        let sliced = Cursor::new(sliced.cursor_values().slice(2, 1));
        let mut expected = new_row_cursor(&cols);
        expected.advance();
        expected.advance();
        expected.advance();
        assert_eq!(
            sliced.cmp(&expected),
            Ordering::Equal,
            "should respect previous slice/windowed boundaries, when re-slicing"
        );
    }

    #[test]
    #[should_panic(expected = "slice offset is out of bounds")]
    fn test_slice_rows_can_panic() {
        let cols = [
            Arc::new(Int16Array::from_iter([Some(1)])) as ArrayRef,
            Arc::new(Float32Array::from_iter([Some(1.3)])) as ArrayRef,
        ];

        let cursor = new_row_cursor(&cols);

        cursor
            .cursor_values()
            .slice(cursor.cursor_values().len(), 1);
    }

    fn new_bytearray_cursor(
        values_str: &str,
        offsets: Vec<i32>,
    ) -> Cursor<ArrayValues<ByteArrayValues<i32>>> {
        let values = Buffer::from_slice_ref(values_str);
        let offsets = OffsetBuffer::new(offsets.into());

        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        Cursor::new(ArrayValues {
            values: ByteArrayValues {
                offsets: offsets.clone(),
                values: values.clone(),
            },
            null_threshold: 0,
            options,
        })
    }

    #[test]
    fn test_slice_bytearray() {
        let mut a = new_bytearray_cursor("hellorainbowworldzoo", vec![0, 5, 12, 17, 20]);
        let mut b = new_bytearray_cursor("hellorainbowworldzoo", vec![0, 5, 12, 17, 20]);

        let is_hello = new_bytearray_cursor("hello", vec![0, 5]);
        let is_rainbow = new_bytearray_cursor("rainbow", vec![0, 7]);
        let is_world = new_bytearray_cursor("world", vec![0, 5]);
        let is_zoo = new_bytearray_cursor("zoo", vec![0, 3]);

        // hello == hello
        assert_eq!(a.cmp(&b), Ordering::Equal);

        // advance A. slice B full length.
        // rainbow > hello
        a.advance();
        b = Cursor::new(b.cursor_values().slice(0, 3));
        assert_eq!(a.cmp(&is_rainbow), Ordering::Equal);
        assert_eq!(b.cmp(&is_hello), Ordering::Equal);
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // slice B ahead by 2.
        // rainbow < world
        b = Cursor::new(b.cursor_values().slice(2, 1));
        assert_eq!(b.cmp(&is_world), Ordering::Equal);
        assert_eq!(a.cmp(&b), Ordering::Less);

        // advanced cursor vs sliced cursor
        assert_eq!(a.cursor_values().len(), 4);
        assert_eq!(b.cursor_values().len(), 1);

        // slicing on a slice (a.k.a. combining offsets)
        let cursor = new_bytearray_cursor("hellorainbowworldzoo", vec![0, 5, 12, 17, 20]);
        let sliced = Cursor::new(cursor.cursor_values().slice(1, 3));
        let sliced = Cursor::new(sliced.cursor_values().slice(2, 1));
        assert_eq!(
            sliced.cmp(&is_zoo),
            Ordering::Equal,
            "should respect previous slice/windowed boundaries, when re-slicing"
        );
    }

    #[test]
    #[should_panic(expected = "slice offset is out of bounds")]
    fn test_slice_bytearray_should_panic() {
        let cursor = new_bytearray_cursor("hellorainbowworld", vec![0, 5, 12, 17]);
        cursor
            .cursor_values()
            .slice(cursor.cursor_values().len(), 1);
    }
}
