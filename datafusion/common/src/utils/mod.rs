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

//! This module provides the bisect function, which implements binary search.

pub mod memory;
pub mod proxy;

use crate::error::{_internal_datafusion_err, _internal_err};
use crate::{arrow_datafusion_err, DataFusionError, Result, ScalarValue};
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute;
use arrow::compute::{partition, SortColumn, SortOptions};
use arrow::datatypes::{Field, SchemaRef, UInt32Type};
use arrow::record_batch::RecordBatch;
use arrow_array::{
    Array, FixedSizeListArray, LargeListArray, ListArray, RecordBatchOptions,
};
use arrow_schema::DataType;
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

/// Applies an optional projection to a [`SchemaRef`], returning the
/// projected schema
///
/// Example:
/// ```
/// use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
/// use datafusion_common::project_schema;
///
/// // Schema with columns 'a', 'b', and 'c'
/// let schema = SchemaRef::new(Schema::new(vec![
///   Field::new("a", DataType::Int32, true),
///   Field::new("b", DataType::Int64, true),
///   Field::new("c", DataType::Utf8, true),
/// ]));
///
/// // Pick columns 'c' and 'b'
/// let projection = Some(vec![2,1]);
/// let projected_schema = project_schema(
///    &schema,
///    projection.as_ref()
///  ).unwrap();
///
/// let expected_schema = SchemaRef::new(Schema::new(vec![
///   Field::new("c", DataType::Utf8, true),
///   Field::new("b", DataType::Int64, true),
/// ]));
///
/// assert_eq!(projected_schema, expected_schema);
/// ```
pub fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<SchemaRef> {
    let schema = match projection {
        Some(columns) => Arc::new(schema.project(columns)?),
        None => Arc::clone(schema),
    };
    Ok(schema)
}

/// Given column vectors, returns row at `idx`.
pub fn get_row_at_idx(columns: &[ArrayRef], idx: usize) -> Result<Vec<ScalarValue>> {
    columns
        .iter()
        .map(|arr| ScalarValue::try_from_array(arr, idx))
        .collect()
}

/// Construct a new RecordBatch from the rows of the `record_batch` at the `indices`.
pub fn get_record_batch_at_indices(
    record_batch: &RecordBatch,
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<RecordBatch> {
    let new_columns = get_arrayref_at_indices(record_batch.columns(), indices)?;
    RecordBatch::try_new_with_options(
        record_batch.schema(),
        new_columns,
        &RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )
    .map_err(|e| arrow_datafusion_err!(e))
}

/// This function compares two tuples depending on the given sort options.
pub fn compare_rows(
    x: &[ScalarValue],
    y: &[ScalarValue],
    sort_options: &[SortOptions],
) -> Result<Ordering> {
    let zip_it = x.iter().zip(y.iter()).zip(sort_options.iter());
    // Preserving lexical ordering.
    for ((lhs, rhs), sort_options) in zip_it {
        // Consider all combinations of NULLS FIRST/LAST and ASC/DESC configurations.
        let result = match (lhs.is_null(), rhs.is_null(), sort_options.nulls_first) {
            (true, false, false) | (false, true, true) => Ordering::Greater,
            (true, false, true) | (false, true, false) => Ordering::Less,
            (false, false, _) => if sort_options.descending {
                rhs.partial_cmp(lhs)
            } else {
                lhs.partial_cmp(rhs)
            }
            .ok_or_else(|| {
                _internal_datafusion_err!("Column array shouldn't be empty")
            })?,
            (true, true, _) => continue,
        };
        if result != Ordering::Equal {
            return Ok(result);
        }
    }
    Ok(Ordering::Equal)
}

/// This function searches for a tuple of given values (`target`) among the given
/// rows (`item_columns`) using the bisection algorithm. It assumes that `item_columns`
/// is sorted according to `sort_options` and returns the insertion index of `target`.
/// Template argument `SIDE` being `true`/`false` means left/right insertion.
pub fn bisect<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    sort_options: &[SortOptions],
) -> Result<usize> {
    let low: usize = 0;
    let high: usize = item_columns
        .first()
        .ok_or_else(|| {
            DataFusionError::Internal("Column array shouldn't be empty".to_string())
        })?
        .len();
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
        let cmp = compare_rows(current, target, sort_options)?;
        Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
    };
    find_bisect_point(item_columns, target, compare_fn, low, high)
}

/// This function searches for a tuple of given values (`target`) among a slice of
/// the given rows (`item_columns`) using the bisection algorithm. The slice starts
/// at the index `low` and ends at the index `high`. The boolean-valued function
/// `compare_fn` specifies whether we bisect on the left (by returning `false`),
/// or on the right (by returning `true`) when we compare the target value with
/// the current value as we iteratively bisect the input.
pub fn find_bisect_point<F>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    compare_fn: F,
    mut low: usize,
    mut high: usize,
) -> Result<usize>
where
    F: Fn(&[ScalarValue], &[ScalarValue]) -> Result<bool>,
{
    while low < high {
        let mid = ((high - low) / 2) + low;
        let val = get_row_at_idx(item_columns, mid)?;
        if compare_fn(&val, target)? {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

/// This function searches for a tuple of given values (`target`) among the given
/// rows (`item_columns`) via a linear scan. It assumes that `item_columns` is sorted
/// according to `sort_options` and returns the insertion index of `target`.
/// Template argument `SIDE` being `true`/`false` means left/right insertion.
pub fn linear_search<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    sort_options: &[SortOptions],
) -> Result<usize> {
    let low: usize = 0;
    let high: usize = item_columns
        .first()
        .ok_or_else(|| {
            DataFusionError::Internal("Column array shouldn't be empty".to_string())
        })?
        .len();
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
        let cmp = compare_rows(current, target, sort_options)?;
        Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
    };
    search_in_slice(item_columns, target, compare_fn, low, high)
}

/// This function searches for a tuple of given values (`target`) among a slice of
/// the given rows (`item_columns`) via a linear scan. The slice starts at the index
/// `low` and ends at the index `high`. The boolean-valued function `compare_fn`
/// specifies the stopping criterion.
pub fn search_in_slice<F>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    compare_fn: F,
    mut low: usize,
    high: usize,
) -> Result<usize>
where
    F: Fn(&[ScalarValue], &[ScalarValue]) -> Result<bool>,
{
    while low < high {
        let val = get_row_at_idx(item_columns, low)?;
        if !compare_fn(&val, target)? {
            break;
        }
        low += 1;
    }
    Ok(low)
}

/// Given a list of 0 or more already sorted columns, finds the
/// partition ranges that would partition equally across columns.
///
/// See [`partition`] for more details.
pub fn evaluate_partition_ranges(
    num_rows: usize,
    partition_columns: &[SortColumn],
) -> Result<Vec<Range<usize>>> {
    Ok(if partition_columns.is_empty() {
        vec![Range {
            start: 0,
            end: num_rows,
        }]
    } else {
        let cols: Vec<_> = partition_columns
            .iter()
            .map(|x| Arc::clone(&x.values))
            .collect();
        partition(&cols)?.ranges()
    })
}

/// Wraps identifier string in double quotes, escaping any double quotes in
/// the identifier by replacing it with two double quotes
///
/// e.g. identifier `tab.le"name` becomes `"tab.le""name"`
pub fn quote_identifier(s: &str) -> Cow<str> {
    if needs_quotes(s) {
        Cow::Owned(format!("\"{}\"", s.replace('"', "\"\"")))
    } else {
        Cow::Borrowed(s)
    }
}

/// returns true if this identifier needs quotes
fn needs_quotes(s: &str) -> bool {
    let mut chars = s.chars();

    // first char can not be a number unless escaped
    if let Some(first_char) = chars.next() {
        if !(first_char.is_ascii_lowercase() || first_char == '_') {
            return true;
        }
    }

    !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

pub(crate) fn parse_identifiers(s: &str) -> Result<Vec<Ident>> {
    let dialect = GenericDialect;
    let mut parser = Parser::new(&dialect).try_with_sql(s)?;
    let idents = parser.parse_multipart_identifier()?;
    Ok(idents)
}

/// Construct a new [`Vec`] of [`ArrayRef`] from the rows of the `arrays` at the `indices`.
pub fn get_arrayref_at_indices(
    arrays: &[ArrayRef],
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Vec<ArrayRef>> {
    arrays
        .iter()
        .map(|array| {
            compute::take(
                array.as_ref(),
                indices,
                None, // None: no index check
            )
            .map_err(|e| arrow_datafusion_err!(e))
        })
        .collect()
}

pub(crate) fn parse_identifiers_normalized(s: &str, ignore_case: bool) -> Vec<String> {
    parse_identifiers(s)
        .unwrap_or_default()
        .into_iter()
        .map(|id| match id.quote_style {
            Some(_) => id.value,
            None if ignore_case => id.value,
            _ => id.value.to_ascii_lowercase(),
        })
        .collect::<Vec<_>>()
}

/// This function "takes" the elements at `indices` from the slice `items`.
pub fn get_at_indices<T: Clone, I: Borrow<usize>>(
    items: &[T],
    indices: impl IntoIterator<Item = I>,
) -> Result<Vec<T>> {
    indices
        .into_iter()
        .map(|idx| items.get(*idx.borrow()).cloned())
        .collect::<Option<Vec<T>>>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Expects indices to be in the range of searched vector".to_string(),
            )
        })
}

/// This function finds the longest prefix of the form 0, 1, 2, ... within the
/// collection `sequence`. Examples:
/// - For 0, 1, 2, 4, 5; we would produce 3, meaning 0, 1, 2 is the longest satisfying
/// prefix.
/// - For 1, 2, 3, 4; we would produce 0, meaning there is no such prefix.
pub fn longest_consecutive_prefix<T: Borrow<usize>>(
    sequence: impl IntoIterator<Item = T>,
) -> usize {
    let mut count = 0;
    for item in sequence {
        if !count.eq(item.borrow()) {
            break;
        }
        count += 1;
    }
    count
}

/// Array Utils

/// Wrap an array into a single element `ListArray`.
/// For example `[1, 2, 3]` would be converted into `[[1, 2, 3]]`
/// The field in the list array is nullable.
pub fn array_into_list_array_nullable(arr: ArrayRef) -> ListArray {
    array_into_list_array(arr, true)
}

/// Array Utils

/// Wrap an array into a single element `ListArray`.
/// For example `[1, 2, 3]` would be converted into `[[1, 2, 3]]`
pub fn array_into_list_array(arr: ArrayRef, nullable: bool) -> ListArray {
    let offsets = OffsetBuffer::from_lengths([arr.len()]);
    ListArray::new(
        Arc::new(Field::new_list_field(arr.data_type().to_owned(), nullable)),
        offsets,
        arr,
        None,
    )
}

/// Wrap an array into a single element `LargeListArray`.
/// For example `[1, 2, 3]` would be converted into `[[1, 2, 3]]`
pub fn array_into_large_list_array(arr: ArrayRef) -> LargeListArray {
    let offsets = OffsetBuffer::from_lengths([arr.len()]);
    LargeListArray::new(
        Arc::new(Field::new_list_field(arr.data_type().to_owned(), true)),
        offsets,
        arr,
        None,
    )
}

pub fn array_into_fixed_size_list_array(
    arr: ArrayRef,
    list_size: usize,
) -> FixedSizeListArray {
    let list_size = list_size as i32;
    FixedSizeListArray::new(
        Arc::new(Field::new_list_field(arr.data_type().to_owned(), true)),
        list_size,
        arr,
        None,
    )
}

/// Wrap arrays into a single element `ListArray`.
///
/// Example:
/// ```
/// use arrow::array::{Int32Array, ListArray, ArrayRef};
/// use arrow::datatypes::{Int32Type, Field};
/// use std::sync::Arc;
///
/// let arr1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
/// let arr2 = Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef;
///
/// let list_arr = datafusion_common::utils::arrays_into_list_array([arr1, arr2]).unwrap();
///
/// let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
///    vec![
///     Some(vec![Some(1), Some(2), Some(3)]),
///     Some(vec![Some(4), Some(5), Some(6)]),
///    ]
/// );
///
/// assert_eq!(list_arr, expected);
pub fn arrays_into_list_array(
    arr: impl IntoIterator<Item = ArrayRef>,
) -> Result<ListArray> {
    let arr = arr.into_iter().collect::<Vec<_>>();
    if arr.is_empty() {
        return _internal_err!("Cannot wrap empty array into list array");
    }

    let lens = arr.iter().map(|x| x.len()).collect::<Vec<_>>();
    // Assume data type is consistent
    let data_type = arr[0].data_type().to_owned();
    let values = arr.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
    Ok(ListArray::new(
        Arc::new(Field::new_list_field(data_type, true)),
        OffsetBuffer::from_lengths(lens),
        arrow::compute::concat(values.as_slice())?,
        None,
    ))
}

/// Get the base type of a data type.
///
/// Example
/// ```
/// use arrow::datatypes::{DataType, Field};
/// use datafusion_common::utils::base_type;
/// use std::sync::Arc;
///
/// let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
/// assert_eq!(base_type(&data_type), DataType::Int32);
///
/// let data_type = DataType::Int32;
/// assert_eq!(base_type(&data_type), DataType::Int32);
/// ```
pub fn base_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => base_type(field.data_type()),
        _ => data_type.to_owned(),
    }
}

/// A helper function to coerce base type in List.
///
/// Example
/// ```
/// use arrow::datatypes::{DataType, Field};
/// use datafusion_common::utils::coerced_type_with_base_type_only;
/// use std::sync::Arc;
///
/// let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
/// let base_type = DataType::Float64;
/// let coerced_type = coerced_type_with_base_type_only(&data_type, &base_type);
/// assert_eq!(coerced_type, DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))));
pub fn coerced_type_with_base_type_only(
    data_type: &DataType,
    base_type: &DataType,
) -> DataType {
    match data_type {
        DataType::List(field) | DataType::FixedSizeList(field, _) => {
            let field_type =
                coerced_type_with_base_type_only(field.data_type(), base_type);

            DataType::List(Arc::new(Field::new(
                field.name(),
                field_type,
                field.is_nullable(),
            )))
        }
        DataType::LargeList(field) => {
            let field_type =
                coerced_type_with_base_type_only(field.data_type(), base_type);

            DataType::LargeList(Arc::new(Field::new(
                field.name(),
                field_type,
                field.is_nullable(),
            )))
        }

        _ => base_type.clone(),
    }
}

/// Recursively coerce and `FixedSizeList` elements to `List`
pub fn coerced_fixed_size_list_to_list(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) | DataType::FixedSizeList(field, _) => {
            let field_type = coerced_fixed_size_list_to_list(field.data_type());

            DataType::List(Arc::new(Field::new(
                field.name(),
                field_type,
                field.is_nullable(),
            )))
        }
        DataType::LargeList(field) => {
            let field_type = coerced_fixed_size_list_to_list(field.data_type());

            DataType::LargeList(Arc::new(Field::new(
                field.name(),
                field_type,
                field.is_nullable(),
            )))
        }

        _ => data_type.clone(),
    }
}

/// Compute the number of dimensions in a list data type.
pub fn list_ndims(data_type: &DataType) -> u64 {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => 1 + list_ndims(field.data_type()),
        _ => 0,
    }
}

/// Adopted from strsim-rs for string similarity metrics
pub mod datafusion_strsim {
    // Source: https://github.com/dguo/strsim-rs/blob/master/src/lib.rs
    // License: https://github.com/dguo/strsim-rs/blob/master/LICENSE
    use std::cmp::min;
    use std::str::Chars;

    struct StringWrapper<'a>(&'a str);

    impl<'a, 'b> IntoIterator for &'a StringWrapper<'b> {
        type Item = char;
        type IntoIter = Chars<'b>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.chars()
        }
    }

    /// Calculates the minimum number of insertions, deletions, and substitutions
    /// required to change one sequence into the other.
    fn generic_levenshtein<'a, 'b, Iter1, Iter2, Elem1, Elem2>(
        a: &'a Iter1,
        b: &'b Iter2,
    ) -> usize
    where
        &'a Iter1: IntoIterator<Item = Elem1>,
        &'b Iter2: IntoIterator<Item = Elem2>,
        Elem1: PartialEq<Elem2>,
    {
        let b_len = b.into_iter().count();

        if a.into_iter().next().is_none() {
            return b_len;
        }

        let mut cache: Vec<usize> = (1..b_len + 1).collect();

        let mut result = 0;

        for (i, a_elem) in a.into_iter().enumerate() {
            result = i + 1;
            let mut distance_b = i;

            for (j, b_elem) in b.into_iter().enumerate() {
                let cost = if a_elem == b_elem { 0usize } else { 1usize };
                let distance_a = distance_b + cost;
                distance_b = cache[j];
                result = min(result + 1, min(distance_a, distance_b + 1));
                cache[j] = result;
            }
        }

        result
    }

    /// Calculates the minimum number of insertions, deletions, and substitutions
    /// required to change one string into the other.
    ///
    /// ```
    /// use datafusion_common::utils::datafusion_strsim::levenshtein;
    ///
    /// assert_eq!(3, levenshtein("kitten", "sitting"));
    /// ```
    pub fn levenshtein(a: &str, b: &str) -> usize {
        generic_levenshtein(&StringWrapper(a), &StringWrapper(b))
    }
}

/// Merges collections `first` and `second`, removes duplicates and sorts the
/// result, returning it as a [`Vec`].
pub fn merge_and_order_indices<T: Borrow<usize>, S: Borrow<usize>>(
    first: impl IntoIterator<Item = T>,
    second: impl IntoIterator<Item = S>,
) -> Vec<usize> {
    let mut result: Vec<_> = first
        .into_iter()
        .map(|e| *e.borrow())
        .chain(second.into_iter().map(|e| *e.borrow()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    result.sort();
    result
}

/// Calculates the set difference between sequences `first` and `second`,
/// returning the result as a [`Vec`]. Preserves the ordering of `first`.
pub fn set_difference<T: Borrow<usize>, S: Borrow<usize>>(
    first: impl IntoIterator<Item = T>,
    second: impl IntoIterator<Item = S>,
) -> Vec<usize> {
    let set: HashSet<_> = second.into_iter().map(|e| *e.borrow()).collect();
    first
        .into_iter()
        .map(|e| *e.borrow())
        .filter(|e| !set.contains(e))
        .collect()
}

/// Checks whether the given index sequence is monotonically non-decreasing.
pub fn is_sorted<T: Borrow<usize>>(sequence: impl IntoIterator<Item = T>) -> bool {
    // TODO: Remove this function when `is_sorted` graduates from Rust nightly.
    let mut previous = 0;
    for item in sequence.into_iter() {
        let current = *item.borrow();
        if current < previous {
            return false;
        }
        previous = current;
    }
    true
}

/// Find indices of each element in `targets` inside `items`. If one of the
/// elements is absent in `items`, returns an error.
pub fn find_indices<T: PartialEq, S: Borrow<T>>(
    items: &[T],
    targets: impl IntoIterator<Item = S>,
) -> Result<Vec<usize>> {
    targets
        .into_iter()
        .map(|target| items.iter().position(|e| target.borrow().eq(e)))
        .collect::<Option<_>>()
        .ok_or_else(|| DataFusionError::Execution("Target not found".to_string()))
}

/// Transposes the given vector of vectors.
pub fn transpose<T>(original: Vec<Vec<T>>) -> Vec<Vec<T>> {
    match original.as_slice() {
        [] => vec![],
        [first, ..] => {
            let mut result = (0..first.len()).map(|_| vec![]).collect::<Vec<_>>();
            for row in original {
                for (item, transposed_row) in row.into_iter().zip(&mut result) {
                    transposed_row.push(item);
                }
            }
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ScalarValue::Null;
    use arrow::array::Float64Array;

    use super::*;

    #[test]
    fn test_bisect_linear_left_and_right() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from(vec![2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from(vec![15.0, 13.0, 8.0, 5., 0.0])),
        ];
        let search_tuple: Vec<ScalarValue> = vec![
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(3.0)),
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(8.0)),
        ];
        let ords = [
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ];
        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);
        Ok(())
    }

    #[test]
    fn vector_ord() {
        assert!(vec![1, 0, 0, 0, 0, 0, 0, 1] < vec![1, 0, 0, 0, 0, 0, 0, 2]);
        assert!(vec![1, 0, 0, 0, 0, 0, 1, 1] > vec![1, 0, 0, 0, 0, 0, 0, 2]);
        assert!(
            vec![
                ScalarValue::Int32(Some(2)),
                Null,
                ScalarValue::Int32(Some(0)),
            ] < vec![
                ScalarValue::Int32(Some(2)),
                Null,
                ScalarValue::Int32(Some(1)),
            ]
        );
        assert!(
            vec![
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(None),
                ScalarValue::Int32(Some(0)),
            ] < vec![
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(None),
                ScalarValue::Int32(Some(1)),
            ]
        );
    }

    #[test]
    fn ord_same_type() {
        assert!((ScalarValue::Int32(Some(2)) < ScalarValue::Int32(Some(3))));
    }

    #[test]
    fn test_bisect_linear_left_and_right_diff_sort() -> Result<()> {
        // Descending, left
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0, 0.0]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(4.0))];
        let ords = [SortOptions {
            descending: true,
            nulls_first: true,
        }];
        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 0);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 0);

        // Descending, right
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0, 0.0]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(4.0))];
        let ords = [SortOptions {
            descending: true,
            nulls_first: true,
        }];
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);

        // Ascending, left
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 9., 10.]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);

        // Ascending, right
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 9., 10.]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from(vec![10.0, 9.0, 8.0, 7.5, 7., 6.])),
        ];
        let search_tuple: Vec<ScalarValue> = vec![
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(8.0)),
        ];
        let ords = [
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ];
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);

        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        Ok(())
    }

    #[test]
    fn test_evaluate_partition_ranges() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.0, 1.0, 1.0, 2.0, 2.0, 2.0])),
            Arc::new(Float64Array::from(vec![4.0, 4.0, 3.0, 2.0, 1.0, 1.0])),
        ];
        let n_row = arrays[0].len();
        let options: Vec<SortOptions> = vec![
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];
        let sort_columns = arrays
            .into_iter()
            .zip(options)
            .map(|(values, options)| SortColumn {
                values,
                options: Some(options),
            })
            .collect::<Vec<_>>();
        let ranges = evaluate_partition_ranges(n_row, &sort_columns)?;
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], Range { start: 0, end: 2 });
        assert_eq!(ranges[1], Range { start: 2, end: 3 });
        assert_eq!(ranges[2], Range { start: 3, end: 4 });
        assert_eq!(ranges[3], Range { start: 4, end: 6 });
        Ok(())
    }

    #[test]
    fn test_quote_identifier() -> Result<()> {
        let cases = vec![
            ("foo", r#"foo"#),
            ("_foo", r#"_foo"#),
            ("foo_bar", r#"foo_bar"#),
            ("foo-bar", r#""foo-bar""#),
            // name itself has a period, needs to be quoted
            ("foo.bar", r#""foo.bar""#),
            ("Foo", r#""Foo""#),
            ("Foo.Bar", r#""Foo.Bar""#),
            // name starting with a number needs to be quoted
            ("test1", r#"test1"#),
            ("1test", r#""1test""#),
        ];

        for (identifier, quoted_identifier) in cases {
            println!("input: \n{identifier}\nquoted_identifier:\n{quoted_identifier}");

            assert_eq!(quote_identifier(identifier), quoted_identifier);

            // When parsing the quoted identifier, it should be a
            // a single identifier without normalization, and not in multiple parts
            let quote_style = if quoted_identifier.starts_with('"') {
                Some('"')
            } else {
                None
            };

            let expected_parsed = vec![Ident {
                value: identifier.to_string(),
                quote_style,
            }];

            assert_eq!(
                parse_identifiers(quoted_identifier).unwrap(),
                expected_parsed
            );
        }

        Ok(())
    }

    #[test]
    fn test_get_arrayref_at_indices() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from(vec![2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from(vec![15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let row_indices_vec: Vec<Vec<u32>> = vec![
            // Get rows 0 and 1
            vec![0, 1],
            // Get rows 0 and 1
            vec![0, 2],
            // Get rows 1 and 3
            vec![1, 3],
            // Get rows 2 and 4
            vec![2, 4],
        ];
        for row_indices in row_indices_vec {
            let indices = PrimitiveArray::from_iter_values(row_indices.iter().cloned());
            let chunk = get_arrayref_at_indices(&arrays, &indices)?;
            for (arr_orig, arr_chunk) in arrays.iter().zip(&chunk) {
                for (idx, orig_idx) in row_indices.iter().enumerate() {
                    let res1 = ScalarValue::try_from_array(arr_orig, *orig_idx as usize)?;
                    let res2 = ScalarValue::try_from_array(arr_chunk, idx)?;
                    assert_eq!(res1, res2);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_get_at_indices() -> Result<()> {
        let in_vec = vec![1, 2, 3, 4, 5, 6, 7];
        assert_eq!(get_at_indices(&in_vec, [0, 2])?, vec![1, 3]);
        assert_eq!(get_at_indices(&in_vec, [4, 2])?, vec![5, 3]);
        // 7 is outside the range
        assert!(get_at_indices(&in_vec, [7]).is_err());
        Ok(())
    }

    #[test]
    fn test_longest_consecutive_prefix() {
        assert_eq!(longest_consecutive_prefix([0, 3, 4]), 1);
        assert_eq!(longest_consecutive_prefix([0, 1, 3, 4]), 2);
        assert_eq!(longest_consecutive_prefix([0, 1, 2, 3, 4]), 5);
        assert_eq!(longest_consecutive_prefix([1, 2, 3, 4]), 0);
    }

    #[test]
    fn test_merge_and_order_indices() {
        assert_eq!(
            merge_and_order_indices([0, 3, 4], [1, 3, 5]),
            vec![0, 1, 3, 4, 5]
        );
        // Result should be ordered, even if inputs are not
        assert_eq!(
            merge_and_order_indices([3, 0, 4], [5, 1, 3]),
            vec![0, 1, 3, 4, 5]
        );
    }

    #[test]
    fn test_set_difference() {
        assert_eq!(set_difference([0, 3, 4], [1, 2]), vec![0, 3, 4]);
        assert_eq!(set_difference([0, 3, 4], [1, 2, 4]), vec![0, 3]);
        // return value should have same ordering with the in1
        assert_eq!(set_difference([3, 4, 0], [1, 2, 4]), vec![3, 0]);
        assert_eq!(set_difference([0, 3, 4], [4, 1, 2]), vec![0, 3]);
        assert_eq!(set_difference([3, 4, 0], [4, 1, 2]), vec![3, 0]);
    }

    #[test]
    fn test_is_sorted() {
        assert!(is_sorted::<usize>([]));
        assert!(is_sorted([0]));
        assert!(is_sorted([0, 3, 4]));
        assert!(is_sorted([0, 1, 2]));
        assert!(is_sorted([0, 1, 4]));
        assert!(is_sorted([0usize; 0]));
        assert!(is_sorted([1, 2]));
        assert!(!is_sorted([3, 2]));
    }

    #[test]
    fn test_find_indices() -> Result<()> {
        assert_eq!(find_indices(&[0, 3, 4], [0, 3, 4])?, vec![0, 1, 2]);
        assert_eq!(find_indices(&[0, 3, 4], [0, 4, 3])?, vec![0, 2, 1]);
        assert_eq!(find_indices(&[3, 0, 4], [0, 3])?, vec![1, 0]);
        assert!(find_indices(&[0, 3], [0, 3, 4]).is_err());
        assert!(find_indices(&[0, 3, 4], [0, 2]).is_err());
        Ok(())
    }

    #[test]
    fn test_transpose() -> Result<()> {
        let in_data = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let transposed = transpose(in_data);
        let expected = vec![vec![1, 4], vec![2, 5], vec![3, 6]];
        assert_eq!(expected, transposed);
        Ok(())
    }
}
