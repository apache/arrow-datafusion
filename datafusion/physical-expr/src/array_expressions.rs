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

//! Array expressions

use std::any::type_name;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::compute;
use arrow::datatypes::{DataType, Field, UInt64Type};
use arrow::row::{RowConverter, SortField};
use arrow_buffer::NullBuffer;

use arrow_schema::{FieldRef, SortOptions};
use datafusion_common::cast::{
    as_generic_list_array, as_generic_string_array, as_int64_array, as_large_list_array,
    as_list_array, as_null_array, as_string_array,
};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{
    exec_err, internal_err, not_impl_err, plan_err, DataFusionError, Result,
};

use itertools::Itertools;

macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast to {}",
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

/// Downcasts multiple arguments into a single concrete type
/// $ARGS:  &[ArrayRef]
/// $ARRAY_TYPE: type to downcast to
///
/// $returns a Vec<$ARRAY_TYPE>
macro_rules! downcast_vec {
    ($ARGS:expr, $ARRAY_TYPE:ident) => {{
        $ARGS
            .iter()
            .map(|e| match e.as_any().downcast_ref::<$ARRAY_TYPE>() {
                Some(array) => Ok(array),
                _ => internal_err!("failed to downcast"),
            })
    }};
}

/// Computes a BooleanArray indicating equality or inequality between elements in a list array and a specified element array.
///
/// # Arguments
///
/// * `list_array_row` - A reference to a trait object implementing the Arrow `Array` trait. It represents the list array for which the equality or inequality will be compared.
///
/// * `element_array` - A reference to a trait object implementing the Arrow `Array` trait. It represents the array with which each element in the `list_array_row` will be compared.
///
/// * `row_index` - The index of the row in the `element_array` and `list_array` to use for the comparison.
///
/// * `eq` - A boolean flag. If `true`, the function computes equality; if `false`, it computes inequality.
///
/// # Returns
///
/// Returns a `Result<BooleanArray>` representing the comparison results. The result may contain an error if there are issues with the computation.
///
/// # Example
///
/// ```text
/// compare_element_to_list(
///     [1, 2, 3], [1, 2, 3], 0, true => [true, false, false]
///     [1, 2, 3, 3, 2, 1], [1, 2, 3], 1, true => [false, true, false, false, true, false]
///
///     [[1, 2, 3], [2, 3, 4], [3, 4, 5]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 0, true => [true, false, false]
///     [[1, 2, 3], [2, 3, 4], [2, 3, 4]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 1, false => [true, false, false]
/// )
/// ```
fn compare_element_to_list(
    list_array_row: &dyn Array,
    element_array: &dyn Array,
    row_index: usize,
    eq: bool,
) -> Result<BooleanArray> {
    if list_array_row.data_type() != element_array.data_type() {
        return exec_err!(
            "compare_element_to_list received incompatible types: '{:?}' and '{:?}'.",
            list_array_row.data_type(),
            element_array.data_type()
        );
    }

    let indices = UInt32Array::from(vec![row_index as u32]);
    let element_array_row = arrow::compute::take(element_array, &indices, None)?;

    // Compute all positions in list_row_array (that is itself an
    // array) that are equal to `from_array_row`
    let res = match element_array_row.data_type() {
        // arrow_ord::cmp::eq does not support ListArray, so we need to compare it by loop
        DataType::List(_) => {
            // compare each element of the from array
            let element_array_row_inner = as_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        DataType::LargeList(_) => {
            // compare each element of the from array
            let element_array_row_inner =
                as_large_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_large_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        _ => {
            let element_arr = Scalar::new(element_array_row);
            // use not_distinct so we can compare NULL
            if eq {
                arrow_ord::cmp::not_distinct(&list_array_row, &element_arr)?
            } else {
                arrow_ord::cmp::distinct(&list_array_row, &element_arr)?
            }
        }
    };

    Ok(res)
}

/// Returns the length of a concrete array dimension
fn compute_array_length(
    arr: Option<ArrayRef>,
    dimension: Option<i64>,
) -> Result<Option<u64>> {
    let mut current_dimension: i64 = 1;
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let dimension = match dimension {
        Some(value) => {
            if value < 1 {
                return Ok(None);
            }

            value
        }
        None => return Ok(None),
    };

    loop {
        if current_dimension == dimension {
            return Ok(Some(value.len() as u64));
        }

        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                current_dimension += 1;
            }
            DataType::LargeList(..) => {
                value = downcast_arg!(value, LargeListArray).value(0);
                current_dimension += 1;
            }
            _ => return Ok(None),
        }
    }
}

/// Returns the length of each array dimension
fn compute_array_dims(arr: Option<ArrayRef>) -> Result<Option<Vec<Option<u64>>>> {
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    if value.is_empty() {
        return Ok(None);
    }
    let mut res = vec![Some(value.len() as u64)];

    loop {
        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                res.push(Some(value.len() as u64));
            }
            _ => return Ok(Some(res)),
        }
    }
}

fn check_datatypes(name: &str, args: &[&ArrayRef]) -> Result<()> {
    let data_type = args[0].data_type();
    if !args.iter().all(|arg| {
        arg.data_type().equals_datatype(data_type)
            || arg.data_type().equals_datatype(&DataType::Null)
    }) {
        let types = args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
        return plan_err!("{name} received incompatible types: '{types:?}'.");
    }

    Ok(())
}

macro_rules! call_array_function {
    ($DATATYPE:expr, false) => {
        match $DATATYPE {
            DataType::Utf8 => array_function!(StringArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            _ => unreachable!(),
        }
    };
    ($DATATYPE:expr, $INCLUDE_LIST:expr) => {{
        match $DATATYPE {
            DataType::List(_) => array_function!(ListArray),
            DataType::Utf8 => array_function!(StringArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            _ => unreachable!(),
        }
    }};
}

/// Convert one or more [`ArrayRef`] of the same type into a
/// `ListArray` or 'LargeListArray' depending on the offset size.
///
/// # Example (non nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are non nested
/// would return a single new `ListArray`, where each row was a list
/// of 2 elements:
///
/// ```text
/// ┌─────────┐   ┌─────────┐           ┌──────────────┐
/// │ ┌─────┐ │   │ ┌─────┐ │           │ ┌──────────┐ │
/// │ │  A  │ │   │ │  X  │ │           │ │  [A, X]  │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │NULL │ │   │ │  Y  │ │──────────▶│ │[NULL, Y] │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │  C  │ │   │ │  Z  │ │           │ │  [C, Z]  │ │
/// │ └─────┘ │   │ └─────┘ │           │ └──────────┘ │
/// └─────────┘   └─────────┘           └──────────────┘
///   col1           col2                    output
/// ```
///
/// # Example (nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are lists
/// would return a single new `ListArray`, where each row was a list
/// of the corresponding elements of col1 and col2.
///
/// ``` text
/// ┌──────────────┐   ┌──────────────┐        ┌─────────────────────────────┐
/// │ ┌──────────┐ │   │ ┌──────────┐ │        │ ┌────────────────────────┐  │
/// │ │  [A, X]  │ │   │ │    []    │ │        │ │    [[A, X], []]        │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────┤  │
/// │ │[NULL, Y] │ │   │ │[Q, R, S] │ │───────▶│ │ [[NULL, Y], [Q, R, S]] │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────│  │
/// │ │  [C, Z]  │ │   │ │   NULL   │ │        │ │    [[C, Z], NULL]      │  │
/// │ └──────────┘ │   │ └──────────┘ │        │ └────────────────────────┘  │
/// └──────────────┘   └──────────────┘        └─────────────────────────────┘
///      col1               col2                         output
/// ```
fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    let mut data = vec![];
    let mut total_len = 0;
    for arg in args {
        let arg_data = if arg.as_any().is::<NullArray>() {
            ArrayData::new_empty(&data_type)
        } else {
            arg.to_data()
        };
        total_len += arg_data.len();
        data.push(arg_data);
    }

    let mut offsets: Vec<O> = Vec::with_capacity(total_len);
    offsets.push(O::usize_as(0));

    let capacity = Capacities::Array(total_len);
    let data_ref = data.iter().collect::<Vec<_>>();
    let mut mutable = MutableArrayData::with_capacities(data_ref, true, capacity);

    let num_rows = args[0].len();
    for row_idx in 0..num_rows {
        for (arr_idx, arg) in args.iter().enumerate() {
            if !arg.as_any().is::<NullArray>()
                && !arg.is_null(row_idx)
                && arg.is_valid(row_idx)
            {
                mutable.extend(arr_idx, row_idx, row_idx + 1);
            } else {
                mutable.extend_nulls(1);
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}

/// `make_array` SQL function
pub fn make_array(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let mut data_type = DataType::Null;
    for arg in arrays {
        let arg_data_type = arg.data_type();
        if !arg_data_type.equals_datatype(&DataType::Null) {
            data_type = arg_data_type.clone();
            break;
        }
    }

    match data_type {
        // Either an empty array or all nulls:
        DataType::Null => {
            let array =
                new_null_array(&DataType::Null, arrays.iter().map(|a| a.len()).sum());
            Ok(Arc::new(array_into_list_array(array)))
        }
        DataType::LargeList(..) => array_array::<i64>(arrays, data_type),
        _ => array_array::<i32>(arrays, data_type),
    }
}

fn general_array_element<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    indexes: &Int64Array,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    // use_nulls: true, we don't construct List for array_element, so we need explicit nulls.
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    fn adjusted_array_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
    where
        i64: TryInto<O>,
    {
        let index: O = index.try_into().map_err(|_| {
            DataFusionError::Execution(format!(
                "array_element got invalid index: {}",
                index
            ))
        })?;
        // 0 ~ len - 1
        let adjusted_zero_index = if index < O::usize_as(0) {
            index + len
        } else {
            index - O::usize_as(1)
        };

        if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
            Ok(Some(adjusted_zero_index))
        } else {
            // Out of bounds
            Ok(None)
        }
    }

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // array is null
        if len == O::usize_as(0) {
            mutable.extend_nulls(1);
            continue;
        }

        let index = adjusted_array_index::<O>(indexes.value(row_index), len)?;

        if let Some(index) = index {
            let start = start.as_usize() + index.as_usize();
            mutable.extend(0, start, start + 1_usize);
        } else {
            // Index out of bounds
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(arrow_array::make_array(data))
}

/// array_element SQL function
///
/// There are two arguments for array_element, the first one is the array, the second one is the 1-indexed index.
/// `array_element(array, index)`
///
/// For example:
/// > array_element(\[1, 2, 3], 2) -> 2
pub fn array_element(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_element needs two arguments");
    }

    match &args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i32>(array, indexes)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i64>(array, indexes)
        }
        _ => exec_err!(
            "array_element does not support type: {:?}",
            args[0].data_type()
        ),
    }
}

fn general_except<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<GenericListArray<OffsetSize>> {
    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;

    let l_values = l.values().to_owned();
    let r_values = r.values().to_owned();
    let l_values = converter.convert_columns(&[l_values])?;
    let r_values = converter.convert_columns(&[r_values])?;

    let mut offsets = Vec::<OffsetSize>::with_capacity(l.len() + 1);
    offsets.push(OffsetSize::usize_as(0));

    let mut rows = Vec::with_capacity(l_values.num_rows());
    let mut dedup = HashSet::new();

    for (l_w, r_w) in l.offsets().windows(2).zip(r.offsets().windows(2)) {
        let l_slice = l_w[0].as_usize()..l_w[1].as_usize();
        let r_slice = r_w[0].as_usize()..r_w[1].as_usize();
        for i in r_slice {
            let right_row = r_values.row(i);
            dedup.insert(right_row);
        }
        for i in l_slice {
            let left_row = l_values.row(i);
            if dedup.insert(left_row) {
                rows.push(left_row);
            }
        }

        offsets.push(OffsetSize::usize_as(rows.len()));
        dedup.clear();
    }

    if let Some(values) = converter.convert_rows(rows)?.first() {
        Ok(GenericListArray::<OffsetSize>::new(
            field.to_owned(),
            OffsetBuffer::new(offsets.into()),
            values.to_owned(),
            l.nulls().cloned(),
        ))
    } else {
        internal_err!("array_except failed to convert rows")
    }
}

pub fn array_except(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return internal_err!("array_except needs two arguments");
    }

    let array1 = &args[0];
    let array2 = &args[1];

    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, _) | (_, DataType::Null) => Ok(array1.to_owned()),
        (DataType::List(field), DataType::List(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i32>();
            let list2 = array2.as_list::<i32>();
            let result = general_except::<i32>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (DataType::LargeList(field), DataType::LargeList(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i64>();
            let list2 = array2.as_list::<i64>();
            let result = general_except::<i64>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (dt1, dt2) => {
            internal_err!("array_except got unexpected types: {dt1:?} and {dt2:?}")
        }
    }
}

/// array_slice SQL function
///
/// We follow the behavior of array_slice in DuckDB
/// Note that array_slice is 1-indexed. And there are two additional arguments `from` and `to` in array_slice.
///
/// > array_slice(array, from, to)
///
/// Positive index is treated as the index from the start of the array. If the
/// `from` index is smaller than 1, it is treated as 1. If the `to` index is larger than the
/// length of the array, it is treated as the length of the array.
///
/// Negative index is treated as the index from the end of the array. If the index
/// is larger than the length of the array, it is NOT VALID, either in `from` or `to`.
/// The `to` index is exclusive like python slice syntax.
///
/// See test cases in `array.slt` for more details.
pub fn array_slice(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_slice needs three arguments");
    }

    let array_data_type = args[0].data_type();
    match array_data_type {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            let from_array = as_int64_array(&args[1])?;
            let to_array = as_int64_array(&args[2])?;
            general_array_slice::<i32>(array, from_array, to_array)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            let from_array = as_int64_array(&args[1])?;
            let to_array = as_int64_array(&args[2])?;
            general_array_slice::<i64>(array, from_array, to_array)
        }
        _ => exec_err!("array_slice does not support type: {:?}", array_data_type),
    }
}

fn general_array_slice<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    from_array: &Int64Array,
    to_array: &Int64Array,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    // use_nulls: false, we don't need nulls but empty array for array_slice, so we don't need explicit nulls but adjust offset to indicate nulls.
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);

    // We have the slice syntax compatible with DuckDB v0.8.1.
    // The rule `adjusted_from_index` and `adjusted_to_index` follows the rule of array_slice in duckdb.

    fn adjusted_from_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
    where
        i64: TryInto<O>,
    {
        // 0 ~ len - 1
        let adjusted_zero_index = if index < 0 {
            if let Ok(index) = index.try_into() {
                index + len
            } else {
                return exec_err!("array_slice got invalid index: {}", index);
            }
        } else {
            // array_slice(arr, 1, to) is the same as array_slice(arr, 0, to)
            if let Ok(index) = index.try_into() {
                std::cmp::max(index - O::usize_as(1), O::usize_as(0))
            } else {
                return exec_err!("array_slice got invalid index: {}", index);
            }
        };

        if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
            Ok(Some(adjusted_zero_index))
        } else {
            // Out of bounds
            Ok(None)
        }
    }

    fn adjusted_to_index<O: OffsetSizeTrait>(index: i64, len: O) -> Result<Option<O>>
    where
        i64: TryInto<O>,
    {
        // 0 ~ len - 1
        let adjusted_zero_index = if index < 0 {
            // array_slice in duckdb with negative to_index is python-like, so index itself is exclusive
            if let Ok(index) = index.try_into() {
                index + len - O::usize_as(1)
            } else {
                return exec_err!("array_slice got invalid index: {}", index);
            }
        } else {
            // array_slice(arr, from, len + 1) is the same as array_slice(arr, from, len)
            if let Ok(index) = index.try_into() {
                std::cmp::min(index - O::usize_as(1), len - O::usize_as(1))
            } else {
                return exec_err!("array_slice got invalid index: {}", index);
            }
        };

        if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
            Ok(Some(adjusted_zero_index))
        } else {
            // Out of bounds
            Ok(None)
        }
    }

    let mut offsets = vec![O::usize_as(0)];

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // len 0 indicate array is null, return empty array in this row.
        if len == O::usize_as(0) {
            offsets.push(offsets[row_index]);
            continue;
        }

        // If index is null, we consider it as the minimum / maximum index of the array.
        let from_index = if from_array.is_null(row_index) {
            Some(O::usize_as(0))
        } else {
            adjusted_from_index::<O>(from_array.value(row_index), len)?
        };

        let to_index = if to_array.is_null(row_index) {
            Some(len - O::usize_as(1))
        } else {
            adjusted_to_index::<O>(to_array.value(row_index), len)?
        };

        if let (Some(from), Some(to)) = (from_index, to_index) {
            if from <= to {
                assert!(start + to <= end);
                mutable.extend(
                    0,
                    (start + from).to_usize().unwrap(),
                    (start + to + O::usize_as(1)).to_usize().unwrap(),
                );
                offsets.push(offsets[row_index] + (to - from + O::usize_as(1)));
            } else {
                // invalid range, return empty array
                offsets.push(offsets[row_index]);
            }
        } else {
            // invalid range, return empty array
            offsets.push(offsets[row_index]);
        }
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", array.value_type(), true)),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}

fn general_pop_front_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let from_array = Int64Array::from(vec![2; array.len()]);
    let to_array = Int64Array::from(
        array
            .iter()
            .map(|arr| arr.map_or(0, |arr| arr.len() as i64))
            .collect::<Vec<i64>>(),
    );
    general_array_slice::<O>(array, &from_array, &to_array)
}

fn general_pop_back_list<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let from_array = Int64Array::from(vec![1; array.len()]);
    let to_array = Int64Array::from(
        array
            .iter()
            .map(|arr| arr.map_or(0, |arr| arr.len() as i64 - 1))
            .collect::<Vec<i64>>(),
    );
    general_array_slice::<O>(array, &from_array, &to_array)
}

/// array_pop_front SQL function
pub fn array_pop_front(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array_data_type = args[0].data_type();
    match array_data_type {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            general_pop_front_list::<i32>(array)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_pop_front_list::<i64>(array)
        }
        _ => exec_err!(
            "array_pop_front does not support type: {:?}",
            array_data_type
        ),
    }
}

/// array_pop_back SQL function
pub fn array_pop_back(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_pop_back needs one argument");
    }

    let array_data_type = args[0].data_type();
    match array_data_type {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            general_pop_back_list::<i32>(array)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_pop_back_list::<i64>(array)
        }
        _ => exec_err!(
            "array_pop_back does not support type: {:?}",
            array_data_type
        ),
    }
}

/// Appends or prepends elements to a ListArray.
///
/// This function takes a ListArray, an ArrayRef, a FieldRef, and a boolean flag
/// indicating whether to append or prepend the elements. It returns a `Result<ArrayRef>`
/// representing the resulting ListArray after the operation.
///
/// # Arguments
///
/// * `list_array` - A reference to the ListArray to which elements will be appended/prepended.
/// * `element_array` - A reference to the Array containing elements to be appended/prepended.
/// * `field` - A reference to the Field describing the data type of the arrays.
/// * `is_append` - A boolean flag indicating whether to append (`true`) or prepend (`false`) elements.
///
/// # Examples
///
/// general_append_and_prepend(
///     [1, 2, 3], 4, append => [1, 2, 3, 4]
///     5, [6, 7, 8], prepend => [5, 6, 7, 8]
/// )
fn general_append_and_prepend(
    list_array: &ListArray,
    element_array: &ArrayRef,
    data_type: &DataType,
    is_append: bool,
) -> Result<ArrayRef> {
    let mut offsets = vec![0];
    let values = list_array.values();
    let original_data = values.to_data();
    let element_data = element_array.to_data();
    let capacity = Capacities::Array(original_data.len() + element_data.len());

    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &element_data],
        false,
        capacity,
    );

    let values_index = 0;
    let element_index = 1;

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        let start = offset_window[0] as usize;
        let end = offset_window[1] as usize;
        if is_append {
            mutable.extend(values_index, start, end);
            mutable.extend(element_index, row_index, row_index + 1);
        } else {
            mutable.extend(element_index, row_index, row_index + 1);
            mutable.extend(values_index, start, end);
        }
        offsets.push(offsets[row_index] + (end - start + 1) as i32);
    }

    let data = mutable.freeze();

    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", data_type.to_owned(), true)),
        OffsetBuffer::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?))
}

/// Generates an array of integers from start to stop with a given step.
///
/// This function takes 1 to 3 ArrayRefs as arguments, representing start, stop, and step values.
/// It returns a `Result<ArrayRef>` representing the resulting ListArray after the operation.
///
/// # Arguments
///
/// * `args` - An array of 1 to 3 ArrayRefs representing start, stop, and step(step value can not be zero.) values.
///
/// # Examples
///
/// gen_range(3) => [0, 1, 2]
/// gen_range(1, 4) => [1, 2, 3]
/// gen_range(1, 7, 2) => [1, 3, 5]
pub fn gen_range(args: &[ArrayRef]) -> Result<ArrayRef> {
    let (start_array, stop_array, step_array) = match args.len() {
        1 => (None, as_int64_array(&args[0])?, None),
        2 => (
            Some(as_int64_array(&args[0])?),
            as_int64_array(&args[1])?,
            None,
        ),
        3 => (
            Some(as_int64_array(&args[0])?),
            as_int64_array(&args[1])?,
            Some(as_int64_array(&args[2])?),
        ),
        _ => return internal_err!("gen_range expects 1 to 3 arguments"),
    };

    let mut values = vec![];
    let mut offsets = vec![0];
    for (idx, stop) in stop_array.iter().enumerate() {
        let stop = stop.unwrap_or(0);
        let start = start_array.as_ref().map(|arr| arr.value(idx)).unwrap_or(0);
        let step = step_array.as_ref().map(|arr| arr.value(idx)).unwrap_or(1);
        if step == 0 {
            return exec_err!("step can't be 0 for function range(start [, stop, step]");
        }
        if step < 0 {
            // Decreasing range
            values.extend((stop + 1..start + 1).rev().step_by((-step) as usize));
        } else {
            // Increasing range
            values.extend((start..stop).step_by(step as usize));
        }

        offsets.push(values.len() as i32);
    }
    let arr = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(Int64Array::from(values)),
        None,
    )?);
    Ok(arr)
}

/// Array_append SQL function
pub fn array_append(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_append expects two arguments");
    }

    let list_array = as_list_array(&args[0])?;
    let element_array = &args[1];

    let res = match list_array.value_type() {
        DataType::List(_) => concat_internal(args)?,
        DataType::Null => {
            return make_array(&[
                list_array.values().to_owned(),
                element_array.to_owned(),
            ]);
        }
        data_type => {
            return general_append_and_prepend(
                list_array,
                element_array,
                &data_type,
                true,
            );
        }
    };

    Ok(res)
}

/// Array_sort SQL function
pub fn array_sort(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    let sort_option = match args.len() {
        1 => None,
        2 => {
            let sort = as_string_array(&args[1])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: true,
            })
        }
        3 => {
            let sort = as_string_array(&args[1])?.value(0);
            let nulls_first = as_string_array(&args[2])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: order_nulls_first(nulls_first)?,
            })
        }
        _ => return internal_err!("array_sort expects 1 to 3 arguments"),
    };

    let list_array = as_list_array(&args[0])?;
    let row_count = list_array.len();

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = BooleanBufferBuilder::new(row_count);
    for i in 0..row_count {
        if list_array.is_null(i) {
            array_lengths.push(0);
            valid.append(false);
        } else {
            let arr_ref = list_array.value(i);
            let arr_ref = arr_ref.as_ref();

            let sorted_array = compute::sort(arr_ref, sort_option)?;
            array_lengths.push(sorted_array.len());
            arrays.push(sorted_array);
            valid.append(true);
        }
    }

    // Assume all arrays have the same data type
    let data_type = list_array.value_type();
    let buffer = valid.finish();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = ListArray::new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(compute::concat(elements.as_slice())?),
        Some(NullBuffer::new(buffer)),
    );
    Ok(Arc::new(list_arr))
}

fn order_desc(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "DESC" => Ok(true),
        "ASC" => Ok(false),
        _ => internal_err!("the second parameter of array_sort expects DESC or ASC"),
    }
}

fn order_nulls_first(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "NULLS FIRST" => Ok(true),
        "NULLS LAST" => Ok(false),
        _ => internal_err!(
            "the third parameter of array_sort expects NULLS FIRST or NULLS LAST"
        ),
    }
}

/// Array_prepend SQL function
pub fn array_prepend(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_prepend expects two arguments");
    }

    let list_array = as_list_array(&args[1])?;
    let element_array = &args[0];

    check_datatypes("array_prepend", &[element_array, list_array.values()])?;
    let res = match list_array.value_type() {
        DataType::List(_) => concat_internal(args)?,
        DataType::Null => return make_array(&[element_array.to_owned()]),
        data_type => {
            return general_append_and_prepend(
                list_array,
                element_array,
                &data_type,
                false,
            );
        }
    };

    Ok(res)
}

fn align_array_dimensions(args: Vec<ArrayRef>) -> Result<Vec<ArrayRef>> {
    let args_ndim = args
        .iter()
        .map(|arg| datafusion_common::utils::list_ndims(arg.data_type()))
        .collect::<Vec<_>>();
    let max_ndim = args_ndim.iter().max().unwrap_or(&0);

    // Align the dimensions of the arrays
    let aligned_args: Result<Vec<ArrayRef>> = args
        .into_iter()
        .zip(args_ndim.iter())
        .map(|(array, ndim)| {
            if ndim < max_ndim {
                let mut aligned_array = array.clone();
                for _ in 0..(max_ndim - ndim) {
                    let data_type = aligned_array.data_type().to_owned();
                    let array_lengths = vec![1; aligned_array.len()];
                    let offsets = OffsetBuffer::<i32>::from_lengths(array_lengths);

                    aligned_array = Arc::new(ListArray::try_new(
                        Arc::new(Field::new("item", data_type, true)),
                        offsets,
                        aligned_array,
                        None,
                    )?)
                }
                Ok(aligned_array)
            } else {
                Ok(array.clone())
            }
        })
        .collect();

    aligned_args
}

// Concatenate arrays on the same row.
fn concat_internal(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = align_array_dimensions(args.to_vec())?;

    let list_arrays =
        downcast_vec!(args, ListArray).collect::<Result<Vec<&ListArray>>>()?;

    // Assume number of rows is the same for all arrays
    let row_count = list_arrays[0].len();

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = BooleanBufferBuilder::new(row_count);
    for i in 0..row_count {
        let nulls = list_arrays
            .iter()
            .map(|arr| arr.is_null(i))
            .collect::<Vec<_>>();

        // If all the arrays are null, the concatenated array is null
        let is_null = nulls.iter().all(|&x| x);
        if is_null {
            array_lengths.push(0);
            valid.append(false);
        } else {
            // Get all the arrays on i-th row
            let values = list_arrays
                .iter()
                .map(|arr| arr.value(i))
                .collect::<Vec<_>>();

            let elements = values
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<&dyn Array>>();

            // Concatenated array on i-th row
            let concated_array = compute::concat(elements.as_slice())?;
            array_lengths.push(concated_array.len());
            arrays.push(concated_array);
            valid.append(true);
        }
    }
    // Assume all arrays have the same data type
    let data_type = list_arrays[0].value_type();
    let buffer = valid.finish();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = ListArray::new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(compute::concat(elements.as_slice())?),
        Some(NullBuffer::new(buffer)),
    );

    Ok(Arc::new(list_arr))
}

/// Array_concat/Array_cat SQL function
pub fn array_concat(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("array_concat expects at least one arguments");
    }

    let mut new_args = vec![];
    for arg in args {
        let data_type = arg.data_type();
        if let DataType::List(_) = data_type {
            new_args.push(arg.clone());
        } else if data_type.eq(&DataType::Null) {
            // Null type is valid.
            continue;
        } else {
            return internal_err!("Expect Array type, found {:?}", data_type);
        }
    }

    // All the arguments are null, return null
    if new_args.is_empty() {
        return Ok(new_null_array(&DataType::Null, 0));
    }

    concat_internal(new_args.as_slice())
}

/// Array_empty SQL function
pub fn array_empty(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_empty expects one argument");
    }

    if as_null_array(&args[0]).is_ok() {
        // Make sure to return Boolean type.
        return Ok(Arc::new(BooleanArray::new_null(args[0].len())));
    }
    let array_type = args[0].data_type();

    match array_type {
        DataType::List(_) => array_empty_dispatch::<i32>(&args[0]),
        DataType::LargeList(_) => array_empty_dispatch::<i64>(&args[0]),
        _ => internal_err!("array_empty does not support type '{array_type:?}'."),
    }
}

fn array_empty_dispatch<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let array = as_generic_list_array::<O>(array)?;
    let builder = array
        .iter()
        .map(|arr| arr.map(|arr| arr.len() == arr.null_count()))
        .collect::<BooleanArray>();
    Ok(Arc::new(builder))
}

/// Array_repeat SQL function
pub fn array_repeat(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_repeat expects two arguments");
    }

    let element = &args[0];
    let count_array = as_int64_array(&args[1])?;

    match element.data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(element)?;
            general_list_repeat(list_array, count_array)
        }
        _ => general_repeat(element, count_array),
    }
}

/// For each element of `array[i]` repeat `count_array[i]` times.
///
/// Assumption for the input:
///     1. `count[i] >= 0`
///     2. `array.len() == count_array.len()`
///
/// For example,
/// ```text
/// array_repeat(
///     [1, 2, 3], [2, 0, 1] => [[1, 1], [], [3]]
/// )
/// ```
fn general_repeat(array: &ArrayRef, count_array: &Int64Array) -> Result<ArrayRef> {
    let data_type = array.data_type();
    let mut new_values = vec![];

    let count_vec = count_array
        .values()
        .to_vec()
        .iter()
        .map(|x| *x as usize)
        .collect::<Vec<_>>();

    for (row_index, &count) in count_vec.iter().enumerate() {
        let repeated_array = if array.is_null(row_index) {
            new_null_array(data_type, count)
        } else {
            let original_data = array.to_data();
            let capacity = Capacities::Array(count);
            let mut mutable =
                MutableArrayData::with_capacities(vec![&original_data], false, capacity);

            for _ in 0..count {
                mutable.extend(0, row_index, row_index + 1);
            }

            let data = mutable.freeze();
            arrow_array::make_array(data)
        };
        new_values.push(repeated_array);
    }

    let new_values: Vec<_> = new_values.iter().map(|a| a.as_ref()).collect();
    let values = compute::concat(&new_values)?;

    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", data_type.to_owned(), true)),
        OffsetBuffer::from_lengths(count_vec),
        values,
        None,
    )?))
}

/// Handle List version of `general_repeat`
///
/// For each element of `list_array[i]` repeat `count_array[i]` times.
///
/// For example,
/// ```text
/// array_repeat(
///     [[1, 2, 3], [4, 5], [6]], [2, 0, 1] => [[[1, 2, 3], [1, 2, 3]], [], [[6]]]
/// )
/// ```
fn general_list_repeat(
    list_array: &ListArray,
    count_array: &Int64Array,
) -> Result<ArrayRef> {
    let data_type = list_array.data_type();
    let value_type = list_array.value_type();
    let mut new_values = vec![];

    let count_vec = count_array
        .values()
        .to_vec()
        .iter()
        .map(|x| *x as usize)
        .collect::<Vec<_>>();

    for (list_array_row, &count) in list_array.iter().zip(count_vec.iter()) {
        let list_arr = match list_array_row {
            Some(list_array_row) => {
                let original_data = list_array_row.to_data();
                let capacity = Capacities::Array(original_data.len() * count);
                let mut mutable = MutableArrayData::with_capacities(
                    vec![&original_data],
                    false,
                    capacity,
                );

                for _ in 0..count {
                    mutable.extend(0, 0, original_data.len());
                }

                let data = mutable.freeze();
                let repeated_array = arrow_array::make_array(data);

                let list_arr = ListArray::try_new(
                    Arc::new(Field::new("item", value_type.clone(), true)),
                    OffsetBuffer::from_lengths(vec![original_data.len(); count]),
                    repeated_array,
                    None,
                )?;
                Arc::new(list_arr) as ArrayRef
            }
            None => new_null_array(data_type, count),
        };
        new_values.push(list_arr);
    }

    let lengths = new_values.iter().map(|a| a.len()).collect::<Vec<_>>();
    let new_values: Vec<_> = new_values.iter().map(|a| a.as_ref()).collect();
    let values = compute::concat(&new_values)?;

    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", data_type.to_owned(), true)),
        OffsetBuffer::from_lengths(lengths),
        values,
        None,
    )?))
}

/// Array_position SQL function
pub fn array_position(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }

    let list_array = as_list_array(&args[0])?;
    let element_array = &args[1];

    check_datatypes("array_position", &[list_array.values(), element_array])?;

    let arr_from = if args.len() == 3 {
        as_int64_array(&args[2])?
            .values()
            .to_vec()
            .iter()
            .map(|&x| x - 1)
            .collect::<Vec<_>>()
    } else {
        vec![0; list_array.len()]
    };

    // if `start_from` index is out of bounds, return error
    for (arr, &from) in list_array.iter().zip(arr_from.iter()) {
        if let Some(arr) = arr {
            if from < 0 || from as usize >= arr.len() {
                return internal_err!("start_from index out of bounds");
            }
        } else {
            // We will get null if we got null in the array, so we don't need to check
        }
    }

    general_position::<i32>(list_array, element_array, arr_from)
}

fn general_position<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_from: Vec<i64>, // 0-indexed
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, (list_array_row, &from)) in
        list_array.iter().zip(arr_from.iter()).enumerate()
    {
        let from = from as usize;

        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let index = eq_array
                .iter()
                .skip(from)
                .position(|e| e == Some(true))
                .map(|index| (from + index + 1) as u64);

            data.push(index);
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(UInt64Array::from(data)))
}

/// Array_positions SQL function
pub fn array_positions(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_positions expects two arguments");
    }

    let element = &args[1];

    match &args[0].data_type() {
        DataType::List(_) => {
            let arr = as_list_array(&args[0])?;
            check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i32>(arr, element)
        }
        DataType::LargeList(_) => {
            let arr = as_large_list_array(&args[0])?;
            check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i64>(arr, element)
        }
        array_type => {
            exec_err!("array_positions does not support type '{array_type:?}'.")
        }
    }
}

fn general_positions<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, list_array_row) in list_array.iter().enumerate() {
        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let indexes = eq_array
                .iter()
                .positions(|e| e == Some(true))
                .map(|index| Some(index as u64 + 1))
                .collect::<Vec<_>>();

            data.push(Some(indexes));
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(
        ListArray::from_iter_primitive::<UInt64Type, _, _>(data),
    ))
}

/// For each element of `list_array[i]`, removed up to `arr_n[i]`  occurences
/// of `element_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `element_array`. This function also handles nested arrays
/// ([`ListArray`] of [`ListArray`]s)
///
/// For example, when called to remove a list array (where each element is a
/// list of int32s, the second argument are int32 arrays, and the
/// third argument is the number of occurrences to remove
///
/// ```text
/// general_remove(
///   [1, 2, 3, 2], 2, 1    ==> [1, 3, 2]   (only the first 2 is removed)
///   [4, 5, 6, 5], 5, 2    ==> [4, 6]  (both 5s are removed)
/// )
/// ```
fn general_remove<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    let data_type = list_array.value_type();
    let mut new_values = vec![];
    // Build up the offsets for the final output array
    let mut offsets = Vec::<OffsetSize>::with_capacity(arr_n.len() + 1);
    offsets.push(OffsetSize::zero());

    // n is the number of elements to remove in this row
    for (row_index, (list_array_row, n)) in
        list_array.iter().zip(arr_n.iter()).enumerate()
    {
        match list_array_row {
            Some(list_array_row) => {
                let eq_array = compare_element_to_list(
                    &list_array_row,
                    element_array,
                    row_index,
                    false,
                )?;

                // We need to keep at most first n elements as `false`, which represent the elements to remove.
                let eq_array = if eq_array.false_count() < *n as usize {
                    eq_array
                } else {
                    let mut count = 0;
                    eq_array
                        .iter()
                        .map(|e| {
                            // Keep first n `false` elements, and reverse other elements to `true`.
                            if let Some(false) = e {
                                if count < *n {
                                    count += 1;
                                    e
                                } else {
                                    Some(true)
                                }
                            } else {
                                e
                            }
                        })
                        .collect::<BooleanArray>()
                };

                let filtered_array = arrow::compute::filter(&list_array_row, &eq_array)?;
                offsets.push(
                    offsets[row_index] + OffsetSize::usize_as(filtered_array.len()),
                );
                new_values.push(filtered_array);
            }
            None => {
                // Null element results in a null row (no new offsets)
                offsets.push(offsets[row_index]);
            }
        }
    }

    let values = if new_values.is_empty() {
        new_empty_array(&data_type)
    } else {
        let new_values = new_values.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        arrow::compute::concat(&new_values)?
    };

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        values,
        list_array.nulls().cloned(),
    )?))
}

fn array_remove_internal(
    array: &ArrayRef,
    element_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_remove::<i32>(list_array, element_array, arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_remove::<i64>(list_array, element_array, arr_n)
        }
        _ => internal_err!("array_remove_all expects a list array"),
    }
}

pub fn array_remove_all(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_remove_all expects two arguments");
    }

    let arr_n = vec![i64::MAX; args[0].len()];
    array_remove_internal(&args[0], &args[1], arr_n)
}

pub fn array_remove(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_remove expects two arguments");
    }

    let arr_n = vec![1; args[0].len()];
    array_remove_internal(&args[0], &args[1], arr_n)
}

pub fn array_remove_n(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_remove_n expects three arguments");
    }

    let arr_n = as_int64_array(&args[2])?.values().to_vec();
    array_remove_internal(&args[0], &args[1], arr_n)
}

/// For each element of `list_array[i]`, replaces up to `arr_n[i]`  occurences
/// of `from_array[i]`, `to_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `from_array` and `to_array`. This function also handles nested arrays
/// ([`ListArray`] of [`ListArray`]s)
///
/// For example, when called to replace a list array (where each element is a
/// list of int32s, the second and third argument are int32 arrays, and the
/// fourth argument is the number of occurrences to replace
///
/// ```text
/// general_replace(
///   [1, 2, 3, 2], 2, 10, 1    ==> [1, 10, 3, 2]   (only the first 2 is replaced)
///   [4, 5, 6, 5], 5, 20, 2    ==> [4, 20, 6, 20]  (both 5s are replaced)
/// )
/// ```
fn general_replace<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    from_array: &ArrayRef,
    to_array: &ArrayRef,
    arr_n: Vec<i64>,
) -> Result<ArrayRef> {
    // Build up the offsets for the final output array
    let mut offsets: Vec<O> = vec![O::usize_as(0)];
    let values = list_array.values();
    let original_data = values.to_data();
    let to_data = to_array.to_data();
    let capacity = Capacities::Array(original_data.len());

    // First array is the original array, second array is the element to replace with.
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &to_data],
        false,
        capacity,
    );

    let mut valid = BooleanBufferBuilder::new(list_array.len());

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        if list_array.is_null(row_index) {
            offsets.push(offsets[row_index]);
            valid.append(false);
            continue;
        }

        let start = offset_window[0];
        let end = offset_window[1];

        let list_array_row = list_array.value(row_index);

        // Compute all positions in list_row_array (that is itself an
        // array) that are equal to `from_array_row`
        let eq_array =
            compare_element_to_list(&list_array_row, &from_array, row_index, true)?;

        let original_idx = O::usize_as(0);
        let replace_idx = O::usize_as(1);
        let n = arr_n[row_index];
        let mut counter = 0;

        // All elements are false, no need to replace, just copy original data
        if eq_array.false_count() == eq_array.len() {
            mutable.extend(
                original_idx.to_usize().unwrap(),
                start.to_usize().unwrap(),
                end.to_usize().unwrap(),
            );
            offsets.push(offsets[row_index] + (end - start));
            valid.append(true);
            continue;
        }

        for (i, to_replace) in eq_array.iter().enumerate() {
            let i = O::usize_as(i);
            if let Some(true) = to_replace {
                mutable.extend(replace_idx.to_usize().unwrap(), row_index, row_index + 1);
                counter += 1;
                if counter == n {
                    // copy original data for any matches past n
                    mutable.extend(
                        original_idx.to_usize().unwrap(),
                        (start + i).to_usize().unwrap() + 1,
                        end.to_usize().unwrap(),
                    );
                    break;
                }
            } else {
                // copy original data for false / null matches
                mutable.extend(
                    original_idx.to_usize().unwrap(),
                    (start + i).to_usize().unwrap(),
                    (start + i).to_usize().unwrap() + 1,
                );
            }
        }

        offsets.push(offsets[row_index] + (end - start));
        valid.append(true);
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", list_array.value_type(), true)),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        Some(NullBuffer::new(valid.finish())),
    )?))
}

pub fn array_replace(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_replace expects three arguments");
    }

    // replace at most one occurence for each element
    let arr_n = vec![1; args[0].len()];
    let array = &args[0];
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, &args[1], &args[2], arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, &args[1], &args[2], arr_n)
        }
        array_type => exec_err!("array_replace does not support type '{array_type:?}'."),
    }
}

pub fn array_replace_n(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 4 {
        return exec_err!("array_replace_n expects four arguments");
    }

    // replace the specified number of occurences
    let arr_n = as_int64_array(&args[3])?.values().to_vec();
    let array = &args[0];
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, &args[1], &args[2], arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, &args[1], &args[2], arr_n)
        }
        array_type => {
            exec_err!("array_replace_n does not support type '{array_type:?}'.")
        }
    }
}

pub fn array_replace_all(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_replace_all expects three arguments");
    }

    // replace all occurrences (up to "i64::MAX")
    let arr_n = vec![i64::MAX; args[0].len()];
    let array = &args[0];
    match array.data_type() {
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            general_replace::<i32>(list_array, &args[1], &args[2], arr_n)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_list::<i64>();
            general_replace::<i64>(list_array, &args[1], &args[2], arr_n)
        }
        array_type => {
            exec_err!("array_replace_all does not support type '{array_type:?}'.")
        }
    }
}

macro_rules! to_string {
    ($ARG:expr, $ARRAY:expr, $DELIMITER:expr, $NULL_STRING:expr, $WITH_NULL_STRING:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        for x in arr {
            match x {
                Some(x) => {
                    $ARG.push_str(&x.to_string());
                    $ARG.push_str($DELIMITER);
                }
                None => {
                    if $WITH_NULL_STRING {
                        $ARG.push_str($NULL_STRING);
                        $ARG.push_str($DELIMITER);
                    }
                }
            }
        }
        Ok($ARG)
    }};
}

#[derive(Debug, PartialEq)]
enum SetOp {
    Union,
    Intersect,
}

impl Display for SetOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOp::Union => write!(f, "array_union"),
            SetOp::Intersect => write!(f, "array_intersect"),
        }
    }
}

fn generic_set_lists<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
    set_op: SetOp,
) -> Result<ArrayRef> {
    if matches!(l.value_type(), DataType::Null) {
        let field = Arc::new(Field::new("item", r.value_type(), true));
        return general_array_distinct::<OffsetSize>(r, &field);
    } else if matches!(r.value_type(), DataType::Null) {
        let field = Arc::new(Field::new("item", l.value_type(), true));
        return general_array_distinct::<OffsetSize>(l, &field);
    }

    if l.value_type() != r.value_type() {
        return internal_err!("{set_op:?} is not implemented for '{l:?}' and '{r:?}'");
    }

    let dt = l.value_type();

    let mut offsets = vec![OffsetSize::usize_as(0)];
    let mut new_arrays = vec![];

    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    for (first_arr, second_arr) in l.iter().zip(r.iter()) {
        if let (Some(first_arr), Some(second_arr)) = (first_arr, second_arr) {
            let l_values = converter.convert_columns(&[first_arr])?;
            let r_values = converter.convert_columns(&[second_arr])?;

            let l_iter = l_values.iter().sorted().dedup();
            let values_set: HashSet<_> = l_iter.clone().collect();
            let mut rows = if set_op == SetOp::Union {
                l_iter.collect::<Vec<_>>()
            } else {
                vec![]
            };
            for r_val in r_values.iter().sorted().dedup() {
                match set_op {
                    SetOp::Union => {
                        if !values_set.contains(&r_val) {
                            rows.push(r_val);
                        }
                    }
                    SetOp::Intersect => {
                        if values_set.contains(&r_val) {
                            rows.push(r_val);
                        }
                    }
                }
            }

            let last_offset = match offsets.last().copied() {
                Some(offset) => offset,
                None => return internal_err!("offsets should not be empty"),
            };
            offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
            let arrays = converter.convert_rows(rows)?;
            let array = match arrays.first() {
                Some(array) => array.clone(),
                None => {
                    return internal_err!("{set_op}: failed to get array from rows");
                }
            };
            new_arrays.push(array);
        }
    }

    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    let arr = GenericListArray::<OffsetSize>::try_new(field, offsets, values, None)?;
    Ok(Arc::new(arr))
}

fn general_set_op(
    array1: &ArrayRef,
    array2: &ArrayRef,
    set_op: SetOp,
) -> Result<ArrayRef> {
    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, DataType::List(field)) => {
            if set_op == SetOp::Intersect {
                return Ok(new_empty_array(&DataType::Null));
            }
            let array = as_list_array(&array2)?;
            general_array_distinct::<i32>(array, field)
        }

        (DataType::List(field), DataType::Null) => {
            if set_op == SetOp::Intersect {
                return make_array(&[]);
            }
            let array = as_list_array(&array1)?;
            general_array_distinct::<i32>(array, field)
        }
        (DataType::Null, DataType::LargeList(field)) => {
            if set_op == SetOp::Intersect {
                return Ok(new_empty_array(&DataType::Null));
            }
            let array = as_large_list_array(&array2)?;
            general_array_distinct::<i64>(array, field)
        }
        (DataType::LargeList(field), DataType::Null) => {
            if set_op == SetOp::Intersect {
                return make_array(&[]);
            }
            let array = as_large_list_array(&array1)?;
            general_array_distinct::<i64>(array, field)
        }
        (DataType::Null, DataType::Null) => Ok(new_empty_array(&DataType::Null)),

        (DataType::List(field), DataType::List(_)) => {
            let array1 = as_list_array(&array1)?;
            let array2 = as_list_array(&array2)?;
            generic_set_lists::<i32>(array1, array2, field.clone(), set_op)
        }
        (DataType::LargeList(field), DataType::LargeList(_)) => {
            let array1 = as_large_list_array(&array1)?;
            let array2 = as_large_list_array(&array2)?;
            generic_set_lists::<i64>(array1, array2, field.clone(), set_op)
        }
        (data_type1, data_type2) => {
            internal_err!(
                "{set_op} does not support types '{data_type1:?}' and '{data_type2:?}'"
            )
        }
    }
}

/// Array_union SQL function
pub fn array_union(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_union needs two arguments");
    }
    let array1 = &args[0];
    let array2 = &args[1];

    general_set_op(array1, array2, SetOp::Union)
}

/// array_intersect SQL function
pub fn array_intersect(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_intersect needs two arguments");
    }

    let array1 = &args[0];
    let array2 = &args[1];

    general_set_op(array1, array2, SetOp::Intersect)
}

/// Array_to_string SQL function
pub fn array_to_string(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_to_string expects two or three arguments");
    }

    let arr = &args[0];

    let delimiters = as_string_array(&args[1])?;
    let delimiters: Vec<Option<&str>> = delimiters.iter().collect();

    let mut null_string = String::from("");
    let mut with_null_string = false;
    if args.len() == 3 {
        null_string = as_string_array(&args[2])?.value(0).to_string();
        with_null_string = true;
    }

    fn compute_array_to_string(
        arg: &mut String,
        arr: ArrayRef,
        delimiter: String,
        null_string: String,
        with_null_string: bool,
    ) -> Result<&mut String> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(arr, ListArray);

                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            DataType::Null => Ok(arg),
            data_type => {
                macro_rules! array_function {
                    ($ARRAY_TYPE:ident) => {
                        to_string!(
                            arg,
                            arr,
                            &delimiter,
                            &null_string,
                            with_null_string,
                            $ARRAY_TYPE
                        )
                    };
                }
                call_array_function!(data_type, false)
            }
        }
    }

    let mut arg = String::from("");
    let mut res: Vec<Option<String>> = Vec::new();

    match arr.data_type() {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let list_array = arr.as_list::<i32>();
            for (arr, &delimiter) in list_array.iter().zip(delimiters.iter()) {
                if let (Some(arr), Some(delimiter)) = (arr, delimiter) {
                    arg = String::from("");
                    let s = compute_array_to_string(
                        &mut arg,
                        arr,
                        delimiter.to_string(),
                        null_string.clone(),
                        with_null_string,
                    )?
                    .clone();

                    if let Some(s) = s.strip_suffix(delimiter) {
                        res.push(Some(s.to_string()));
                    } else {
                        res.push(Some(s));
                    }
                } else {
                    res.push(None);
                }
            }
        }
        _ => {
            // delimiter length is 1
            assert_eq!(delimiters.len(), 1);
            let delimiter = delimiters[0].unwrap();
            let s = compute_array_to_string(
                &mut arg,
                arr.clone(),
                delimiter.to_string(),
                null_string,
                with_null_string,
            )?
            .clone();

            if !s.is_empty() {
                let s = s.strip_suffix(delimiter).unwrap().to_string();
                res.push(Some(s));
            } else {
                res.push(Some(s));
            }
        }
    }

    Ok(Arc::new(StringArray::from(res)))
}

/// Cardinality SQL function
pub fn cardinality(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("cardinality expects one argument");
    }

    let list_array = as_list_array(&args[0])?.clone();

    let result = list_array
        .iter()
        .map(|arr| match compute_array_dims(arr)? {
            Some(vector) => Ok(Some(vector.iter().map(|x| x.unwrap()).product::<u64>())),
            None => Ok(None),
        })
        .collect::<Result<UInt64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

// Create new offsets that are euqiavlent to `flatten` the array.
fn get_offsets_for_flatten(
    offsets: OffsetBuffer<i32>,
    indexes: OffsetBuffer<i32>,
) -> OffsetBuffer<i32> {
    let buffer = offsets.into_inner();
    let offsets: Vec<i32> = indexes.iter().map(|i| buffer[*i as usize]).collect();
    OffsetBuffer::new(offsets.into())
}

fn flatten_internal(
    array: &dyn Array,
    indexes: Option<OffsetBuffer<i32>>,
) -> Result<ListArray> {
    let list_arr = as_list_array(array)?;
    let (field, offsets, values, _) = list_arr.clone().into_parts();
    let data_type = field.data_type();

    match data_type {
        // Recursively get the base offsets for flattened array
        DataType::List(_) => {
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                flatten_internal(&values, Some(offsets))
            } else {
                flatten_internal(&values, Some(offsets))
            }
        }
        // Reach the base level, create a new list array
        _ => {
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                let list_arr = ListArray::new(field, offsets, values, None);
                Ok(list_arr)
            } else {
                Ok(list_arr.clone())
            }
        }
    }
}

/// Flatten SQL function
pub fn flatten(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("flatten expects one argument");
    }

    let flattened_array = flatten_internal(&args[0], None)?;
    Ok(Arc::new(flattened_array) as ArrayRef)
}

/// Dispatch array length computation based on the offset type.
fn array_length_dispatch<O: OffsetSizeTrait>(array: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&array[0])?;
    let dimension = if array.len() == 2 {
        as_int64_array(&array[1])?.clone()
    } else {
        Int64Array::from_value(1, list_array.len())
    };

    let result = list_array
        .iter()
        .zip(dimension.iter())
        .map(|(arr, dim)| compute_array_length(arr, dim))
        .collect::<Result<UInt64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_length SQL function
pub fn array_length(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!("array_length expects one or two arguments");
    }

    match &args[0].data_type() {
        DataType::List(_) => array_length_dispatch::<i32>(args),
        DataType::LargeList(_) => array_length_dispatch::<i64>(args),
        _ => internal_err!(
            "array_length does not support type '{:?}'",
            args[0].data_type()
        ),
    }
}

/// Array_dims SQL function
pub fn array_dims(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_dims needs one argument");
    }

    let data = match args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        _ => {
            return exec_err!(
                "array_dims does not support type '{:?}'",
                args[0].data_type()
            );
        }
    };

    let result = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_ndims SQL function
pub fn array_ndims(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_ndims needs one argument");
    }

    if let Some(list_array) = args[0].as_list_opt::<i32>() {
        let ndims = datafusion_common::utils::list_ndims(list_array.data_type());

        let mut data = vec![];
        for arr in list_array.iter() {
            if arr.is_some() {
                data.push(Some(ndims))
            } else {
                data.push(None)
            }
        }

        Ok(Arc::new(UInt64Array::from(data)) as ArrayRef)
    } else {
        Ok(Arc::new(UInt64Array::from(vec![0; args[0].len()])) as ArrayRef)
    }
}

/// Represents the type of comparison for array_has.
#[derive(Debug, PartialEq)]
enum ComparisonType {
    // array_has_all
    All,
    // array_has_any
    Any,
    // array_has
    Single,
}

fn general_array_has_dispatch<O: OffsetSizeTrait>(
    array: &ArrayRef,
    sub_array: &ArrayRef,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let array = if comparison_type == ComparisonType::Single {
        let arr = as_generic_list_array::<O>(array)?;
        check_datatypes("array_has", &[arr.values(), sub_array])?;
        arr
    } else {
        check_datatypes("array_has", &[array, sub_array])?;
        as_generic_list_array::<O>(array)?
    };

    let mut boolean_builder = BooleanArray::builder(array.len());

    let converter = RowConverter::new(vec![SortField::new(array.value_type())])?;

    let element = sub_array.clone();
    let sub_array = if comparison_type != ComparisonType::Single {
        as_generic_list_array::<O>(sub_array)?
    } else {
        array
    };

    for (row_idx, (arr, sub_arr)) in array.iter().zip(sub_array.iter()).enumerate() {
        if let (Some(arr), Some(sub_arr)) = (arr, sub_arr) {
            let arr_values = converter.convert_columns(&[arr])?;
            let sub_arr_values = if comparison_type != ComparisonType::Single {
                converter.convert_columns(&[sub_arr])?
            } else {
                converter.convert_columns(&[element.clone()])?
            };

            let mut res = match comparison_type {
                ComparisonType::All => sub_arr_values
                    .iter()
                    .dedup()
                    .all(|elem| arr_values.iter().dedup().any(|x| x == elem)),
                ComparisonType::Any => sub_arr_values
                    .iter()
                    .dedup()
                    .any(|elem| arr_values.iter().dedup().any(|x| x == elem)),
                ComparisonType::Single => arr_values
                    .iter()
                    .dedup()
                    .any(|x| x == sub_arr_values.row(row_idx)),
            };

            if comparison_type == ComparisonType::Any {
                res |= res;
            }

            boolean_builder.append_value(res);
        }
    }
    Ok(Arc::new(boolean_builder.finish()))
}

/// Array_has SQL function
pub fn array_has(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_has needs two arguments");
    }

    let array_type = args[0].data_type();

    match array_type {
        DataType::List(_) => {
            general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::Single)
        }
        DataType::LargeList(_) => {
            general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::Single)
        }
        _ => exec_err!("array_has does not support type '{array_type:?}'."),
    }
}

/// Array_has_any SQL function
pub fn array_has_any(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_has_any needs two arguments");
    }

    let array_type = args[0].data_type();

    match array_type {
        DataType::List(_) => {
            general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::Any)
        }
        DataType::LargeList(_) => {
            general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::Any)
        }
        _ => internal_err!("array_has_any does not support type '{array_type:?}'."),
    }
}

/// Array_has_all SQL function
pub fn array_has_all(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_has_all needs two arguments");
    }

    let array_type = args[0].data_type();

    match array_type {
        DataType::List(_) => {
            general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::All)
        }
        DataType::LargeList(_) => {
            general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::All)
        }
        _ => internal_err!("array_has_all does not support type '{array_type:?}'."),
    }
}

/// Splits string at occurrences of delimiter and returns an array of parts
/// string_to_array('abc~@~def~@~ghi', '~@~') = '["abc", "def", "ghi"]'
pub fn string_to_array<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;

    let mut list_builder = ListBuilder::new(StringBuilder::with_capacity(
        string_array.len(),
        string_array.get_buffer_memory_size(),
    ));

    match args.len() {
        2 => {
            string_array.iter().zip(delimiter_array.iter()).for_each(
                |(string, delimiter)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            list_builder.values().append_value(string);
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                list_builder.values().append_value(s);
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                list_builder.values().append_value(c);
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                },
            );
        }

        3 => {
            let null_value_array = as_generic_string_array::<T>(&args[2])?;
            string_array
                .iter()
                .zip(delimiter_array.iter())
                .zip(null_value_array.iter())
                .for_each(|((string, delimiter), null_value)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            if Some(string) == null_value {
                                list_builder.values().append_null();
                            } else {
                                list_builder.values().append_value(string);
                            }
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                if Some(s) == null_value {
                                    list_builder.values().append_null();
                                } else {
                                    list_builder.values().append_value(s);
                                }
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                if Some(c.as_str()) == null_value {
                                    list_builder.values().append_null();
                                } else {
                                    list_builder.values().append_value(c);
                                }
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                });
        }
        _ => {
            return internal_err!(
                "Expect string_to_array function to take two or three parameters"
            )
        }
    }

    let list_array = list_builder.finish();
    Ok(Arc::new(list_array) as ArrayRef)
}

pub fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len());
    offsets.push(OffsetSize::usize_as(0));
    let mut new_arrays = Vec::with_capacity(array.len());
    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    // distinct for each list in ListArray
    for arr in array.iter().flatten() {
        let values = converter.convert_columns(&[arr])?;
        // sort elements in list and remove duplicates
        let rows = values.iter().sorted().dedup().collect::<Vec<_>>();
        let last_offset: OffsetSize = offsets.last().copied().unwrap();
        offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
        let arrays = converter.convert_rows(rows)?;
        let array = match arrays.first() {
            Some(array) => array.clone(),
            None => {
                return internal_err!("array_distinct: failed to get array from rows")
            }
        };
        new_arrays.push(array);
    }
    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field.clone(),
        offsets,
        values,
        None,
    )?))
}

/// array_distinct SQL function
/// example: from list [1, 3, 2, 3, 1, 2, 4] to [1, 2, 3, 4]
pub fn array_distinct(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_distinct needs one argument");
    }

    // handle null
    if args[0].data_type() == &DataType::Null {
        return Ok(args[0].clone());
    }

    // handle for list & largelist
    match args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0])?;
            general_array_distinct(array, field)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0])?;
            general_array_distinct(array, field)
        }
        _ => internal_err!("array_distinct only support list array"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Int64Type;

    /// Only test internal functions, array-related sql functions will be tested in sqllogictest `array.slt`
    #[test]
    fn test_align_array_dimensions() {
        let array1d_1 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
            ]));
        let array1d_2 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(6), Some(7), Some(8)]),
            ]));

        let array2d_1 = Arc::new(array_into_list_array(array1d_1.clone())) as ArrayRef;
        let array2d_2 = Arc::new(array_into_list_array(array1d_2.clone())) as ArrayRef;

        let res =
            align_array_dimensions(vec![array1d_1.to_owned(), array2d_2.to_owned()])
                .unwrap();

        let expected = as_list_array(&array2d_1).unwrap();
        let expected_dim = datafusion_common::utils::list_ndims(array2d_1.data_type());
        assert_ne!(as_list_array(&res[0]).unwrap(), expected);
        assert_eq!(
            datafusion_common::utils::list_ndims(res[0].data_type()),
            expected_dim
        );

        let array3d_1 = Arc::new(array_into_list_array(array2d_1)) as ArrayRef;
        let array3d_2 = array_into_list_array(array2d_2.to_owned());
        let res =
            align_array_dimensions(vec![array1d_1, Arc::new(array3d_2.clone())]).unwrap();

        let expected = as_list_array(&array3d_1).unwrap();
        let expected_dim = datafusion_common::utils::list_ndims(array3d_1.data_type());
        assert_ne!(as_list_array(&res[0]).unwrap(), expected);
        assert_eq!(
            datafusion_common::utils::list_ndims(res[0].data_type()),
            expected_dim
        );
    }
}
