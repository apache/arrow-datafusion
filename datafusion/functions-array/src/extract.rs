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

//! [`ScalarUDFImpl`] definitions for array_element, array_slice, array_pop_front and array_pop_back functions.

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::ArrowNativeTypeOp;
use arrow::array::Capacities;
use arrow::array::GenericListArray;
use arrow::array::Int64Array;
use arrow::array::MutableArrayData;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow_schema::DataType::{FixedSizeList, LargeList, List};
use arrow_schema::Field;
use datafusion_common::cast::as_int64_array;
use datafusion_common::cast::as_large_list_array;
use datafusion_common::cast::as_list_array;
use datafusion_common::{
    exec_err, internal_datafusion_err, plan_err, DataFusionError, Result,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

use crate::utils::make_scalar_function;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    ArrayElement,
    array_element,
    array element,
    "extracts the element with the index n from the array.",
    array_element_udf
);

make_udf_function!(
    ArraySlice,
    array_slice,
    array begin end stride,
    "returns a slice of the array.",
    array_slice_udf
);

make_udf_function!(
    ArrayPopFront,
    array_pop_front,
    array,
    "returns the array without the first element.",
    array_pop_front_udf
);

make_udf_function!(
    ArrayPopBack,
    array_pop_back,
    array,
    "returns the array without the last element.",
    array_pop_back_udf
);

#[derive(Debug)]
pub(super) struct ArrayElement {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayElement {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_index(Volatility::Immutable),
            aliases: vec![
                String::from("array_element"),
                String::from("array_extract"),
                String::from("list_element"),
                String::from("list_extract"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayElement {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_element"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(field)
            | LargeList(field)
            | FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!(
                "ArrayElement can only accept List, LargeList or FixedSizeList as the first argument"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_element_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// array_element SQL function
///
/// There are two arguments for array_element, the first one is the array, the second one is the 1-indexed index.
/// `array_element(array, index)`
///
/// For example:
/// > array_element(\[1, 2, 3], 2) -> 2
fn array_element_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_element needs two arguments");
    }

    match &args[0].data_type() {
        List(_) => {
            let array = as_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i32>(array, indexes)
        }
        LargeList(_) => {
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
    Ok(arrow::array::make_array(data))
}

#[derive(Debug)]
pub(super) struct ArraySlice {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArraySlice {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("array_slice"), String::from("list_slice")],
        }
    }
}

impl ScalarUDFImpl for ArraySlice {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_slice_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
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
fn array_slice_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if args_len != 3 && args_len != 4 {
        return exec_err!("array_slice needs three or four arguments");
    }

    let stride = if args_len == 4 {
        Some(as_int64_array(&args[3])?)
    } else {
        None
    };

    let from_array = as_int64_array(&args[1])?;
    let to_array = as_int64_array(&args[2])?;

    let array_data_type = args[0].data_type();
    match array_data_type {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_array_slice::<i32>(array, from_array, to_array, stride)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            let from_array = as_int64_array(&args[1])?;
            let to_array = as_int64_array(&args[2])?;
            general_array_slice::<i64>(array, from_array, to_array, stride)
        }
        _ => exec_err!("array_slice does not support type: {:?}", array_data_type),
    }
}

fn general_array_slice<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    from_array: &Int64Array,
    to_array: &Int64Array,
    stride: Option<&Int64Array>,
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
                index + len
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
            let stride = stride.map(|s| s.value(row_index));
            // array_slice with stride in duckdb, return empty array if stride is not supported and from > to.
            if stride.is_none() && from > to {
                // return empty array
                offsets.push(offsets[row_index]);
                continue;
            }
            let stride = stride.unwrap_or(1);
            if stride.is_zero() {
                return exec_err!(
                    "array_slice got invalid stride: {:?}, it cannot be 0",
                    stride
                );
            } else if from <= to && stride.is_negative() {
                // return empty array
                offsets.push(offsets[row_index]);
                continue;
            }

            let stride: O = stride.try_into().map_err(|_| {
                internal_datafusion_err!("array_slice got invalid stride: {}", stride)
            })?;

            if from <= to {
                assert!(start + to <= end);
                if stride.eq(&O::one()) {
                    // stride is default to 1
                    mutable.extend(
                        0,
                        (start + from).to_usize().unwrap(),
                        (start + to + O::usize_as(1)).to_usize().unwrap(),
                    );
                    offsets.push(offsets[row_index] + (to - from + O::usize_as(1)));
                    continue;
                }
                let mut index = start + from;
                let mut cnt = 0;
                while index <= start + to {
                    mutable.extend(
                        0,
                        index.to_usize().unwrap(),
                        index.to_usize().unwrap() + 1,
                    );
                    index += stride;
                    cnt += 1;
                }
                offsets.push(offsets[row_index] + O::usize_as(cnt));
            } else {
                let mut index = start + from;
                let mut cnt = 0;
                while index >= start + to {
                    mutable.extend(
                        0,
                        index.to_usize().unwrap(),
                        index.to_usize().unwrap() + 1,
                    );
                    index += stride;
                    cnt += 1;
                }
                // invalid range, return empty array
                offsets.push(offsets[row_index] + O::usize_as(cnt));
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

#[derive(Debug)]
pub(super) struct ArrayPopFront {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPopFront {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![
                String::from("array_pop_front"),
                String::from("list_pop_front"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPopFront {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_pop_front"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_pop_front_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// array_pop_front SQL function
fn array_pop_front_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array_data_type = args[0].data_type();
    match array_data_type {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_pop_front_list::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_pop_front_list::<i64>(array)
        }
        _ => exec_err!(
            "array_pop_front does not support type: {:?}",
            array_data_type
        ),
    }
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
    general_array_slice::<O>(array, &from_array, &to_array, None)
}

#[derive(Debug)]
pub(super) struct ArrayPopBack {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPopBack {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![
                String::from("array_pop_back"),
                String::from("list_pop_back"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPopBack {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_pop_back"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_pop_back_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// array_pop_back SQL function
fn array_pop_back_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_pop_back needs one argument");
    }

    let array_data_type = args[0].data_type();
    match array_data_type {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_pop_back_list::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_pop_back_list::<i64>(array)
        }
        _ => exec_err!(
            "array_pop_back does not support type: {:?}",
            array_data_type
        ),
    }
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
    general_array_slice::<O>(array, &from_array, &to_array, None)
}
