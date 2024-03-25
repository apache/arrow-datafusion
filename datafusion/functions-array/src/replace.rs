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

//! [`ScalarUDFImpl`] definitions for array_replace, array_replace_n and array_replace_all functions.

use arrow::array::{
    Array, ArrayRef, AsArray, Capacities, MutableArrayData, OffsetSizeTrait,
};
use arrow::datatypes::DataType;

use arrow_array::GenericListArray;
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::compare_element_to_list;
use crate::utils::make_scalar_function;

use std::any::Any;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_function!(ArrayReplace,
    array_replace,
    array from to,
    "replaces the first occurrence of the specified element with another specified element.",
    array_replace_udf
);
make_udf_function!(ArrayReplaceN,
    array_replace_n,
    array from to max,
    "replaces the first `max` occurrences of the specified element with another specified element.",
    array_replace_n_udf
);
make_udf_function!(ArrayReplaceAll,
    array_replace_all,
    array from to,
    "replaces all occurrences of the specified element with another specified element.",
    array_replace_all_udf
);

#[derive(Debug)]
pub(super) struct ArrayReplace {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayReplace {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            aliases: vec![String::from("array_replace"), String::from("list_replace")],
        }
    }
}

impl ScalarUDFImpl for ArrayReplace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_replace_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayReplaceN {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayReplaceN {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
            aliases: vec![
                String::from("array_replace_n"),
                String::from("list_replace_n"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayReplaceN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_replace_n"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_replace_n_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayReplaceAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayReplaceAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            aliases: vec![
                String::from("array_replace_all"),
                String::from("list_replace_all"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayReplaceAll {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_replace_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_replace_all_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// For each element of `list_array[i]`, replaces up to `arr_n[i]`  occurrences
/// of `from_array[i]`, `to_array[i]`.
///
/// The type of each **element** in `list_array` must be the same as the type of
/// `from_array` and `to_array`. This function also handles nested arrays
/// (\[`ListArray`\] of \[`ListArray`\]s)
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

pub(crate) fn array_replace_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("array_replace expects three arguments");
    }

    // replace at most one occurrence for each element
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

pub(crate) fn array_replace_n_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 4 {
        return exec_err!("array_replace_n expects four arguments");
    }

    // replace the specified number of occurrences
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

pub(crate) fn array_replace_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
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
