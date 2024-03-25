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

//! [`ScalarUDFImpl`] definitions for array_sort function.

use crate::utils::make_scalar_function;
use arrow::compute;
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::DataType::{FixedSizeList, LargeList, List};
use arrow_schema::{DataType, Field, SortOptions};
use datafusion_common::cast::{as_list_array, as_string_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

make_udf_function!(
    ArraySort,
    array_sort,
    array desc null_first,
    "returns sorted array.",
    array_sort_udf
);

#[derive(Debug)]
pub(super) struct ArraySort {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArraySort {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["array_sort".to_string(), "list_sort".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySort {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(field) | FixedSizeList(field, _) => Ok(List(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            LargeList(field) => Ok(LargeList(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_sort_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_sort SQL function
pub fn array_sort_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
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
        _ => return exec_err!("array_sort expects 1 to 3 arguments"),
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
        _ => exec_err!("the second parameter of array_sort expects DESC or ASC"),
    }
}

fn order_nulls_first(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "NULLS FIRST" => Ok(true),
        "NULLS LAST" => Ok(false),
        _ => exec_err!(
            "the third parameter of array_sort expects NULLS FIRST or NULLS LAST"
        ),
    }
}
