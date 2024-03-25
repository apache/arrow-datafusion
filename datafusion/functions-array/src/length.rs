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

//! [`ScalarUDFImpl`] definitions for array_length function.

use crate::utils::{downcast_arg, make_scalar_function};
use arrow_array::{
    Array, ArrayRef, Int64Array, LargeListArray, ListArray, OffsetSizeTrait, UInt64Array,
};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, LargeList, List, UInt64};
use core::any::type_name;
use datafusion_common::cast::{as_generic_list_array, as_int64_array};
use datafusion_common::DataFusionError;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

make_udf_function!(
    ArrayLength,
    array_length,
    array,
    "returns the length of the array dimension.",
    array_length_udf
);

#[derive(Debug)]
pub(super) struct ArrayLength {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayLength {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("array_length"), String::from("list_length")],
        }
    }
}

impl ScalarUDFImpl for ArrayLength {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The array_length function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_length_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_length SQL function
pub fn array_length_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!("array_length expects one or two arguments");
    }

    match &args[0].data_type() {
        List(_) => general_array_length::<i32>(args),
        LargeList(_) => general_array_length::<i64>(args),
        array_type => exec_err!("array_length does not support type '{array_type:?}'"),
    }
}

/// Dispatch array length computation based on the offset type.
fn general_array_length<O: OffsetSizeTrait>(array: &[ArrayRef]) -> Result<ArrayRef> {
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
            List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                current_dimension += 1;
            }
            LargeList(..) => {
                value = downcast_arg!(value, LargeListArray).value(0);
                current_dimension += 1;
            }
            _ => return Ok(None),
        }
    }
}
