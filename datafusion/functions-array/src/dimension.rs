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

//! [`ScalarUDFImpl`] definitions for array_dims and array_ndims functions.

use arrow::array::{
    Array, ArrayRef, GenericListArray, ListArray, OffsetSizeTrait, UInt64Array,
};
use arrow::datatypes::{DataType, UInt64Type};
use std::any::Any;

use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{exec_err, plan_err, Result};

use crate::utils::{compute_array_dims, make_scalar_function};
use arrow_schema::DataType::{FixedSizeList, LargeList, List, UInt64};
use arrow_schema::Field;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

make_udf_function!(
    ArrayDims,
    array_dims,
    array,
    "returns an array of the array's dimensions.",
    array_dims_udf
);

#[derive(Debug)]
pub(super) struct ArrayDims {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDims {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["array_dims".to_string(), "list_dims".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayDims {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_dims"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => {
                List(Arc::new(Field::new("item", UInt64, true)))
            }
            _ => {
                return plan_err!("The array_dims function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_dims_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    ArrayNdims,
    array_ndims,
    array,
    "returns the number of dimensions of the array.",
    array_ndims_udf
);

#[derive(Debug)]
pub(super) struct ArrayNdims {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayNdims {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("array_ndims"), String::from("list_ndims")],
        }
    }
}

impl ScalarUDFImpl for ArrayNdims {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_ndims"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The array_ndims function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_ndims_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_dims SQL function
pub fn array_dims_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_dims needs one argument");
    }

    let data = match args[0].data_type() {
        List(_) => {
            let array = as_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        array_type => {
            return exec_err!("array_dims does not support type '{array_type:?}'");
        }
    };

    let result = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_ndims SQL function
pub fn array_ndims_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_ndims needs one argument");
    }

    fn general_list_ndims<O: OffsetSizeTrait>(
        array: &GenericListArray<O>,
    ) -> Result<ArrayRef> {
        let mut data = Vec::new();
        let ndims = datafusion_common::utils::list_ndims(array.data_type());

        for arr in array.iter() {
            if arr.is_some() {
                data.push(Some(ndims))
            } else {
                data.push(None)
            }
        }

        Ok(Arc::new(UInt64Array::from(data)) as ArrayRef)
    }
    match args[0].data_type() {
        List(_) => {
            let array = as_list_array(&args[0])?;
            general_list_ndims::<i32>(array)
        }
        LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_list_ndims::<i64>(array)
        }
        array_type => exec_err!("array_ndims does not support type {array_type:?}"),
    }
}
