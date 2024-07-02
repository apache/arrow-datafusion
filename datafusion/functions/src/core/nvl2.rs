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

use arrow::array::Array;
use arrow::compute::is_not_null;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::{
    type_coercion::binary::comparison_coercion, ColumnarValue, ScalarUDFImpl, Signature,
    Volatility,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct NVL2Func {
    signature: Signature,
}

impl Default for NVL2Func {
    fn default() -> Self {
        Self::new()
    }
}

impl NVL2Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NVL2Func {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nvl2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        nvl2_func(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return exec_err!(
                "NVL2 takes exactly three arguments, but got {}",
                arg_types.len()
            );
        }
        let new_type = arg_types.iter().skip(1).try_fold(
            arg_types.first().unwrap().clone(),
            |acc, x| {
                // The coerced types found by `comparison_coercion` are not guaranteed to be
                // coercible for the arguments. `comparison_coercion` returns more loose
                // types that can be coerced to both `acc` and `x` for comparison purpose.
                // See `maybe_data_types` for the actual coercion.
                let coerced_type = comparison_coercion(&acc, x);
                if let Some(coerced_type) = coerced_type {
                    Ok(coerced_type)
                } else {
                    internal_err!("Coercion from {acc:?} to {x:?} failed.")
                }
            },
        )?;
        Ok(vec![new_type; arg_types.len()])
    }
}

fn nvl2_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return internal_err!(
            "{:?} args were supplied but NVL2 takes exactly three args",
            args.len()
        );
    }
    let mut len = 1;
    let mut is_array = false;
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            len = array.len();
            is_array = true;
            break;
        }
    }
    if is_array {
        let args = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len),
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
            })
            .collect::<Result<Vec<_>>>()?;
        let to_apply = is_not_null(&args[0])?;
        let value = zip(&to_apply, &args[1], &args[2])?;
        Ok(ColumnarValue::Array(value))
    } else {
        let mut current_value = &args[1];
        match &args[0] {
            ColumnarValue::Array(_) => {
                internal_err!("except Scalar value, but got Array")
            }
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    current_value = &args[2];
                }
                Ok(current_value.clone())
            }
        }
    }
}
