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

//! [`ScalarUDFImpl`] definitions for cardinality function.

use crate::utils::make_scalar_function;
use arrow_array::{ArrayRef, GenericListArray, OffsetSizeTrait, UInt64Array};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, LargeList, List, UInt64};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::Result;
use datafusion_common::{exec_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

make_udf_function!(
    Cardinality,
    cardinality,
    array,
    "returns the total number of elements in the array.",
    cardinality_udf
);

impl Cardinality {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![String::from("cardinality")],
        }
    }
}

#[derive(Debug)]
pub(super) struct Cardinality {
    signature: Signature,
    aliases: Vec<String>,
}
impl ScalarUDFImpl for Cardinality {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "cardinality"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => UInt64,
            _ => {
                return plan_err!("The cardinality function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(cardinality_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Cardinality SQL function
pub fn cardinality_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("cardinality expects one argument");
    }

    match &args[0].data_type() {
        List(_) => {
            let list_array = as_list_array(&args[0])?;
            generic_list_cardinality::<i32>(list_array)
        }
        LargeList(_) => {
            let list_array = as_large_list_array(&args[0])?;
            generic_list_cardinality::<i64>(list_array)
        }
        other => {
            exec_err!("cardinality does not support type '{:?}'", other)
        }
    }
}

fn generic_list_cardinality<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let result = array
        .iter()
        .map(|arr| match crate::utils::compute_array_dims(arr)? {
            Some(vector) => Ok(Some(vector.iter().map(|x| x.unwrap()).product::<u64>())),
            None => Ok(None),
        })
        .collect::<Result<UInt64Array>>()?;
    Ok(Arc::new(result) as ArrayRef)
}
