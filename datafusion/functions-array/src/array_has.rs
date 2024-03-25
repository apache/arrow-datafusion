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

//! [`ScalarUDFImpl`] definitions for array_has, array_has_all and array_has_any functions.

use arrow::array::{Array, ArrayRef, BooleanArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, SortField};
use datafusion_common::cast::as_generic_list_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use itertools::Itertools;

use crate::utils::check_datatypes;

use std::any::Any;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_function!(ArrayHas,
    array_has,
    first_array second_array, // arg name
    "returns true, if the element appears in the first array, otherwise false.", // doc
    array_has_udf // internal function name
);
make_udf_function!(ArrayHasAll,
    array_has_all,
    first_array second_array, // arg name
    "returns true if each element of the second array appears in the first array; otherwise, it returns false.", // doc
    array_has_all_udf // internal function name
);
make_udf_function!(ArrayHasAny,
    array_has_any,
    first_array second_array, // arg name
    "returns true if at least one element of the second array appears in the first array; otherwise, it returns false.", // doc
    array_has_any_udf // internal function name
);

#[derive(Debug)]
pub(super) struct ArrayHas {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayHas {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![
                String::from("array_has"),
                String::from("list_has"),
                String::from("array_contains"),
                String::from("list_contains"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayHas {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if args.len() != 2 {
            return exec_err!("array_has needs two arguments");
        }

        let array_type = args[0].data_type();

        match array_type {
            DataType::List(_) => general_array_has_dispatch::<i32>(
                &args[0],
                &args[1],
                ComparisonType::Single,
            )
            .map(ColumnarValue::Array),
            DataType::LargeList(_) => general_array_has_dispatch::<i64>(
                &args[0],
                &args[1],
                ComparisonType::Single,
            )
            .map(ColumnarValue::Array),
            _ => exec_err!("array_has does not support type '{array_type:?}'."),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayHasAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayHasAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("array_has_all"), String::from("list_has_all")],
        }
    }
}

impl ScalarUDFImpl for ArrayHasAll {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        if args.len() != 2 {
            return exec_err!("array_has_all needs two arguments");
        }

        let array_type = args[0].data_type();

        match array_type {
            DataType::List(_) => {
                general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::All)
                    .map(ColumnarValue::Array)
            }
            DataType::LargeList(_) => {
                general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::All)
                    .map(ColumnarValue::Array)
            }
            _ => exec_err!("array_has_all does not support type '{array_type:?}'."),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayHasAny {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayHasAny {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("array_has_any"), String::from("list_has_any")],
        }
    }
}

impl ScalarUDFImpl for ArrayHasAny {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has_any"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if args.len() != 2 {
            return exec_err!("array_has_any needs two arguments");
        }

        let array_type = args[0].data_type();

        match array_type {
            DataType::List(_) => {
                general_array_has_dispatch::<i32>(&args[0], &args[1], ComparisonType::Any)
                    .map(ColumnarValue::Array)
            }
            DataType::LargeList(_) => {
                general_array_has_dispatch::<i64>(&args[0], &args[1], ComparisonType::Any)
                    .map(ColumnarValue::Array)
            }
            _ => exec_err!("array_has_any does not support type '{array_type:?}'."),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
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
