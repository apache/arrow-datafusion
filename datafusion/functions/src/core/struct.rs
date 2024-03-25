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

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

fn array_struct(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return exec_err!("struct requires at least one argument");
    }

    let vec: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            let field_name = format!("c{i}");
            Ok((
                Arc::new(Field::new(
                    field_name.as_str(),
                    arg.data_type().clone(),
                    true,
                )),
                arg.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(StructArray::from(vec)))
}

/// put values in a struct array.
fn struct_expr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    Ok(ColumnarValue::Array(array_struct(arrays.as_slice())?))
}
#[derive(Debug)]
pub(super) struct StructFunc {
    signature: Signature,
}

impl StructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "struct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let return_fields = arg_types
            .iter()
            .enumerate()
            .map(|(pos, dt)| Field::new(format!("c{pos}"), dt.clone(), true))
            .collect::<Vec<Field>>();
        Ok(DataType::Struct(Fields::from(return_fields)))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        struct_expr(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion_common::cast::as_struct_array;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_struct() {
        // struct(1, 2, 3) = {"c0": 1, "c1": 2, "c2": 3}
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let struc = struct_expr(&args)
            .expect("failed to initialize function struct")
            .into_array(1)
            .expect("Failed to convert to array");
        let result =
            as_struct_array(&struc).expect("failed to initialize function struct");
        assert_eq!(
            &Int64Array::from(vec![1]),
            result
                .column_by_name("c0")
                .unwrap()
                .clone()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        );
        assert_eq!(
            &Int64Array::from(vec![2]),
            result
                .column_by_name("c1")
                .unwrap()
                .clone()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        );
        assert_eq!(
            &Int64Array::from(vec![3]),
            result
                .column_by_name("c2")
                .unwrap()
                .clone()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        );
    }
}
