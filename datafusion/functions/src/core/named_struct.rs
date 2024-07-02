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

use arrow::array::StructArray;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::logical_type::ExtensionType;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// put values in a struct array.
fn named_struct_expr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return exec_err!(
            "named_struct requires at least one pair of arguments, got 0 instead"
        );
    }

    if args.len() % 2 != 0 {
        return exec_err!(
            "named_struct requires an even number of arguments, got {} instead",
            args.len()
        );
    }

    let (names, values): (Vec<_>, Vec<_>) = args
        .chunks_exact(2)
        .enumerate()
        .map(|(i, chunk)| {

            let name_column = &chunk[0];

            let name = match name_column {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(name_scalar))) => name_scalar,
                _ => return exec_err!("named_struct even arguments must be string literals, got {name_column:?} instead at position {}", i * 2)
            };

            Ok((name, chunk[1].clone()))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    let arrays = ColumnarValue::values_to_arrays(&values)?;

    let fields = names
        .into_iter()
        .zip(arrays)
        .map(|(name, value)| {
            (
                Arc::new(Field::new(name, value.data_type().clone(), true)),
                value,
            )
        })
        .collect::<Vec<_>>();

    Ok(ColumnarValue::Array(Arc::new(StructArray::from(fields))))
}

#[derive(Debug)]
pub struct NamedStructFunc {
    signature: Signature,
}

impl Default for NamedStructFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NamedStructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NamedStructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "named_struct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "named_struct: return_type called instead of return_type_from_exprs"
        )
    }

    fn return_type_from_exprs(
        &self,
        args: &[datafusion_expr::Expr],
        schema: &dyn datafusion_common::ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        // do not accept 0 arguments.
        if args.is_empty() {
            return exec_err!(
                "named_struct requires at least one pair of arguments, got 0 instead"
            );
        }

        if args.len() % 2 != 0 {
            return exec_err!(
                "named_struct requires an even number of arguments, got {} instead",
                args.len()
            );
        }

        let return_fields = args
            .chunks_exact(2)
            .enumerate()
            .map(|(i, chunk)| {
                let name = &chunk[0];
                let value = &chunk[1];

                if let Expr::Literal(ScalarValue::Utf8(Some(name))) = name {
                    Ok(Field::new(name, value.get_type(schema)?.physical().clone(), true))
                } else {
                    exec_err!("named_struct even arguments must be string literals, got {name} instead at position {}", i * 2)
                }
            })
            .collect::<Result<Vec<Field>>>()?;
        Ok(DataType::Struct(Fields::from(return_fields)))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        named_struct_expr(args)
    }
}
