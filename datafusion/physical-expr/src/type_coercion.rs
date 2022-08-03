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

//! Type coercion rules for functions with multiple valid signatures
//!
//! Coercion is performed automatically by DataFusion when the types
//! of arguments passed to a function do not exacty match the types
//! required by that function. In this case, DataFusion will attempt to
//! *coerce* the arguments to types accepted by the function by
//! inserting CAST operations.
//!
//! CAST operations added by coercion are lossless and never discard
//! information. For example coercion from i32 -> i64 might be
//! performed because all valid i32 values can be represented using an
//! i64. However, i64 -> i32 is never performed as there are i64
//! values which can not be represented by i32 values.

use super::PhysicalExpr;
use crate::expressions::try_cast;
use arrow::datatypes::Schema;
use datafusion_common::Result;
use datafusion_expr::{type_coercion::data_types, Signature};
use std::{sync::Arc, vec};

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
pub fn coerce(
    expressions: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    signature: &Signature,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    if expressions.is_empty() {
        return Ok(vec![]);
    }

    let current_types = expressions
        .iter()
        .map(|e| e.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types(&current_types, signature)?;

    expressions
        .iter()
        .enumerate()
        .map(|(i, expr)| try_cast(expr.clone(), schema, new_types[i].clone()))
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_expr::Volatility;

    #[test]
    fn test_coerce() -> Result<()> {
        // create a schema
        let schema = |t: Vec<DataType>| {
            Schema::new(
                t.iter()
                    .enumerate()
                    .map(|(i, t)| Field::new(&format!("c{}", i), t.clone(), true))
                    .collect(),
            )
        };

        // create a vector of expressions
        let expressions = |t: Vec<DataType>, schema| -> Result<Vec<_>> {
            t.iter()
                .enumerate()
                .map(|(i, t)| {
                    try_cast(col(&format!("c{}", i), &schema)?, &schema, t.clone())
                })
                .collect::<Result<Vec<_>>>()
        };

        // create a case: input + expected result
        let case =
            |observed: Vec<DataType>, valid, expected: Vec<DataType>| -> Result<_> {
                let schema = schema(observed.clone());
                let expr = expressions(observed, schema.clone())?;
                let expected = expressions(expected, schema.clone())?;
                Ok((expr.clone(), schema, valid, expected))
            };

        let cases = vec![
            // u16 -> u32
            case(
                vec![DataType::UInt16],
                Signature::uniform(1, vec![DataType::UInt32], Volatility::Immutable),
                vec![DataType::UInt32],
            )?,
            // same type
            case(
                vec![DataType::UInt32, DataType::UInt32],
                Signature::uniform(2, vec![DataType::UInt32], Volatility::Immutable),
                vec![DataType::UInt32, DataType::UInt32],
            )?,
            case(
                vec![DataType::UInt32],
                Signature::uniform(
                    1,
                    vec![DataType::Float32, DataType::Float64],
                    Volatility::Immutable,
                ),
                vec![DataType::Float32],
            )?,
            // u32 -> f32
            case(
                vec![DataType::UInt32, DataType::UInt32],
                Signature::variadic(vec![DataType::Float32], Volatility::Immutable),
                vec![DataType::Float32, DataType::Float32],
            )?,
            // u32 -> f32
            case(
                vec![DataType::Float32, DataType::UInt32],
                Signature::variadic_equal(Volatility::Immutable),
                vec![DataType::Float32, DataType::Float32],
            )?,
            // common type is u64
            case(
                vec![DataType::UInt32, DataType::UInt64],
                Signature::variadic(
                    vec![DataType::UInt32, DataType::UInt64],
                    Volatility::Immutable,
                ),
                vec![DataType::UInt64, DataType::UInt64],
            )?,
            // f32 -> f32
            case(
                vec![DataType::Float32],
                Signature::any(1, Volatility::Immutable),
                vec![DataType::Float32],
            )?,
        ];

        for case in cases {
            let observed = format!("{:?}", coerce(&case.0, &case.1, &case.2)?);
            let expected = format!("{:?}", case.3);
            assert_eq!(observed, expected);
        }

        // now cases that are expected to fail
        let cases = vec![
            // we do not know how to cast bool to UInt16 => fail
            case(
                vec![DataType::Boolean],
                Signature::uniform(1, vec![DataType::UInt16], Volatility::Immutable),
                vec![],
            )?,
            // u32 and bool are not uniform
            case(
                vec![DataType::UInt32, DataType::Boolean],
                Signature::variadic_equal(Volatility::Immutable),
                vec![],
            )?,
            // bool is not castable to u32
            case(
                vec![DataType::Boolean, DataType::Boolean],
                Signature::variadic(vec![DataType::UInt32], Volatility::Immutable),
                vec![],
            )?,
            // expected two arguments
            case(
                vec![DataType::UInt32],
                Signature::any(2, Volatility::Immutable),
                vec![],
            )?,
        ];

        for case in cases {
            if coerce(&case.0, &case.1, &case.2).is_ok() {
                return Err(DataFusionError::Plan(format!(
                    "Error was expected in {:?}",
                    case
                )));
            }
        }

        Ok(())
    }
}
