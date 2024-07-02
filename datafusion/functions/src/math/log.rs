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

//! Math function: `log()`.

use std::any::Any;
use std::sync::Arc;

use super::power::PowerFunc;

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, plan_err, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{lit, ColumnarValue, Expr, ScalarUDF, TypeSignature::*};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct LogFunc {
    signature: Signature,
}

impl Default for LogFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LogFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Float32]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32, Float32]),
                    Exact(vec![Float64, Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LogFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "log"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        match (input[0].sort_properties, input[1].sort_properties) {
            (first @ SortProperties::Ordered(value), SortProperties::Ordered(base))
                if !value.descending && base.descending
                    || value.descending && !base.descending =>
            {
                Ok(first)
            }
            (
                first @ (SortProperties::Ordered(_) | SortProperties::Singleton),
                SortProperties::Singleton,
            ) => Ok(first),
            (SortProperties::Singleton, second @ SortProperties::Ordered(_)) => {
                Ok(-second)
            }
            _ => Ok(SortProperties::Unordered),
        }
    }

    // Support overloaded log(base, x) and log(x) which defaults to log(10, x)
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let mut base = ColumnarValue::Scalar(ScalarValue::Float32(Some(10.0)));

        let mut x = &args[0];
        if args.len() == 2 {
            x = &args[1];
            base = ColumnarValue::Array(Arc::clone(&args[0]));
        }
        // note in f64::log params order is different than in sql. e.g in sql log(base, x) == f64::log(x, base)
        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    Arc::new(make_function_scalar_inputs!(x, "x", Float64Array, {
                        |value: f64| f64::log(value, base as f64)
                    }))
                }
                ColumnarValue::Array(base) => Arc::new(make_function_inputs2!(
                    x,
                    base,
                    "x",
                    "base",
                    Float64Array,
                    { f64::log }
                )),
                _ => {
                    return exec_err!("log function requires a scalar or array for base")
                }
            },

            DataType::Float32 => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    Arc::new(make_function_scalar_inputs!(x, "x", Float32Array, {
                        |value: f32| f32::log(value, base)
                    }))
                }
                ColumnarValue::Array(base) => Arc::new(make_function_inputs2!(
                    x,
                    base,
                    "x",
                    "base",
                    Float32Array,
                    { f32::log }
                )),
                _ => {
                    return exec_err!("log function requires a scalar or array for base")
                }
            },
            other => {
                return exec_err!("Unsupported data type {other:?} for function log")
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `log` function by the relevant rules:
    /// 1. Log(a, 1) ===> 0
    /// 2. Log(a, Power(a, b)) ===> b
    /// 3. Log(a, a) ===> 1
    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // Args are either
        // log(number)
        // log(base, number)
        let num_args = args.len();
        if num_args > 2 {
            return plan_err!("Expected log to have 1 or 2 arguments, got {num_args}");
        }
        let number = args.pop().ok_or_else(|| {
            plan_datafusion_err!("Expected log to have 1 or 2 arguments, got 0")
        })?;
        let number_datatype = info.get_data_type(&number)?;
        // default to base 10
        let base = if let Some(base) = args.pop() {
            base
        } else {
            lit(ScalarValue::new_ten(&number_datatype)?)
        };

        match number {
            Expr::Literal(value) if value == ScalarValue::new_one(&number_datatype)? => {
                Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_zero(
                    &info.get_data_type(&base)?,
                )?)))
            }
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if is_pow(&func) && args.len() == 2 && base == args[0] =>
            {
                let b = args.pop().unwrap(); // length checked above
                Ok(ExprSimplifyResult::Simplified(b))
            }
            number => {
                if number == base {
                    Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_one(
                        &number_datatype,
                    )?)))
                } else {
                    let args = match num_args {
                        1 => vec![number],
                        2 => vec![base, number],
                        _ => {
                            return internal_err!(
                                "Unexpected number of arguments in log::simplify"
                            )
                        }
                    };
                    Ok(ExprSimplifyResult::Original(args))
                }
            }
        }
    }
}

/// Returns true if the function is `PowerFunc`
fn is_pow(func: &ScalarUDF) -> bool {
    func.inner().as_any().downcast_ref::<PowerFunc>().is_some()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::SimplifyContext;

    #[test]
    fn test_log_f64() {
        let args = [
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![2.0, 2.0, 3.0, 5.0]))), // base
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                8.0, 4.0, 81.0, 625.0,
            ]))), // num
        ];

        let result = LogFunc::new()
            .invoke(&args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 3.0);
                assert_eq!(floats.value(1), 2.0);
                assert_eq!(floats.value(2), 4.0);
                assert_eq!(floats.value(3), 4.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f32() {
        let args = [
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![2.0, 2.0, 3.0, 5.0]))), // base
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                8.0, 4.0, 81.0, 625.0,
            ]))), // num
        ];

        let result = LogFunc::new()
            .invoke(&args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 3.0);
                assert_eq!(floats.value(1), 2.0);
                assert_eq!(floats.value(2), 4.0);
                assert_eq!(floats.value(3), 4.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
    #[test]
    // Test log() simplification errors
    fn test_log_simplify_errors() {
        let props = ExecutionProps::new();
        let schema =
            Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new()).unwrap());
        let context = SimplifyContext::new(&props).with_schema(schema);
        // Expect 0 args to error
        let _ = LogFunc::new().simplify(vec![], &context).unwrap_err();
        // Expect 3 args to error
        let _ = LogFunc::new()
            .simplify(vec![lit(1), lit(2), lit(3)], &context)
            .unwrap_err();
    }

    #[test]
    // Test that non-simplifiable log() expressions are unchanged after simplification
    fn test_log_simplify_original() {
        let props = ExecutionProps::new();
        let schema =
            Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new()).unwrap());
        let context = SimplifyContext::new(&props).with_schema(schema);
        // One argument with no simplifications
        let result = LogFunc::new().simplify(vec![lit(2)], &context).unwrap();
        let ExprSimplifyResult::Original(args) = result else {
            panic!("Expected ExprSimplifyResult::Original")
        };
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], lit(2));
        // Two arguments with no simplifications
        let result = LogFunc::new()
            .simplify(vec![lit(2), lit(3)], &context)
            .unwrap();
        let ExprSimplifyResult::Original(args) = result else {
            panic!("Expected ExprSimplifyResult::Original")
        };
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], lit(2));
        assert_eq!(args[1], lit(3));
    }
}
