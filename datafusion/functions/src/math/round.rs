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

use std::any::Any;
use std::sync::Arc;

use crate::utils::make_scalar_function;

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Float32, Float64, Int32};
use datafusion_common::{
    exec_datafusion_err, exec_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct RoundFunc {
    signature: Signature,
}

impl Default for RoundFunc {
    fn default() -> Self {
        RoundFunc::new()
    }
}

impl RoundFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Float64, Int64]),
                    Exact(vec![Float32, Int64]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RoundFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(round, vec![])(args)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // round preserves the order of the first argument
        let value = &input[0];
        let precision = input.get(1);

        if precision
            .map(|r| r.sort_properties.eq(&SortProperties::Singleton))
            .unwrap_or(true)
        {
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
}

/// Round SQL function
pub fn round(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!(
            "round function requires one or two arguments, got {}",
            args.len()
        );
    }

    let mut decimal_places = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

    if args.len() == 2 {
        decimal_places = ColumnarValue::Array(Arc::clone(&args[1]));
    }

    match args[0].data_type() {
        DataType::Float64 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places: i32 = decimal_places.try_into().map_err(|e| {
                    exec_datafusion_err!(
                        "Invalid value for decimal places: {decimal_places}: {e}"
                    )
                })?;

                Ok(Arc::new(make_function_scalar_inputs!(
                    &args[0],
                    "value",
                    Float64Array,
                    {
                        |value: f64| {
                            (value * 10.0_f64.powi(decimal_places)).round()
                                / 10.0_f64.powi(decimal_places)
                        }
                    }
                )) as ArrayRef)
            }
            ColumnarValue::Array(decimal_places) => {
                let options = CastOptions {
                    safe: false, // raise error if the cast is not possible
                    ..Default::default()
                };
                let decimal_places = cast_with_options(&decimal_places, &Int32, &options)
                    .map_err(|e| {
                        exec_datafusion_err!("Invalid values for decimal places: {e}")
                    })?;
                Ok(Arc::new(make_function_inputs2!(
                    &args[0],
                    decimal_places,
                    "value",
                    "decimal_places",
                    Float64Array,
                    Int32Array,
                    {
                        |value: f64, decimal_places: i32| {
                            (value * 10.0_f64.powi(decimal_places)).round()
                                / 10.0_f64.powi(decimal_places)
                        }
                    }
                )) as ArrayRef)
            }
            _ => {
                exec_err!("round function requires a scalar or array for decimal_places")
            }
        },

        DataType::Float32 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places: i32 = decimal_places.try_into().map_err(|e| {
                    exec_datafusion_err!(
                        "Invalid value for decimal places: {decimal_places}: {e}"
                    )
                })?;

                Ok(Arc::new(make_function_scalar_inputs!(
                    &args[0],
                    "value",
                    Float32Array,
                    {
                        |value: f32| {
                            (value * 10.0_f32.powi(decimal_places)).round()
                                / 10.0_f32.powi(decimal_places)
                        }
                    }
                )) as ArrayRef)
            }
            ColumnarValue::Array(_) => {
                let ColumnarValue::Array(decimal_places) =
                    decimal_places.cast_to(&Int32, None).map_err(|e| {
                        exec_datafusion_err!("Invalid values for decimal places: {e}")
                    })?
                else {
                    panic!("Unexpected result of ColumnarValue::Array.cast")
                };

                Ok(Arc::new(make_function_inputs2!(
                    &args[0],
                    decimal_places,
                    "value",
                    "decimal_places",
                    Float32Array,
                    Int32Array,
                    {
                        |value: f32, decimal_places: i32| {
                            (value * 10.0_f32.powi(decimal_places)).round()
                                / 10.0_f32.powi(decimal_places)
                        }
                    }
                )) as ArrayRef)
            }
            _ => {
                exec_err!("round function requires a scalar or array for decimal_places")
            }
        },

        other => exec_err!("Unsupported data type {other:?} for function round"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::round::round;

    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::DataFusionError;

    #[test]
    fn test_round_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345; 10])), // input
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])), // decimal_places
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![
            125.0, 125.2, 125.23, 125.235, 125.2345, 125.2345, 130.0, 100.0, 0.0, 0.0,
        ]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345; 10])), // input
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])), // decimal_places
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![
            125.0, 125.2, 125.23, 125.235, 125.2345, 125.2345, 130.0, 100.0, 0.0, 0.0,
        ]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_one_input() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![125.0, 12.0, 1.0, 0.0]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64_one_input() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![125.0, 12.0, 1.0, 0.0]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_cast_fail() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345])), // input
            Arc::new(Int64Array::from(vec![2147483648])), // decimal_places
        ];

        let result = round(&args);

        assert!(result.is_err());
        assert!(matches!(result, Err(DataFusionError::Execution { .. })));
    }
}
