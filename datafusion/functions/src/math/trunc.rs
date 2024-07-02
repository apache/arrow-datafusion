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

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Float32, Float64};
use datafusion_common::ScalarValue::Int64;
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct TruncFunc {
    signature: Signature,
}

impl Default for TruncFunc {
    fn default() -> Self {
        TruncFunc::new()
    }
}

impl TruncFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            // math expressions expect 1 argument of type f64 or f32
            // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
            // return the best approximation for it (in f64).
            // We accept f32 because in this case it is clear that the best approximation
            // will be as good as the number of digits in the number
            signature: Signature::one_of(
                vec![
                    Exact(vec![Float32, Int64]),
                    Exact(vec![Float64, Int64]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TruncFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "trunc"
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
        make_scalar_function(trunc, vec![])(args)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // trunc preserves the order of the first argument
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

/// Truncate(numeric, decimalPrecision) and trunc(numeric) SQL function
fn trunc(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!(
            "truncate function requires one or two arguments, got {}",
            args.len()
        );
    }

    //if only one arg then invoke toolchain trunc(num) and precision = 0 by default
    //or then invoke the compute_truncate method to process precision
    let num = &args[0];
    let precision = if args.len() == 1 {
        ColumnarValue::Scalar(Int64(Some(0)))
    } else {
        ColumnarValue::Array(Arc::clone(&args[1]))
    };

    match args[0].data_type() {
        Float64 => match precision {
            ColumnarValue::Scalar(Int64(Some(0))) => Ok(Arc::new(
                make_function_scalar_inputs!(num, "num", Float64Array, { f64::trunc }),
            ) as ArrayRef),
            ColumnarValue::Array(precision) => Ok(Arc::new(make_function_inputs2!(
                num,
                precision,
                "x",
                "y",
                Float64Array,
                Int64Array,
                { compute_truncate64 }
            )) as ArrayRef),
            _ => exec_err!("trunc function requires a scalar or array for precision"),
        },
        Float32 => match precision {
            ColumnarValue::Scalar(Int64(Some(0))) => Ok(Arc::new(
                make_function_scalar_inputs!(num, "num", Float32Array, { f32::trunc }),
            ) as ArrayRef),
            ColumnarValue::Array(precision) => Ok(Arc::new(make_function_inputs2!(
                num,
                precision,
                "x",
                "y",
                Float32Array,
                Int64Array,
                { compute_truncate32 }
            )) as ArrayRef),
            _ => exec_err!("trunc function requires a scalar or array for precision"),
        },
        other => exec_err!("Unsupported data type {other:?} for function trunc"),
    }
}

fn compute_truncate32(x: f32, y: i64) -> f32 {
    let factor = 10.0_f32.powi(y as i32);
    (x * factor).round() / factor
}

fn compute_truncate64(x: f64, y: i64) -> f64 {
    let factor = 10.0_f64.powi(y as i32);
    (x * factor).round() / factor
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::trunc::trunc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};

    #[test]
    fn test_truncate_32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                15.0,
                1_234.267_8,
                1_233.123_4,
                3.312_979_2,
                -21.123_4,
            ])),
            Arc::new(Int64Array::from(vec![0, 3, 2, 5, 6])),
        ];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float32_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 15.0);
        assert_eq!(floats.value(1), 1_234.268);
        assert_eq!(floats.value(2), 1_233.12);
        assert_eq!(floats.value(3), 3.312_98);
        assert_eq!(floats.value(4), -21.123_4);
    }

    #[test]
    fn test_truncate_64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                5.0,
                234.267_812_176,
                123.123_456_789,
                123.312_979_313_2,
                -321.123_1,
            ])),
            Arc::new(Int64Array::from(vec![0, 3, 2, 5, 6])),
        ];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float64_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 5.0);
        assert_eq!(floats.value(1), 234.268);
        assert_eq!(floats.value(2), 123.12);
        assert_eq!(floats.value(3), 123.312_98);
        assert_eq!(floats.value(4), -321.123_1);
    }

    #[test]
    fn test_truncate_64_one_arg() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            5.0,
            234.267_812,
            123.123_45,
            123.312_979_313_2,
            -321.123,
        ]))];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float64_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 5.0);
        assert_eq!(floats.value(1), 234.0);
        assert_eq!(floats.value(2), 123.0);
        assert_eq!(floats.value(3), 123.0);
        assert_eq!(floats.value(4), -321.0);
    }
}
