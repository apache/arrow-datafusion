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

//! Math expressions

use arrow::array::ArrayRef;
use arrow::array::{Float32Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_float32_array, as_float64_array, as_int64_array};
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::Float32;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use rand::{thread_rng, Rng};
use std::any::type_name;
use std::iter;
use std::mem::swap;
use std::sync::Arc;

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $TYPE =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

macro_rules! unary_primitive_array_op {
    ($VALUE:expr, $NAME:expr, $FUNC:ident) => {{
        match ($VALUE) {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Float32 => {
                    let result = downcast_compute_op!(array, $NAME, $FUNC, Float32Array);
                    Ok(ColumnarValue::Array(result?))
                }
                DataType::Float64 => {
                    let result = downcast_compute_op!(array, $NAME, $FUNC, Float64Array);
                    Ok(ColumnarValue::Array(result?))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    other, $NAME,
                ))),
            },
            ColumnarValue::Scalar(a) => match a {
                ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float32(a.map(|x| x.$FUNC())),
                )),
                ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(a.map(|x| x.$FUNC())),
                )),
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    ($VALUE).data_type(),
                    $NAME,
                ))),
            },
        }
    }};
}

macro_rules! math_unary_function {
    ($NAME:expr, $FUNC:ident) => {
        /// mathematical function that accepts f32 or f64 and returns f64
        pub fn $FUNC(args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unary_primitive_array_op!(&args[0], $NAME, $FUNC)
        }
    };
}

macro_rules! downcast_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to {}",
                $NAME,
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! make_function_scalar_inputs {
    ($ARG: expr, $NAME:expr, $ARRAY_TYPE:ident, $FUNC: block) => {{
        let arg = downcast_arg!($ARG, $NAME, $ARRAY_TYPE);

        arg.iter()
            .map(|a| match a {
                Some(a) => Some($FUNC(a)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE>()
    }};
}

macro_rules! make_function_inputs2 {
    ($ARG1: expr, $ARG2: expr, $NAME1:expr, $NAME2: expr, $ARRAY_TYPE:ident, $FUNC: block) => {{
        let arg1 = downcast_arg!($ARG1, $NAME1, $ARRAY_TYPE);
        let arg2 = downcast_arg!($ARG2, $NAME2, $ARRAY_TYPE);

        arg1.iter()
            .zip(arg2.iter())
            .map(|(a1, a2)| match (a1, a2) {
                (Some(a1), Some(a2)) => Some($FUNC(a1, a2.try_into().ok()?)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE>()
    }};
    ($ARG1: expr, $ARG2: expr, $NAME1:expr, $NAME2: expr, $ARRAY_TYPE1:ident, $ARRAY_TYPE2:ident, $FUNC: block) => {{
        let arg1 = downcast_arg!($ARG1, $NAME1, $ARRAY_TYPE1);
        let arg2 = downcast_arg!($ARG2, $NAME2, $ARRAY_TYPE2);

        arg1.iter()
            .zip(arg2.iter())
            .map(|(a1, a2)| match (a1, a2) {
                (Some(a1), Some(a2)) => Some($FUNC(a1, a2.try_into().ok()?)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE1>()
    }};
}

math_unary_function!("sqrt", sqrt);
math_unary_function!("cbrt", cbrt);
math_unary_function!("sin", sin);
math_unary_function!("cos", cos);
math_unary_function!("tan", tan);
math_unary_function!("sinh", sinh);
math_unary_function!("cosh", cosh);
math_unary_function!("tanh", tanh);
math_unary_function!("asin", asin);
math_unary_function!("acos", acos);
math_unary_function!("atan", atan);
math_unary_function!("asinh", asinh);
math_unary_function!("acosh", acosh);
math_unary_function!("atanh", atanh);
math_unary_function!("floor", floor);
math_unary_function!("ceil", ceil);
math_unary_function!("trunc", trunc);
math_unary_function!("abs", abs);
math_unary_function!("signum", signum);
math_unary_function!("exp", exp);
math_unary_function!("ln", ln);
math_unary_function!("log2", log2);
math_unary_function!("log10", log10);
math_unary_function!("degrees", to_degrees);
math_unary_function!("radians", to_radians);

/// Factorial SQL function
pub fn factorial(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Int64 => Ok(Arc::new(make_function_scalar_inputs!(
            &args[0],
            "value",
            Int64Array,
            { |value: i64| { (1..=value).product() } }
        )) as ArrayRef),
        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function factorial."
        ))),
    }
}

/// Computes greatest common divisor using Binary GCD algorithm.
fn compute_gcd(x: i64, y: i64) -> i64 {
    let mut a = x.wrapping_abs();
    let mut b = y.wrapping_abs();

    if a == 0 {
        return b;
    }
    if b == 0 {
        return a;
    }

    let shift = (a | b).trailing_zeros();
    a >>= shift;
    b >>= shift;
    a >>= a.trailing_zeros();

    loop {
        b >>= b.trailing_zeros();
        if a > b {
            swap(&mut a, &mut b);
        }

        b -= a;

        if b == 0 {
            return a << shift;
        }
    }
}

/// Gcd SQL function
pub fn gcd(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Int64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "x",
            "y",
            Int64Array,
            Int64Array,
            { compute_gcd }
        )) as ArrayRef),
        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function gcd"
        ))),
    }
}

/// Lcm SQL function
pub fn lcm(args: &[ArrayRef]) -> Result<ArrayRef> {
    let compute_lcm = |x: i64, y: i64| {
        let a = x.wrapping_abs();
        let b = y.wrapping_abs();

        if a == 0 || b == 0 {
            return 0;
        }
        a / compute_gcd(a, b) * b
    };

    match args[0].data_type() {
        DataType::Int64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "x",
            "y",
            Int64Array,
            Int64Array,
            { compute_lcm }
        )) as ArrayRef),
        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function lcm"
        ))),
    }
}

/// Pi SQL function
pub fn pi(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if !matches!(&args[0], ColumnarValue::Array(_)) {
        return Err(DataFusionError::Internal(
            "Expect pi function to take no param".to_string(),
        ));
    }
    let array = Float64Array::from_value(std::f64::consts::PI, 1);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

/// Random SQL function
pub fn random(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len: usize = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        _ => {
            return Err(DataFusionError::Internal(
                "Expect random function to take no param".to_string(),
            ))
        }
    };
    let mut rng = thread_rng();
    let values = iter::repeat_with(|| rng.gen_range(0.0..1.0)).take(len);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

/// Round SQL function
pub fn round(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "round function requires one or two arguments, got {}",
            args.len()
        )));
    }

    let mut decimal_places = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

    if args.len() == 2 {
        decimal_places = ColumnarValue::Array(args[1].clone());
    }

    match args[0].data_type() {
        DataType::Float64 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places = round_downcast_decimal_places(decimal_places)?;

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
                let values = as_float64_array(&args[0])?;
                let decimal_places = as_int64_array(&decimal_places)?;
                let arr = values
                    .iter()
                    .zip(decimal_places.iter())
                    .map(|(val, dp)| match (val, dp) {
                        (Some(val), Some(dp)) => Ok(Some(
                            (val * 10.0_f64.powi(round_downcast_decimal_places(dp)?))
                                .round()
                                / 10.0_f64.powi(round_downcast_decimal_places(dp)?),
                        )),
                        _ => Ok(None),
                    })
                    .collect::<Result<Float64Array>>()?;
                Ok(Arc::new(arr) as ArrayRef)
            }
            _ => Err(DataFusionError::Internal(
                "round function requires a scalar or array for decimal_places"
                    .to_string(),
            )),
        },

        DataType::Float32 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places = round_downcast_decimal_places(decimal_places)?;

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
            ColumnarValue::Array(decimal_places) => {
                let values = as_float32_array(&args[0])?;
                let decimal_places = as_int64_array(&decimal_places)?;
                let arr = values
                    .iter()
                    .zip(decimal_places.iter())
                    .map(|(val, dp)| match (val, dp) {
                        (Some(val), Some(dp)) => Ok(Some(
                            (val * 10.0_f32.powi(round_downcast_decimal_places(dp)?))
                                .round()
                                / 10.0_f32.powi(round_downcast_decimal_places(dp)?),
                        )),
                        _ => Ok(None),
                    })
                    .collect::<Result<Float32Array>>()?;
                Ok(Arc::new(arr) as ArrayRef)
            }
            _ => Err(DataFusionError::Internal(
                "round function requires a scalar or array for decimal_places"
                    .to_string(),
            )),
        },

        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function round"
        ))),
    }
}

#[inline]
fn round_downcast_decimal_places(decimal_places: i64) -> Result<i32> {
    decimal_places.try_into().map_err(|_| {
        DataFusionError::Execution(
            "round function requires decimal_places as 32 bit integers".to_string(),
        )
    })
}

/// Power SQL function
pub fn power(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "base",
            "exponent",
            Float64Array,
            { f64::powf }
        )) as ArrayRef),

        DataType::Int64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "base",
            "exponent",
            Int64Array,
            { i64::pow }
        )) as ArrayRef),

        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function power"
        ))),
    }
}

/// Atan2 SQL function
pub fn atan2(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "y",
            "x",
            Float64Array,
            { f64::atan2 }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "y",
            "x",
            Float32Array,
            { f32::atan2 }
        )) as ArrayRef),

        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function atan2"
        ))),
    }
}

/// Log SQL function
pub fn log(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Support overloaded log(base, x) and log(x) which defaults to log(10, x)
    // note in f64::log params order is different than in sql. e.g in sql log(base, x) == f64::log(x, base)
    let mut base = ColumnarValue::Scalar(Float32(Some(10.0)));

    let mut x = &args[0];
    if args.len() == 2 {
        x = &args[1];
        base = ColumnarValue::Array(args[0].clone());
    }
    match args[0].data_type() {
        DataType::Float64 => match base {
            ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                let base = base as f64;
                Ok(
                    Arc::new(make_function_scalar_inputs!(x, "x", Float64Array, {
                        |value: f64| f64::log(value, base)
                    })) as ArrayRef,
                )
            }
            ColumnarValue::Array(base) => Ok(Arc::new(make_function_inputs2!(
                x,
                base,
                "x",
                "base",
                Float64Array,
                { f64::log }
            )) as ArrayRef),
            _ => Err(DataFusionError::Internal(
                "log function requires a scalar or array for base".to_string(),
            )),
        },

        DataType::Float32 => match base {
            ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => Ok(Arc::new(
                make_function_scalar_inputs!(x, "x", Float32Array, {
                    |value: f32| f32::log(value, base)
                }),
            )
                as ArrayRef),
            ColumnarValue::Array(base) => Ok(Arc::new(make_function_inputs2!(
                x,
                base,
                "x",
                "base",
                Float32Array,
                { f32::log }
            )) as ArrayRef),
            _ => Err(DataFusionError::Internal(
                "log function requires a scalar or array for base".to_string(),
            )),
        },

        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {other:?} for function log"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::array::{Float64Array, NullArray};
    use datafusion_common::cast::{as_float32_array, as_float64_array, as_int64_array};

    #[test]
    fn test_random_expression() {
        let args = vec![ColumnarValue::Array(Arc::new(NullArray::new(1)))];
        let array = random(&args)
            .expect("failed to initialize function random")
            .into_array(1);
        let floats =
            as_float64_array(&array).expect("failed to initialize function random");

        assert_eq!(floats.len(), 1);
        assert!(0.0 <= floats.value(0) && floats.value(0) < 1.0);
    }

    #[test]
    fn test_power_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![2.0, 2.0, 3.0, 5.0])), // base
            Arc::new(Float64Array::from(vec![3.0, 2.0, 4.0, 4.0])), // exponent
        ];

        let result = power(&args).expect("failed to initialize function power");
        let floats =
            as_float64_array(&result).expect("failed to initialize function power");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 8.0);
        assert_eq!(floats.value(1), 4.0);
        assert_eq!(floats.value(2), 81.0);
        assert_eq!(floats.value(3), 625.0);
    }

    #[test]
    fn test_power_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![2, 2, 3, 5])), // base
            Arc::new(Int64Array::from(vec![3, 2, 4, 4])), // exponent
        ];

        let result = power(&args).expect("failed to initialize function power");
        let floats =
            as_int64_array(&result).expect("failed to initialize function power");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 8);
        assert_eq!(floats.value(1), 4);
        assert_eq!(floats.value(2), 81);
        assert_eq!(floats.value(3), 625);
    }

    #[test]
    fn test_atan2_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![2.0, -3.0, 4.0, -5.0])), // y
            Arc::new(Float64Array::from(vec![1.0, 2.0, -3.0, -4.0])), // x
        ];

        let result = atan2(&args).expect("failed to initialize function atan2");
        let floats =
            as_float64_array(&result).expect("failed to initialize function atan2");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), (2.0_f64).atan2(1.0));
        assert_eq!(floats.value(1), (-3.0_f64).atan2(2.0));
        assert_eq!(floats.value(2), (4.0_f64).atan2(-3.0));
        assert_eq!(floats.value(3), (-5.0_f64).atan2(-4.0));
    }

    #[test]
    fn test_atan2_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![2.0, -3.0, 4.0, -5.0])), // y
            Arc::new(Float32Array::from(vec![1.0, 2.0, -3.0, -4.0])), // x
        ];

        let result = atan2(&args).expect("failed to initialize function atan2");
        let floats =
            as_float32_array(&result).expect("failed to initialize function atan2");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), (2.0_f32).atan2(1.0));
        assert_eq!(floats.value(1), (-3.0_f32).atan2(2.0));
        assert_eq!(floats.value(2), (4.0_f32).atan2(-3.0));
        assert_eq!(floats.value(3), (-5.0_f32).atan2(-4.0));
    }

    #[test]
    fn test_log_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![2.0, 2.0, 3.0, 5.0])), // base
            Arc::new(Float64Array::from(vec![8.0, 4.0, 81.0, 625.0])), // x
        ];

        let result = log(&args).expect("failed to initialize function log");
        let floats =
            as_float64_array(&result).expect("failed to initialize function log");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 3.0);
        assert_eq!(floats.value(1), 2.0);
        assert_eq!(floats.value(2), 4.0);
        assert_eq!(floats.value(3), 4.0);
    }

    #[test]
    fn test_log_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![2.0, 2.0, 3.0, 5.0])), // base
            Arc::new(Float32Array::from(vec![8.0, 4.0, 81.0, 625.0])), // x
        ];

        let result = log(&args).expect("failed to initialize function log");
        let floats =
            as_float32_array(&result).expect("failed to initialize function log");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 3.0);
        assert_eq!(floats.value(1), 2.0);
        assert_eq!(floats.value(2), 4.0);
        assert_eq!(floats.value(3), 4.0);
    }

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
    fn test_factorial_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 4])), // input
        ];

        let result = factorial(&args).expect("failed to initialize function factorial");
        let ints =
            as_int64_array(&result).expect("failed to initialize function factorial");

        let expected = Int64Array::from(vec![1, 1, 2, 24]);

        assert_eq!(ints, &expected);
    }

    #[test]
    fn test_gcd_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 3, 25, -16])), // x
            Arc::new(Int64Array::from(vec![0, -2, 15, 8])),  // y
        ];

        let result = gcd(&args).expect("failed to initialize function gcd");
        let ints = as_int64_array(&result).expect("failed to initialize function gcd");

        assert_eq!(ints.len(), 4);
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(1), 1);
        assert_eq!(ints.value(2), 5);
        assert_eq!(ints.value(3), 8);
    }

    #[test]
    fn test_lcm_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 3, 25, -16])), // x
            Arc::new(Int64Array::from(vec![0, -2, 15, 8])),  // y
        ];

        let result = lcm(&args).expect("failed to initialize function lcm");
        let ints = as_int64_array(&result).expect("failed to initialize function lcm");

        assert_eq!(ints.len(), 4);
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(1), 6);
        assert_eq!(ints.value(2), 75);
        assert_eq!(ints.value(3), 16);
    }

    #[test]
    fn test_round_f64_decimal_places() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![125.2, 12.35, 1.234, 0.1234]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64_decimal_places_overflow() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345])), // input
            Arc::new(Int64Array::from(vec![2147483648])),
        ];

        let e = round(&args).expect_err("expect throwing overflow");
        assert_eq!(
            e.to_string(),
            "Execution error: round function requires decimal_places as 32 bit integers"
                .to_string()
        );
    }

    #[test]
    fn test_round_f32_decimal_places() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![125.2, 12.35, 1.234, 0.1234]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_decimal_places_overflow() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345])), // input
            Arc::new(Int64Array::from(vec![2147483648])),
        ];

        let e = round(&args).expect_err("expect throwing overflow");
        assert_eq!(
            e.to_string(),
            "Execution error: round function requires decimal_places as 32 bit integers"
                .to_string()
        );
    }
}
