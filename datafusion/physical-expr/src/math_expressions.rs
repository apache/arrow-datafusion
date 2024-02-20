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
use arrow::array::{
    BooleanArray, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array,
};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::{Float32, Int64};
use datafusion_common::{internal_err, not_impl_err};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use rand::{thread_rng, Rng};
use std::any::type_name;
use std::iter;
use std::mem::swap;
use std::sync::Arc;

type MathArrayFunction = fn(&[ArrayRef]) -> Result<ArrayRef>;

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $TYPE =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
                Ok(Arc::new(res))
            }
            _ => internal_err!("Invalid data type for {}", $NAME),
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
                other => internal_err!(
                    "Unsupported data type {:?} for function {}",
                    other,
                    $NAME
                ),
            },
            ColumnarValue::Scalar(a) => match a {
                ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float32(a.map(|x| x.$FUNC())),
                )),
                ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(a.map(|x| x.$FUNC())),
                )),
                _ => internal_err!(
                    "Unsupported data type {:?} for function {}",
                    ($VALUE).data_type(),
                    $NAME
                ),
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

macro_rules! make_function_scalar_inputs_return_type {
    ($ARG: expr, $NAME:expr, $ARGS_TYPE:ident, $RETURN_TYPE:ident, $FUNC: block) => {{
        let arg = downcast_arg!($ARG, $NAME, $ARGS_TYPE);

        arg.iter()
            .map(|a| match a {
                Some(a) => Some($FUNC(a)),
                _ => None,
            })
            .collect::<$RETURN_TYPE>()
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
math_unary_function!("abs", abs);
math_unary_function!("signum", signum);
math_unary_function!("exp", exp);
math_unary_function!("ln", ln);
math_unary_function!("log2", log2);
math_unary_function!("log10", log10);
math_unary_function!("degrees", to_degrees);
math_unary_function!("radians", to_radians);

///
pub fn floor(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int32 | DataType::Int64 => Ok(ColumnarValue::Array(array.clone())),
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "floor", floor, Float32Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "floor", floor, Float64Array);
                Ok(ColumnarValue::Array(result?))
            }
            other => internal_err!("Unsupported data type {other:?} for function floor"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int32(_) | ScalarValue::Int64(_) => Ok(ColumnarValue::Scalar(a.clone())),
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float32(a.map(|x| x.floor())),
            )),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(a.map(|x| x.floor())),
            )),
            _ => internal_err!("Unsupported data type {a:?} for function floor"),
        },
    }
}

///
pub fn ceil(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int32 | DataType::Int64 => Ok(ColumnarValue::Array(array.clone())),
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "ceil", ceil, Float32Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "ceil", ceil, Float64Array);
                Ok(ColumnarValue::Array(result?))
            }
            other => internal_err!("Unsupported data type {other:?} for function ceil"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int32(_) | ScalarValue::Int64(_) => Ok(ColumnarValue::Scalar(a.clone())),
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float32(a.map(|x| x.ceil())),
            )),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(a.map(|x| x.ceil())),
            )),
            _ => internal_err!("Unsupported data type {a:?} for function ceil"),
        },
    }
}

/// Factorial SQL function
pub fn factorial(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Int64 => Ok(Arc::new(make_function_scalar_inputs!(
            &args[0],
            "value",
            Int64Array,
            { |value: i64| { (1..=value).product() } }
        )) as ArrayRef),
        other => internal_err!("Unsupported data type {other:?} for function factorial."),
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
        other => internal_err!("Unsupported data type {other:?} for function gcd"),
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
        other => internal_err!("Unsupported data type {other:?} for function lcm"),
    }
}

/// Nanvl SQL function
pub fn nanvl(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => {
            let compute_nanvl = |x: f64, y: f64| {
                if x.is_nan() {
                    y
                } else {
                    x
                }
            };

            Ok(Arc::new(make_function_inputs2!(
                &args[0],
                &args[1],
                "x",
                "y",
                Float64Array,
                { compute_nanvl }
            )) as ArrayRef)
        }

        DataType::Float32 => {
            let compute_nanvl = |x: f32, y: f32| {
                if x.is_nan() {
                    y
                } else {
                    x
                }
            };

            Ok(Arc::new(make_function_inputs2!(
                &args[0],
                &args[1],
                "x",
                "y",
                Float32Array,
                { compute_nanvl }
            )) as ArrayRef)
        }

        other => internal_err!("Unsupported data type {other:?} for function nanvl"),
    }
}

/// Isnan SQL function
pub fn isnan(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float64Array,
            BooleanArray,
            { f64::is_nan }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float32Array,
            BooleanArray,
            { f32::is_nan }
        )) as ArrayRef),

        other => internal_err!("Unsupported data type {other:?} for function isnan"),
    }
}

/// Iszero SQL function
pub fn iszero(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float64Array,
            BooleanArray,
            { |x: f64| { x == 0_f64 } }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float32Array,
            BooleanArray,
            { |x: f32| { x == 0_f32 } }
        )) as ArrayRef),

        other => internal_err!("Unsupported data type {other:?} for function iszero"),
    }
}

/// Pi SQL function
pub fn pi(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if !matches!(&args[0], ColumnarValue::Array(_)) {
        return internal_err!("Expect pi function to take no param");
    }
    let array = Float64Array::from_value(std::f64::consts::PI, 1);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

/// Random SQL function
pub fn random(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len: usize = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        _ => return internal_err!("Expect random function to take no param"),
    };
    let mut rng = thread_rng();
    let values = iter::repeat_with(|| rng.gen_range(0.0..1.0)).take(len);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

/// Round SQL function
pub fn round(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return internal_err!(
            "round function requires one or two arguments, got {}",
            args.len()
        );
    }

    let mut decimal_places = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

    if args.len() == 2 {
        decimal_places = ColumnarValue::Array(args[1].clone());
    }

    match args[0].data_type() {
        DataType::Float64 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places = decimal_places.try_into().unwrap();

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
            ColumnarValue::Array(decimal_places) => Ok(Arc::new(make_function_inputs2!(
                &args[0],
                decimal_places,
                "value",
                "decimal_places",
                Float64Array,
                Int64Array,
                {
                    |value: f64, decimal_places: i64| {
                        (value * 10.0_f64.powi(decimal_places.try_into().unwrap()))
                            .round()
                            / 10.0_f64.powi(decimal_places.try_into().unwrap())
                    }
                }
            )) as ArrayRef),
            _ => internal_err!(
                "round function requires a scalar or array for decimal_places"
            ),
        },

        DataType::Float32 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places = decimal_places.try_into().unwrap();

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
            ColumnarValue::Array(decimal_places) => Ok(Arc::new(make_function_inputs2!(
                &args[0],
                decimal_places,
                "value",
                "decimal_places",
                Float32Array,
                Int64Array,
                {
                    |value: f32, decimal_places: i64| {
                        (value * 10.0_f32.powi(decimal_places.try_into().unwrap()))
                            .round()
                            / 10.0_f32.powi(decimal_places.try_into().unwrap())
                    }
                }
            )) as ArrayRef),
            _ => internal_err!(
                "round function requires a scalar or array for decimal_places"
            ),
        },

        other => internal_err!("Unsupported data type {other:?} for function round"),
    }
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

        other => internal_err!("Unsupported data type {other:?} for function power"),
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

        other => internal_err!("Unsupported data type {other:?} for function atan2"),
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
            _ => internal_err!("log function requires a scalar or array for base"),
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
            _ => internal_err!("log function requires a scalar or array for base"),
        },

        other => internal_err!("Unsupported data type {other:?} for function log"),
    }
}

///cot SQL function
pub fn cot(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_scalar_inputs!(
            &args[0],
            "x",
            Float64Array,
            { compute_cot64 }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_scalar_inputs!(
            &args[0],
            "x",
            Float32Array,
            { compute_cot32 }
        )) as ArrayRef),

        other => internal_err!("Unsupported data type {other:?} for function cot"),
    }
}

fn compute_cot32(x: f32) -> f32 {
    let a = f32::tan(x);
    1.0 / a
}

fn compute_cot64(x: f64) -> f64 {
    let a = f64::tan(x);
    1.0 / a
}

/// Truncate(numeric, decimalPrecision) and trunc(numeric) SQL function
pub fn trunc(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return internal_err!(
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
        ColumnarValue::Array(args[1].clone())
    };

    match args[0].data_type() {
        DataType::Float64 => match precision {
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
            _ => internal_err!("trunc function requires a scalar or array for precision"),
        },
        DataType::Float32 => match precision {
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
            _ => internal_err!("trunc function requires a scalar or array for precision"),
        },
        other => internal_err!("Unsupported data type {other:?} for function trunc"),
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

macro_rules! make_abs_function {
    ($ARRAY_TYPE:ident) => {{
        |args: &[ArrayRef]| {
            let array = downcast_arg!(&args[0], "abs arg", $ARRAY_TYPE);
            let res: $ARRAY_TYPE = array.unary(|x| x.abs());
            Ok(Arc::new(res) as ArrayRef)
        }
    }};
}

macro_rules! make_try_abs_function {
    ($ARRAY_TYPE:ident) => {{
        |args: &[ArrayRef]| {
            let array = downcast_arg!(&args[0], "abs arg", $ARRAY_TYPE);
            let res: $ARRAY_TYPE = array.try_unary(|x| {
                x.checked_abs().ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "{} overflow on abs({})",
                        stringify!($ARRAY_TYPE),
                        x
                    ))
                })
            })?;
            Ok(Arc::new(res) as ArrayRef)
        }
    }};
}

macro_rules! make_decimal_abs_function {
    ($ARRAY_TYPE:ident) => {{
        |args: &[ArrayRef]| {
            let array = downcast_arg!(&args[0], "abs arg", $ARRAY_TYPE);
            let res: $ARRAY_TYPE = array
                .unary(|x| x.wrapping_abs())
                .with_data_type(args[0].data_type().clone());
            Ok(Arc::new(res) as ArrayRef)
        }
    }};
}

/// Abs SQL function
/// Return different implementations based on input datatype to reduce branches during execution
pub(super) fn create_abs_function(
    input_data_type: &DataType,
) -> Result<MathArrayFunction> {
    match input_data_type {
        DataType::Float32 => Ok(make_abs_function!(Float32Array)),
        DataType::Float64 => Ok(make_abs_function!(Float64Array)),

        // Types that may overflow, such as abs(-128_i8).
        DataType::Int8 => Ok(make_try_abs_function!(Int8Array)),
        DataType::Int16 => Ok(make_try_abs_function!(Int16Array)),
        DataType::Int32 => Ok(make_try_abs_function!(Int32Array)),
        DataType::Int64 => Ok(make_try_abs_function!(Int64Array)),

        // Types of results are the same as the input.
        DataType::Null
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(|args: &[ArrayRef]| Ok(args[0].clone())),

        // Decimal types
        DataType::Decimal128(_, _) => Ok(make_decimal_abs_function!(Decimal128Array)),
        DataType::Decimal256(_, _) => Ok(make_decimal_abs_function!(Decimal256Array)),

        other => not_impl_err!("Unsupported data type {other:?} for function abs"),
    }
}

/// abs() SQL function implementation
pub fn abs_invoke(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return internal_err!("abs function requires 1 argument, got {}", args.len());
    }

    let input_data_type = args[0].data_type();
    let abs_fun = create_abs_function(input_data_type)?;

    abs_fun(args)
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::array::{Float64Array, NullArray};
    use datafusion_common::cast::{
        as_boolean_array, as_float32_array, as_float64_array, as_int64_array,
    };

    #[test]
    fn test_random_expression() {
        let args = vec![ColumnarValue::Array(Arc::new(NullArray::new(1)))];
        let array = random(&args)
            .expect("failed to initialize function random")
            .into_array(1)
            .expect("Failed to convert to array");
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
    fn test_cot_f32() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float32Array::from(vec![12.1, 30.0, 90.0, -30.0]))];
        let result = cot(&args).expect("failed to initialize function cot");
        let floats =
            as_float32_array(&result).expect("failed to initialize function cot");

        let expected = Float32Array::from(vec![
            -1.986_460_4,
            -0.156_119_96,
            -0.501_202_8,
            0.156_119_96,
        ]);

        let eps = 1e-6;
        assert_eq!(floats.len(), 4);
        assert!((floats.value(0) - expected.value(0)).abs() < eps);
        assert!((floats.value(1) - expected.value(1)).abs() < eps);
        assert!((floats.value(2) - expected.value(2)).abs() < eps);
        assert!((floats.value(3) - expected.value(3)).abs() < eps);
    }

    #[test]
    fn test_cot_f64() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![12.1, 30.0, 90.0, -30.0]))];
        let result = cot(&args).expect("failed to initialize function cot");
        let floats =
            as_float64_array(&result).expect("failed to initialize function cot");

        let expected = Float64Array::from(vec![
            -1.986_458_685_881_4,
            -0.156_119_952_161_6,
            -0.501_202_783_380_1,
            0.156_119_952_161_6,
        ]);

        let eps = 1e-12;
        assert_eq!(floats.len(), 4);
        assert!((floats.value(0) - expected.value(0)).abs() < eps);
        assert!((floats.value(1) - expected.value(1)).abs() < eps);
        assert!((floats.value(2) - expected.value(2)).abs() < eps);
        assert!((floats.value(3) - expected.value(3)).abs() < eps);
    }

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

    #[test]
    fn test_nanvl_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, 3.0, f64::NAN])), // y
            Arc::new(Float64Array::from(vec![5.0, 6.0, f64::NAN, f64::NAN])), // x
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float64_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }

    #[test]
    fn test_nanvl_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0, f32::NAN])), // y
            Arc::new(Float32Array::from(vec![5.0, 6.0, f32::NAN, f32::NAN])), // x
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float32_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }

    #[test]
    fn test_isnan_f64() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            1.0,
            f64::NAN,
            3.0,
            -f64::NAN,
        ]))];

        let result = isnan(&args).expect("failed to initialize function isnan");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function isnan");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }

    #[test]
    fn test_isnan_f32() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float32Array::from(vec![
            1.0,
            f32::NAN,
            3.0,
            f32::NAN,
        ]))];

        let result = isnan(&args).expect("failed to initialize function isnan");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function isnan");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }

    #[test]
    fn test_iszero_f64() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 0.0, 3.0, -0.0]))];

        let result = iszero(&args).expect("failed to initialize function iszero");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function iszero");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }

    #[test]
    fn test_iszero_f32() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float32Array::from(vec![1.0, 0.0, 3.0, -0.0]))];

        let result = iszero(&args).expect("failed to initialize function iszero");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function iszero");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }
}
