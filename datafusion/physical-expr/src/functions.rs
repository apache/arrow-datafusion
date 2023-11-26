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

//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also supports coercion to improve user experience: if
//! an argument i32 is passed to a function that supports f64, the
//! argument is automatically is coerced to f64.

use crate::execution_props::ExecutionProps;
use crate::sort_properties::SortProperties;
use crate::{
    array_expressions, conditional_expressions, datetime_expressions,
    expressions::nullif_func, math_expressions, string_expressions, struct_expressions,
    PhysicalExpr, ScalarFunctionExpr,
};
use arrow::{
    array::ArrayRef,
    compute::kernels::length::{bit_length, length},
    datatypes::{DataType, Int32Type, Int64Type, Schema},
};
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
pub use datafusion_expr::FuncMonotonicity;
use datafusion_expr::{
    type_coercion::functions::data_types, BuiltinScalarFunction, ColumnarValue,
    ScalarFunctionImplementation,
};
use std::ops::Neg;
use std::sync::Arc;

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &BuiltinScalarFunction,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_expr_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // verify that input data types is consistent with function's `TypeSignature`
    data_types(&input_expr_types, &fun.signature())?;

    let data_type = fun.return_type(&input_expr_types)?;

    let fun_expr: ScalarFunctionImplementation =
        create_physical_fun(fun, execution_props)?;

    let monotonicity = fun.monotonicity();

    Ok(Arc::new(ScalarFunctionExpr::new(
        &format!("{fun}"),
        fun_expr,
        input_phy_exprs.to_vec(),
        data_type,
        monotonicity,
    )))
}

#[cfg(feature = "encoding_expressions")]
macro_rules! invoke_if_encoding_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {{
        use crate::encoding_expressions;
        encoding_expressions::$FUNC
    }};
}

#[cfg(not(feature = "encoding_expressions"))]
macro_rules! invoke_if_encoding_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {
        |_: &[ColumnarValue]| -> Result<ColumnarValue> {
            internal_err!(
                "function {} requires compilation with feature flag: encoding_expressions.",
                $NAME
            )
        }
    };
}

#[cfg(feature = "crypto_expressions")]
macro_rules! invoke_if_crypto_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {{
        use crate::crypto_expressions;
        crypto_expressions::$FUNC
    }};
}

#[cfg(not(feature = "crypto_expressions"))]
macro_rules! invoke_if_crypto_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {
        |_: &[ColumnarValue]| -> Result<ColumnarValue> {
            internal_err!(
                "function {} requires compilation with feature flag: crypto_expressions.",
                $NAME
            )
        }
    };
}

#[cfg(feature = "regex_expressions")]
macro_rules! invoke_on_array_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use crate::regex_expressions;
        regex_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "regex_expressions"))]
macro_rules! invoke_on_array_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {
        |_: &[ArrayRef]| -> Result<ArrayRef> {
            internal_err!(
                "function {} requires compilation with feature flag: regex_expressions.",
                $NAME
            )
        }
    };
}

#[cfg(feature = "regex_expressions")]
macro_rules! invoke_on_columnar_value_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use crate::regex_expressions;
        regex_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "regex_expressions"))]
macro_rules! invoke_on_columnar_value_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {
        |_: &[ColumnarValue]| -> Result<ScalarFunctionImplementation> {
            internal_err!(
                "function {} requires compilation with feature flag: regex_expressions.",
                $NAME
            )
        }
    };
}

#[cfg(feature = "unicode_expressions")]
macro_rules! invoke_if_unicode_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use crate::unicode_expressions;
        unicode_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "unicode_expressions"))]
macro_rules! invoke_if_unicode_expressions_feature_flag {
  ($FUNC:ident, $T:tt, $NAME:expr) => {
    |_: &[ArrayRef]| -> Result<ArrayRef> {
      internal_err!(
        "function {} requires compilation with feature flag: unicode_expressions.",
        $NAME
      )
    }
  };
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Hint {
    /// Indicates the argument needs to be padded if it is scalar
    Pad,
    /// Indicates the argument can be converted to an array of length 1
    AcceptsSingular,
}

/// decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    make_scalar_function_with_hints(inner, vec![])
}

/// Just like [`make_scalar_function`], decorates the given function to handle both [`ScalarValue`]s and arrays.
/// Additionally can receive a `hints` vector which can be used to control the output arrays when generating them
/// from [`ScalarValue`]s.
///
/// Each element of the `hints` vector gets mapped to the corresponding argument of the function. The number of hints
/// can be less or greater than the number of arguments (for functions with variable number of arguments). Each unmapped
/// argument will assume the default hint (for padding, it is [`Hint::Pad`]).
pub(crate) fn make_scalar_function_with_hints<F>(
    inner: F,
    hints: Vec<Hint>,
) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

/// Create a physical scalar function.
pub fn create_physical_fun(
    fun: &BuiltinScalarFunction,
    execution_props: &ExecutionProps,
) -> Result<ScalarFunctionImplementation> {
    Ok(match fun {
        // math functions
        BuiltinScalarFunction::Abs => {
            Arc::new(|args| make_scalar_function(math_expressions::abs_invoke)(args))
        }
        BuiltinScalarFunction::Acos => Arc::new(math_expressions::acos),
        BuiltinScalarFunction::Asin => Arc::new(math_expressions::asin),
        BuiltinScalarFunction::Atan => Arc::new(math_expressions::atan),
        BuiltinScalarFunction::Acosh => Arc::new(math_expressions::acosh),
        BuiltinScalarFunction::Asinh => Arc::new(math_expressions::asinh),
        BuiltinScalarFunction::Atanh => Arc::new(math_expressions::atanh),
        BuiltinScalarFunction::Ceil => Arc::new(math_expressions::ceil),
        BuiltinScalarFunction::Cos => Arc::new(math_expressions::cos),
        BuiltinScalarFunction::Cosh => Arc::new(math_expressions::cosh),
        BuiltinScalarFunction::Degrees => Arc::new(math_expressions::to_degrees),
        BuiltinScalarFunction::Exp => Arc::new(math_expressions::exp),
        BuiltinScalarFunction::Factorial => {
            Arc::new(|args| make_scalar_function(math_expressions::factorial)(args))
        }
        BuiltinScalarFunction::Floor => Arc::new(math_expressions::floor),
        BuiltinScalarFunction::Gcd => {
            Arc::new(|args| make_scalar_function(math_expressions::gcd)(args))
        }
        BuiltinScalarFunction::Isnan => {
            Arc::new(|args| make_scalar_function(math_expressions::isnan)(args))
        }
        BuiltinScalarFunction::Iszero => {
            Arc::new(|args| make_scalar_function(math_expressions::iszero)(args))
        }
        BuiltinScalarFunction::Lcm => {
            Arc::new(|args| make_scalar_function(math_expressions::lcm)(args))
        }
        BuiltinScalarFunction::Ln => Arc::new(math_expressions::ln),
        BuiltinScalarFunction::Log10 => Arc::new(math_expressions::log10),
        BuiltinScalarFunction::Log2 => Arc::new(math_expressions::log2),
        BuiltinScalarFunction::Nanvl => {
            Arc::new(|args| make_scalar_function(math_expressions::nanvl)(args))
        }
        BuiltinScalarFunction::Radians => Arc::new(math_expressions::to_radians),
        BuiltinScalarFunction::Random => Arc::new(math_expressions::random),
        BuiltinScalarFunction::Round => {
            Arc::new(|args| make_scalar_function(math_expressions::round)(args))
        }
        BuiltinScalarFunction::Signum => Arc::new(math_expressions::signum),
        BuiltinScalarFunction::Sin => Arc::new(math_expressions::sin),
        BuiltinScalarFunction::Sinh => Arc::new(math_expressions::sinh),
        BuiltinScalarFunction::Sqrt => Arc::new(math_expressions::sqrt),
        BuiltinScalarFunction::Cbrt => Arc::new(math_expressions::cbrt),
        BuiltinScalarFunction::Tan => Arc::new(math_expressions::tan),
        BuiltinScalarFunction::Tanh => Arc::new(math_expressions::tanh),
        BuiltinScalarFunction::Trunc => {
            Arc::new(|args| make_scalar_function(math_expressions::trunc)(args))
        }
        BuiltinScalarFunction::Pi => Arc::new(math_expressions::pi),
        BuiltinScalarFunction::Power => {
            Arc::new(|args| make_scalar_function(math_expressions::power)(args))
        }
        BuiltinScalarFunction::Atan2 => {
            Arc::new(|args| make_scalar_function(math_expressions::atan2)(args))
        }
        BuiltinScalarFunction::Log => {
            Arc::new(|args| make_scalar_function(math_expressions::log)(args))
        }
        BuiltinScalarFunction::Cot => {
            Arc::new(|args| make_scalar_function(math_expressions::cot)(args))
        }

        // array functions
        BuiltinScalarFunction::ArrayAggregate => {
            unimplemented!("ArrayAggregate reused the same function as AggregateExpr")
        }
        BuiltinScalarFunction::ArrayAppend => {
            Arc::new(|args| make_scalar_function(array_expressions::array_append)(args))
        }
        BuiltinScalarFunction::ArrayConcat => {
            Arc::new(|args| make_scalar_function(array_expressions::array_concat)(args))
        }
        BuiltinScalarFunction::ArrayEmpty => {
            Arc::new(|args| make_scalar_function(array_expressions::array_empty)(args))
        }
        BuiltinScalarFunction::ArrayHasAll => {
            Arc::new(|args| make_scalar_function(array_expressions::array_has_all)(args))
        }
        BuiltinScalarFunction::ArrayHasAny => {
            Arc::new(|args| make_scalar_function(array_expressions::array_has_any)(args))
        }
        BuiltinScalarFunction::ArrayHas => {
            Arc::new(|args| make_scalar_function(array_expressions::array_has)(args))
        }
        BuiltinScalarFunction::ArrayDims => {
            Arc::new(|args| make_scalar_function(array_expressions::array_dims)(args))
        }
        BuiltinScalarFunction::ArrayElement => {
            Arc::new(|args| make_scalar_function(array_expressions::array_element)(args))
        }
        BuiltinScalarFunction::ArrayExcept => {
            Arc::new(|args| make_scalar_function(array_expressions::array_except)(args))
        }
        BuiltinScalarFunction::ArrayLength => {
            Arc::new(|args| make_scalar_function(array_expressions::array_length)(args))
        }
        BuiltinScalarFunction::Flatten => {
            Arc::new(|args| make_scalar_function(array_expressions::flatten)(args))
        }
        BuiltinScalarFunction::ArrayNdims => {
            Arc::new(|args| make_scalar_function(array_expressions::array_ndims)(args))
        }
        BuiltinScalarFunction::ArrayPopFront => Arc::new(|args| {
            make_scalar_function(array_expressions::array_pop_front)(args)
        }),
        BuiltinScalarFunction::ArrayPopBack => {
            Arc::new(|args| make_scalar_function(array_expressions::array_pop_back)(args))
        }
        BuiltinScalarFunction::ArrayPosition => {
            Arc::new(|args| make_scalar_function(array_expressions::array_position)(args))
        }
        BuiltinScalarFunction::ArrayPositions => Arc::new(|args| {
            make_scalar_function(array_expressions::array_positions)(args)
        }),
        BuiltinScalarFunction::ArrayPrepend => {
            Arc::new(|args| make_scalar_function(array_expressions::array_prepend)(args))
        }
        BuiltinScalarFunction::ArrayRepeat => {
            Arc::new(|args| make_scalar_function(array_expressions::array_repeat)(args))
        }
        BuiltinScalarFunction::ArrayRemove => {
            Arc::new(|args| make_scalar_function(array_expressions::array_remove)(args))
        }
        BuiltinScalarFunction::ArrayRemoveN => {
            Arc::new(|args| make_scalar_function(array_expressions::array_remove_n)(args))
        }
        BuiltinScalarFunction::ArrayRemoveAll => Arc::new(|args| {
            make_scalar_function(array_expressions::array_remove_all)(args)
        }),
        BuiltinScalarFunction::ArrayReplace => {
            Arc::new(|args| make_scalar_function(array_expressions::array_replace)(args))
        }
        BuiltinScalarFunction::ArrayReplaceN => Arc::new(|args| {
            make_scalar_function(array_expressions::array_replace_n)(args)
        }),
        BuiltinScalarFunction::ArrayReplaceAll => Arc::new(|args| {
            make_scalar_function(array_expressions::array_replace_all)(args)
        }),
        BuiltinScalarFunction::ArraySlice => {
            Arc::new(|args| make_scalar_function(array_expressions::array_slice)(args))
        }
        BuiltinScalarFunction::ArrayToString => Arc::new(|args| {
            make_scalar_function(array_expressions::array_to_string)(args)
        }),
        BuiltinScalarFunction::ArrayIntersect => Arc::new(|args| {
            make_scalar_function(array_expressions::array_intersect)(args)
        }),
        BuiltinScalarFunction::Range => {
            Arc::new(|args| make_scalar_function(array_expressions::gen_range)(args))
        }
        BuiltinScalarFunction::Cardinality => {
            Arc::new(|args| make_scalar_function(array_expressions::cardinality)(args))
        }
        BuiltinScalarFunction::MakeArray => {
            Arc::new(|args| make_scalar_function(array_expressions::make_array)(args))
        }
        BuiltinScalarFunction::ArrayUnion => {
            Arc::new(|args| make_scalar_function(array_expressions::array_union)(args))
        }
        // struct functions
        BuiltinScalarFunction::Struct => Arc::new(struct_expressions::struct_expr),

        // string functions
        BuiltinScalarFunction::Ascii => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::ascii::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::ascii::<i64>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function ascii"),
        }),
        BuiltinScalarFunction::BitLength => Arc::new(|args| match &args[0] {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(bit_length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| (x.len() * 8) as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| (x.len() * 8) as i64)),
                )),
                _ => unreachable!(),
            },
        }),
        BuiltinScalarFunction::Btrim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::btrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::btrim::<i64>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function btrim"),
        }),
        BuiltinScalarFunction::CharacterLength => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        character_length,
                        Int32Type,
                        "character_length"
                    );
                    make_scalar_function(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        character_length,
                        Int64Type,
                        "character_length"
                    );
                    make_scalar_function(func)(args)
                }
                other => internal_err!(
                    "Unsupported data type {other:?} for function character_length"
                ),
            })
        }
        BuiltinScalarFunction::Chr => {
            Arc::new(|args| make_scalar_function(string_expressions::chr)(args))
        }
        BuiltinScalarFunction::Coalesce => Arc::new(conditional_expressions::coalesce),
        BuiltinScalarFunction::Concat => Arc::new(string_expressions::concat),
        BuiltinScalarFunction::ConcatWithSeparator => {
            Arc::new(|args| make_scalar_function(string_expressions::concat_ws)(args))
        }
        BuiltinScalarFunction::DatePart => Arc::new(datetime_expressions::date_part),
        BuiltinScalarFunction::DateTrunc => Arc::new(datetime_expressions::date_trunc),
        BuiltinScalarFunction::DateBin => Arc::new(datetime_expressions::date_bin),
        BuiltinScalarFunction::Now => {
            // bind value for now at plan time
            Arc::new(datetime_expressions::make_now(
                execution_props.query_execution_start_time,
            ))
        }
        BuiltinScalarFunction::CurrentDate => {
            // bind value for current_date at plan time
            Arc::new(datetime_expressions::make_current_date(
                execution_props.query_execution_start_time,
            ))
        }
        BuiltinScalarFunction::CurrentTime => {
            // bind value for current_time at plan time
            Arc::new(datetime_expressions::make_current_time(
                execution_props.query_execution_start_time,
            ))
        }
        BuiltinScalarFunction::ToTimestamp => {
            Arc::new(datetime_expressions::to_timestamp_invoke)
        }
        BuiltinScalarFunction::ToTimestampMillis => {
            Arc::new(datetime_expressions::to_timestamp_millis_invoke)
        }
        BuiltinScalarFunction::ToTimestampMicros => {
            Arc::new(datetime_expressions::to_timestamp_micros_invoke)
        }
        BuiltinScalarFunction::ToTimestampNanos => {
            Arc::new(datetime_expressions::to_timestamp_nanos_invoke)
        }
        BuiltinScalarFunction::ToTimestampSeconds => {
            Arc::new(datetime_expressions::to_timestamp_seconds_invoke)
        }
        BuiltinScalarFunction::FromUnixtime => {
            Arc::new(datetime_expressions::from_unixtime_invoke)
        }
        BuiltinScalarFunction::InitCap => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::initcap::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::initcap::<i64>)(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function initcap")
            }
        }),
        BuiltinScalarFunction::Left => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(left, i32, "left");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(left, i64, "left");
                make_scalar_function(func)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function left"),
        }),
        BuiltinScalarFunction::Lower => Arc::new(string_expressions::lower),
        BuiltinScalarFunction::Lpad => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(lpad, i32, "lpad");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(lpad, i64, "lpad");
                make_scalar_function(func)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function lpad"),
        }),
        BuiltinScalarFunction::Ltrim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::ltrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::ltrim::<i64>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function ltrim"),
        }),
        BuiltinScalarFunction::MD5 => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(md5, "md5"))
        }
        BuiltinScalarFunction::Digest => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(digest, "digest"))
        }
        BuiltinScalarFunction::Decode => Arc::new(
            invoke_if_encoding_expressions_feature_flag!(decode, "decode"),
        ),
        BuiltinScalarFunction::Encode => Arc::new(
            invoke_if_encoding_expressions_feature_flag!(encode, "encode"),
        ),
        BuiltinScalarFunction::NullIf => Arc::new(nullif_func),
        BuiltinScalarFunction::OctetLength => Arc::new(|args| match &args[0] {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| x.len() as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| x.len() as i64)),
                )),
                _ => unreachable!(),
            },
        }),
        BuiltinScalarFunction::RegexpMatch => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let func = invoke_on_array_if_regex_expressions_feature_flag!(
                        regexp_match,
                        i32,
                        "regexp_match"
                    );
                    make_scalar_function(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_on_array_if_regex_expressions_feature_flag!(
                        regexp_match,
                        i64,
                        "regexp_match"
                    );
                    make_scalar_function(func)(args)
                }
                other => internal_err!(
                    "Unsupported data type {other:?} for function regexp_match"
                ),
            })
        }
        BuiltinScalarFunction::RegexpReplace => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let specializer_func = invoke_on_columnar_value_if_regex_expressions_feature_flag!(
                        specialize_regexp_replace,
                        i32,
                        "regexp_replace"
                    );
                    let func = specializer_func(args)?;
                    func(args)
                }
                DataType::LargeUtf8 => {
                    let specializer_func = invoke_on_columnar_value_if_regex_expressions_feature_flag!(
                        specialize_regexp_replace,
                        i64,
                        "regexp_replace"
                    );
                    let func = specializer_func(args)?;
                    func(args)
                }
                other => internal_err!(
                    "Unsupported data type {other:?} for function regexp_replace"
                ),
            })
        }
        BuiltinScalarFunction::Repeat => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::repeat::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::repeat::<i64>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function repeat"),
        }),
        BuiltinScalarFunction::Replace => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::replace::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::replace::<i64>)(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function replace")
            }
        }),
        BuiltinScalarFunction::Reverse => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(reverse, i32, "reverse");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(reverse, i64, "reverse");
                make_scalar_function(func)(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function reverse")
            }
        }),
        BuiltinScalarFunction::Right => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(right, i32, "right");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(right, i64, "right");
                make_scalar_function(func)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function right"),
        }),
        BuiltinScalarFunction::Rpad => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(rpad, i32, "rpad");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(rpad, i64, "rpad");
                make_scalar_function(func)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function rpad"),
        }),
        BuiltinScalarFunction::Rtrim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::rtrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::rtrim::<i64>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function rtrim"),
        }),
        BuiltinScalarFunction::SHA224 => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(sha224, "sha224"))
        }
        BuiltinScalarFunction::SHA256 => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(sha256, "sha256"))
        }
        BuiltinScalarFunction::SHA384 => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(sha384, "sha384"))
        }
        BuiltinScalarFunction::SHA512 => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(sha512, "sha512"))
        }
        BuiltinScalarFunction::SplitPart => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::split_part::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::split_part::<i64>)(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function split_part")
            }
        }),
        BuiltinScalarFunction::StringToArray => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    make_scalar_function(array_expressions::string_to_array::<i32>)(args)
                }
                DataType::LargeUtf8 => {
                    make_scalar_function(array_expressions::string_to_array::<i64>)(args)
                }
                other => {
                    internal_err!(
                        "Unsupported data type {other:?} for function string_to_array"
                    )
                }
            })
        }
        BuiltinScalarFunction::StartsWith => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::starts_with::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::starts_with::<i64>)(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function starts_with")
            }
        }),
        BuiltinScalarFunction::Strpos => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    strpos, Int32Type, "strpos"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    strpos, Int64Type, "strpos"
                );
                make_scalar_function(func)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function strpos"),
        }),
        BuiltinScalarFunction::Substr => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(substr, i32, "substr");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(substr, i64, "substr");
                make_scalar_function(func)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function substr"),
        }),
        BuiltinScalarFunction::ToHex => Arc::new(|args| match args[0].data_type() {
            DataType::Int32 => {
                make_scalar_function(string_expressions::to_hex::<Int32Type>)(args)
            }
            DataType::Int64 => {
                make_scalar_function(string_expressions::to_hex::<Int64Type>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function to_hex"),
        }),
        BuiltinScalarFunction::Translate => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    translate,
                    i32,
                    "translate"
                );
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    translate,
                    i64,
                    "translate"
                );
                make_scalar_function(func)(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function translate")
            }
        }),
        BuiltinScalarFunction::Trim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::btrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::btrim::<i64>)(args)
            }
            other => internal_err!("Unsupported data type {other:?} for function trim"),
        }),
        BuiltinScalarFunction::Upper => Arc::new(string_expressions::upper),
        BuiltinScalarFunction::Uuid => Arc::new(string_expressions::uuid),
        BuiltinScalarFunction::ArrowTypeof => Arc::new(move |args| {
            if args.len() != 1 {
                return internal_err!(
                    "arrow_typeof function requires 1 arguments, got {}",
                    args.len()
                );
            }

            let input_data_type = args[0].data_type();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(format!(
                "{input_data_type}"
            )))))
        }),
        BuiltinScalarFunction::OverLay => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::overlay::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::overlay::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function overlay",
            ))),
        }),
        BuiltinScalarFunction::Levenshtein => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    make_scalar_function(string_expressions::levenshtein::<i32>)(args)
                }
                DataType::LargeUtf8 => {
                    make_scalar_function(string_expressions::levenshtein::<i64>)(args)
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function levenshtein",
                ))),
            })
        }
    })
}

#[deprecated(
    since = "32.0.0",
    note = "Moved to `expr` crate. Please use `BuiltinScalarFunction::monotonicity()` instead"
)]
pub fn get_func_monotonicity(fun: &BuiltinScalarFunction) -> Option<FuncMonotonicity> {
    fun.monotonicity()
}

/// Determines a [`ScalarFunctionExpr`]'s monotonicity for the given arguments
/// and the function's behavior depending on its arguments.
pub fn out_ordering(
    func: &FuncMonotonicity,
    arg_orderings: &[SortProperties],
) -> SortProperties {
    func.iter().zip(arg_orderings).fold(
        SortProperties::Singleton,
        |prev_sort, (item, arg)| {
            let current_sort = func_order_in_one_dimension(item, arg);

            match (prev_sort, current_sort) {
                (_, SortProperties::Unordered) => SortProperties::Unordered,
                (SortProperties::Singleton, SortProperties::Ordered(_)) => current_sort,
                (SortProperties::Ordered(prev), SortProperties::Ordered(current))
                    if prev.descending != current.descending =>
                {
                    SortProperties::Unordered
                }
                _ => prev_sort,
            }
        },
    )
}

/// This function decides the monotonicity property of a [`ScalarFunctionExpr`] for a single argument (i.e. across a single dimension), given that argument's sort properties.
fn func_order_in_one_dimension(
    func_monotonicity: &Option<bool>,
    arg: &SortProperties,
) -> SortProperties {
    if *arg == SortProperties::Singleton {
        SortProperties::Singleton
    } else {
        match func_monotonicity {
            None => SortProperties::Unordered,
            Some(false) => {
                if let SortProperties::Ordered(_) = arg {
                    arg.neg()
                } else {
                    SortProperties::Unordered
                }
            }
            Some(true) => {
                if let SortProperties::Ordered(_) = arg {
                    *arg
                } else {
                    SortProperties::Unordered
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::try_cast;
    use crate::expressions::{col, lit};
    use arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array,
            Int32Array, StringArray, UInt64Array,
        },
        datatypes::Field,
        record_batch::RecordBatch,
    };
    use datafusion_common::cast::as_uint64_array;
    use datafusion_common::{exec_err, plan_err};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::type_coercion::functions::data_types;
    use datafusion_expr::Signature;

    /// $FUNC function to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<Option<$EXPECTED_TYPE>> where Result allows testing errors and Option allows testing Null
    /// $EXPECTED_TYPE is the expected value type
    /// $DATA_TYPE is the function to test result type
    /// $ARRAY_TYPE is the column type after function applied
    macro_rules! test_function {
        ($FUNC:ident, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $DATA_TYPE: ident, $ARRAY_TYPE:ident) => {
            // used to provide type annotation
            let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
            let execution_props = ExecutionProps::new();

            // any type works here: we evaluate against a literal of `value`
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];

            let expr =
                create_physical_expr_with_type_coercion(&BuiltinScalarFunction::$FUNC, $ARGS, &schema, &execution_props)?;

            // type is correct
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TYPE);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

            match expected {
                Ok(expected) => {
                    let result = expr.evaluate(&batch)?;
                    let result = result.into_array(batch.num_rows()).expect("Failed to convert to array");
                    let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

                    // value is correct
                    match expected {
                        Some(v) => assert_eq!(result.value(0), v),
                        None => assert!(result.is_null(0)),
                    };
                }
                Err(expected_error) => {
                    // evaluate is expected error - cannot use .expect_err() due to Debug not being implemented
                    match expr.evaluate(&batch) {
                        Ok(_) => assert!(false, "expected error"),
                        Err(error) => {
                            assert!(expected_error.strip_backtrace().starts_with(&error.strip_backtrace()));
                        }
                    }
                }
            };
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(Ascii, &[lit("x")], Ok(Some(120)), i32, Int32, Int32Array);
        test_function!(Ascii, &[lit("ésoj")], Ok(Some(233)), i32, Int32, Int32Array);
        test_function!(
            Ascii,
            &[lit("💯")],
            Ok(Some(128175)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit("💯a")],
            Ok(Some(128175)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(Ascii, &[lit("")], Ok(Some(0)), i32, Int32, Int32Array);
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit("chars")],
            Ok(Some(40)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit("josé")],
            Ok(Some(40)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(BitLength, &[lit("")], Ok(Some(0)), i32, Int32, Int32Array);
        test_function!(
            Btrim,
            &[lit(" trim ")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(" trim")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit("trim ")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit("\n trim \n")],
            Ok(Some("\n trim \n")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit("xyxtrimyyx"), lit("xyz"),],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit("\nxyxtrimyyx\n"), lit("xyz\n"),],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(None)), lit("xyz"),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit("xyxtrimyyx"), lit(ScalarValue::Utf8(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit("chars")],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit("josé")],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit("")],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            CharacterLength,
            &[lit("josé")],
            internal_err!(
                "function character_length requires compilation with feature flag: unicode_expressions."
            ),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(128175)))],
            Ok(Some("💯")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(120)))],
            Ok(Some("x")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(128175)))],
            Ok(Some("💯")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(0)))],
            exec_err!("null character not permitted."),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(i64::MAX)))],
            exec_err!("requested character too large for encoding."),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[lit("aa"), lit("bb"), lit("cc"),],
            Ok(Some("aabbcc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[lit("aa"), lit(ScalarValue::Utf8(None)), lit("cc"),],
            Ok(Some("aacc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[lit(ScalarValue::Utf8(None))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[lit("|"), lit("aa"), lit("bb"), lit("cc"),],
            Ok(Some("aa|bb|cc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[lit("|"), lit(ScalarValue::Utf8(None)),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(None)),
                lit("aa"),
                lit("bb"),
                lit("cc"),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[lit("|"), lit("aa"), lit(ScalarValue::Utf8(None)), lit("cc"),],
            Ok(Some("aa|cc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::Int32(Some(1)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::UInt32(Some(1)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::UInt64(Some(1)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::Float64(Some(1.0)))],
            Ok(Some((1.0_f64).exp())),
            f64,
            Float64,
            Float64Array
        );
        test_function!(
            Exp,
            &[lit(ScalarValue::Float32(Some(1.0)))],
            Ok(Some((1.0_f32).exp())),
            f32,
            Float32,
            Float32Array
        );
        test_function!(
            InitCap,
            &[lit("hi THOMAS")],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(InitCap, &[lit("")], Ok(Some("")), &str, Utf8, StringArray);
        test_function!(InitCap, &[lit("")], Ok(Some("")), &str, Utf8, StringArray);
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("abcde"), lit(ScalarValue::Int8(Some(2))),],
            Ok(Some("ab")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(200))),],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(-2))),],
            Ok(Some("abc")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(-200))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(0))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("abcde"), lit(ScalarValue::Int64(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("joséésoj"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[lit("joséésoj"), lit(ScalarValue::Int64(Some(-3))),],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Left,
            &[
                lit("abcde"),
                lit(ScalarValue::Int8(Some(2))),
            ],
            internal_err!(
                "function left requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("josé"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some(" josé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some("   hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(0))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))), lit("xy"),],
            Ok(Some("xyxhi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(21))), lit("abcdef"),],
            Ok(Some("abcdefabcdefabcdefahi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))), lit(" "),],
            Ok(Some("   hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))), lit(""),],
            Ok(Some("hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
                lit("xy"),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("hi"), lit(ScalarValue::Int64(None)), lit("xy"),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit("hi"),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("josé"), lit(ScalarValue::Int64(Some(10))), lit("xy"),],
            Ok(Some("xyxyxyjosé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[lit("josé"), lit(ScalarValue::Int64(Some(10))), lit("éñ"),],
            Ok(Some("éñéñéñjosé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Lpad,
            &[
                lit("josé"),
                lit(ScalarValue::Int64(Some(5))),
            ],
            internal_err!(
                "function lpad requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(" trim")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(" trim ")],
            Ok(Some("trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit("trim ")],
            Ok(Some("trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit("trim")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit("\n trim ")],
            Ok(Some("\n trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit("tom")],
            Ok(Some("34b7da764b21d298ef307d04d8152dc5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit("")],
            Ok(Some("d41d8cd98f00b204e9800998ecf8427e")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            MD5,
            &[lit("tom")],
            internal_err!(
                "function md5 requires compilation with feature flag: crypto_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            OctetLength,
            &[lit("chars")],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLength,
            &[lit("josé")],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(OctetLength, &[lit("")], Ok(Some(0)), i32, Int32, Int32Array);
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[lit("Thomas"), lit(".[mN]a."), lit("M"),],
            Ok(Some("ThM")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[lit("foobarbaz"), lit("b.."), lit("X"),],
            Ok(Some("fooXbaz")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[lit("foobarbaz"), lit("b.."), lit("X"), lit("g"),],
            Ok(Some("fooXX")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[lit("foobarbaz"), lit("b(..)"), lit("X\\1Y"), lit("g"),],
            Ok(Some("fooXarYXazY")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(None)),
                lit("b(..)"),
                lit("X\\1Y"),
                lit("g"),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit("foobarbaz"),
                lit(ScalarValue::Utf8(None)),
                lit("X\\1Y"),
                lit("g"),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit("foobarbaz"),
                lit("b(..)"),
                lit(ScalarValue::Utf8(None)),
                lit("g"),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit("foobarbaz"),
                lit("b(..)"),
                lit("X\\1Y"),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[lit("ABCabcABC"), lit("(abc)"), lit("X"), lit("gi"),],
            Ok(Some("XXX")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[lit("ABCabcABC"), lit("(abc)"), lit("X"), lit("i"),],
            Ok(Some("XabcABC")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "regex_expressions"))]
        test_function!(
            RegexpReplace,
            &[
                lit("foobarbaz"),
                lit("b.."),
                lit("X"),
            ],
            internal_err!(
                "function regexp_replace requires compilation with feature flag: regex_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[lit("Pg"), lit(ScalarValue::Int64(Some(4))),],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[lit("Pg"), lit(ScalarValue::Int64(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit("abcde")],
            Ok(Some("edcba")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit("loẅks")],
            Ok(Some("sk̈wol")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit("loẅks")],
            Ok(Some("sk̈wol")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Reverse,
            &[lit("abcde")],
            internal_err!(
                "function reverse requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("abcde"), lit(ScalarValue::Int8(Some(2))),],
            Ok(Some("de")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(200))),],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(-2))),],
            Ok(Some("cde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(-200))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("abcde"), lit(ScalarValue::Int64(Some(0))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("abcde"), lit(ScalarValue::Int64(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("joséésoj"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[lit("joséésoj"), lit(ScalarValue::Int64(Some(-3))),],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Right,
            &[
                lit("abcde"),
                lit(ScalarValue::Int8(Some(2))),
            ],
            internal_err!(
                "function right requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("josé"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some("josé ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(0))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))), lit("xy"),],
            Ok(Some("hixyx")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(21))), lit("abcdef"),],
            Ok(Some("hiabcdefabcdefabcdefa")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))), lit(" "),],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(Some(5))), lit(""),],
            Ok(Some("hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Int64(Some(5))),
                lit("xy"),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("hi"), lit(ScalarValue::Int64(None)), lit("xy"),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit("hi"),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("josé"), lit(ScalarValue::Int64(Some(10))), lit("xy"),],
            Ok(Some("joséxyxyxy")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[lit("josé"), lit(ScalarValue::Int64(Some(10))), lit("éñ"),],
            Ok(Some("josééñéñéñ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Rpad,
            &[
                lit("josé"),
                lit(ScalarValue::Int64(Some(5))),
            ],
            internal_err!(
                "function rpad requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit("trim ")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(" trim ")],
            Ok(Some(" trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(" trim \n")],
            Ok(Some(" trim \n")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(" trim")],
            Ok(Some(" trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit("trim")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit("tom")],
            Ok(Some(&[
                11u8, 246u8, 203u8, 98u8, 100u8, 156u8, 66u8, 169u8, 174u8, 56u8, 118u8,
                171u8, 111u8, 109u8, 146u8, 173u8, 54u8, 203u8, 84u8, 20u8, 228u8, 149u8,
                248u8, 135u8, 50u8, 146u8, 190u8, 77u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit("")],
            Ok(Some(&[
                209u8, 74u8, 2u8, 140u8, 42u8, 58u8, 43u8, 201u8, 71u8, 97u8, 2u8, 187u8,
                40u8, 130u8, 52u8, 196u8, 21u8, 162u8, 176u8, 31u8, 130u8, 142u8, 166u8,
                42u8, 197u8, 179u8, 228u8, 47u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA224,
            &[lit("tom")],
            internal_err!(
                "function sha224 requires compilation with feature flag: crypto_expressions."
            ),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit("tom")],
            Ok(Some(&[
                225u8, 96u8, 143u8, 117u8, 197u8, 215u8, 129u8, 63u8, 61u8, 64u8, 49u8,
                203u8, 48u8, 191u8, 183u8, 134u8, 80u8, 125u8, 152u8, 19u8, 117u8, 56u8,
                255u8, 142u8, 18u8, 138u8, 111u8, 247u8, 78u8, 132u8, 230u8, 67u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit("")],
            Ok(Some(&[
                227u8, 176u8, 196u8, 66u8, 152u8, 252u8, 28u8, 20u8, 154u8, 251u8, 244u8,
                200u8, 153u8, 111u8, 185u8, 36u8, 39u8, 174u8, 65u8, 228u8, 100u8, 155u8,
                147u8, 76u8, 164u8, 149u8, 153u8, 27u8, 120u8, 82u8, 184u8, 85u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA256,
            &[lit("tom")],
            internal_err!(
                "function sha256 requires compilation with feature flag: crypto_expressions."
            ),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit("tom")],
            Ok(Some(&[
                9u8, 111u8, 91u8, 104u8, 170u8, 119u8, 132u8, 142u8, 79u8, 223u8, 92u8,
                28u8, 11u8, 53u8, 13u8, 226u8, 219u8, 250u8, 214u8, 15u8, 253u8, 124u8,
                37u8, 217u8, 234u8, 7u8, 198u8, 193u8, 155u8, 138u8, 77u8, 85u8, 169u8,
                24u8, 126u8, 177u8, 23u8, 197u8, 87u8, 136u8, 63u8, 88u8, 193u8, 109u8,
                250u8, 195u8, 227u8, 67u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit("")],
            Ok(Some(&[
                56u8, 176u8, 96u8, 167u8, 81u8, 172u8, 150u8, 56u8, 76u8, 217u8, 50u8,
                126u8, 177u8, 177u8, 227u8, 106u8, 33u8, 253u8, 183u8, 17u8, 20u8, 190u8,
                7u8, 67u8, 76u8, 12u8, 199u8, 191u8, 99u8, 246u8, 225u8, 218u8, 39u8,
                78u8, 222u8, 191u8, 231u8, 111u8, 101u8, 251u8, 213u8, 26u8, 210u8,
                241u8, 72u8, 152u8, 185u8, 91u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA384,
            &[lit("tom")],
            internal_err!(
                "function sha384 requires compilation with feature flag: crypto_expressions."
            ),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit("tom")],
            Ok(Some(&[
                110u8, 27u8, 155u8, 63u8, 232u8, 64u8, 104u8, 14u8, 55u8, 5u8, 31u8,
                122u8, 213u8, 233u8, 89u8, 214u8, 243u8, 154u8, 208u8, 248u8, 136u8,
                93u8, 133u8, 81u8, 102u8, 245u8, 92u8, 101u8, 148u8, 105u8, 211u8, 200u8,
                183u8, 129u8, 24u8, 196u8, 74u8, 42u8, 73u8, 199u8, 45u8, 219u8, 72u8,
                28u8, 214u8, 216u8, 115u8, 16u8, 52u8, 225u8, 28u8, 192u8, 48u8, 7u8,
                11u8, 168u8, 67u8, 169u8, 11u8, 52u8, 149u8, 203u8, 141u8, 62u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit("")],
            Ok(Some(&[
                207u8, 131u8, 225u8, 53u8, 126u8, 239u8, 184u8, 189u8, 241u8, 84u8, 40u8,
                80u8, 214u8, 109u8, 128u8, 7u8, 214u8, 32u8, 228u8, 5u8, 11u8, 87u8,
                21u8, 220u8, 131u8, 244u8, 169u8, 33u8, 211u8, 108u8, 233u8, 206u8, 71u8,
                208u8, 209u8, 60u8, 93u8, 133u8, 242u8, 176u8, 255u8, 131u8, 24u8, 210u8,
                135u8, 126u8, 236u8, 47u8, 99u8, 185u8, 49u8, 189u8, 71u8, 65u8, 122u8,
                129u8, 165u8, 56u8, 50u8, 122u8, 249u8, 39u8, 218u8, 62u8
            ])),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            BinaryArray
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA512,
            &[lit("tom")],
            internal_err!(
                "function sha512 requires compilation with feature flag: crypto_expressions."
            ),
            &[u8],
            Binary,
            BinaryArray
        );
        test_function!(
            SplitPart,
            &[
                lit("abc~@~def~@~ghi"),
                lit("~@~"),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("def")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPart,
            &[
                lit("abc~@~def~@~ghi"),
                lit("~@~"),
                lit(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPart,
            &[
                lit("abc~@~def~@~ghi"),
                lit("~@~"),
                lit(ScalarValue::Int64(Some(-1))),
            ],
            exec_err!("field position must be greater than zero"),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            StartsWith,
            &[lit("alphabet"), lit("alph"),],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[lit("alphabet"), lit("blph"),],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[lit(ScalarValue::Utf8(None)), lit("alph"),],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[lit("alphabet"), lit(ScalarValue::Utf8(None)),],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[lit("abc"), lit("c"),],
            Ok(Some(3)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[lit("josé"), lit("é"),],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[lit("joséésoj"), lit("so"),],
            Ok(Some(6)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[lit("joséésoj"), lit("abc"),],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[lit(ScalarValue::Utf8(None)), lit("abc"),],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[lit("joséésoj"), lit(ScalarValue::Utf8(None)),],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Strpos,
            &[
                lit("joséésoj"),
                lit(ScalarValue::Utf8(None)),
            ],
            internal_err!(
                "function strpos requires compilation with feature flag: unicode_expressions."
            ),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(Some(0))),],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("joséésoj"), lit(ScalarValue::Int64(Some(5))),],
            Ok(Some("ésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("joséésoj"), lit(ScalarValue::Int64(Some(-5))),],
            Ok(Some("joséésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(Some(1))),],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(Some(2))),],
            Ok(Some("lphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(Some(3))),],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(Some(-3))),],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(Some(30))),],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[lit("alphabet"), lit(ScalarValue::Int64(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(3))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("ph")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(3))),
                lit(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(0))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("alph")),
            &str,
            Utf8,
            StringArray
        );
        // starting from 5 (10 + -5)
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(-5))),
                lit(ScalarValue::Int64(Some(10))),
            ],
            Ok(Some("alph")),
            &str,
            Utf8,
            StringArray
        );
        // starting from -1 (4 + -5)
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(-5))),
                lit(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        // starting from 0 (5 + -5)
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(-5))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(None)),
                lit(ScalarValue::Int64(Some(20))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(3))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(-1))),
            ],
            exec_err!("negative substring length not allowed: substr(<str>, 1, -1)"),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit("joséésoj"),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("és")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Substr,
            &[
                lit("alphabet"),
                lit(ScalarValue::Int64(Some(0))),
            ],
            internal_err!(
                "function substr requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[lit("12345"), lit("143"), lit("ax"),],
            Ok(Some("a2x5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[lit(ScalarValue::Utf8(None)), lit("143"), lit("ax"),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[lit("12345"), lit(ScalarValue::Utf8(None)), lit("ax"),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[lit("12345"), lit("143"), lit(ScalarValue::Utf8(None)),],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[lit("é2íñ5"), lit("éñí"), lit("óü"),],
            Ok(Some("ó2ü5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Translate,
            &[
                lit("12345"),
                lit("143"),
                lit("ax"),
            ],
            internal_err!(
                "function translate requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(" trim ")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit("trim ")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(" trim")],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit("upper")],
            Ok(Some("UPPER")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit("UPPER")],
            Ok(Some("UPPER")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        Ok(())
    }

    #[test]
    fn test_empty_arguments_error() -> Result<()> {
        let execution_props = ExecutionProps::new();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // pick some arbitrary functions to test
        let funs = [
            BuiltinScalarFunction::Concat,
            BuiltinScalarFunction::ToTimestamp,
            BuiltinScalarFunction::Abs,
            BuiltinScalarFunction::Repeat,
        ];

        for fun in funs.iter() {
            let expr = create_physical_expr_with_type_coercion(
                fun,
                &[],
                &schema,
                &execution_props,
            );

            match expr {
                Ok(..) => {
                    return plan_err!(
                        "Builtin scalar function {fun} does not support empty arguments"
                    );
                }
                Err(DataFusionError::Plan(_)) => {
                    // Continue the loop
                }
                Err(..) => {
                    return internal_err!(
                        "Builtin scalar function {fun} didn't got the right error with empty arguments");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_empty_arguments() -> Result<()> {
        let execution_props = ExecutionProps::new();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let funs = [
            BuiltinScalarFunction::Now,
            BuiltinScalarFunction::Pi,
            BuiltinScalarFunction::Random,
            BuiltinScalarFunction::Uuid,
        ];

        for fun in funs.iter() {
            create_physical_expr_with_type_coercion(fun, &[], &schema, &execution_props)?;
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "regex_expressions")]
    fn test_regexp_match() -> Result<()> {
        use datafusion_common::cast::{as_list_array, as_string_array};
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let execution_props = ExecutionProps::new();

        let col_value: ArrayRef = Arc::new(StringArray::from(vec!["aaa-555"]));
        let pattern = lit(r".*-(\d*)");
        let columns: Vec<ArrayRef> = vec![col_value];
        let expr = create_physical_expr_with_type_coercion(
            &BuiltinScalarFunction::RegexpMatch,
            &[col("a", &schema)?, pattern],
            &schema,
            &execution_props,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");

        // downcast works
        let result = as_list_array(&result)?;
        let first_row = result.value(0);
        let first_row = as_string_array(&first_row)?;

        // value is correct
        let expected = "555".to_string();
        assert_eq!(first_row.value(0), expected);

        Ok(())
    }

    #[test]
    #[cfg(feature = "regex_expressions")]
    fn test_regexp_match_all_literals() -> Result<()> {
        use datafusion_common::cast::{as_list_array, as_string_array};
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let execution_props = ExecutionProps::new();

        let col_value = lit("aaa-555");
        let pattern = lit(r".*-(\d*)");
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];
        let expr = create_physical_expr_with_type_coercion(
            &BuiltinScalarFunction::RegexpMatch,
            &[col_value, pattern],
            &schema,
            &execution_props,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");

        // downcast works
        let result = as_list_array(&result)?;
        let first_row = result.value(0);
        let first_row = as_string_array(&first_row)?;

        // value is correct
        let expected = "555".to_string();
        assert_eq!(first_row.value(0), expected);

        Ok(())
    }

    // Helper function just for testing.
    // Returns `expressions` coerced to types compatible with
    // `signature`, if possible.
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

    // Helper function just for testing.
    // The type coercion will be done in the logical phase, should do the type coercion for the test
    fn create_physical_expr_with_type_coercion(
        fun: &BuiltinScalarFunction,
        input_phy_exprs: &[Arc<dyn PhysicalExpr>],
        input_schema: &Schema,
        execution_props: &ExecutionProps,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let type_coerced_phy_exprs =
            coerce(input_phy_exprs, input_schema, &fun.signature()).unwrap();
        create_physical_expr(fun, &type_coerced_phy_exprs, input_schema, execution_props)
    }

    fn dummy_function(args: &[ArrayRef]) -> Result<ArrayRef> {
        let result: UInt64Array =
            args.iter().map(|array| Some(array.len() as u64)).collect();
        Ok(Arc::new(result) as ArrayRef)
    }

    fn unpack_uint64_array(col: Result<ColumnarValue>) -> Result<Vec<u64>> {
        if let ColumnarValue::Array(array) = col? {
            Ok(as_uint64_array(&array)?.values().to_vec())
        } else {
            internal_err!("Unexpected scalar created by a test function")
        }
    }

    #[test]
    fn test_make_scalar_function() -> Result<()> {
        let adapter_func = make_scalar_function(dummy_function);

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_no_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(dummy_function, vec![]);

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 1]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_hints_on_arrays() -> Result<()> {
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular],
        );

        let result = unpack_uint64_array(adapter_func(&[array_arg.clone(), array_arg]))?;
        assert_eq!(result, vec![5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_mixed_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::Pad],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[
            array_arg,
            scalar_arg.clone(),
            scalar_arg,
        ]))?;
        assert_eq!(result, vec![5, 1, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_more_arguments_than_hints() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::Pad],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[
            array_arg.clone(),
            scalar_arg.clone(),
            scalar_arg,
            array_arg,
        ]))?;
        assert_eq!(result, vec![5, 1, 5, 5]);

        Ok(())
    }

    #[test]
    fn test_make_scalar_function_with_hints_than_arguments() -> Result<()> {
        let adapter_func = make_scalar_function_with_hints(
            dummy_function,
            vec![
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::Pad,
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::Pad,
            ],
        );

        let scalar_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let array_arg = ColumnarValue::Array(
            ScalarValue::Int64(Some(1))
                .to_array_of_size(5)
                .expect("Failed to convert to array of size"),
        );
        let result = unpack_uint64_array(adapter_func(&[array_arg, scalar_arg]))?;
        assert_eq!(result, vec![5, 1]);

        Ok(())
    }
}
