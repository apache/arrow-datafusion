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

use std::ops::Neg;
use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Int32Type, Int64Type, Schema},
};
use arrow_array::Array;

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
pub use datafusion_expr::FuncMonotonicity;
use datafusion_expr::ScalarFunctionDefinition;
use datafusion_expr::{
    type_coercion::functions::data_types, BuiltinScalarFunction, ColumnarValue,
    ScalarFunctionImplementation,
};

use crate::sort_properties::SortProperties;
use crate::{
    conditional_expressions, math_expressions, string_expressions, PhysicalExpr,
    ScalarFunctionExpr,
};

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &BuiltinScalarFunction,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    _execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_expr_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // verify that input data types is consistent with function's `TypeSignature`
    data_types(&input_expr_types, &fun.signature())?;

    let data_type = fun.return_type(&input_expr_types)?;

    let monotonicity = fun.monotonicity();

    let fun_def = ScalarFunctionDefinition::BuiltIn(*fun);
    Ok(Arc::new(ScalarFunctionExpr::new(
        &format!("{fun}"),
        fun_def,
        input_phy_exprs.to_vec(),
        data_type,
        monotonicity,
        fun.signature().type_signature.supports_zero_argument(),
    )))
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
pub enum Hint {
    /// Indicates the argument needs to be padded if it is scalar
    Pad,
    /// Indicates the argument can be converted to an array of length 1
    AcceptsSingular,
}

#[deprecated(since = "36.0.0", note = "Use ColumarValue::values_to_arrays instead")]
pub fn columnar_values_to_array(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    ColumnarValue::values_to_arrays(args)
}

/// Decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
/// Note that this function makes a scalar function with no arguments or all scalar inputs return a scalar.
/// That's said its output will be same for all input rows in a batch.
#[deprecated(
    since = "36.0.0",
    note = "Implement your function directly in terms of ColumnarValue or use `ScalarUDF` instead"
)]
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    make_scalar_function_inner(inner)
}

/// Internal implementation, see comments on `make_scalar_function` for caveats
pub(crate) fn make_scalar_function_inner<F>(inner: F) -> ScalarFunctionImplementation
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
) -> Result<ScalarFunctionImplementation> {
    Ok(match fun {
        // math functions
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
            Arc::new(|args| make_scalar_function_inner(math_expressions::factorial)(args))
        }
        BuiltinScalarFunction::Floor => Arc::new(math_expressions::floor),
        BuiltinScalarFunction::Gcd => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::gcd)(args))
        }
        BuiltinScalarFunction::Iszero => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::iszero)(args))
        }
        BuiltinScalarFunction::Lcm => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::lcm)(args))
        }
        BuiltinScalarFunction::Ln => Arc::new(math_expressions::ln),
        BuiltinScalarFunction::Log10 => Arc::new(math_expressions::log10),
        BuiltinScalarFunction::Log2 => Arc::new(math_expressions::log2),
        BuiltinScalarFunction::Nanvl => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::nanvl)(args))
        }
        BuiltinScalarFunction::Radians => Arc::new(math_expressions::to_radians),
        BuiltinScalarFunction::Random => Arc::new(math_expressions::random),
        BuiltinScalarFunction::Round => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::round)(args))
        }
        BuiltinScalarFunction::Signum => Arc::new(math_expressions::signum),
        BuiltinScalarFunction::Sin => Arc::new(math_expressions::sin),
        BuiltinScalarFunction::Sinh => Arc::new(math_expressions::sinh),
        BuiltinScalarFunction::Sqrt => Arc::new(math_expressions::sqrt),
        BuiltinScalarFunction::Cbrt => Arc::new(math_expressions::cbrt),
        BuiltinScalarFunction::Trunc => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::trunc)(args))
        }
        BuiltinScalarFunction::Pi => Arc::new(math_expressions::pi),
        BuiltinScalarFunction::Power => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::power)(args))
        }
        BuiltinScalarFunction::Atan2 => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::atan2)(args))
        }
        BuiltinScalarFunction::Log => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::log)(args))
        }
        BuiltinScalarFunction::Cot => {
            Arc::new(|args| make_scalar_function_inner(math_expressions::cot)(args))
        }
        // string functions
        BuiltinScalarFunction::CharacterLength => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        character_length,
                        Int32Type,
                        "character_length"
                    );
                    make_scalar_function_inner(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        character_length,
                        Int64Type,
                        "character_length"
                    );
                    make_scalar_function_inner(func)(args)
                }
                other => exec_err!(
                    "Unsupported data type {other:?} for function character_length"
                ),
            })
        }
        BuiltinScalarFunction::Coalesce => Arc::new(conditional_expressions::coalesce),
        BuiltinScalarFunction::Concat => Arc::new(string_expressions::concat),
        BuiltinScalarFunction::ConcatWithSeparator => Arc::new(|args| {
            make_scalar_function_inner(string_expressions::concat_ws)(args)
        }),
        BuiltinScalarFunction::InitCap => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function_inner(string_expressions::initcap::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function_inner(string_expressions::initcap::<i64>)(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function initcap")
            }
        }),
        BuiltinScalarFunction::Left => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(left, i32, "left");
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(left, i64, "left");
                make_scalar_function_inner(func)(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function left"),
        }),
        BuiltinScalarFunction::Lpad => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(lpad, i32, "lpad");
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(lpad, i64, "lpad");
                make_scalar_function_inner(func)(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function lpad"),
        }),
        BuiltinScalarFunction::Reverse => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(reverse, i32, "reverse");
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(reverse, i64, "reverse");
                make_scalar_function_inner(func)(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function reverse")
            }
        }),
        BuiltinScalarFunction::Right => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(right, i32, "right");
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(right, i64, "right");
                make_scalar_function_inner(func)(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function right"),
        }),
        BuiltinScalarFunction::Rpad => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(rpad, i32, "rpad");
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(rpad, i64, "rpad");
                make_scalar_function_inner(func)(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function rpad"),
        }),
        BuiltinScalarFunction::EndsWith => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function_inner(string_expressions::ends_with::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function_inner(string_expressions::ends_with::<i64>)(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function ends_with")
            }
        }),
        BuiltinScalarFunction::Strpos => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    strpos, Int32Type, "strpos"
                );
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    strpos, Int64Type, "strpos"
                );
                make_scalar_function_inner(func)(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function strpos"),
        }),
        BuiltinScalarFunction::Substr => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(substr, i32, "substr");
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(substr, i64, "substr");
                make_scalar_function_inner(func)(args)
            }
            other => exec_err!("Unsupported data type {other:?} for function substr"),
        }),
        BuiltinScalarFunction::Translate => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    translate,
                    i32,
                    "translate"
                );
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    translate,
                    i64,
                    "translate"
                );
                make_scalar_function_inner(func)(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function translate")
            }
        }),
        BuiltinScalarFunction::SubstrIndex => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        substr_index,
                        i32,
                        "substr_index"
                    );
                    make_scalar_function_inner(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        substr_index,
                        i64,
                        "substr_index"
                    );
                    make_scalar_function_inner(func)(args)
                }
                other => {
                    exec_err!("Unsupported data type {other:?} for function substr_index")
                }
            })
        }
        BuiltinScalarFunction::FindInSet => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    find_in_set,
                    Int32Type,
                    "find_in_set"
                );
                make_scalar_function_inner(func)(args)
            }
            DataType::LargeUtf8 => {
                let func = invoke_if_unicode_expressions_feature_flag!(
                    find_in_set,
                    Int64Type,
                    "find_in_set"
                );
                make_scalar_function_inner(func)(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function find_in_set")
            }
        }),
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
    use arrow::{
        array::{
            Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array,
            StringArray, UInt64Array,
        },
        datatypes::Field,
        record_batch::RecordBatch,
    };

    use datafusion_common::cast::as_uint64_array;
    use datafusion_common::{exec_err, internal_err, plan_err};
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use datafusion_expr::type_coercion::functions::data_types;
    use datafusion_expr::Signature;

    use crate::expressions::lit;
    use crate::expressions::try_cast;

    use super::*;

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
            EndsWith,
            &[lit("alphabet"), lit("alph"),],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWith,
            &[lit("alphabet"), lit("bet"),],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWith,
            &[lit(ScalarValue::Utf8(None)), lit("alph"),],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWith,
            &[lit("alphabet"), lit(ScalarValue::Utf8(None)),],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
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
        Ok(())
    }

    #[test]
    fn test_empty_arguments_error() -> Result<()> {
        let execution_props = ExecutionProps::new();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // pick some arbitrary functions to test
        let funs = [BuiltinScalarFunction::Concat];

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

        let funs = [BuiltinScalarFunction::Pi, BuiltinScalarFunction::Random];

        for fun in funs.iter() {
            create_physical_expr_with_type_coercion(fun, &[], &schema, &execution_props)?;
        }
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
        let adapter_func = make_scalar_function_inner(dummy_function);

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
