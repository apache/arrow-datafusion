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
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use super::{
    type_coercion::{coerce, data_types},
    ColumnarValue, PhysicalExpr,
};
use crate::execution::context::ExecutionProps;
use crate::physical_plan::expressions::{
    cast_column, nullif_func, SUPPORTED_NULLIF_TYPES,
};
use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::array::{Array, PrimitiveArray, Utf8Array};
use arrow::error::{Error as ArrowError, Result as ArrowResult};
use arrow::types::{NativeType, Offset};
use arrow::{
    array::ArrayRef,
    compute::length::length,
    datatypes::TimeUnit,
    datatypes::{DataType, Field, Schema},
};
use datafusion_expr::ScalarFunctionImplementation;
pub use datafusion_expr::{BuiltinScalarFunction, Signature, TypeSignature, Volatility};
use datafusion_physical_expr::array_expressions;
use datafusion_physical_expr::datetime_expressions;
use datafusion_physical_expr::expressions::DEFAULT_DATAFUSION_CAST_OPTIONS;
use datafusion_physical_expr::math_expressions;
use datafusion_physical_expr::string_expressions;
use std::sync::Arc;

macro_rules! make_utf8_to_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                DataType::LargeUtf8 => $largeUtf8Type,
                DataType::Utf8 => $utf8Type,
                _ => {
                    // this error is internal as `data_types` should have captured this.
                    return Err(DataFusionError::Internal(format!(
                        "The {:?} function can only accept strings.",
                        name
                    )));
                }
            })
        }
    };
}

make_utf8_to_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);
make_utf8_to_return_type!(utf8_to_int_type, DataType::Int64, DataType::Int32);
make_utf8_to_return_type!(utf8_to_binary_type, DataType::Binary, DataType::Binary);

/// Returns the datatype of the scalar function
pub fn return_type(
    fun: &BuiltinScalarFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    if input_expr_types.is_empty() && !fun.supports_zero_argument() {
        return Err(DataFusionError::Internal(format!(
            "Builtin scalar function {} does not support empty arguments",
            fun
        )));
    }

    // verify that this is a valid set of data types for this function
    data_types(input_expr_types, &signature(fun))?;

    // the return type of the built in function.
    // Some built-in functions' return type depends on the incoming type.
    match fun {
        BuiltinScalarFunction::Array => Ok(DataType::FixedSizeList(
            Box::new(Field::new("item", input_expr_types[0].clone(), true)),
            input_expr_types.len(),
        )),
        BuiltinScalarFunction::Ascii => Ok(DataType::Int32),
        BuiltinScalarFunction::BitLength => {
            utf8_to_int_type(&input_expr_types[0], "bit_length")
        }
        BuiltinScalarFunction::Btrim => utf8_to_str_type(&input_expr_types[0], "btrim"),
        BuiltinScalarFunction::CharacterLength => {
            utf8_to_int_type(&input_expr_types[0], "character_length")
        }
        BuiltinScalarFunction::Chr => Ok(DataType::Utf8),
        BuiltinScalarFunction::Concat => Ok(DataType::Utf8),
        BuiltinScalarFunction::ConcatWithSeparator => Ok(DataType::Utf8),
        BuiltinScalarFunction::DatePart => Ok(DataType::Int32),
        BuiltinScalarFunction::DateTrunc => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        BuiltinScalarFunction::InitCap => {
            utf8_to_str_type(&input_expr_types[0], "initcap")
        }
        BuiltinScalarFunction::Left => utf8_to_str_type(&input_expr_types[0], "left"),
        BuiltinScalarFunction::Lower => utf8_to_str_type(&input_expr_types[0], "lower"),
        BuiltinScalarFunction::Lpad => utf8_to_str_type(&input_expr_types[0], "lpad"),
        BuiltinScalarFunction::Ltrim => utf8_to_str_type(&input_expr_types[0], "ltrim"),
        BuiltinScalarFunction::MD5 => utf8_to_str_type(&input_expr_types[0], "md5"),
        BuiltinScalarFunction::NullIf => {
            // NULLIF has two args and they might get coerced, get a preview of this
            let coerced_types = data_types(input_expr_types, &signature(fun));
            coerced_types.map(|typs| typs[0].clone())
        }
        BuiltinScalarFunction::OctetLength => {
            utf8_to_int_type(&input_expr_types[0], "octet_length")
        }
        BuiltinScalarFunction::Random => Ok(DataType::Float64),
        BuiltinScalarFunction::RegexpReplace => {
            utf8_to_str_type(&input_expr_types[0], "regex_replace")
        }
        BuiltinScalarFunction::Repeat => utf8_to_str_type(&input_expr_types[0], "repeat"),
        BuiltinScalarFunction::Replace => {
            utf8_to_str_type(&input_expr_types[0], "replace")
        }
        BuiltinScalarFunction::Reverse => {
            utf8_to_str_type(&input_expr_types[0], "reverse")
        }
        BuiltinScalarFunction::Right => utf8_to_str_type(&input_expr_types[0], "right"),
        BuiltinScalarFunction::Rpad => utf8_to_str_type(&input_expr_types[0], "rpad"),
        BuiltinScalarFunction::Rtrim => utf8_to_str_type(&input_expr_types[0], "rtrimp"),
        BuiltinScalarFunction::SHA224 => {
            utf8_to_binary_type(&input_expr_types[0], "sha224")
        }
        BuiltinScalarFunction::SHA256 => {
            utf8_to_binary_type(&input_expr_types[0], "sha256")
        }
        BuiltinScalarFunction::SHA384 => {
            utf8_to_binary_type(&input_expr_types[0], "sha384")
        }
        BuiltinScalarFunction::SHA512 => {
            utf8_to_binary_type(&input_expr_types[0], "sha512")
        }
        BuiltinScalarFunction::Digest => {
            utf8_to_binary_type(&input_expr_types[0], "digest")
        }
        BuiltinScalarFunction::SplitPart => {
            utf8_to_str_type(&input_expr_types[0], "split_part")
        }
        BuiltinScalarFunction::StartsWith => Ok(DataType::Boolean),
        BuiltinScalarFunction::Strpos => utf8_to_int_type(&input_expr_types[0], "strpos"),
        BuiltinScalarFunction::Substr => utf8_to_str_type(&input_expr_types[0], "substr"),
        BuiltinScalarFunction::ToHex => Ok(match input_expr_types[0] {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                DataType::Utf8
            }
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The to_hex function can only accept integers.".to_string(),
                ));
            }
        }),
        BuiltinScalarFunction::ToTimestamp => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        BuiltinScalarFunction::ToTimestampMillis => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        BuiltinScalarFunction::ToTimestampMicros => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        BuiltinScalarFunction::ToTimestampSeconds => {
            Ok(DataType::Timestamp(TimeUnit::Second, None))
        }
        BuiltinScalarFunction::Now => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".to_owned()),
        )),
        BuiltinScalarFunction::Translate => {
            utf8_to_str_type(&input_expr_types[0], "translate")
        }
        BuiltinScalarFunction::Trim => utf8_to_str_type(&input_expr_types[0], "trim"),
        BuiltinScalarFunction::Upper => utf8_to_str_type(&input_expr_types[0], "upper"),
        BuiltinScalarFunction::RegexpMatch => Ok(match input_expr_types[0] {
            DataType::LargeUtf8 => {
                DataType::List(Box::new(Field::new("item", DataType::LargeUtf8, true)))
            }
            DataType::Utf8 => {
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true)))
            }
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The regexp_extract function can only accept strings.".to_string(),
                ));
            }
        }),

        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Acos
        | BuiltinScalarFunction::Asin
        | BuiltinScalarFunction::Atan
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Cos
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Log
        | BuiltinScalarFunction::Ln
        | BuiltinScalarFunction::Log10
        | BuiltinScalarFunction::Log2
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Signum
        | BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Tan
        | BuiltinScalarFunction::Trunc => match input_expr_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        },
    }
}

/// Create a physical (function) expression.
/// This function errors when `args`' can't be coerced to a valid argument type of the function.
pub fn create_physical_expr(
    fun: &BuiltinScalarFunction,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let coerced_phy_exprs = coerce(input_phy_exprs, input_schema, &signature(fun))?;

    let coerced_expr_types = coerced_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    let data_type = return_type(fun, &coerced_expr_types)?;

    let fun_expr: ScalarFunctionImplementation = match fun {
        // These functions need args and input schema to pick an implementation
        // Unlike the string functions, which actually figure out the function to use with each array,
        // here we return either a cast fn or string timestamp translation based on the expression data type
        // so we don't have to pay a per-array/batch cost.
        BuiltinScalarFunction::ToTimestamp => {
            Arc::new(match coerced_phy_exprs[0].data_type(input_schema) {
                Ok(DataType::Int64) | Ok(DataType::Timestamp(_, None)) => {
                    |col_values: &[ColumnarValue]| {
                        cast_column(
                            &col_values[0],
                            &DataType::Timestamp(TimeUnit::Nanosecond, None),
                            DEFAULT_DATAFUSION_CAST_OPTIONS,
                        )
                    }
                }
                Ok(DataType::Utf8) => datetime_expressions::to_timestamp,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {:?} for function to_timestamp",
                        other,
                    )))
                }
            })
        }
        BuiltinScalarFunction::ToTimestampMillis => {
            Arc::new(match coerced_phy_exprs[0].data_type(input_schema) {
                Ok(DataType::Int64) | Ok(DataType::Timestamp(_, None)) => {
                    |col_values: &[ColumnarValue]| {
                        cast_column(
                            &col_values[0],
                            &DataType::Timestamp(TimeUnit::Millisecond, None),
                            DEFAULT_DATAFUSION_CAST_OPTIONS,
                        )
                    }
                }
                Ok(DataType::Utf8) => datetime_expressions::to_timestamp_millis,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {:?} for function to_timestamp_millis",
                        other,
                    )))
                }
            })
        }
        BuiltinScalarFunction::ToTimestampMicros => {
            Arc::new(match coerced_phy_exprs[0].data_type(input_schema) {
                Ok(DataType::Int64) | Ok(DataType::Timestamp(_, None)) => {
                    |col_values: &[ColumnarValue]| {
                        cast_column(
                            &col_values[0],
                            &DataType::Timestamp(TimeUnit::Microsecond, None),
                            DEFAULT_DATAFUSION_CAST_OPTIONS,
                        )
                    }
                }
                Ok(DataType::Utf8) => datetime_expressions::to_timestamp_micros,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {:?} for function to_timestamp_micros",
                        other,
                    )))
                }
            })
        }
        BuiltinScalarFunction::ToTimestampSeconds => Arc::new({
            match coerced_phy_exprs[0].data_type(input_schema) {
                Ok(DataType::Int64) | Ok(DataType::Timestamp(_, None)) => {
                    |col_values: &[ColumnarValue]| {
                        cast_column(
                            &col_values[0],
                            &DataType::Timestamp(TimeUnit::Second, None),
                            DEFAULT_DATAFUSION_CAST_OPTIONS,
                        )
                    }
                }
                Ok(DataType::Utf8) => datetime_expressions::to_timestamp_seconds,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {:?} for function to_timestamp_seconds",
                        other,
                    )))
                }
            }
        }),
        // These don't need args and input schema
        _ => create_physical_fun(fun, execution_props)?,
    };

    Ok(Arc::new(ScalarFunctionExpr::new(
        &format!("{}", fun),
        fun_expr,
        coerced_phy_exprs,
        &data_type,
    )))
}

/// the signatures supported by the function `fun`.
fn signature(fun: &BuiltinScalarFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        BuiltinScalarFunction::Array => Signature::variadic(
            array_expressions::SUPPORTED_ARRAY_TYPES.to_vec(),
            fun.volatility(),
        ),
        BuiltinScalarFunction::Concat | BuiltinScalarFunction::ConcatWithSeparator => {
            Signature::variadic(vec![DataType::Utf8], fun.volatility())
        }
        BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::BitLength
        | BuiltinScalarFunction::CharacterLength
        | BuiltinScalarFunction::InitCap
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::MD5
        | BuiltinScalarFunction::OctetLength
        | BuiltinScalarFunction::Reverse
        | BuiltinScalarFunction::SHA224
        | BuiltinScalarFunction::SHA256
        | BuiltinScalarFunction::SHA384
        | BuiltinScalarFunction::SHA512
        | BuiltinScalarFunction::Trim
        | BuiltinScalarFunction::Upper => Signature::uniform(
            1,
            vec![DataType::Utf8, DataType::LargeUtf8],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Btrim
        | BuiltinScalarFunction::Ltrim
        | BuiltinScalarFunction::Rtrim => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Chr | BuiltinScalarFunction::ToHex => {
            Signature::uniform(1, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Lpad | BuiltinScalarFunction::Rpad => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Int64,
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::LargeUtf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Int64,
                    DataType::LargeUtf8,
                ]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::Right => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestamp => Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestampMillis => Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestampMicros => Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestampSeconds => Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Digest => {
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], fun.volatility())
        }
        BuiltinScalarFunction::DateTrunc => Signature::exact(
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::DatePart => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Date32]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Date64]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Second, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::SplitPart => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Utf8,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::LargeUtf8,
                    DataType::Int64,
                ]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Strpos | BuiltinScalarFunction::StartsWith => {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
                ],
                fun.volatility(),
            )
        }

        BuiltinScalarFunction::Substr => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Int64,
                    DataType::Int64,
                ]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Replace | BuiltinScalarFunction::Translate => {
            Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ])],
                fun.volatility(),
            )
        }
        BuiltinScalarFunction::RegexpReplace => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::NullIf => {
            Signature::uniform(2, SUPPORTED_NULLIF_TYPES.to_vec(), fun.volatility())
        }
        BuiltinScalarFunction::RegexpMatch => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Random => Signature::exact(vec![], fun.volatility()),
        // math expressions expect 1 argument of type f64 or f32
        // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
        // return the best approximation for it (in f64).
        // We accept f32 because in this case it is clear that the best approximation
        // will be as good as the number of digits in the number
        _ => Signature::uniform(
            1,
            vec![DataType::Float64, DataType::Float32],
            fun.volatility(),
        ),
    }
}

pub use datafusion_physical_expr::ScalarFunctionExpr;

#[cfg(feature = "crypto_expressions")]
macro_rules! invoke_if_crypto_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {{
        use datafusion_physical_expr::crypto_expressions;
        crypto_expressions::$FUNC
    }};
}

#[cfg(not(feature = "crypto_expressions"))]
macro_rules! invoke_if_crypto_expressions_feature_flag {
    ($FUNC:ident, $NAME:expr) => {
        |_: &[ColumnarValue]| -> Result<ColumnarValue> {
            Err(DataFusionError::Internal(format!(
                "function {} requires compilation with feature flag: crypto_expressions.",
                $NAME
            )))
        }
    };
}

#[cfg(feature = "regex_expressions")]
macro_rules! invoke_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use datafusion_physical_expr::regex_expressions;
        regex_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "regex_expressions"))]
macro_rules! invoke_if_regex_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {
        |_: &[ArrayRef]| -> Result<ArrayRef> {
            Err(DataFusionError::Internal(format!(
                "function {} requires compilation with feature flag: regex_expressions.",
                $NAME
            )))
        }
    };
}

#[cfg(feature = "unicode_expressions")]
macro_rules! invoke_if_unicode_expressions_feature_flag {
    ($FUNC:ident, $T:tt, $NAME:expr) => {{
        use datafusion_physical_expr::unicode_expressions;
        unicode_expressions::$FUNC::<$T>
    }};
}

#[cfg(not(feature = "unicode_expressions"))]
macro_rules! invoke_if_unicode_expressions_feature_flag {
  ($FUNC:ident, $T:tt, $NAME:expr) => {
    |_: &[ArrayRef]| -> Result<ArrayRef> {
      Err(DataFusionError::Internal(format!(
        "function {} requires compilation with feature flag: unicode_expressions.",
        $NAME
      )))
    }
  };
}

/// decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
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

        // to array
        let args = if let Some(len) = len {
            args.iter()
                .map(|arg| arg.clone().into_array(len))
                .collect::<Vec<ArrayRef>>()
        } else {
            args.iter()
                .map(|arg| arg.clone().into_array(1))
                .collect::<Vec<ArrayRef>>()
        };

        let result = (inner)(&args);

        // maybe back to scalar
        if len.is_some() {
            result.map(ColumnarValue::Array)
        } else {
            ScalarValue::try_from_array(&result?, 0).map(ColumnarValue::Scalar)
        }
    })
}

fn unary_offsets_string<O, F>(array: &Utf8Array<O>, op: F) -> PrimitiveArray<O>
where
    O: Offset + NativeType,
    F: Fn(O) -> O,
{
    let values = array
        .offsets()
        .windows(2)
        .map(|offset| op(offset[1] - offset[0]));

    let values = arrow::buffer::Buffer::from_iter(values);

    let data_type = if O::IS_LARGE {
        DataType::Int64
    } else {
        DataType::Int32
    };

    PrimitiveArray::<O>::from_data(data_type, values, array.validity().cloned())
}

/// Returns an array of integers with the number of bits on each string of the array.
/// TODO: contribute this back upstream?
fn bit_length(array: &dyn Array) -> ArrowResult<Box<dyn Array>> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Ok(Box::new(unary_offsets_string::<i32, _>(array, |x| x * 8)))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            Ok(Box::new(unary_offsets_string::<i64, _>(array, |x| x * 8)))
        }
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "length not supported for {:?}",
            array.data_type()
        ))),
    }
}
/// Create a physical scalar function.
pub fn create_physical_fun(
    fun: &BuiltinScalarFunction,
    execution_props: &ExecutionProps,
) -> Result<ScalarFunctionImplementation> {
    Ok(match fun {
        // math functions
        BuiltinScalarFunction::Abs => Arc::new(math_expressions::abs),
        BuiltinScalarFunction::Acos => Arc::new(math_expressions::acos),
        BuiltinScalarFunction::Asin => Arc::new(math_expressions::asin),
        BuiltinScalarFunction::Atan => Arc::new(math_expressions::atan),
        BuiltinScalarFunction::Ceil => Arc::new(math_expressions::ceil),
        BuiltinScalarFunction::Cos => Arc::new(math_expressions::cos),
        BuiltinScalarFunction::Exp => Arc::new(math_expressions::exp),
        BuiltinScalarFunction::Floor => Arc::new(math_expressions::floor),
        BuiltinScalarFunction::Log => Arc::new(math_expressions::log10),
        BuiltinScalarFunction::Ln => Arc::new(math_expressions::ln),
        BuiltinScalarFunction::Log10 => Arc::new(math_expressions::log10),
        BuiltinScalarFunction::Log2 => Arc::new(math_expressions::log2),
        BuiltinScalarFunction::Random => Arc::new(math_expressions::random),
        BuiltinScalarFunction::Round => Arc::new(math_expressions::round),
        BuiltinScalarFunction::Signum => Arc::new(math_expressions::signum),
        BuiltinScalarFunction::Sin => Arc::new(math_expressions::sin),
        BuiltinScalarFunction::Sqrt => Arc::new(math_expressions::sqrt),
        BuiltinScalarFunction::Tan => Arc::new(math_expressions::tan),
        BuiltinScalarFunction::Trunc => Arc::new(math_expressions::trunc),
        // string functions
        BuiltinScalarFunction::Array => Arc::new(array_expressions::array),
        BuiltinScalarFunction::Ascii => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::ascii::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::ascii::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ascii",
                other,
            ))),
        }),
        BuiltinScalarFunction::BitLength => Arc::new(|args| match &args[0] {
            ColumnarValue::Array(v) => {
                Ok(ColumnarValue::Array(bit_length(v.as_ref())?.into()))
            }
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function btrim",
                other,
            ))),
        }),
        BuiltinScalarFunction::CharacterLength => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        character_length,
                        i32,
                        "character_length"
                    );
                    make_scalar_function(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_if_unicode_expressions_feature_flag!(
                        character_length,
                        i64,
                        "character_length"
                    );
                    make_scalar_function(func)(args)
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function character_length",
                    other,
                ))),
            })
        }
        BuiltinScalarFunction::Chr => {
            Arc::new(|args| make_scalar_function(string_expressions::chr)(args))
        }
        BuiltinScalarFunction::Concat => Arc::new(string_expressions::concat),
        BuiltinScalarFunction::ConcatWithSeparator => {
            Arc::new(|args| make_scalar_function(string_expressions::concat_ws)(args))
        }
        BuiltinScalarFunction::DatePart => Arc::new(datetime_expressions::date_part),
        BuiltinScalarFunction::DateTrunc => Arc::new(datetime_expressions::date_trunc),
        BuiltinScalarFunction::Now => {
            // bind value for now at plan time
            Arc::new(datetime_expressions::make_now(
                execution_props.query_execution_start_time,
            ))
        }
        BuiltinScalarFunction::InitCap => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::initcap::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::initcap::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function initcap",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function left",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function lpad",
                other,
            ))),
        }),
        BuiltinScalarFunction::Ltrim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::ltrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::ltrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ltrim",
                other,
            ))),
        }),
        BuiltinScalarFunction::MD5 => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(md5, "md5"))
        }
        BuiltinScalarFunction::Digest => {
            Arc::new(invoke_if_crypto_expressions_feature_flag!(digest, "digest"))
        }
        BuiltinScalarFunction::NullIf => Arc::new(nullif_func),
        BuiltinScalarFunction::OctetLength => Arc::new(|args| match &args[0] {
            ColumnarValue::Array(v) => {
                Ok(ColumnarValue::Array(length(v.as_ref())?.into()))
            }
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
                    let func = invoke_if_regex_expressions_feature_flag!(
                        regexp_match,
                        i32,
                        "regexp_match"
                    );
                    make_scalar_function(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_if_regex_expressions_feature_flag!(
                        regexp_match,
                        i64,
                        "regexp_match"
                    );
                    make_scalar_function(func)(args)
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function regexp_match",
                    other
                ))),
            })
        }
        BuiltinScalarFunction::RegexpReplace => {
            Arc::new(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let func = invoke_if_regex_expressions_feature_flag!(
                        regexp_replace,
                        i32,
                        "regexp_replace"
                    );
                    make_scalar_function(func)(args)
                }
                DataType::LargeUtf8 => {
                    let func = invoke_if_regex_expressions_feature_flag!(
                        regexp_replace,
                        i64,
                        "regexp_replace"
                    );
                    make_scalar_function(func)(args)
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function regexp_replace",
                    other,
                ))),
            })
        }
        BuiltinScalarFunction::Repeat => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::repeat::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::repeat::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function repeat",
                other,
            ))),
        }),
        BuiltinScalarFunction::Replace => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::replace::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::replace::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function replace",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function reverse",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function right",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function rpad",
                other,
            ))),
        }),
        BuiltinScalarFunction::Rtrim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::rtrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::rtrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function rtrim",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function split_part",
                other,
            ))),
        }),
        BuiltinScalarFunction::StartsWith => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::starts_with::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::starts_with::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function starts_with",
                other,
            ))),
        }),
        BuiltinScalarFunction::Strpos => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(strpos, i32, "strpos");
                make_scalar_function(func)(args)
            }
            DataType::LargeUtf8 => {
                let func =
                    invoke_if_unicode_expressions_feature_flag!(strpos, i64, "strpos");
                make_scalar_function(func)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function strpos",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function substr",
                other,
            ))),
        }),
        BuiltinScalarFunction::ToHex => Arc::new(|args| match args[0].data_type() {
            DataType::Int32 => {
                make_scalar_function(string_expressions::to_hex::<i32>)(args)
            }
            DataType::Int64 => {
                make_scalar_function(string_expressions::to_hex::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function to_hex",
                other,
            ))),
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
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function translate",
                other,
            ))),
        }),
        BuiltinScalarFunction::Trim => Arc::new(|args| match args[0].data_type() {
            DataType::Utf8 => {
                make_scalar_function(string_expressions::btrim::<i32>)(args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(string_expressions::btrim::<i64>)(args)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function trim",
                other,
            ))),
        }),
        BuiltinScalarFunction::Upper => Arc::new(string_expressions::upper),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "create_physical_fun: Unsupported scalar function {:?}",
                fun
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result,
        physical_plan::expressions::{col, lit},
        scalar::ScalarValue,
    };
    use arrow::array::*;
    use arrow::datatypes::Field;
    use datafusion_common::field_util::SchemaExt;
    use datafusion_common::record_batch::RecordBatch;

    type StringArray = Utf8Array<i32>;

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
            let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from_slice(&[1]))];

            let expr =
                create_physical_expr(&BuiltinScalarFunction::$FUNC, $ARGS, &schema, &execution_props)?;

            // type is correct
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TYPE);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

            match expected {
                Ok(expected) => {
                    let result = expr.evaluate(&batch)?;
                    let result = result.into_array(batch.num_rows());
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
                            assert_eq!(error.to_string(), expected_error.to_string());
                        }
                    }
                }
            };
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("x".to_string())))],
            Ok(Some(120)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("ésoj".to_string())))],
            Ok(Some(233)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("💯".to_string())))],
            Ok(Some(128175)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("💯a".to_string())))],
            Ok(Some(128175)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Ascii,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
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
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(40)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(40)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            BitLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[lit(ScalarValue::Utf8(Some("\n trim \n".to_string())))],
            Ok(Some("\n trim \n")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(Some("xyxtrimyyx".to_string()))),
                lit(ScalarValue::Utf8(Some("xyz".to_string()))),
            ],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(Some("\nxyxtrimyyx\n".to_string()))),
                lit(ScalarValue::Utf8(Some("xyz\n".to_string()))),
            ],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("xyz".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Btrim,
            &[
                lit(ScalarValue::Utf8(Some("xyxtrimyyx".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            CharacterLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
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
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Err(DataFusionError::Internal(
                "function character_length requires compilation with feature flag: unicode_expressions.".to_string()
            )),
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
            Err(DataFusionError::Execution(
                "null character not permitted.".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Chr,
            &[lit(ScalarValue::Int64(Some(i64::MAX)))],
            Err(DataFusionError::Execution(
                "requested character too large for encoding.".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(Some("bb".to_string()))),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aabbcc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Concat,
            &[
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
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
            &[
                lit(ScalarValue::Utf8(Some("|".to_string()))),
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(Some("bb".to_string()))),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aa|bb|cc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(Some("|".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(Some("bb".to_string()))),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWithSeparator,
            &[
                lit(ScalarValue::Utf8(Some("|".to_string()))),
                lit(ScalarValue::Utf8(Some("aa".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
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
            &[lit(ScalarValue::Utf8(Some("hi THOMAS".to_string())))],
            Ok(Some("Hi Thomas")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            InitCap,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
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
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Ok(Some("ab")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(200))),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("abc")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-200))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
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
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Left,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Err(DataFusionError::Internal(
                "function left requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some(" josé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("   hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
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
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("xyxhi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(21))),
                lit(ScalarValue::Utf8(Some("abcdef".to_string()))),
            ],
            Ok(Some("abcdefabcdefabcdefahi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some(" ".to_string()))),
            ],
            Ok(Some("   hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("".to_string()))),
            ],
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
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
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
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("xyxyxyjosé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("éñ".to_string()))),
            ],
            Ok(Some("éñéñéñjosé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Lpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Err(DataFusionError::Internal(
                "function lpad requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some("trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some("trim".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Ltrim,
            &[lit(ScalarValue::Utf8(Some("\n trim ".to_string())))],
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
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some("34b7da764b21d298ef307d04d8152dc5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            MD5,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
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
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function md5 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(Some("chars".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(Some("josé".to_string())))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLength,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
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
            &[
                lit(ScalarValue::Utf8(Some("Thomas".to_string()))),
                lit(ScalarValue::Utf8(Some(".[mN]a.".to_string()))),
                lit(ScalarValue::Utf8(Some("M".to_string()))),
            ],
            Ok(Some("ThM")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b..".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
            ],
            Ok(Some("fooXbaz")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b..".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
            Ok(Some("fooXX")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
            ],
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
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
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
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
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
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("g".to_string()))),
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
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b(..)".to_string()))),
                lit(ScalarValue::Utf8(Some("X\\1Y".to_string()))),
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
            &[
                lit(ScalarValue::Utf8(Some("ABCabcABC".to_string()))),
                lit(ScalarValue::Utf8(Some("(abc)".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
                lit(ScalarValue::Utf8(Some("gi".to_string()))),
            ],
            Ok(Some("XXX")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "regex_expressions")]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("ABCabcABC".to_string()))),
                lit(ScalarValue::Utf8(Some("(abc)".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
                lit(ScalarValue::Utf8(Some("i".to_string()))),
            ],
            Ok(Some("XabcABC")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "regex_expressions"))]
        test_function!(
            RegexpReplace,
            &[
                lit(ScalarValue::Utf8(Some("foobarbaz".to_string()))),
                lit(ScalarValue::Utf8(Some("b..".to_string()))),
                lit(ScalarValue::Utf8(Some("X".to_string()))),
            ],
            Err(DataFusionError::Internal(
                "function regexp_replace requires compilation with feature flag: regex_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Repeat,
            &[
                lit(ScalarValue::Utf8(Some("Pg".to_string()))),
                lit(ScalarValue::Int64(Some(4))),
            ],
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
            &[
                lit(ScalarValue::Utf8(Some("Pg".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("abcde".to_string())))],
            Ok(Some("edcba")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("loẅks".to_string())))],
            Ok(Some("skẅol")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Reverse,
            &[lit(ScalarValue::Utf8(Some("loẅks".to_string())))],
            Ok(Some("skẅol")),
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
            &[lit(ScalarValue::Utf8(Some("abcde".to_string())))],
            Err(DataFusionError::Internal(
                "function reverse requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Ok(Some("de")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(200))),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("cde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(-200))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
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
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Right,
            &[
                lit(ScalarValue::Utf8(Some("abcde".to_string()))),
                lit(ScalarValue::Int8(Some(2))),
            ],
            Err(DataFusionError::Internal(
                "function right requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("josé ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
            ],
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
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("hixyx")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(21))),
                lit(ScalarValue::Utf8(Some("abcdef".to_string()))),
            ],
            Ok(Some("hiabcdefabcdefabcdefa")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some(" ".to_string()))),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
                lit(ScalarValue::Utf8(Some("".to_string()))),
            ],
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
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
                lit(ScalarValue::Int64(None)),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("hi".to_string()))),
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
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("xy".to_string()))),
            ],
            Ok(Some("joséxyxyxy")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(10))),
                lit(ScalarValue::Utf8(Some("éñ".to_string()))),
            ],
            Ok(Some("josééñéñéñ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Rpad,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Err(DataFusionError::Internal(
                "function rpad requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some(" trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some(" trim \n".to_string())))],
            Ok(Some(" trim \n")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
            Ok(Some(" trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Rtrim,
            &[lit(ScalarValue::Utf8(Some("trim".to_string())))],
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
        type B = BinaryArray<i32>;
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                11u8, 246u8, 203u8, 98u8, 100u8, 156u8, 66u8, 169u8, 174u8, 56u8, 118u8,
                171u8, 111u8, 109u8, 146u8, 173u8, 54u8, 203u8, 84u8, 20u8, 228u8, 149u8,
                248u8, 135u8, 50u8, 146u8, 190u8, 77u8
            ])),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                209u8, 74u8, 2u8, 140u8, 42u8, 58u8, 43u8, 201u8, 71u8, 97u8, 2u8, 187u8,
                40u8, 130u8, 52u8, 196u8, 21u8, 162u8, 176u8, 31u8, 130u8, 142u8, 166u8,
                42u8, 197u8, 179u8, 228u8, 47u8
            ])),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            B
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA224,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha224 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                225u8, 96u8, 143u8, 117u8, 197u8, 215u8, 129u8, 63u8, 61u8, 64u8, 49u8,
                203u8, 48u8, 191u8, 183u8, 134u8, 80u8, 125u8, 152u8, 19u8, 117u8, 56u8,
                255u8, 142u8, 18u8, 138u8, 111u8, 247u8, 78u8, 132u8, 230u8, 67u8
            ])),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                227u8, 176u8, 196u8, 66u8, 152u8, 252u8, 28u8, 20u8, 154u8, 251u8, 244u8,
                200u8, 153u8, 111u8, 185u8, 36u8, 39u8, 174u8, 65u8, 228u8, 100u8, 155u8,
                147u8, 76u8, 164u8, 149u8, 153u8, 27u8, 120u8, 82u8, 184u8, 85u8
            ])),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            B
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA256,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha256 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Ok(Some(&[
                9u8, 111u8, 91u8, 104u8, 170u8, 119u8, 132u8, 142u8, 79u8, 223u8, 92u8,
                28u8, 11u8, 53u8, 13u8, 226u8, 219u8, 250u8, 214u8, 15u8, 253u8, 124u8,
                37u8, 217u8, 234u8, 7u8, 198u8, 193u8, 155u8, 138u8, 77u8, 85u8, 169u8,
                24u8, 126u8, 177u8, 23u8, 197u8, 87u8, 136u8, 63u8, 88u8, 193u8, 109u8,
                250u8, 195u8, 227u8, 67u8
            ])),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
            Ok(Some(&[
                56u8, 176u8, 96u8, 167u8, 81u8, 172u8, 150u8, 56u8, 76u8, 217u8, 50u8,
                126u8, 177u8, 177u8, 227u8, 106u8, 33u8, 253u8, 183u8, 17u8, 20u8, 190u8,
                7u8, 67u8, 76u8, 12u8, 199u8, 191u8, 99u8, 246u8, 225u8, 218u8, 39u8,
                78u8, 222u8, 191u8, 231u8, 111u8, 101u8, 251u8, 213u8, 26u8, 210u8,
                241u8, 72u8, 152u8, 185u8, 91u8
            ])),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            B
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA384,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha384 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
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
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(Some("".to_string())))],
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
            B
        );
        #[cfg(feature = "crypto_expressions")]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(None))],
            Ok(None),
            &[u8],
            Binary,
            B
        );
        #[cfg(not(feature = "crypto_expressions"))]
        test_function!(
            SHA512,
            &[lit(ScalarValue::Utf8(Some("tom".to_string())))],
            Err(DataFusionError::Internal(
                "function sha512 requires compilation with feature flag: crypto_expressions.".to_string()
            )),
            &[u8],
            Binary,
            BinaryArray
        );
        test_function!(
            SplitPart,
            &[
                lit(ScalarValue::Utf8(Some("abc~@~def~@~ghi".to_string()))),
                lit(ScalarValue::Utf8(Some("~@~".to_string()))),
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
                lit(ScalarValue::Utf8(Some("abc~@~def~@~ghi".to_string()))),
                lit(ScalarValue::Utf8(Some("~@~".to_string()))),
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
                lit(ScalarValue::Utf8(Some("abc~@~def~@~ghi".to_string()))),
                lit(ScalarValue::Utf8(Some("~@~".to_string()))),
                lit(ScalarValue::Int64(Some(-1))),
            ],
            Err(DataFusionError::Execution(
                "field position must be greater than zero".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Utf8(Some("alph".to_string()))),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Utf8(Some("blph".to_string()))),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("alph".to_string()))),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            StartsWith,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("abc".to_string()))),
                lit(ScalarValue::Utf8(Some("c".to_string()))),
            ],
            Ok(Some(3)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("josé".to_string()))),
                lit(ScalarValue::Utf8(Some("é".to_string()))),
            ],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(Some("so".to_string()))),
            ],
            Ok(Some(6)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(Some("abc".to_string()))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("abc".to_string()))),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Strpos,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Err(DataFusionError::Internal(
                "function strpos requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            i32,
            Int32,
            Int32Array
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(-5))),
            ],
            Ok(Some("joséésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
                lit(ScalarValue::Int64(Some(-5))),
            ],
            Ok(Some("joséésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("lphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(3))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(30))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(-1))),
            ],
            Err(DataFusionError::Execution(
                "negative substring length not allowed: substr(<str>, 1, -1)".to_string(),
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Substr,
            &[
                lit(ScalarValue::Utf8(Some("joséésoj".to_string()))),
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
                lit(ScalarValue::Utf8(Some("alphabet".to_string()))),
                lit(ScalarValue::Int64(Some(0))),
            ],
            Err(DataFusionError::Internal(
                "function substr requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Ok(Some("a2x5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(None)),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("é2íñ5".to_string()))),
                lit(ScalarValue::Utf8(Some("éñí".to_string()))),
                lit(ScalarValue::Utf8(Some("óü".to_string()))),
            ],
            Ok(Some("ó2ü5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Translate,
            &[
                lit(ScalarValue::Utf8(Some("12345".to_string()))),
                lit(ScalarValue::Utf8(Some("143".to_string()))),
                lit(ScalarValue::Utf8(Some("ax".to_string()))),
            ],
            Err(DataFusionError::Internal(
                "function translate requires compilation with feature flag: unicode_expressions.".to_string()
            )),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(Some(" trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(Some("trim ".to_string())))],
            Ok(Some("trim")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Trim,
            &[lit(ScalarValue::Utf8(Some(" trim".to_string())))],
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
            &[lit(ScalarValue::Utf8(Some("upper".to_string())))],
            Ok(Some("UPPER")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            Upper,
            &[lit(ScalarValue::Utf8(Some("UPPER".to_string())))],
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
            let expr = create_physical_expr(fun, &[], &schema, &execution_props);

            match expr {
                Ok(..) => {
                    return Err(DataFusionError::Plan(format!(
                        "Builtin scalar function {} does not support empty arguments",
                        fun
                    )));
                }
                Err(DataFusionError::Internal(err)) => {
                    if err
                        != format!(
                            "Builtin scalar function {} does not support empty arguments",
                            fun
                        )
                    {
                        return Err(DataFusionError::Internal(format!(
                            "Builtin scalar function {} didn't got the right error message with empty arguments", fun)));
                    }
                }
                Err(..) => {
                    return Err(DataFusionError::Internal(format!(
                        "Builtin scalar function {} didn't got the right error with empty arguments", fun)));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_empty_arguments() -> Result<()> {
        let execution_props = ExecutionProps::new();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let funs = [BuiltinScalarFunction::Now, BuiltinScalarFunction::Random];

        for fun in funs.iter() {
            create_physical_expr(fun, &[], &schema, &execution_props)?;
        }
        Ok(())
    }

    fn generic_test_array(
        value1: ArrayRef,
        value2: ArrayRef,
        expected: ArrayRef,
    ) -> Result<()> {
        // any type works here: we evaluate against a literal of `value`
        let schema = Schema::new(vec![
            Field::new("a", value1.data_type().clone(), false),
            Field::new("b", value2.data_type().clone(), false),
        ]);
        let columns: Vec<ArrayRef> = vec![value1, value2];
        let execution_props = ExecutionProps::new();

        let expr = create_physical_expr(
            &BuiltinScalarFunction::Array,
            &[col("a", &schema)?, col("b", &schema)?],
            &schema,
            &execution_props,
        )?;

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        // value and type is correct
        assert_eq!(result.value(0).as_ref(), expected.as_ref());

        Ok(())
    }

    #[test]
    fn test_array() -> Result<()> {
        generic_test_array(
            Arc::new(StringArray::from_slice(&["aa"])),
            Arc::new(StringArray::from_slice(&["bb"])),
            Arc::new(StringArray::from_slice(&["aa", "bb"])),
        )?;

        // different types, to validate that casting happens
        generic_test_array(
            Arc::new(UInt32Array::from_slice(&[1])),
            Arc::new(UInt64Array::from_slice(&[1])),
            Arc::new(UInt64Array::from_slice(&[1, 1])),
        )?;

        // different types (another order), to validate that casting happens
        generic_test_array(
            Arc::new(UInt64Array::from_slice(&[1])),
            Arc::new(UInt32Array::from_slice(&[1])),
            Arc::new(UInt64Array::from_slice(&[1, 1])),
        )
    }

    #[test]
    #[cfg(feature = "regex_expressions")]
    fn test_regexp_match() -> Result<()> {
        use arrow::array::ListArray;
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let execution_props = ExecutionProps::new();

        // concat(value, value)
        let col_value: ArrayRef = Arc::new(StringArray::from_slice(&["aaa-555"]));
        let pattern = lit(ScalarValue::Utf8(Some(r".*-(\d*)".to_string())));
        let columns: Vec<ArrayRef> = vec![col_value];
        let expr = create_physical_expr(
            &BuiltinScalarFunction::RegexpMatch,
            &[col("a", &schema)?, pattern],
            &schema,
            &execution_props,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true)))
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        let first_row = result.value(0);
        let first_row = first_row.as_any().downcast_ref::<StringArray>().unwrap();

        // value is correct
        let expected = "555".to_string();
        assert_eq!(first_row.value(0), expected);

        Ok(())
    }

    #[test]
    #[cfg(feature = "regex_expressions")]
    fn test_regexp_match_all_literals() -> Result<()> {
        use arrow::array::ListArray;
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let execution_props = ExecutionProps::new();

        let col_value = lit(ScalarValue::Utf8(Some("aaa-555".to_string())));
        let pattern = lit(ScalarValue::Utf8(Some(r".*-(\d*)".to_string())));
        let columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from_slice(&[1]))];
        let expr = create_physical_expr(
            &BuiltinScalarFunction::RegexpMatch,
            &[col_value, pattern],
            &schema,
            &execution_props,
        )?;

        // type is correct
        assert_eq!(
            expr.data_type(&schema)?,
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true)))
        );

        // evaluate works
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());

        // downcast works
        let result = result.as_any().downcast_ref::<ListArray<i32>>().unwrap();
        let first_row = result.value(0);
        let first_row = first_row.as_any().downcast_ref::<StringArray>().unwrap();

        // value is correct
        let expected = "555".to_string();
        assert_eq!(first_row.value(0), expected);

        Ok(())
    }
}
