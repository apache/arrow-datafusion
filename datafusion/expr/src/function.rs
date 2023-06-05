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

//! Function module contains typing and signature for built-in and user defined functions.

use crate::function_err::generate_signature_error_msg;
use crate::nullif::SUPPORTED_NULLIF_TYPES;
use crate::type_coercion::functions::data_types;
use crate::ColumnarValue;
use crate::{
    array_expressions, comparison_expressions, conditional_expressions,
    struct_expressions, Accumulator, BuiltinScalarFunction, Signature, TypeSignature,
};
use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;

/// Scalar function
///
/// The Fn param is the wrapped function but be aware that the function will
/// be passed with the slice / vec of columnar values (either scalar or array)
/// with the exception of zero param function, where a singular element vec
/// will be passed. In that case the single element is a null array to indicate
/// the batch's row count (so that the generative zero-argument function can know
/// the result array size).
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

/// Factory that returns the functions's return type given the input argument types
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;

/// Factory that returns an accumulator for the given aggregate, given
/// its return datatype.
pub type AccumulatorFunctionImplementation =
    Arc<dyn Fn(&DataType) -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// Factory that returns the types used by an aggregator to serialize
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;

macro_rules! make_utf8_to_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                DataType::LargeUtf8 => $largeUtf8Type,
                DataType::Utf8 => $utf8Type,
                DataType::Null => DataType::Null,
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

fn utf8_or_binary_to_binary_type(arg_type: &DataType, name: &str) -> Result<DataType> {
    Ok(match arg_type {
        DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeBinary => DataType::Binary,
        DataType::Null => DataType::Null,
        _ => {
            // this error is internal as `data_types` should have captured this.
            return Err(DataFusionError::Internal(format!(
                "The {name:?} function can only accept strings or binary arrays."
            )));
        }
    })
}

/// Returns the datatype of the scalar function
pub fn return_type(
    fun: &BuiltinScalarFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    if input_expr_types.is_empty() && !fun.supports_zero_argument() {
        return Err(DataFusionError::Plan(generate_signature_error_msg(
            fun,
            input_expr_types,
        )));
    }

    // verify that this is a valid set of data types for this function
    data_types(input_expr_types, &signature(fun)).map_err(|_| {
        DataFusionError::Plan(generate_signature_error_msg(fun, input_expr_types))
    })?;

    // the return type of the built in function.
    // Some built-in functions' return type depends on the incoming type.
    match fun {
        BuiltinScalarFunction::MakeArray => Ok(DataType::FixedSizeList(
            Arc::new(Field::new("item", input_expr_types[0].clone(), true)),
            input_expr_types.len() as i32,
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
        BuiltinScalarFunction::Coalesce => {
            // COALESCE has multiple args and they might get coerced, get a preview of this
            let coerced_types = data_types(input_expr_types, &signature(fun));
            coerced_types.map(|types| types[0].clone())
        }
        BuiltinScalarFunction::Concat => Ok(DataType::Utf8),
        BuiltinScalarFunction::ConcatWithSeparator => Ok(DataType::Utf8),
        BuiltinScalarFunction::DatePart => Ok(DataType::Float64),
        BuiltinScalarFunction::DateTrunc | BuiltinScalarFunction::DateBin => {
            match input_expr_types[1] {
                DataType::Timestamp(TimeUnit::Nanosecond, _) | DataType::Utf8 => {
                    Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    Ok(DataType::Timestamp(TimeUnit::Second, None))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "The {fun} function can only accept timestamp as the second arg."
                ))),
            }
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
        BuiltinScalarFunction::Greatest | BuiltinScalarFunction::Least => {
            // GREATEST and LEAST have multiple args and they might get coerced, get a preview of this
            let coerced_types = data_types(input_expr_types, &signature(fun));
            coerced_types.map(|typs| typs[0].clone())
        }
        BuiltinScalarFunction::OctetLength => {
            utf8_to_int_type(&input_expr_types[0], "octet_length")
        }
        BuiltinScalarFunction::Pi => Ok(DataType::Float64),
        BuiltinScalarFunction::Random => Ok(DataType::Float64),
        BuiltinScalarFunction::Uuid => Ok(DataType::Utf8),
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
            utf8_or_binary_to_binary_type(&input_expr_types[0], "sha224")
        }
        BuiltinScalarFunction::SHA256 => {
            utf8_or_binary_to_binary_type(&input_expr_types[0], "sha256")
        }
        BuiltinScalarFunction::SHA384 => {
            utf8_or_binary_to_binary_type(&input_expr_types[0], "sha384")
        }
        BuiltinScalarFunction::SHA512 => {
            utf8_or_binary_to_binary_type(&input_expr_types[0], "sha512")
        }
        BuiltinScalarFunction::Digest => {
            utf8_or_binary_to_binary_type(&input_expr_types[0], "digest")
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
        BuiltinScalarFunction::FromUnixtime => {
            Ok(DataType::Timestamp(TimeUnit::Second, None))
        }
        BuiltinScalarFunction::Now => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        )),
        BuiltinScalarFunction::CurrentDate => Ok(DataType::Date32),
        BuiltinScalarFunction::CurrentTime => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        BuiltinScalarFunction::Translate => {
            utf8_to_str_type(&input_expr_types[0], "translate")
        }
        BuiltinScalarFunction::Trim => utf8_to_str_type(&input_expr_types[0], "trim"),
        BuiltinScalarFunction::Upper => utf8_to_str_type(&input_expr_types[0], "upper"),
        BuiltinScalarFunction::RegexpMatch => Ok(match input_expr_types[0] {
            DataType::LargeUtf8 => {
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true)))
            }
            DataType::Utf8 => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
            DataType::Null => DataType::Null,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The regexp_extract function can only accept strings.".to_string(),
                ));
            }
        }),

        BuiltinScalarFunction::Factorial
        | BuiltinScalarFunction::Gcd
        | BuiltinScalarFunction::Lcm => Ok(DataType::Int64),

        BuiltinScalarFunction::Power => match &input_expr_types[0] {
            DataType::Int64 => Ok(DataType::Int64),
            _ => Ok(DataType::Float64),
        },

        BuiltinScalarFunction::Struct => Ok(DataType::Struct(Fields::empty())),

        BuiltinScalarFunction::Atan2 => match &input_expr_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        },

        BuiltinScalarFunction::Log => match &input_expr_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        },

        BuiltinScalarFunction::ArrowTypeof => Ok(DataType::Utf8),

        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Acos
        | BuiltinScalarFunction::Asin
        | BuiltinScalarFunction::Atan
        | BuiltinScalarFunction::Acosh
        | BuiltinScalarFunction::Asinh
        | BuiltinScalarFunction::Atanh
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Cos
        | BuiltinScalarFunction::Cosh
        | BuiltinScalarFunction::Degrees
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Ln
        | BuiltinScalarFunction::Log10
        | BuiltinScalarFunction::Log2
        | BuiltinScalarFunction::Radians
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Signum
        | BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Sinh
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Tan
        | BuiltinScalarFunction::Tanh
        | BuiltinScalarFunction::Trunc => match input_expr_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        },
    }
}

/// Return the [`Signature`] supported by the function `fun`.
pub fn signature(fun: &BuiltinScalarFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        BuiltinScalarFunction::MakeArray => Signature::variadic(
            array_expressions::SUPPORTED_ARRAY_TYPES.to_vec(),
            fun.volatility(),
        ),
        BuiltinScalarFunction::Struct => Signature::variadic(
            struct_expressions::SUPPORTED_STRUCT_TYPES.to_vec(),
            fun.volatility(),
        ),
        BuiltinScalarFunction::Concat | BuiltinScalarFunction::ConcatWithSeparator => {
            Signature::variadic(vec![DataType::Utf8], fun.volatility())
        }
        BuiltinScalarFunction::Coalesce => Signature::variadic(
            conditional_expressions::SUPPORTED_COALESCE_TYPES.to_vec(),
            fun.volatility(),
        ),
        BuiltinScalarFunction::SHA224
        | BuiltinScalarFunction::SHA256
        | BuiltinScalarFunction::SHA384
        | BuiltinScalarFunction::SHA512
        | BuiltinScalarFunction::MD5 => Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::LargeUtf8,
                DataType::Binary,
                DataType::LargeBinary,
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::BitLength
        | BuiltinScalarFunction::CharacterLength
        | BuiltinScalarFunction::InitCap
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::OctetLength
        | BuiltinScalarFunction::Reverse
        | BuiltinScalarFunction::Upper => Signature::uniform(
            1,
            vec![DataType::Utf8, DataType::LargeUtf8],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Btrim
        | BuiltinScalarFunction::Ltrim
        | BuiltinScalarFunction::Rtrim
        | BuiltinScalarFunction::Trim => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Chr | BuiltinScalarFunction::ToHex => {
            Signature::uniform(1, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Greatest | BuiltinScalarFunction::Least => {
            Signature::variadic_equal(
                comparison_expressions::SUPPORTED_COMPARISON_TYPES.to_vec(),
                fun.volatility(),
            )
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
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Utf8,
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestampMillis => Signature::uniform(
            1,
            vec![
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Utf8,
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestampMicros => Signature::uniform(
            1,
            vec![
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Utf8,
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestampSeconds => Signature::uniform(
            1,
            vec![
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Utf8,
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::FromUnixtime => {
            Signature::uniform(1, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Digest => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::DateTrunc => Signature::exact(
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::DateBin => {
            let base_sig = |array_type: TimeUnit| {
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::MonthDayNano),
                        DataType::Timestamp(array_type.clone(), None),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::DayTime),
                        DataType::Timestamp(array_type.clone(), None),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::MonthDayNano),
                        DataType::Timestamp(array_type.clone(), None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::DayTime),
                        DataType::Timestamp(array_type, None),
                    ]),
                ]
            };

            let full_sig = [
                TimeUnit::Nanosecond,
                TimeUnit::Microsecond,
                TimeUnit::Millisecond,
                TimeUnit::Second,
            ]
            .into_iter()
            .map(base_sig)
            .collect::<Vec<_>>()
            .concat();

            Signature::one_of(full_sig, fun.volatility())
        }
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
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
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
        BuiltinScalarFunction::Pi => Signature::exact(vec![], fun.volatility()),
        BuiltinScalarFunction::Random => Signature::exact(vec![], fun.volatility()),
        BuiltinScalarFunction::Uuid => Signature::exact(vec![], fun.volatility()),
        BuiltinScalarFunction::Power => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Round => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float32, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Float32]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Atan2 => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Float32, DataType::Float32]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Log => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Float32]),
                TypeSignature::Exact(vec![DataType::Float64]),
                TypeSignature::Exact(vec![DataType::Float32, DataType::Float32]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Factorial => {
            Signature::uniform(1, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => {
            Signature::uniform(2, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ArrowTypeof => Signature::any(1, fun.volatility()),
        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Acos
        | BuiltinScalarFunction::Asin
        | BuiltinScalarFunction::Atan
        | BuiltinScalarFunction::Acosh
        | BuiltinScalarFunction::Asinh
        | BuiltinScalarFunction::Atanh
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Cos
        | BuiltinScalarFunction::Cosh
        | BuiltinScalarFunction::Degrees
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Ln
        | BuiltinScalarFunction::Log10
        | BuiltinScalarFunction::Log2
        | BuiltinScalarFunction::Radians
        | BuiltinScalarFunction::Signum
        | BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Sinh
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Tan
        | BuiltinScalarFunction::Tanh
        | BuiltinScalarFunction::Trunc => {
            // math expressions expect 1 argument of type f64 or f32
            // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
            // return the best approximation for it (in f64).
            // We accept f32 because in this case it is clear that the best approximation
            // will be as good as the number of digits in the number
            Signature::uniform(
                1,
                vec![DataType::Float64, DataType::Float32],
                fun.volatility(),
            )
        }
        BuiltinScalarFunction::Now
        | BuiltinScalarFunction::CurrentDate
        | BuiltinScalarFunction::CurrentTime => {
            Signature::uniform(0, vec![], fun.volatility())
        }
    }
}
