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

//! Coercion rules for matching argument types for binary operators

use crate::Operator;
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE};
use datafusion_common::DataFusionError;
use datafusion_common::Result;

/// Returns the return type of a binary operator or an error when the binary operator cannot
/// perform the computation between the argument's types, even after type coercion.
///
/// This function makes some assumptions about the underlying available computations.
pub fn binary_operator_data_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // validate that it is possible to perform the operation on incoming types.
    // (or the return datatype cannot be inferred)
    let result_type = coerce_types(lhs_type, op, rhs_type)?;

    match op {
        // operators that return a boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::And
        | Operator::Or
        | Operator::Like
        | Operator::NotLike
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => Ok(DataType::Boolean),
        // bitwise operations return the common coerced type
        Operator::BitwiseAnd | Operator::BitwiseOr => Ok(result_type),
        // math operations return the same value as the common coerced type
        Operator::Plus
        | Operator::Minus
        | Operator::Divide
        | Operator::Multiply
        | Operator::Modulo => Ok(result_type),
        // string operations return the same values as the common coerced type
        Operator::StringConcat => Ok(result_type),
    }
}

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
pub fn coerce_types(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // This result MUST be compatible with `binary_coerce`
    let result = match op {
        Operator::BitwiseAnd | Operator::BitwiseOr => {
            bitwise_coercion(lhs_type, rhs_type)
        }
        Operator::And | Operator::Or => match (lhs_type, rhs_type) {
            // logical binary boolean operators can only be evaluated in bools
            (DataType::Boolean, DataType::Boolean) => Some(DataType::Boolean),
            _ => None,
        },
        // logical comparison operators have their own rules, and always return a boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq => comparison_coercion(lhs_type, rhs_type),
        // "like" operators operate on strings and always return a boolean
        Operator::Like | Operator::NotLike => like_coercion(lhs_type, rhs_type),
        // date +/- interval returns date
        Operator::Plus | Operator::Minus
            if (*lhs_type == DataType::Date32 || *lhs_type == DataType::Date64) =>
        {
            match rhs_type {
                DataType::Interval(_) => Some(lhs_type.clone()),
                _ => None,
            }
        }
        // for math expressions, the final value of the coercion is also the return type
        // because coercion favours higher information types
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => mathematics_numerical_coercion(op, lhs_type, rhs_type),
        Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => string_coercion(lhs_type, rhs_type),
        // "||" operator has its own rules, and always return a string type
        Operator::StringConcat => string_concat_coercion(lhs_type, rhs_type),
        Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => {
            eq_coercion(lhs_type, rhs_type)
        }
    };

    // re-write the error message of failed coercions to include the operator's information
    match result {
        None => Err(DataFusionError::Plan(
            format!(
                "'{:?} {} {:?}' can't be evaluated because there isn't a common type to coerce the types to",
                lhs_type, op, rhs_type
            ),
        )),
        Some(t) => Ok(t)
    }
}

fn bitwise_coercion(left_type: &DataType, right_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    if !is_numeric(left_type) || !is_numeric(right_type) {
        return None;
    }
    if left_type == right_type && !is_dictionary(left_type) {
        return Some(left_type.clone());
    }
    // TODO support other data type
    match (left_type, right_type) {
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        _ => None,
    }
}

/// Get the coerced data type for comparison operations such as `eq`, `not eq`, `lt`, `lteq`, `gt`, and `gteq`.
pub fn comparison_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, true))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
        .or_else(|| string_numeric_coercion(lhs_type, rhs_type))
}

fn string_numeric_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, _) if DataType::is_numeric(rhs_type) => Some(Utf8),
        (LargeUtf8, _) if DataType::is_numeric(rhs_type) => Some(LargeUtf8),
        (_, Utf8) if DataType::is_numeric(lhs_type) => Some(Utf8),
        (_, LargeUtf8) if DataType::is_numeric(lhs_type) => Some(LargeUtf8),
        _ => None,
    }
}

fn comparison_binary_numeric_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        // support decimal data type for comparison operation
        (d1 @ Decimal128(_, _), d2 @ Decimal128(_, _)) => get_wider_decimal_type(d1, d2),
        (Decimal128(_, _), _) => get_comparison_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal128(_, _)) => get_comparison_common_decimal_type(rhs_type, lhs_type),
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn get_comparison_common_decimal_type(
    decimal_type: &DataType,
    other_type: &DataType,
) -> Option<DataType> {
    let other_decimal_type = &match other_type {
        // This conversion rule is from spark
        // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
        DataType::Int8 => DataType::Decimal128(3, 0),
        DataType::Int16 => DataType::Decimal128(5, 0),
        DataType::Int32 => DataType::Decimal128(10, 0),
        DataType::Int64 => DataType::Decimal128(20, 0),
        DataType::Float32 => DataType::Decimal128(14, 7),
        DataType::Float64 => DataType::Decimal128(30, 15),
        _ => {
            return None;
        }
    };
    match (decimal_type, &other_decimal_type) {
        (d1 @ DataType::Decimal128(_, _), d2 @ DataType::Decimal128(_, _)) => {
            get_wider_decimal_type(d1, d2)
        }
        _ => None,
    }
}

// Returns a `DataType::Decimal128` that can store any value from either
// `lhs_decimal_type` and `rhs_decimal_type`
// The result decimal type is (max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)).
fn get_wider_decimal_type(
    lhs_decimal_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    match (lhs_decimal_type, rhs_type) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (p1 - s1).max(p2 - s2);
            Some(create_decimal_type(range + s, s))
        }
        (_, _) => None,
    }
}

// Convert the numeric data type to the decimal data type.
// Now, we just support the signed integer type and floating-point type.
fn coerce_numeric_type_to_decimal(numeric_type: &DataType) -> Option<DataType> {
    match numeric_type {
        DataType::Int8 => Some(DataType::Decimal128(3, 0)),
        DataType::Int16 => Some(DataType::Decimal128(5, 0)),
        DataType::Int32 => Some(DataType::Decimal128(10, 0)),
        DataType::Int64 => Some(DataType::Decimal128(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        DataType::Float32 => Some(DataType::Decimal128(14, 7)),
        DataType::Float64 => Some(DataType::Decimal128(30, 15)),
        _ => None,
    }
}

fn mathematics_numerical_coercion(
    mathematics_op: &Operator,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !both_numeric_or_null_and_numeric(lhs_type, rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Decimal128(_, _), Decimal128(_, _)) => {
            coercion_decimal_mathematics_type(mathematics_op, lhs_type, rhs_type)
        }
        (Decimal128(_, _), _) => {
            let converted_decimal_type = coerce_numeric_type_to_decimal(rhs_type);
            match converted_decimal_type {
                None => None,
                Some(right_decimal_type) => coercion_decimal_mathematics_type(
                    mathematics_op,
                    lhs_type,
                    &right_decimal_type,
                ),
            }
        }
        (_, Decimal128(_, _)) => {
            let converted_decimal_type = coerce_numeric_type_to_decimal(lhs_type);
            match converted_decimal_type {
                None => None,
                Some(left_decimal_type) => coercion_decimal_mathematics_type(
                    mathematics_op,
                    &left_decimal_type,
                    rhs_type,
                ),
            }
        }
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn create_decimal_type(precision: usize, scale: usize) -> DataType {
    DataType::Decimal128(
        DECIMAL128_MAX_PRECISION.min(precision),
        DECIMAL128_MAX_SCALE.min(scale),
    )
}

fn coercion_decimal_mathematics_type(
    mathematics_op: &Operator,
    left_decimal_type: &DataType,
    right_decimal_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (left_decimal_type, right_decimal_type) {
        // The coercion rule from spark
        // https://github.com/apache/spark/blob/c20af535803a7250fef047c2bf0fe30be242369d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala#L35
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            match mathematics_op {
                Operator::Plus | Operator::Minus => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // max(s1, s2) + max(p1-s1, p2-s2) + 1
                    let result_precision = result_scale + (*p1 - *s1).max(*p2 - *s2) + 1;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Multiply => {
                    // s1 + s2
                    let result_scale = *s1 + *s2;
                    // p1 + p2 + 1
                    let result_precision = *p1 + *p2 + 1;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Divide => {
                    // max(6, s1 + p2 + 1)
                    let result_scale = 6.max(*s1 + *p2 + 1);
                    // p1 - s1 + s2 + max(6, s1 + p2 + 1)
                    let result_precision = result_scale + *p1 - *s1 + *s2;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Modulo => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // min(p1-s1, p2-s2) + max(s1, s2)
                    let result_precision = result_scale + (*p1 - *s1).min(*p2 - *s2);
                    Some(create_decimal_type(result_precision, result_scale))
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

/// Determine if a DataType is signed numeric or not
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    )
}

/// Determine if a DataType is numeric or not
pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || matches!(
            dt,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        )
}

/// Determine if at least of one of lhs and rhs is numeric, and the other must be NULL or numeric
fn both_numeric_or_null_and_numeric(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    match (lhs_type, rhs_type) {
        (_, DataType::Null) => is_numeric(lhs_type),
        (DataType::Null, _) => is_numeric(rhs_type),
        _ => is_numeric(lhs_type) && is_numeric(rhs_type),
    }
}

/// Coercion rules for dictionary values (aka the type of the  dictionary itself)
fn dictionary_value_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    numerical_coercion(lhs_type, rhs_type).or_else(|| string_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// Not all operators support dictionaries, if `preserve_dictionaries` is true
/// dictionaries will be preserved if possible
fn dictionary_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
    preserve_dictionaries: bool,
) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(_lhs_index_type, lhs_value_type),
            DataType::Dictionary(_rhs_index_type, rhs_value_type),
        ) => dictionary_value_coercion(lhs_value_type, rhs_value_type),
        (d @ DataType::Dictionary(_, value_type), other_type)
        | (other_type, d @ DataType::Dictionary(_, value_type))
            if preserve_dictionaries && value_type.as_ref() == other_type =>
        {
            Some(d.clone())
        }
        (DataType::Dictionary(_index_type, value_type), _) => {
            dictionary_value_coercion(value_type, rhs_type)
        }
        (_, DataType::Dictionary(_index_type, value_type)) => {
            dictionary_value_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for string concat.
/// This is a union of string coercion rules and specified rules:
/// 1. At lease one side of lhs and rhs should be string type (Utf8 / LargeUtf8)
/// 2. Data type of the other side should be able to cast to string type
fn string_concat_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    string_coercion(lhs_type, rhs_type).or(match (lhs_type, rhs_type) {
        (Utf8, from_type) | (from_type, Utf8) => {
            string_concat_internal_coercion(from_type, &Utf8)
        }
        (LargeUtf8, from_type) | (from_type, LargeUtf8) => {
            string_concat_internal_coercion(from_type, &LargeUtf8)
        }
        _ => None,
    })
}

fn string_concat_internal_coercion(
    from_type: &DataType,
    to_type: &DataType,
) -> Option<DataType> {
    if can_cast_types(from_type, to_type) {
        Some(to_type.to_owned())
    } else {
        None
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        _ => None,
    }
}

/// coercion rules for like operations.
/// This is a union of string coercion rules and dictionary coercion rules
fn like_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, false))
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    use arrow::datatypes::TimeUnit;
    match (lhs_type, rhs_type) {
        (Utf8, Date32) => Some(Date32),
        (Date32, Utf8) => Some(Date32),
        (Utf8, Date64) => Some(Date64),
        (Date64, Utf8) => Some(Date64),
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                // can't cast across timezones
                (Some(lhs_tz), Some(rhs_tz)) => {
                    if lhs_tz != rhs_tz {
                        return None;
                    } else {
                        Some(lhs_tz.clone())
                    }
                }
                (Some(lhs_tz), None) => Some(lhs_tz.clone()),
                (None, Some(rhs_tz)) => Some(rhs_tz.clone()),
                (None, None) => None,
            };

            let unit = match (lhs_unit, rhs_unit) {
                (TimeUnit::Second, TimeUnit::Millisecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Microsecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Nanosecond) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Microsecond) => TimeUnit::Millisecond,
                (TimeUnit::Millisecond, TimeUnit::Nanosecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Microsecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Nanosecond) => TimeUnit::Microsecond,
                (TimeUnit::Nanosecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Nanosecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Nanosecond, TimeUnit::Microsecond) => TimeUnit::Microsecond,
                (l, r) => {
                    assert_eq!(l, r);
                    l.clone()
                }
            };

            Some(Timestamp(unit, tz))
        }
        _ => None,
    }
}

pub(crate) fn is_dictionary(t: &DataType) -> bool {
    matches!(t, DataType::Dictionary(_, _))
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // can't compare dictionaries directly due to
    // https://github.com/apache/arrow-rs/issues/1201
    if lhs_type == rhs_type && !is_dictionary(lhs_type) {
        // same type => all good
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// coercion rules for equality operations. This is a superset of all numerical coercion rules.
fn eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    // can't compare dictionaries directly due to
    // https://github.com/apache/arrow-rs/issues/1201
    if lhs_type == rhs_type && !is_dictionary(lhs_type) {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    numerical_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, true))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// coercion rules from NULL type. Since NULL can be casted to most of types in arrow,
/// either lhs or rhs is NULL, if NULL can be casted to type of the other side, the coecion is valid.
fn null_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (DataType::Null, _) => {
            if can_cast_types(&DataType::Null, rhs_type) {
                Some(rhs_type.clone())
            } else {
                None
            }
        }
        (_, DataType::Null) => {
            if can_cast_types(&DataType::Null, lhs_type) {
                Some(lhs_type.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Operator;
    use arrow::datatypes::DataType;
    use datafusion_common::DataFusionError;
    use datafusion_common::Result;

    #[test]
    fn test_coercion_error() -> Result<()> {
        let result_type =
            coerce_types(&DataType::Float32, &Operator::Plus, &DataType::Utf8);

        if let Err(DataFusionError::Plan(e)) = result_type {
            assert_eq!(e, "'Float32 + Utf8' can't be evaluated because there isn't a common type to coerce the types to");
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "Coercion should have returned an DataFusionError::Internal".to_string(),
            ))
        }
    }

    #[test]
    fn test_decimal_binary_comparison_coercion() -> Result<()> {
        let input_decimal = DataType::Decimal128(20, 3);
        let input_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, 10),
            DataType::Decimal128(20, 8),
        ];
        let result_types = [
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(23, 3),
            DataType::Decimal128(24, 7),
            DataType::Decimal128(32, 15),
            DataType::Decimal128(38, 10),
            DataType::Decimal128(25, 8),
        ];
        let comparison_op_types = [
            Operator::NotEq,
            Operator::Eq,
            Operator::Gt,
            Operator::GtEq,
            Operator::Lt,
            Operator::LtEq,
        ];
        for (i, input_type) in input_types.iter().enumerate() {
            let expect_type = &result_types[i];
            for op in comparison_op_types {
                let result_type = coerce_types(&input_decimal, &op, input_type)?;
                assert_eq!(expect_type, &result_type);
            }
        }
        // negative test
        let result_type = coerce_types(&input_decimal, &Operator::Eq, &DataType::Boolean);
        assert!(result_type.is_err());
        Ok(())
    }

    #[test]
    fn test_decimal_mathematics_op_type() {
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
            DataType::Decimal128(3, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
            DataType::Decimal128(5, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
            DataType::Decimal128(20, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
            DataType::Decimal128(14, 7)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
            DataType::Decimal128(30, 15)
        );

        let op = Operator::Plus;
        let left_decimal_type = DataType::Decimal128(10, 3);
        let right_decimal_type = DataType::Decimal128(20, 4);
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(21, 4), result.unwrap());
        let op = Operator::Minus;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(21, 4), result.unwrap());
        let op = Operator::Multiply;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(31, 7), result.unwrap());
        let op = Operator::Divide;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(35, 24), result.unwrap());
        let op = Operator::Modulo;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(11, 4), result.unwrap());
    }

    #[test]
    fn test_dictionary_type_coersion() {
        use DataType::*;

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Int32));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, false),
            Some(Int32)
        );

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), None);

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(lhs_type.clone())
        );

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(rhs_type.clone())
        );
    }
}
