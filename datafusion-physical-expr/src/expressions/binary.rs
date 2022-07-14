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

use std::{any::Any, convert::TryInto, sync::Arc};

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::DataType::Decimal;
use arrow::datatypes::{DataType, Schema};
use arrow::scalar::Scalar;
use arrow::types::NativeType;

use crate::coercion_rule::binary_rule::coerce_types;
use crate::expressions::try_cast;
use crate::PhysicalExpr;
use datafusion_common::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;

// fn as_decimal_array(arr: &dyn Array) -> &Int128Array {
//     arr.as_any()
//         .downcast_ref::<Int128Array>()
//         .expect("Unable to downcast to typed array to DecimalArray")
// }

// /// create a `dyn_op` wrapper function for the specified operation
// /// that call the underlying dyn_op arrow kernel if the type is
// /// supported, and translates ArrowError to DataFusionError
// macro_rules! make_dyn_comp_op {
//     ($OP:tt) => {
//         paste::paste! {
//             /// wrapper over arrow compute kernel that maps Error types and
//             /// patches missing support in arrow
//             fn [<$OP _dyn>] (left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
//                 match (left.data_type(), right.data_type()) {
//                     // Call `op_decimal` (e.g. `eq_decimal) until
//                     // arrow has native support
//                     // https://github.com/apache/arrow-rs/issues/1200
//                     (DataType::Decimal(_, _), DataType::Decimal(_, _)) => {
//                         [<$OP _decimal>](as_decimal_array(left), as_decimal_array(right))
//                     },
//                     // By default call the arrow kernel
//                     _ => {
//                     arrow::compute::comparison::[<$OP _dyn>](left, right)
//                             .map_err(|e| e.into())
//                     }
//                 }
//                 .map(|a| Arc::new(a) as ArrayRef)
//             }
//         }
//     };
// }
//
// // create eq_dyn, gt_dyn, wrappers etc
// make_dyn_comp_op!(eq);
// make_dyn_comp_op!(gt);
// make_dyn_comp_op!(gt_eq);
// make_dyn_comp_op!(lt);
// make_dyn_comp_op!(lt_eq);
// make_dyn_comp_op!(neq);

// Simple (low performance) kernels until optimized kernels are added to arrow
// See https://github.com/apache/arrow-rs/issues/960

fn is_distinct_from_bool(left: &dyn Array, right: &dyn Array) -> BooleanArray {
    // Different from `neq_bool` because `null is distinct from null` is false and not null
    let left = left
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("distinct_from op failed to downcast to boolean array");
    let right = right
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("distinct_from op failed to downcast to boolean array");
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left != right))
        .collect()
}

fn is_not_distinct_from_bool(left: &dyn Array, right: &dyn Array) -> BooleanArray {
    let left = left
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("not_distinct_from op failed to downcast to boolean array");
    let right = right
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("not_distinct_from op failed to downcast to boolean array");
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left == right))
        .collect()
}

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation on an array with a scalar.
macro_rules! binary_bitwise_array_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:tt, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let array = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let scalar = $RIGHT;
        if scalar.is_null() {
            Ok(new_null_array(array.data_type().clone(), len).into())
        } else {
            let right: $TYPE = scalar.try_into().unwrap();
            let result = (0..len)
                .into_iter()
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some(array.value(i) $OP right)
                    }
                })
                .collect::<$ARRAY_TYPE>();
            Ok(Arc::new(result) as ArrayRef)
        }
    }};
}

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation.
macro_rules! binary_bitwise_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:tt, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let left = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let right = $RIGHT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let result = (0..len)
            .into_iter()
            .map(|i| {
                if left.is_null(i) || right.is_null(i) {
                    None
                } else {
                    Some(left.value(i) $OP right.value(i))
                }
            })
            .collect::<$ARRAY_TYPE>();
        Ok(Arc::new(result))
    }};
}

fn bitwise_and(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(left, right, &, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(left, right, &, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(left, right, &, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(left, right, &, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseAnd
        ))),
    }
}

fn bitwise_or(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(left, right, |, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(left, right, |, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(left, right, |, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(left, right, |, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseOr
        ))),
    }
}

fn bitwise_and_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseAnd
        ))),
    };
    Some(result)
}

fn bitwise_or_scalar(array: &dyn Array, scalar: ScalarValue) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(array, scalar, |, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(array, scalar, |, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(array, scalar, |, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(array, scalar, |, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseOr
        ))),
    };
    Some(result)
}

/// Binary expression
#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    /// Create new binary expression
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self { left, op, right }
    }

    /// Get the left side of the binary expression
    pub fn left(&self) -> &Arc<dyn PhysicalExpr> {
        &self.left
    }

    /// Get the right side of the binary expression
    pub fn right(&self) -> &Arc<dyn PhysicalExpr> {
        &self.right
    }

    /// Get the operator for this binary expression
    pub fn op(&self) -> &Operator {
        &self.op
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

/// Invoke a boolean kernel on a pair of arrays
macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:expr) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

#[inline]
fn evaluate_regex<O: Offset>(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
    Ok(compute::regex_match::regex_match::<O>(
        lhs.as_any().downcast_ref().unwrap(),
        rhs.as_any().downcast_ref().unwrap(),
    )?)
}

#[inline]
fn evaluate_regex_case_insensitive<O: Offset>(
    lhs: &dyn Array,
    rhs: &dyn Array,
) -> Result<BooleanArray> {
    let patterns_arr = rhs.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
    // TODO: avoid this pattern array iteration by building the new regex pattern in the match
    // loop. We need to roll our own regex compute kernel instead of using the ones from arrow for
    // postgresql compatibility.
    let patterns = patterns_arr
        .iter()
        .map(|pattern| pattern.map(|s| format!("(?i){}", s)))
        .collect::<Vec<_>>();
    Ok(compute::regex_match::regex_match::<O>(
        lhs.as_any().downcast_ref().unwrap(),
        &Utf8Array::<O>::from(patterns),
    )?)
}

fn evaluate(lhs: &dyn Array, op: &Operator, rhs: &dyn Array) -> Result<Arc<dyn Array>> {
    use Operator::*;
    if matches!(op, Plus) {
        let arr: ArrayRef = match (lhs.data_type(), rhs.data_type()) {
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                let left_array =
                    lhs.as_any().downcast_ref::<PrimitiveArray<i128>>().unwrap();
                let right_array =
                    rhs.as_any().downcast_ref::<PrimitiveArray<i128>>().unwrap();
                Arc::new(if *p1 == *p2 && *s1 == *s2 {
                    compute::arithmetics::decimal::add(left_array, right_array)
                } else {
                    compute::arithmetics::decimal::adaptive_add(left_array, right_array)?
                })
            }
            _ => compute::arithmetics::add(lhs, rhs).into(),
        };
        Ok(arr)
    } else if matches!(op, Minus | Divide | Multiply | Modulo) {
        let arr = match op {
            Operator::Minus => compute::arithmetics::sub(lhs, rhs),
            Operator::Divide => compute::arithmetics::div(lhs, rhs),
            Operator::Multiply => compute::arithmetics::mul(lhs, rhs),
            Operator::Modulo => compute::arithmetics::rem(lhs, rhs),
            // TODO: show proper error message
            _ => unreachable!(),
        };
        Ok(Arc::<dyn Array>::from(arr))
    } else if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq) {
        let arr = match op {
            Operator::Eq => compute::comparison::eq(lhs, rhs),
            Operator::NotEq => compute::comparison::neq(lhs, rhs),
            Operator::Lt => compute::comparison::lt(lhs, rhs),
            Operator::LtEq => compute::comparison::lt_eq(lhs, rhs),
            Operator::Gt => compute::comparison::gt(lhs, rhs),
            Operator::GtEq => compute::comparison::gt_eq(lhs, rhs),
            // TODO: show proper error message
            _ => unreachable!(),
        };
        Ok(Arc::new(arr) as Arc<dyn Array>)
    } else if matches!(op, IsDistinctFrom) {
        is_distinct_from(lhs, rhs)
    } else if matches!(op, IsNotDistinctFrom) {
        is_not_distinct_from(lhs, rhs)
    } else if matches!(op, Or) {
        boolean_op!(lhs, rhs, compute::boolean_kleene::or)
    } else if matches!(op, And) {
        boolean_op!(lhs, rhs, compute::boolean_kleene::and)
    } else if matches!(op, BitwiseOr) {
        bitwise_or(lhs, rhs)
    } else if matches!(op, BitwiseAnd) {
        bitwise_and(lhs, rhs)
    } else {
        match (lhs.data_type(), op, rhs.data_type()) {
            (DataType::Utf8, Like, DataType::Utf8) => {
                Ok(compute::like::like_utf8::<i32>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::LargeUtf8, Like, DataType::LargeUtf8) => {
                Ok(compute::like::like_utf8::<i64>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::Utf8, NotLike, DataType::Utf8) => {
                Ok(compute::like::nlike_utf8::<i32>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::LargeUtf8, NotLike, DataType::LargeUtf8) => {
                Ok(compute::like::nlike_utf8::<i64>(
                    lhs.as_any().downcast_ref().unwrap(),
                    rhs.as_any().downcast_ref().unwrap(),
                )
                .map(Arc::new)?)
            }
            (DataType::Utf8, RegexMatch, DataType::Utf8) => {
                Ok(Arc::new(evaluate_regex::<i32>(lhs, rhs)?))
            }
            (DataType::Utf8, RegexIMatch, DataType::Utf8) => {
                Ok(Arc::new(evaluate_regex_case_insensitive::<i32>(lhs, rhs)?))
            }
            (DataType::Utf8, RegexNotMatch, DataType::Utf8) => {
                let re = evaluate_regex::<i32>(lhs, rhs)?;
                Ok(Arc::new(compute::boolean::not(&re)))
            }
            (DataType::Utf8, RegexNotIMatch, DataType::Utf8) => {
                let re = evaluate_regex_case_insensitive::<i32>(lhs, rhs)?;
                Ok(Arc::new(compute::boolean::not(&re)))
            }
            (DataType::LargeUtf8, RegexMatch, DataType::LargeUtf8) => {
                Ok(Arc::new(evaluate_regex::<i64>(lhs, rhs)?))
            }
            (DataType::LargeUtf8, RegexIMatch, DataType::LargeUtf8) => {
                Ok(Arc::new(evaluate_regex_case_insensitive::<i64>(lhs, rhs)?))
            }
            (DataType::LargeUtf8, RegexNotMatch, DataType::LargeUtf8) => {
                let re = evaluate_regex::<i64>(lhs, rhs)?;
                Ok(Arc::new(compute::boolean::not(&re)))
            }
            (DataType::LargeUtf8, RegexNotIMatch, DataType::LargeUtf8) => {
                let re = evaluate_regex_case_insensitive::<i64>(lhs, rhs)?;
                Ok(Arc::new(compute::boolean::not(&re)))
            }
            (lhs, op, rhs) => Err(DataFusionError::Internal(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                op, lhs, rhs
            ))),
        }
    }
}

macro_rules! dyn_compute_scalar {
    ($lhs:expr, $op:ident, $rhs:expr, $ty:ty) => {{
        Arc::new(compute::arithmetics::basic::$op::<$ty>(
            $lhs.as_any().downcast_ref().unwrap(),
            &$rhs.clone().try_into().unwrap(),
        ))
    }};
}

#[inline]
fn evaluate_regex_scalar<O: Offset>(
    values: &dyn Array,
    regex: &ScalarValue,
) -> Result<BooleanArray> {
    let values = values.as_any().downcast_ref().unwrap();
    let regex = match regex {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Regex pattern is not a valid string, got: {:?}",
                regex,
            )));
        }
    };
    Ok(compute::regex_match::regex_match_scalar::<O>(
        values, regex,
    )?)
}

#[inline]
fn evaluate_regex_scalar_case_insensitive<O: Offset>(
    values: &dyn Array,
    regex: &ScalarValue,
) -> Result<BooleanArray> {
    let values = values.as_any().downcast_ref().unwrap();
    let regex = match regex {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Regex pattern is not a valid string, got: {:?}",
                regex,
            )));
        }
    };
    Ok(compute::regex_match::regex_match_scalar::<O>(
        values,
        &format!("(?i){}", regex),
    )?)
}

macro_rules! with_match_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    match $key_type {
        DataType::Int8 => Some(__with_ty__! { i8 }),
        DataType::Int16 => Some(__with_ty__! { i16 }),
        DataType::Int32 => Some(__with_ty__! { i32 }),
        DataType::Int64 => Some(__with_ty__! { i64 }),
        DataType::UInt8 => Some(__with_ty__! { u8 }),
        DataType::UInt16 => Some(__with_ty__! { u16 }),
        DataType::UInt32 => Some(__with_ty__! { u32 }),
        DataType::UInt64 => Some(__with_ty__! { u64 }),
        DataType::Float32 => Some(__with_ty__! { f32 }),
        DataType::Float64 => Some(__with_ty__! { f64 }),
        _ => None,
    }
})}

fn evaluate_scalar(
    lhs: &dyn Array,
    op: &Operator,
    rhs: &ScalarValue,
) -> Result<Option<Arc<dyn Array>>> {
    use Operator::*;
    if matches!(op, Plus | Minus | Divide | Multiply | Modulo) {
        Ok(match op {
            Plus => {
                with_match_primitive_type!(lhs.data_type(), |$T| {
                    dyn_compute_scalar!(lhs, add_scalar, rhs, $T)
                })
            }
            Minus => {
                with_match_primitive_type!(lhs.data_type(), |$T| {
                    dyn_compute_scalar!(lhs, sub_scalar, rhs, $T)
                })
            }
            Divide => {
                with_match_primitive_type!(lhs.data_type(), |$T| {
                    dyn_compute_scalar!(lhs, div_scalar, rhs, $T)
                })
            }
            Multiply => {
                with_match_primitive_type!(lhs.data_type(), |$T| {
                    dyn_compute_scalar!(lhs, mul_scalar, rhs, $T)
                })
            }
            Modulo => {
                with_match_primitive_type!(lhs.data_type(), |$T| {
                    dyn_compute_scalar!(lhs, rem_scalar, rhs, $T)
                })
            }
            _ => None, // fall back to default comparison below
        })
    } else if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq) {
        let rhs: Result<Box<dyn Scalar>> = rhs.try_into();
        match rhs {
            Ok(rhs) => {
                let arr = match op {
                    Operator::Eq => compute::comparison::eq_scalar(lhs, &*rhs),
                    Operator::NotEq => compute::comparison::neq_scalar(lhs, &*rhs),
                    Operator::Lt => compute::comparison::lt_scalar(lhs, &*rhs),
                    Operator::LtEq => compute::comparison::lt_eq_scalar(lhs, &*rhs),
                    Operator::Gt => compute::comparison::gt_scalar(lhs, &*rhs),
                    Operator::GtEq => compute::comparison::gt_eq_scalar(lhs, &*rhs),
                    _ => unreachable!(),
                };
                Ok(Some(Arc::new(arr) as Arc<dyn Array>))
            }
            Err(_) => {
                // fall back to default comparison below
                Ok(None)
            }
        }
    } else if matches!(op, Or | And) {
        // TODO: optimize scalar Or | And
        Ok(None)
    } else if matches!(op, BitwiseOr) {
        bitwise_or_scalar(lhs, rhs.clone()).transpose()
    } else if matches!(op, BitwiseAnd) {
        bitwise_and_scalar(lhs, rhs.clone()).transpose()
    } else {
        match (lhs.data_type(), op) {
            (DataType::Utf8, RegexMatch) => {
                Ok(Some(Arc::new(evaluate_regex_scalar::<i32>(lhs, rhs)?)))
            }
            (DataType::Utf8, RegexIMatch) => Ok(Some(Arc::new(
                evaluate_regex_scalar_case_insensitive::<i32>(lhs, rhs)?,
            ))),
            (DataType::Utf8, RegexNotMatch) => Ok(Some(Arc::new(compute::boolean::not(
                &evaluate_regex_scalar::<i32>(lhs, rhs)?,
            )))),
            (DataType::Utf8, RegexNotIMatch) => {
                Ok(Some(Arc::new(compute::boolean::not(
                    &evaluate_regex_scalar_case_insensitive::<i32>(lhs, rhs)?,
                ))))
            }
            (DataType::LargeUtf8, RegexMatch) => {
                Ok(Some(Arc::new(evaluate_regex_scalar::<i64>(lhs, rhs)?)))
            }
            (DataType::LargeUtf8, RegexIMatch) => Ok(Some(Arc::new(
                evaluate_regex_scalar_case_insensitive::<i64>(lhs, rhs)?,
            ))),
            (DataType::LargeUtf8, RegexNotMatch) => Ok(Some(Arc::new(
                compute::boolean::not(&evaluate_regex_scalar::<i64>(lhs, rhs)?),
            ))),
            (DataType::LargeUtf8, RegexNotIMatch) => {
                Ok(Some(Arc::new(compute::boolean::not(
                    &evaluate_regex_scalar_case_insensitive::<i64>(lhs, rhs)?,
                ))))
            }
            _ => Ok(None),
        }
    }
}

fn evaluate_inverse_scalar(
    lhs: &ScalarValue,
    op: &Operator,
    rhs: &dyn Array,
) -> Result<Option<Arc<dyn Array>>> {
    use Operator::*;
    match op {
        Lt => evaluate_scalar(rhs, &Gt, lhs),
        Gt => evaluate_scalar(rhs, &Lt, lhs),
        GtEq => evaluate_scalar(rhs, &LtEq, lhs),
        LtEq => evaluate_scalar(rhs, &GtEq, lhs),
        Eq => evaluate_scalar(rhs, &Eq, lhs),
        NotEq => evaluate_scalar(rhs, &NotEq, lhs),
        Plus => evaluate_scalar(rhs, &Plus, lhs),
        Multiply => evaluate_scalar(rhs, &Multiply, lhs),
        _ => Ok(None),
    }
}

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
    }
}

impl PhysicalExpr for BinaryExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        binary_operator_data_type(
            &self.left.data_type(input_schema)?,
            &self.op,
            &self.right.data_type(input_schema)?,
        )
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left_value = self.left.evaluate(batch)?;
        let right_value = self.right.evaluate(batch)?;
        let left_data_type = left_value.data_type();
        let right_data_type = right_value.data_type();

        if left_data_type != right_data_type {
            return Err(DataFusionError::Internal(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op, left_data_type, right_data_type
            )));
        }

        // Attempt to use special kernels if one input is scalar and the other is an array
        let scalar_result = match (&left_value, &right_value) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                evaluate_scalar(array.as_ref(), &self.op, scalar)
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(array)) => {
                evaluate_inverse_scalar(scalar, &self.op, array.as_ref())
            }
            (_, _) => Ok(None),
        }?;

        if let Some(result) = scalar_result {
            return Ok(ColumnarValue::Array(result));
        }

        // if both arrays or both literals - extract arrays and continue execution
        let (left, right) = (
            left_value.into_array(batch.num_rows()),
            right_value.into_array(batch.num_rows()),
        );

        let result = evaluate(left.as_ref(), &self.op, right.as_ref());
        result.map(|a| ColumnarValue::Array(a))
    }
}

fn is_distinct_from_primitive<T: NativeType>(
    left: &dyn Array,
    right: &dyn Array,
) -> BooleanArray {
    let left = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("distinct_from op failed to downcast to primitive array");
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("distinct_from op failed to downcast to primitive array");
    left.iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect()
}

fn is_not_distinct_from_primitive<T: NativeType>(
    left: &dyn Array,
    right: &dyn Array,
) -> BooleanArray {
    let left = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("not_distinct_from op failed to downcast to primitive array");
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("not_distinct_from op failed to downcast to primitive array");
    left.iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect()
}

fn is_distinct_from_utf8<O: Offset>(left: &dyn Array, right: &dyn Array) -> BooleanArray {
    let left = left
        .as_any()
        .downcast_ref::<Utf8Array<O>>()
        .expect("distinct_from op failed to downcast to utf8 array");
    let right = right
        .as_any()
        .downcast_ref::<Utf8Array<O>>()
        .expect("distinct_from op failed to downcast to utf8 array");
    left.iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect()
}

fn is_not_distinct_from_utf8<O: Offset>(
    left: &dyn Array,
    right: &dyn Array,
) -> BooleanArray {
    let left = left
        .as_any()
        .downcast_ref::<Utf8Array<O>>()
        .expect("not_distinct_from op failed to downcast to utf8 array");
    let right = right
        .as_any()
        .downcast_ref::<Utf8Array<O>>()
        .expect("not_distinct_from op failed to downcast to utf8 array");
    left.iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect()
}

fn is_distinct_from(left: &dyn Array, right: &dyn Array) -> Result<Arc<dyn Array>> {
    match (left.data_type(), right.data_type()) {
        (DataType::Int8, DataType::Int8) => {
            Ok(Arc::new(is_distinct_from_primitive::<i8>(left, right)))
        }
        (DataType::Int32, DataType::Int32) => {
            Ok(Arc::new(is_distinct_from_primitive::<i32>(left, right)))
        }
        (DataType::Int64, DataType::Int64) => {
            Ok(Arc::new(is_distinct_from_primitive::<i64>(left, right)))
        }
        (DataType::UInt8, DataType::UInt8) => {
            Ok(Arc::new(is_distinct_from_primitive::<u8>(left, right)))
        }
        (DataType::UInt16, DataType::UInt16) => {
            Ok(Arc::new(is_distinct_from_primitive::<u16>(left, right)))
        }
        (DataType::UInt32, DataType::UInt32) => {
            Ok(Arc::new(is_distinct_from_primitive::<u32>(left, right)))
        }
        (DataType::UInt64, DataType::UInt64) => {
            Ok(Arc::new(is_distinct_from_primitive::<u64>(left, right)))
        }
        (DataType::Float32, DataType::Float32) => {
            Ok(Arc::new(is_distinct_from_primitive::<f32>(left, right)))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(Arc::new(is_distinct_from_primitive::<f64>(left, right)))
        }
        (DataType::Boolean, DataType::Boolean) => {
            Ok(Arc::new(is_distinct_from_bool(left, right)))
        }
        (DataType::Utf8, DataType::Utf8) => {
            Ok(Arc::new(is_distinct_from_utf8::<i32>(left, right)))
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            Ok(Arc::new(is_distinct_from_utf8::<i64>(left, right)))
        }
        (lhs, rhs) => Err(DataFusionError::Internal(format!(
            "Cannot evaluate is_distinct_from expression with types {:?} and {:?}",
            lhs, rhs
        ))),
    }
}

fn is_not_distinct_from(left: &dyn Array, right: &dyn Array) -> Result<Arc<dyn Array>> {
    match (left.data_type(), right.data_type()) {
        (DataType::Int8, DataType::Int8) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<i8>(left, right)))
        }
        (DataType::Int32, DataType::Int32) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<i32>(left, right)))
        }
        (DataType::Int64, DataType::Int64) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<i64>(left, right)))
        }
        (DataType::UInt8, DataType::UInt8) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<u8>(left, right)))
        }
        (DataType::UInt16, DataType::UInt16) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<u16>(left, right)))
        }
        (DataType::UInt32, DataType::UInt32) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<u32>(left, right)))
        }
        (DataType::UInt64, DataType::UInt64) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<u64>(left, right)))
        }
        (DataType::Float32, DataType::Float32) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<f32>(left, right)))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(Arc::new(is_not_distinct_from_primitive::<f64>(left, right)))
        }
        (DataType::Boolean, DataType::Boolean) => {
            Ok(Arc::new(is_not_distinct_from_bool(left, right)))
        }
        (DataType::Utf8, DataType::Utf8) => {
            Ok(Arc::new(is_not_distinct_from_utf8::<i32>(left, right)))
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            Ok(Arc::new(is_not_distinct_from_utf8::<i64>(left, right)))
        }
        (lhs, rhs) => Err(DataFusionError::Internal(format!(
            "Cannot evaluate is_not_distinct_from expression with types {:?} and {:?}",
            lhs, rhs
        ))),
    }
}

/// return two physical expressions that are optionally coerced to a
/// common type that the binary operator supports.
fn binary_cast(
    lhs: Arc<dyn PhysicalExpr>,
    op: &Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let lhs_type = &lhs.data_type(input_schema)?;
    let rhs_type = &rhs.data_type(input_schema)?;

    let result_type = coerce_types(lhs_type, op, rhs_type)?;

    Ok((
        try_cast(lhs, input_schema, result_type.clone())?,
        try_cast(rhs, input_schema, result_type)?,
    ))
}

/// Create a binary expression whose arguments are correctly coerced.
/// This function errors if it is not possible to coerce the arguments
/// to computational types supported by the operator.
pub fn binary(
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (l, r) = binary_cast(lhs, &op, rhs, input_schema)?;
    Ok(Arc::new(BinaryExpr::new(l, op, r)))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::*;
    use arrow::{array::*, types::NativeType};

    use super::*;

    use crate::expressions::{col, lit};
    use crate::test_util::create_decimal_array;
    use arrow::datatypes::{Field, SchemaRef};
    use arrow::error::Error as ArrowError;
    use datafusion_common::field_util::SchemaExt;

    // TODO add iter for decimal array
    // TODO move this to arrow-rs
    // https://github.com/apache/arrow-rs/issues/1083
    pub(super) fn eq_decimal_scalar(
        left: &Int128Array,
        right: i128,
    ) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) == right))?;
            }
        }
        Ok(bool_builder.into())
    }

    pub(super) fn eq_decimal(
        left: &Int128Array,
        right: &Int128Array,
    ) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) == right.value(i)))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn neq_decimal_scalar(left: &Int128Array, right: i128) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) != right))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn neq_decimal(left: &Int128Array, right: &Int128Array) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) != right.value(i)))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn lt_decimal_scalar(left: &Int128Array, right: i128) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) < right))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn lt_decimal(left: &Int128Array, right: &Int128Array) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) < right.value(i)))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn lt_eq_decimal_scalar(left: &Int128Array, right: i128) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) <= right))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn lt_eq_decimal(left: &Int128Array, right: &Int128Array) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) <= right.value(i)))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn gt_decimal_scalar(left: &Int128Array, right: i128) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) > right))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn gt_decimal(left: &Int128Array, right: &Int128Array) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) > right.value(i)))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn gt_eq_decimal_scalar(left: &Int128Array, right: i128) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) >= right))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn gt_eq_decimal(left: &Int128Array, right: &Int128Array) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                bool_builder.push(None);
            } else {
                bool_builder.try_push(Some(left.value(i) >= right.value(i)))?;
            }
        }
        Ok(bool_builder.into())
    }

    fn is_distinct_from_decimal(
        left: &Int128Array,
        right: &Int128Array,
    ) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            match (left.is_null(i), right.is_null(i)) {
                (true, true) => bool_builder.try_push(Some(false))?,
                (true, false) | (false, true) => bool_builder.try_push(Some(true))?,
                (_, _) => bool_builder.try_push(Some(left.value(i) != right.value(i)))?,
            }
        }
        Ok(bool_builder.into())
    }

    fn is_not_distinct_from_decimal(
        left: &Int128Array,
        right: &Int128Array,
    ) -> Result<BooleanArray> {
        let mut bool_builder = MutableBooleanArray::with_capacity(left.len());
        for i in 0..left.len() {
            match (left.is_null(i), right.is_null(i)) {
                (true, true) => bool_builder.try_push(Some(true))?,
                (true, false) | (false, true) => bool_builder.try_push(Some(false))?,
                (_, _) => bool_builder.try_push(Some(left.value(i) == right.value(i)))?,
            }
        }
        Ok(bool_builder.into())
    }

    fn add_decimal(left: &Int128Array, right: &Int128Array) -> Result<Int128Array> {
        let mut decimal_builder = Int128Vec::from_data(
            left.data_type().clone(),
            Vec::<i128>::with_capacity(left.len()),
            None,
        );
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                decimal_builder.push(None);
            } else {
                decimal_builder.try_push(Some(left.value(i) + right.value(i)))?;
            }
        }
        Ok(decimal_builder.into())
    }

    fn subtract_decimal(left: &Int128Array, right: &Int128Array) -> Result<Int128Array> {
        let mut decimal_builder = Int128Vec::from_data(
            left.data_type().clone(),
            Vec::<i128>::with_capacity(left.len()),
            None,
        );
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                decimal_builder.push(None);
            } else {
                decimal_builder.try_push(Some(left.value(i) - right.value(i)))?;
            }
        }
        Ok(decimal_builder.into())
    }

    fn multiply_decimal(
        left: &Int128Array,
        right: &Int128Array,
        scale: u32,
    ) -> Result<Int128Array> {
        let mut decimal_builder = Int128Vec::from_data(
            left.data_type().clone(),
            Vec::<i128>::with_capacity(left.len()),
            None,
        );
        let divide = 10_i128.pow(scale);
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                decimal_builder.push(None);
            } else {
                decimal_builder
                    .try_push(Some(left.value(i) * right.value(i) / divide))?;
            }
        }
        Ok(decimal_builder.into())
    }

    fn divide_decimal(
        left: &Int128Array,
        right: &Int128Array,
        scale: i32,
    ) -> Result<Int128Array> {
        let mut decimal_builder = Int128Vec::from_data(
            left.data_type().clone(),
            Vec::<i128>::with_capacity(left.len()),
            None,
        );
        let mul = 10_f64.powi(scale);
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                decimal_builder.push(None);
            } else if right.value(i) == 0 {
                return Err(DataFusionError::ArrowError(
                    ArrowError::InvalidArgumentError("Cannot divide by zero".to_string()),
                ));
            } else {
                let l_value = left.value(i) as f64;
                let r_value = right.value(i) as f64;
                let result = ((l_value / r_value) * mul) as i128;
                decimal_builder.try_push(Some(result))?;
            }
        }
        Ok(decimal_builder.into())
    }

    fn modulus_decimal(left: &Int128Array, right: &Int128Array) -> Result<Int128Array> {
        let mut decimal_builder = Int128Vec::from_data(
            left.data_type().clone(),
            Vec::<i128>::with_capacity(left.len()),
            None,
        );
        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                decimal_builder.push(None);
            } else if right.value(i) == 0 {
                return Err(DataFusionError::ArrowError(
                    ArrowError::InvalidArgumentError("Cannot divide by zero".to_string()),
                ));
            } else {
                decimal_builder.try_push(Some(left.value(i) % right.value(i)))?;
            }
        }
        Ok(decimal_builder.into())
    }

    // Create a binary expression without coercion. Used here when we do not want to coerce the expressions
    // to valid types. Usage can result in an execution (after plan) error.
    fn binary_simple(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        binary(l, op, r, input_schema).unwrap()
    }

    #[test]
    fn binary_comparison() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from_slice(vec![1, 2, 4, 8, 16]);

        // expression: "a < b"
        let lt = binary_simple(
            col("a", &schema)?,
            Operator::Lt,
            col("b", &schema)?,
            &schema,
        );
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        let result = lt.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![false, false, true, true, true];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for (i, &expected_item) in expected.iter().enumerate().take(5) {
            assert_eq!(result.value(i), expected_item);
        }

        Ok(())
    }

    #[test]
    fn binary_nested() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(vec![2, 4, 6, 8, 10]);
        let b = Int32Array::from_slice(vec![2, 5, 4, 8, 8]);

        // expression: "a < b OR a == b"
        let expr = binary_simple(
            binary_simple(
                col("a", &schema)?,
                Operator::Lt,
                col("b", &schema)?,
                &schema,
            ),
            Operator::Or,
            binary_simple(
                col("a", &schema)?,
                Operator::Eq,
                col("b", &schema)?,
                &schema,
            ),
            &schema,
        );
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        assert_eq!("a@0 < b@1 OR a@0 = b@1", format!("{}", expr));

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![true, true, false, true, false];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for (i, &expected_item) in expected.iter().enumerate().take(5) {
            assert_eq!(result.value(i), expected_item);
        }

        Ok(())
    }

    // runs an end-to-end test of physical type coercion:
    // 1. construct a record batch with two columns of type A and B
    //  (*_ARRAY is the Rust Arrow array type, and *_TYPE is the DataType of the elements)
    // 2. construct a physical expression of A OP B
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type C
    // 5. verify that the results of evaluation are $VEC
    macro_rules! test_coercion {
        ($A_ARRAY:ident, $B_ARRAY:ident, $OP:expr, $C_ARRAY:ident) => {{
            let schema = Schema::new(vec![
                Field::new("a", $A_ARRAY.data_type().clone(), false),
                Field::new("b", $B_ARRAY.data_type().clone(), false),
            ]);
            // verify that we can construct the expression
            let expression =
                binary(col("a", &schema)?, $OP, col("b", &schema)?, &schema)?;
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new($A_ARRAY), Arc::new($B_ARRAY)],
            )?;

            // verify that the expression's type is correct
            assert_eq!(&expression.data_type(&schema)?, $C_ARRAY.data_type());

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array is equal
            assert_eq!($C_ARRAY, result.as_ref());
        }};
    }

    #[test]
    fn test_type_coersion() -> Result<()> {
        let a = Int32Array::from_slice(&[1, 2]);
        let b = UInt32Array::from_slice(&[1, 2]);
        let c = Int32Array::from_slice(&[2, 4]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Int32Array::from_slice(&[1]);
        let b = UInt32Array::from_slice(&[1]);
        let c = Int32Array::from_slice(&[2]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Int32Array::from_slice(&[1]);
        let b = UInt16Array::from_slice(&[1]);
        let c = Int32Array::from_slice(&[2]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Float32Array::from_slice(&[1.0]);
        let b = UInt16Array::from_slice(&[1]);
        let c = Float32Array::from_slice(&[2.0]);
        test_coercion!(a, b, Operator::Plus, c);

        let a = Float32Array::from_slice(&[1.0]);
        let b = UInt16Array::from_slice(&[1]);
        let c = Float32Array::from_slice(&[1.0]);
        test_coercion!(a, b, Operator::Multiply, c);

        let a = Utf8Array::<i32>::from_slice(&["hello world"]);
        let b = Utf8Array::<i32>::from_slice(&["%hello%"]);
        let c = BooleanArray::from_slice(&[true]);
        test_coercion!(a, b, Operator::Like, c);

        let a = Utf8Array::<i32>::from_slice(&["1994-12-13"]);
        let b = Int32Array::from_slice(&[9112]).to(DataType::Date32);
        let c = BooleanArray::from_slice(&[true]);
        test_coercion!(a, b, Operator::Eq, c);

        let a = Utf8Array::<i32>::from_slice(&["1994-12-13", "1995-01-26"]);
        let b = Int32Array::from_slice(&[9113, 9154]).to(DataType::Date32);
        let c = BooleanArray::from_slice(&[true, false]);
        test_coercion!(a, b, Operator::Lt, c);

        let a =
            Utf8Array::<i32>::from_slice(&["1994-12-13T12:34:56", "1995-01-26T01:23:45"]);
        let b =
            Int64Array::from_slice(&[787322096000, 791083425000]).to(DataType::Date64);
        let c = BooleanArray::from_slice(&[true, true]);
        test_coercion!(a, b, Operator::Eq, c);

        let a =
            Utf8Array::<i32>::from_slice(&["1994-12-13T12:34:56", "1995-01-26T01:23:45"]);
        let b =
            Int64Array::from_slice(&[787322096001, 791083424999]).to(DataType::Date64);
        let c = BooleanArray::from_slice(&[true, false]);
        test_coercion!(a, b, Operator::Lt, c);

        let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[true, false, true, false, false]);
        test_coercion!(a, b, Operator::RegexMatch, c);

        let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[true, true, true, true, false]);
        test_coercion!(a, b, Operator::RegexIMatch, c);

        let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[false, true, false, true, true]);
        test_coercion!(a, b, Operator::RegexNotMatch, c);

        let a = Utf8Array::<i32>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i32>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[false, false, false, false, true]);
        test_coercion!(a, b, Operator::RegexNotIMatch, c);

        let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[true, false, true, false, false]);
        test_coercion!(a, b, Operator::RegexMatch, c);

        let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[true, true, true, true, false]);
        test_coercion!(a, b, Operator::RegexIMatch, c);

        let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[false, true, false, true, true]);
        test_coercion!(a, b, Operator::RegexNotMatch, c);

        let a = Utf8Array::<i64>::from_slice(["abc"; 5]);
        let b = Utf8Array::<i64>::from_slice(["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"]);
        let c = BooleanArray::from_slice(&[false, false, false, false, true]);
        test_coercion!(a, b, Operator::RegexNotIMatch, c);

        let a = Int16Array::from_slice(&[1i16, 2i16, 3i16]);
        let b = Int64Array::from_slice(&[10i64, 4i64, 5i64]);
        let c = Int64Array::from_slice(&[0i64, 0i64, 1i64]);
        test_coercion!(a, b, Operator::BitwiseAnd, c);
        Ok(())
    }

    // Note it would be nice to use the same test_coercion macro as
    // above, but sadly the type of the values of the dictionary are
    // not encoded in the rust type of the DictionaryArray. Thus there
    // is no way at the time of this writing to create a dictionary
    // array using the `From` trait
    #[test]
    fn test_dictionary_type_to_array_coersion() -> Result<()> {
        // Test string  a string dictionary

        let data = vec![Some("one"), None, Some("three"), Some("four")];

        let mut dict_array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
        dict_array.try_extend(data)?;
        let dict_array = dict_array.into_arc();

        let str_array =
            Utf8Array::<i32>::from(&[Some("not one"), Some("two"), None, Some("four")]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_array.data_type().clone(), true),
            Field::new("str", str_array.data_type().clone(), true),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![dict_array, Arc::new(str_array)])?;

        let expected = BooleanArray::from(&[Some(false), None, None, Some(true)]);

        // Test 1: dict = str

        // verify that we can construct the expression
        let expression = binary(
            col("dict", &schema)?,
            Operator::Eq,
            col("str", &schema)?,
            &schema,
        )?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, result.as_ref());

        // Test 2: now test the other direction
        // str = dict

        // verify that we can construct the expression
        let expression = binary(
            col("str", &schema)?,
            Operator::Eq,
            col("dict", &schema)?,
            &schema,
        )?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, result.as_ref());

        Ok(())
    }

    #[test]
    fn plus_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from_slice(vec![1, 2, 4, 8, 16]);

        apply_arithmetic::<i32>(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
            Operator::Plus,
            Int32Array::from_slice(vec![2, 4, 7, 12, 21]),
        )?;

        Ok(())
    }

    #[test]
    fn minus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(vec![1, 2, 4, 8, 16]));
        let b = Arc::new(Int32Array::from_slice(vec![1, 2, 3, 4, 5]));

        apply_arithmetic::<i32>(
            schema.clone(),
            vec![a.clone(), b.clone()],
            Operator::Minus,
            Int32Array::from_slice(vec![0, 0, 1, 4, 11]),
        )?;

        // should handle have negative values in result (for signed)
        apply_arithmetic::<i32>(
            schema,
            vec![b, a],
            Operator::Minus,
            Int32Array::from_slice(vec![0, 0, -1, -4, -11]),
        )?;

        Ok(())
    }

    #[test]
    fn multiply_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(vec![4, 8, 16, 32, 64]));
        let b = Arc::new(Int32Array::from_slice(vec![2, 4, 8, 16, 32]));

        apply_arithmetic::<i32>(
            schema,
            vec![a, b],
            Operator::Multiply,
            Int32Array::from_slice(vec![8, 32, 128, 512, 2048]),
        )?;

        Ok(())
    }

    #[test]
    fn divide_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(vec![8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from_slice(vec![2, 4, 8, 16, 32]));

        apply_arithmetic::<i32>(
            schema,
            vec![a, b],
            Operator::Divide,
            Int32Array::from_slice(vec![4, 8, 16, 32, 64]),
        )?;

        Ok(())
    }

    fn apply_arithmetic<T: NativeType>(
        schema: Arc<Schema>,
        data: Vec<Arc<dyn Array>>,
        op: Operator,
        expected: PrimitiveArray<T>,
    ) -> Result<()> {
        let arithmetic_op =
            binary_simple(col("a", &schema)?, op, col("b", &schema)?, &schema);
        let batch = RecordBatch::try_new(schema, data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    fn apply_logic_op(
        schema: &Arc<Schema>,
        left: &ArrayRef,
        right: &ArrayRef,
        op: Operator,
        expected: ArrayRef,
    ) -> Result<()> {
        let arithmetic_op =
            binary_simple(col("a", schema)?, op, col("b", schema)?, schema);
        let data: Vec<ArrayRef> = vec![left.clone(), right.clone()];
        let batch = RecordBatch::try_new(schema.clone(), data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn modulus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 7, 14, 32]));

        apply_arithmetic::<i32>(
            schema,
            vec![a, b],
            Operator::Modulo,
            Int32Array::from_slice(&[0, 0, 2, 8, 0]),
        )?;

        Ok(())
    }

    // Test `scalar <op> arr` produces expected
    fn apply_logic_op_scalar_arr(
        schema: &SchemaRef,
        scalar: &ScalarValue,
        arr: &ArrayRef,
        op: Operator,
        expected: &BooleanArray,
    ) -> Result<()> {
        let scalar = lit(scalar.clone());

        let arithmetic_op = binary_simple(scalar, op, col("a", schema)?, schema);
        let batch = RecordBatch::try_new(Arc::clone(schema), vec![Arc::clone(arr)])?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.as_ref(), expected as &dyn Array);

        Ok(())
    }

    // Test `arr <op> scalar` produces expected
    fn apply_logic_op_arr_scalar(
        schema: &SchemaRef,
        arr: &ArrayRef,
        scalar: &ScalarValue,
        op: Operator,
        expected: &BooleanArray,
    ) -> Result<()> {
        let scalar = lit(scalar.clone());

        let arithmetic_op = binary_simple(col("a", schema)?, op, scalar, schema);
        let batch = RecordBatch::try_new(Arc::clone(schema), vec![Arc::clone(arr)])?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.as_ref(), expected as &dyn Array);

        Ok(())
    }

    #[test]
    fn and_with_nulls_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = Arc::new(BooleanArray::from_iter(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ])) as ArrayRef;
        let b = Arc::new(BooleanArray::from_iter(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ])) as ArrayRef;

        let expected = BooleanArray::from_iter(vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            None,
        ]);
        apply_logic_op(&Arc::new(schema), &a, &b, Operator::And, Arc::new(expected))?;

        Ok(())
    }

    #[test]
    fn or_with_nulls_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = Arc::new(BooleanArray::from_iter(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ])) as ArrayRef;
        let b = Arc::new(BooleanArray::from_iter(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ])) as ArrayRef;

        let expected = BooleanArray::from_iter(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            None,
        ]);
        apply_logic_op(&Arc::new(schema), &a, &b, Operator::Or, Arc::new(expected))?;

        Ok(())
    }

    /// Returns (schema, a: BooleanArray, b: BooleanArray) with all possible inputs
    ///
    /// a: [true, true, true,  NULL, NULL, NULL,  false, false, false]
    /// b: [true, NULL, false, true, NULL, false, true,  NULL,  false]
    fn bool_test_arrays() -> (SchemaRef, ArrayRef, ArrayRef) {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]);
        let a: BooleanArray = [
            Some(true),
            Some(true),
            Some(true),
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
        ]
        .iter()
        .collect();
        let b: BooleanArray = [
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
        ]
        .iter()
        .collect();
        (Arc::new(schema), Arc::new(a), Arc::new(b))
    }

    /// Returns (schema, BooleanArray) with [true, NULL, false]
    fn scalar_bool_test_array() -> (SchemaRef, ArrayRef) {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let a: BooleanArray = vec![Some(true), None, Some(false)].iter().collect();
        (Arc::new(schema), Arc::new(a))
    }

    #[test]
    fn eq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter(vec![
            Some(true),
            None,
            Some(false),
            None,
            None,
            None,
            Some(false),
            None,
            Some(true),
        ]);
        apply_logic_op(&schema, &a, &b, Operator::Eq, Arc::new(expected)).unwrap();
    }

    #[test]
    fn eq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::Eq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::Eq,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::Eq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::Eq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn neq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(false),
            None,
            Some(true),
            None,
            None,
            None,
            Some(true),
            None,
            Some(false),
        ]);
        apply_logic_op(&schema, &a, &b, Operator::NotEq, Arc::new(expected)).unwrap();
    }

    #[test]
    fn neq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::NotEq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::NotEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::NotEq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::NotEq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn lt_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(false),
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
            None,
            Some(false),
        ]);
        apply_logic_op(&schema, &a, &b, Operator::Lt, Arc::new(expected)).unwrap();
    }

    #[test]
    fn lt_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(false), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::Lt,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::Lt,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::Lt,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(false)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::Lt,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn lt_eq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(true),
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
            None,
            Some(true),
        ]);
        apply_logic_op(&schema, &a, &b, Operator::LtEq, Arc::new(expected)).unwrap();
    }

    #[test]
    fn lt_eq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::LtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::LtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::LtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::LtEq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn gt_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(false),
            None,
            Some(true),
            None,
            None,
            None,
            Some(false),
            None,
            Some(false),
        ]);
        apply_logic_op(&schema, &a, &b, Operator::Gt, Arc::new(expected)).unwrap();
    }

    #[test]
    fn gt_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = BooleanArray::from_iter([Some(false), None, Some(true)]);
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::Gt,
            &expected,
        )
        .unwrap();

        let expected = BooleanArray::from_iter([Some(false), None, Some(false)]);
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::Gt,
            &expected,
        )
        .unwrap();

        let expected = BooleanArray::from_iter([Some(false), None, Some(false)]);
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::Gt,
            &expected,
        )
        .unwrap();

        let expected = BooleanArray::from_iter([Some(true), None, Some(false)]);
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::Gt,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn gt_eq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(true),
            None,
            Some(true),
            None,
            None,
            None,
            Some(false),
            None,
            Some(true),
        ]);
        apply_logic_op(&schema, &a, &b, Operator::GtEq, Arc::new(expected)).unwrap();
    }

    #[test]
    fn gt_eq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = BooleanArray::from_iter([Some(true), None, Some(true)]);
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::GtEq,
            &expected,
        )
        .unwrap();

        let expected = BooleanArray::from_iter([Some(true), None, Some(false)]);
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::GtEq,
            &expected,
        )
        .unwrap();

        let expected = BooleanArray::from_iter([Some(false), None, Some(true)]);
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::GtEq,
            &expected,
        )
        .unwrap();

        let expected = BooleanArray::from_iter([Some(true), None, Some(true)]);
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::GtEq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn is_distinct_from_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
        ]);
        apply_logic_op(
            &schema,
            &a,
            &b,
            Operator::IsDistinctFrom,
            Arc::new(expected),
        )
        .unwrap();
    }

    #[test]
    fn is_not_distinct_from_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = BooleanArray::from_iter([
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ]);
        apply_logic_op(
            &schema,
            &a,
            &b,
            Operator::IsNotDistinctFrom,
            Arc::new(expected),
        )
        .unwrap();
    }

    #[test]
    fn relatively_deeply_nested() {
        // Reproducer for https://github.com/apache/arrow-datafusion/issues/419

        // where even relatively shallow binary expressions overflowed
        // the stack in debug builds

        let input: Vec<_> = vec![1, 2, 3, 4, 5].into_iter().map(Some).collect();
        let a: Int32Array = input.iter().collect();

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(a) as _)]).unwrap();
        let schema = batch.schema();

        // build a left deep tree ((((a + a) + a) + a ....
        let tree_depth: i32 = 100;
        let expr = (0..tree_depth)
            .into_iter()
            .map(|_| col("a", schema.as_ref()).unwrap())
            .reduce(|l, r| binary_simple(l, Operator::Plus, r, schema))
            .unwrap();

        let result = expr
            .evaluate(&batch)
            .expect("evaluation")
            .into_array(batch.num_rows());

        let expected: Int32Array = input
            .into_iter()
            .map(|i| i.map(|i| i * tree_depth))
            .collect();
        assert_eq!(result.as_ref(), &expected as &dyn Array);
    }

    #[test]
    fn comparison_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        )?;
        // eq: array = i128
        let result = eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(true), None, Some(false), Some(false)]),
            result
        );
        // neq: array != i128
        let result = neq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(false), None, Some(true), Some(true)]),
            result
        );
        // lt: array < i128
        let result = lt_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(false), None, Some(true), Some(false)]),
            result
        );
        // lt_eq: array <= i128
        let result = lt_eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(true), None, Some(true), Some(false)]),
            result
        );
        // gt: array > i128
        let result = gt_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(false), None, Some(false), Some(true)]),
            result
        );
        // gt_eq: array >= i128
        let result = gt_eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(true), None, Some(false), Some(true)]),
            result
        );

        let left_decimal_array = decimal_array;
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128 - 1),
                Some(value_i128),
                Some(value_i128 + 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        )?;
        // eq: left == right
        let result = eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(false), None, Some(false), Some(true)]),
            result
        );
        // neq: left != right
        let result = neq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(true), None, Some(true), Some(false)]),
            result
        );
        // lt: left < right
        let result = lt_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(false), None, Some(true), Some(false)]),
            result
        );
        // lt_eq: left <= right
        let result = lt_eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(false), None, Some(true), Some(true)]),
            result
        );
        // gt: left > right
        let result = gt_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(true), None, Some(false), Some(false)]),
            result
        );
        // gt_eq: left >= right
        let result = gt_eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![Some(true), None, Some(false), Some(true)]),
            result
        );
        // is_distinct: left distinct right
        let result = is_distinct_from_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false)
            ]),
            result
        );
        // is_distinct: left distinct right
        let result =
            is_not_distinct_from_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from_iter(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true)
            ]),
            result
        );
        Ok(())
    }

    #[test]
    fn comparison_decimal_expr_test() -> Result<()> {
        let decimal_scalar = ScalarValue::Decimal128(Some(123_456), 10, 3);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        // scalar == array
        apply_logic_op_scalar_arr(
            &schema,
            &decimal_scalar,
            &(Arc::new(Int64Array::from_iter(vec![Some(124), None])) as ArrayRef),
            Operator::Eq,
            &BooleanArray::from_iter(vec![Some(false), None]),
        )
        .unwrap();

        // array != scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Int64Array::from_iter(vec![Some(123), None, Some(1)]))
                as ArrayRef),
            &decimal_scalar,
            Operator::NotEq,
            &BooleanArray::from_iter(vec![Some(true), None, Some(true)]),
        )
        .unwrap();

        // array < scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Int64Array::from_iter(vec![Some(123), None, Some(124)]))
                as ArrayRef),
            &decimal_scalar,
            Operator::Lt,
            &BooleanArray::from_iter(vec![Some(true), None, Some(false)]),
        )
        .unwrap();

        // array > scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Int64Array::from_iter(vec![Some(123), None, Some(124)]))
                as ArrayRef),
            &decimal_scalar,
            Operator::Gt,
            &BooleanArray::from_iter(vec![Some(false), None, Some(true)]),
        )
        .unwrap();

        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        // array == scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Float64Array::from_iter(vec![
                Some(123.456),
                None,
                Some(123.457),
            ])) as ArrayRef),
            &decimal_scalar,
            Operator::Eq,
            &BooleanArray::from_iter(vec![Some(true), None, Some(false)]),
        )
        .unwrap();

        // array <= scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Float64Array::from_iter(vec![
                Some(123.456),
                None,
                Some(123.457),
                Some(123.45),
            ])) as ArrayRef),
            &decimal_scalar,
            Operator::LtEq,
            &BooleanArray::from_iter(vec![Some(true), None, Some(false), Some(true)]),
        )
        .unwrap();
        // array >= scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Float64Array::from_iter(vec![
                Some(123.456),
                None,
                Some(123.457),
                Some(123.45),
            ])) as ArrayRef),
            &decimal_scalar,
            Operator::GtEq,
            &BooleanArray::from_iter(vec![Some(true), None, Some(true), Some(false)]),
        )
        .unwrap();

        // compare decimal array with other array type
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Decimal(10, 0), true),
        ]));

        let value: i64 = 123;

        let decimal_array = Arc::new(create_decimal_array(
            &[
                Some(value as i128),
                None,
                Some((value - 1) as i128),
                Some((value + 1) as i128),
            ],
            10,
            0,
        )?) as ArrayRef;

        let int64_array = Arc::new(Int64Array::from_iter(vec![
            Some(value),
            Some(value - 1),
            Some(value),
            Some(value + 1),
        ])) as ArrayRef;

        // eq: int64array == decimal array
        apply_logic_op(
            &schema,
            &int64_array,
            &decimal_array,
            Operator::Eq,
            Arc::new(BooleanArray::from_iter(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
        )
        .unwrap();
        // neq: int64array != decimal array
        apply_logic_op(
            &schema,
            &int64_array,
            &decimal_array,
            Operator::NotEq,
            Arc::new(BooleanArray::from_iter(vec![
                Some(false),
                None,
                Some(true),
                Some(false),
            ])),
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));

        let value: i128 = 123;
        let decimal_array = Arc::new(create_decimal_array(
            &[
                Some(value as i128), // 1.23
                None,
                Some((value - 1) as i128), // 1.22
                Some((value + 1) as i128), // 1.24
            ],
            10,
            2,
        )?) as ArrayRef;
        let float64_array = Arc::new(Float64Array::from_iter(vec![
            Some(1.23),
            Some(1.22),
            Some(1.23),
            Some(1.24),
        ])) as ArrayRef;
        // lt: float64array < decimal array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::Lt,
            Arc::new(BooleanArray::from_iter(vec![
                Some(false),
                None,
                Some(false),
                Some(false),
            ])),
        )
        .unwrap();
        // lt_eq: float64array <= decimal array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::LtEq,
            Arc::new(BooleanArray::from_iter(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
        )
        .unwrap();
        // gt: float64array > decimal array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::Gt,
            Arc::new(BooleanArray::from_iter(vec![
                Some(false),
                None,
                Some(true),
                Some(false),
            ])),
        )
        .unwrap();
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::GtEq,
            Arc::new(BooleanArray::from_iter(vec![
                Some(true),
                None,
                Some(true),
                Some(true),
            ])),
        )
        .unwrap();
        // is distinct: float64array is distinct decimal array
        // TODO: now we do not refactor the `is distinct or is not distinct` rule of coercion.
        // traced by https://github.com/apache/arrow-datafusion/issues/1590
        // the decimal array will be casted to float64array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::IsDistinctFrom,
            Arc::new(BooleanArray::from_iter(vec![
                Some(false),
                Some(true),
                Some(true),
                Some(false),
            ])),
        )
        .unwrap();
        // is not distinct
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::IsNotDistinctFrom,
            Arc::new(BooleanArray::from_iter(vec![
                Some(true),
                Some(false),
                Some(false),
                Some(true),
            ])),
        )
        .unwrap();

        Ok(())
    }

    #[test]
    fn arithmetic_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let left_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        )?;
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
            ],
            25,
            3,
        )?;
        // add
        let result = add_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect =
            create_decimal_array(&[Some(246), None, Some(245), Some(247)], 25, 3)?;
        assert_eq!(expect, result);
        // subtract
        let result = subtract_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(0), None, Some(-1), Some(1)], 25, 3)?;
        assert_eq!(expect, result);
        // multiply
        let result = multiply_decimal(&left_decimal_array, &right_decimal_array, 3)?;
        let expect = create_decimal_array(&[Some(15), None, Some(15), Some(15)], 25, 3)?;
        assert_eq!(expect, result);
        // divide
        let left_decimal_array = create_decimal_array(
            &[Some(1234567), None, Some(1234567), Some(1234567)],
            25,
            3,
        )?;
        let right_decimal_array =
            create_decimal_array(&[Some(10), Some(100), Some(55), Some(-123)], 25, 3)?;
        let result = divide_decimal(&left_decimal_array, &right_decimal_array, 3)?;
        let expect = create_decimal_array(
            &[Some(123456700), None, Some(22446672), Some(-10037130)],
            25,
            3,
        )?;
        assert_eq!(expect, result);
        // modulus
        let result = modulus_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(7), None, Some(37), Some(16)], 25, 3)?;
        assert_eq!(expect, result);

        Ok(())
    }

    fn apply_arithmetic_op(
        schema: &SchemaRef,
        left: &ArrayRef,
        right: &ArrayRef,
        op: Operator,
        expected: ArrayRef,
    ) -> Result<()> {
        let arithmetic_op =
            binary_simple(col("a", schema)?, op, col("b", schema)?, schema);
        let data: Vec<ArrayRef> = vec![left.clone(), right.clone()];
        let batch = RecordBatch::try_new(schema.clone(), data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(result.as_ref(), expected.as_ref());
        Ok(())
    }

    #[test]
    fn arithmetic_decimal_expr_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));
        let value: i128 = 123;
        let decimal_array = Arc::new(create_decimal_array(
            &[
                Some(value as i128), // 1.23
                None,
                Some((value - 1) as i128), // 1.22
                Some((value + 1) as i128), // 1.24
            ],
            10,
            2,
        )?) as ArrayRef;
        let int32_array = Arc::new(Int32Array::from_iter(vec![
            Some(123),
            Some(122),
            Some(123),
            Some(124),
        ])) as ArrayRef;

        // add: Int32array add decimal array
        let expect = Arc::new(create_decimal_array(
            &[Some(12423), None, Some(12422), Some(12524)],
            13,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Plus,
            expect,
        )
        .unwrap();

        // subtract: decimal array subtract int32 array
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Decimal(10, 2), true),
        ]));
        let expect = Arc::new(create_decimal_array(
            &[Some(-12177), None, Some(-12178), Some(-12276)],
            13,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Minus,
            expect,
        )
        .unwrap();

        // multiply: decimal array multiply int32 array
        let expect = Arc::new(create_decimal_array(
            &[Some(15129), None, Some(15006), Some(15376)],
            21,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Multiply,
            expect,
        )
        .unwrap();
        // divide: int32 array divide decimal array
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));
        let expect = Arc::new(create_decimal_array(
            &[
                Some(10000000000000),
                None,
                Some(10081967213114),
                Some(10000000000000),
            ],
            23,
            11,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Divide,
            expect,
        )
        .unwrap();
        // modulus: int32 array modulus decimal array
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));
        let expect = Arc::new(create_decimal_array(
            &[Some(000), None, Some(100), Some(000)],
            10,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Modulo,
            expect,
        )
        .unwrap();

        Ok(())
    }

    #[test]
    fn bitwise_array_test() -> Result<()> {
        let left =
            Arc::new(Int32Array::from_iter(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right =
            Arc::new(Int32Array::from_iter(vec![Some(1), Some(3), Some(7)])) as ArrayRef;
        let result = bitwise_and(left.as_ref(), right.as_ref())?;
        let expected = Int32Vec::from(vec![Some(0), None, Some(3)]).as_arc();
        assert_eq!(result.as_ref(), expected.as_ref());

        let result = bitwise_or(left.as_ref(), right.as_ref())?;
        let expected = Int32Vec::from(vec![Some(13), None, Some(15)]).as_arc();
        assert_eq!(result.as_ref(), expected.as_ref());
        Ok(())
    }

    #[test]
    fn bitwise_scalar_test() -> Result<()> {
        let left =
            Arc::new(Int32Array::from_iter(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right = ScalarValue::from(3i32);
        let result = bitwise_and_scalar(left.as_ref(), right.clone()).unwrap()?;
        let expected = Int32Vec::from(vec![Some(0), None, Some(3)]).as_arc();
        assert_eq!(result.as_ref(), expected.as_ref());

        let result = bitwise_and_scalar(left.as_ref(), right).unwrap()?;
        let expected = Int32Vec::from(vec![Some(15), None, Some(11)]).as_arc();
        assert_eq!(result.as_ref(), expected.as_ref());
        Ok(())
    }
}
