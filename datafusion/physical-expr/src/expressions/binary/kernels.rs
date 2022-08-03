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

//! This module contains computation kernels that are specific to
//! datafusion and not (yet) targeted to  port upstream to arrow
use arrow::array::*;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Operator;
use std::sync::Arc;

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

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation on an array with a scalar.
macro_rules! binary_bitwise_array_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:tt, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let array = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let scalar = $RIGHT;
        if scalar.is_null() {
            Ok(new_null_array(array.data_type(), len))
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

pub(crate) fn bitwise_and(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
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

pub(crate) fn bitwise_or(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
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

pub(crate) fn bitwise_and_scalar(
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

pub(crate) fn bitwise_or_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
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

/// Concat lhs and rhs String Array, any `NULL` exists on lhs or rhs will come out result `NULL`
/// 1. 'a' || 'b' || 32 = 'ab32'
/// 2. 'a' || NULL = NULL
pub(crate) fn string_concat(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    let left_array = left.as_any().downcast_ref::<StringArray>().unwrap();
    let right_array = right.as_any().downcast_ref::<StringArray>().unwrap();
    let result = (0..left.len())
        .into_iter()
        .map(|i| {
            if left.is_null(i) || right.is_null(i) {
                None
            } else {
                let mut owned_string: String = "".to_owned();
                owned_string.push_str(left_array.value(i));
                owned_string.push_str(right_array.value(i));
                Some(owned_string)
            }
        })
        .collect::<StringArray>();
    Ok(Arc::new(result) as ArrayRef)
}
