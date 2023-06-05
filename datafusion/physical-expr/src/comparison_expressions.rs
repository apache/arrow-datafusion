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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! Comparison expressions

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow_ord::comparison::{gt_dyn, lt_dyn};
use arrow_select::zip::zip;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

#[derive(Debug, Clone, PartialEq)]
enum ComparisonOperator {
    Greatest,
    Least,
}

macro_rules! compare_scalar_typed {
    ($op:expr, $args:expr, $data_type:ident) => {{
        let value = $args
            .iter()
            .filter_map(|scalar| match scalar {
                ScalarValue::$data_type(v) => v.clone(),
                _ => panic!("Impossibly got non-scalar values"),
            })
            .reduce(|a, b| match $op {
                ComparisonOperator::Greatest => a.max(b),
                ComparisonOperator::Least => a.min(b),
            });
        ScalarValue::$data_type(value)
    }};
}

/// Evaluate a greatest or least function for the case when all arguments are scalars
fn compare_scalars(
    data_type: DataType,
    op: ComparisonOperator,
    args: &[ScalarValue],
) -> ScalarValue {
    match data_type {
        DataType::Boolean => compare_scalar_typed!(op, args, Boolean),
        DataType::Int8 => compare_scalar_typed!(op, args, Int8),
        DataType::Int16 => compare_scalar_typed!(op, args, Int16),
        DataType::Int32 => compare_scalar_typed!(op, args, Int32),
        DataType::Int64 => compare_scalar_typed!(op, args, Int64),
        DataType::UInt8 => compare_scalar_typed!(op, args, UInt8),
        DataType::UInt16 => compare_scalar_typed!(op, args, UInt16),
        DataType::UInt32 => compare_scalar_typed!(op, args, UInt32),
        DataType::UInt64 => compare_scalar_typed!(op, args, UInt64),
        DataType::Float32 => compare_scalar_typed!(op, args, Float32),
        DataType::Float64 => compare_scalar_typed!(op, args, Float64),
        DataType::Utf8 => compare_scalar_typed!(op, args, Utf8),
        DataType::LargeUtf8 => compare_scalar_typed!(op, args, LargeUtf8),
        _ => panic!("Unsupported data type for comparison: {:?}", data_type),
    }
}

/// Evaluate a greatest or least function
fn compare(op: ComparisonOperator, args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "{:?} expressions require at least one argument",
            op
        )));
    } else if args.len() == 1 {
        return Ok(args[0].clone());
    }

    let args_types = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.data_type().clone(),
            ColumnarValue::Scalar(scalar) => scalar.get_datatype(),
        })
        .collect::<Vec<_>>();

    if args_types.iter().any(|t| t != &args_types[0]) {
        return Err(DataFusionError::Internal(format!(
            "{:?} expressions require all arguments to be of the same type",
            op
        )));
    }

    let mut arg_lengths = args
        .iter()
        .filter_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .collect::<Vec<_>>();
    arg_lengths.dedup();

    if arg_lengths.len() > 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} expressions require all arguments to be of the same length",
            op
        )));
    }

    // scalars have no lengths, so if there are no lengths, all arguments are scalars
    let all_scalars = arg_lengths.is_empty();

    if all_scalars {
        let args: Vec<_> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(_) => {
                    panic!("Internal error: all arguments should be scalars")
                }
                ColumnarValue::Scalar(scalar) => scalar.clone(),
            })
            .collect();
        Ok(ColumnarValue::Scalar(compare_scalars(
            args_types[0].clone(),
            op,
            &args,
        )))
    } else {
        let cmp = match op {
            ComparisonOperator::Greatest => gt_dyn,
            ComparisonOperator::Least => lt_dyn,
        };
        let length = arg_lengths[0];
        let first_arg = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(length),
        };
        args[1..]
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => array.clone(),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(length),
            })
            // we cannot use try_reduce as it is still nightly
            .try_fold(first_arg, |a, b| {
                // mask will be true if cmp holds for a to be otherwise false
                let mask = cmp(&a, &b)?;
                // then the zip can pluck values accordingly from a and b
                let value = zip(&mask, &a, &b)?;
                Ok(value)
            })
            .map(ColumnarValue::Array)
    }
}

pub fn greatest(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    compare(ComparisonOperator::Greatest, args)
}

pub fn least(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    compare(ComparisonOperator::Least, args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_compare_scalars() {
        let args = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int32(Some(3)),
            ScalarValue::Int32(Some(2)),
        ];
        let result =
            compare_scalars(DataType::Int32, ComparisonOperator::Greatest, &args);
        assert_eq!(result, ScalarValue::Int32(Some(3)));
    }

    #[test]
    #[should_panic]
    fn test_compare_scalars_unsupported_types() {
        let args = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Utf8(Some("foo".to_string())),
        ];
        let _ = compare_scalars(DataType::Int32, ComparisonOperator::Greatest, &args);
    }

    #[test]
    fn test_compare_i32_arrays() {
        let args = vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(Int32Array::from(vec![Some(3), Some(2), Some(1)])),
            Arc::new(Int32Array::from(vec![Some(2), Some(3), Some(1)])),
        ];
        let args = args
            .iter()
            .map(|array| ColumnarValue::Array(array.clone()))
            .collect::<Vec<_>>();
        // compare to greatest
        let result = compare(ComparisonOperator::Greatest, &args).unwrap();
        let array_value = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Internal error: expected array"),
        };
        let primitive_array = array_value.as_any().downcast_ref::<Int32Array>().unwrap();
        let value_vec = primitive_array.values().to_vec();
        assert_eq!(value_vec, vec![3, 3, 3]);
        // compare to least
        let result = compare(ComparisonOperator::Least, &args).unwrap();
        let array_value = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Internal error: expected array"),
        };
        let primitive_array = array_value.as_any().downcast_ref::<Int32Array>().unwrap();
        let value_vec = primitive_array.values().to_vec();
        assert_eq!(value_vec, vec![1, 2, 1]);
    }

    #[test]
    fn test_compare_i32_array_scalar() {
        let args = vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(2),
                Some(3),
                Some(1),
            ]))),
        ];
        // compare to greatest
        let result = compare(ComparisonOperator::Greatest, &args).unwrap();
        let array_value = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Internal error: expected array"),
        };
        let primitive_array = array_value.as_any().downcast_ref::<Int32Array>().unwrap();
        let value_vec = primitive_array.values().to_vec();
        assert_eq!(value_vec, vec![3, 3, 3]);
        // compare to least
        let result = compare(ComparisonOperator::Least, &args).unwrap();
        let array_value = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Internal error: expected array"),
        };
        let primitive_array = array_value.as_any().downcast_ref::<Int32Array>().unwrap();
        let value_vec = primitive_array.values().to_vec();
        assert_eq!(value_vec, vec![1, 2, 1]);
    }
}
