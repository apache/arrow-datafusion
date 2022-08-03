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

//! This module contains computation kernels that are eventually
//! destined for arrow-rs but are in datafusion until they are ported.

use arrow::error::ArrowError;
use arrow::{array::*, datatypes::ArrowNumericType};
use datafusion_common::{DataFusionError, Result};

// Simple (low performance) kernels until optimized kernels are added to arrow
// See https://github.com/apache/arrow-rs/issues/960

pub(crate) fn is_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    // Different from `neq_bool` because `null is distinct from null` is false and not null
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left != right))
        .collect())
}

pub(crate) fn is_not_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left == right))
        .collect())
}

pub(crate) fn is_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

pub(crate) fn is_distinct_from_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

pub(crate) fn is_distinct_from_null(
    left: &NullArray,
    _right: &NullArray,
) -> Result<BooleanArray> {
    let length = left.len();
    make_boolean_array(length, false)
}

pub(crate) fn is_not_distinct_from_null(
    left: &NullArray,
    _right: &NullArray,
) -> Result<BooleanArray> {
    let length = left.len();
    make_boolean_array(length, true)
}

fn make_boolean_array(length: usize, value: bool) -> Result<BooleanArray> {
    Ok((0..length).into_iter().map(|_| Some(value)).collect())
}

pub(crate) fn is_not_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

pub(crate) fn is_not_distinct_from_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

// TODO move decimal kernels to to arrow-rs
// https://github.com/apache/arrow-rs/issues/1200

/// Creates an BooleanArray the same size as `left`,
/// applying `op` to all non-null elements of left
pub(crate) fn compare_decimal_scalar<F>(
    left: &Decimal128Array,
    right: i128,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(i128, i128) -> bool,
{
    Ok(left
        .iter()
        .map(|left| left.map(|left| op(left, right)))
        .collect())
}

/// Creates an BooleanArray the same size as `left`,
/// by applying `op` to all non-null elements of left and right
pub(crate) fn compare_decimal<F>(
    left: &Decimal128Array,
    right: &Decimal128Array,
    op: F,
) -> Result<BooleanArray>
where
    F: Fn(i128, i128) -> bool,
{
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| {
            if let (Some(left), Some(right)) = (left, right) {
                Some(op(left, right))
            } else {
                None
            }
        })
        .collect())
}

pub(crate) fn eq_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<BooleanArray> {
    compare_decimal_scalar(left, right, |left, right| left == right)
}

pub(crate) fn eq_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    compare_decimal(left, right, |left, right| left == right)
}

pub(crate) fn neq_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<BooleanArray> {
    compare_decimal_scalar(left, right, |left, right| left != right)
}

pub(crate) fn neq_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    compare_decimal(left, right, |left, right| left != right)
}

pub(crate) fn lt_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<BooleanArray> {
    compare_decimal_scalar(left, right, |left, right| left < right)
}

pub(crate) fn lt_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    compare_decimal(left, right, |left, right| left < right)
}

pub(crate) fn lt_eq_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<BooleanArray> {
    compare_decimal_scalar(left, right, |left, right| left <= right)
}

pub(crate) fn lt_eq_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    compare_decimal(left, right, |left, right| left <= right)
}

pub(crate) fn gt_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<BooleanArray> {
    compare_decimal_scalar(left, right, |left, right| left > right)
}

pub(crate) fn gt_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    compare_decimal(left, right, |left, right| left > right)
}

pub(crate) fn gt_eq_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<BooleanArray> {
    compare_decimal_scalar(left, right, |left, right| left >= right)
}

pub(crate) fn gt_eq_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    compare_decimal(left, right, |left, right| left >= right)
}

pub(crate) fn is_distinct_from_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| match (left, right) {
            (None, None) => Some(false),
            (None, Some(_)) | (Some(_), None) => Some(true),
            (Some(left), Some(right)) => Some(left != right),
        })
        .collect())
}

pub(crate) fn is_not_distinct_from_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| match (left, right) {
            (None, None) => Some(true),
            (None, Some(_)) | (Some(_), None) => Some(false),
            (Some(left), Some(right)) => Some(left == right),
        })
        .collect())
}

/// Creates an Decimal128Array the same size as `left`,
/// by applying `op` to all non-null elements of left and right
pub(crate) fn arith_decimal<F>(
    left: &Decimal128Array,
    right: &Decimal128Array,
    op: F,
) -> Result<Decimal128Array>
where
    F: Fn(i128, i128) -> Result<i128>,
{
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            if let (Some(left), Some(right)) = (left, right) {
                Some(op(left, right)).transpose()
            } else {
                Ok(None)
            }
        })
        .collect()
}

pub(crate) fn arith_decimal_scalar<F>(
    left: &Decimal128Array,
    right: i128,
    op: F,
) -> Result<Decimal128Array>
where
    F: Fn(i128, i128) -> Result<i128>,
{
    left.iter()
        .map(|left| {
            if let Some(left) = left {
                Some(op(left, right)).transpose()
            } else {
                Ok(None)
            }
        })
        .collect()
}

pub(crate) fn add_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<Decimal128Array> {
    let array = arith_decimal(left, right, |left, right| Ok(left + right))?
        .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn add_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<Decimal128Array> {
    let array = arith_decimal_scalar(left, right, |left, right| Ok(left + right))?
        .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn subtract_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<Decimal128Array> {
    let array = arith_decimal(left, right, |left, right| Ok(left - right))?
        .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn subtract_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<Decimal128Array> {
    let array = arith_decimal_scalar(left, right, |left, right| Ok(left - right))?
        .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn multiply_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<Decimal128Array> {
    let divide = 10_i128.pow(left.scale() as u32);
    let array = arith_decimal(left, right, |left, right| Ok(left * right / divide))?
        .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn multiply_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<Decimal128Array> {
    let divide = 10_i128.pow(left.scale() as u32);
    let array =
        arith_decimal_scalar(left, right, |left, right| Ok(left * right / divide))?
            .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn divide_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<Decimal128Array> {
    let mul = 10_f64.powi(left.scale() as i32);
    let array = arith_decimal(left, right, |left, right| {
        let l_value = left as f64;
        let r_value = right as f64;
        let result = ((l_value / r_value) * mul) as i128;
        Ok(result)
    })?
    .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn divide_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<Decimal128Array> {
    let mul = 10_f64.powi(left.scale() as i32);
    let array = arith_decimal_scalar(left, right, |left, right| {
        let l_value = left as f64;
        let r_value = right as f64;
        let result = ((l_value / r_value) * mul) as i128;
        Ok(result)
    })?
    .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn modulus_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<Decimal128Array> {
    let array = arith_decimal(left, right, |left, right| {
        if right == 0 {
            Err(DataFusionError::ArrowError(ArrowError::DivideByZero))
        } else {
            Ok(left % right)
        }
    })?
    .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

pub(crate) fn modulus_decimal_scalar(
    left: &Decimal128Array,
    right: i128,
) -> Result<Decimal128Array> {
    if right == 0 {
        return Err(DataFusionError::ArrowError(ArrowError::DivideByZero));
    }
    let array = arith_decimal_scalar(left, right, |left, right| Ok(left % right))?
        .with_precision_and_scale(left.precision(), left.scale())?;
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_decimal_array(
        array: &[Option<i128>],
        precision: usize,
        scale: usize,
    ) -> Decimal128Array {
        let mut decimal_builder = Decimal128Builder::new(array.len(), precision, scale);
        for value in array {
            match value {
                None => {
                    decimal_builder.append_null();
                }
                Some(v) => {
                    decimal_builder.append_value(*v).expect("valid value");
                }
            }
        }
        decimal_builder.finish()
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
        );
        // eq: array = i128
        let result = eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false)]),
            result
        );
        // neq: array != i128
        let result = neq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true)]),
            result
        );
        // lt: array < i128
        let result = lt_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]),
            result
        );
        // lt_eq: array <= i128
        let result = lt_eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]),
            result
        );
        // gt: array > i128
        let result = gt_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)]),
            result
        );
        // gt_eq: array >= i128
        let result = gt_eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
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
        );
        // eq: left == right
        let result = eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)]),
            result
        );
        // neq: left != right
        let result = neq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]),
            result
        );
        // lt: left < right
        let result = lt_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]),
            result
        );
        // lt_eq: left <= right
        let result = lt_eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true)]),
            result
        );
        // gt: left > right
        let result = gt_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false)]),
            result
        );
        // gt_eq: left >= right
        let result = gt_eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
            result
        );
        // is_distinct: left distinct right
        let result = is_distinct_from_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false)]),
            result
        );
        // is_distinct: left distinct right
        let result =
            is_not_distinct_from_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true)]),
            result
        );
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
        );
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
            ],
            25,
            3,
        );
        // add
        let result = add_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect =
            create_decimal_array(&[Some(246), None, Some(245), Some(247)], 25, 3);
        assert_eq!(expect, result);
        let result = add_decimal_scalar(&left_decimal_array, 10)?;
        let expect =
            create_decimal_array(&[Some(133), None, Some(132), Some(134)], 25, 3);
        assert_eq!(expect, result);
        // subtract
        let result = subtract_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(0), None, Some(-1), Some(1)], 25, 3);
        assert_eq!(expect, result);
        let result = subtract_decimal_scalar(&left_decimal_array, 10)?;
        let expect =
            create_decimal_array(&[Some(113), None, Some(112), Some(114)], 25, 3);
        assert_eq!(expect, result);
        // multiply
        let result = multiply_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(15), None, Some(15), Some(15)], 25, 3);
        assert_eq!(expect, result);
        let result = multiply_decimal_scalar(&left_decimal_array, 10)?;
        let expect = create_decimal_array(&[Some(1), None, Some(1), Some(1)], 25, 3);
        assert_eq!(expect, result);
        // divide
        let left_decimal_array = create_decimal_array(
            &[Some(1234567), None, Some(1234567), Some(1234567)],
            25,
            3,
        );
        let right_decimal_array =
            create_decimal_array(&[Some(10), Some(100), Some(55), Some(-123)], 25, 3);
        let result = divide_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(
            &[Some(123456700), None, Some(22446672), Some(-10037130)],
            25,
            3,
        );
        assert_eq!(expect, result);
        let result = divide_decimal_scalar(&left_decimal_array, 10)?;
        let expect = create_decimal_array(
            &[Some(123456700), None, Some(123456700), Some(123456700)],
            25,
            3,
        );
        assert_eq!(expect, result);
        // modulus
        let result = modulus_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(7), None, Some(37), Some(16)], 25, 3);
        assert_eq!(expect, result);
        let result = modulus_decimal_scalar(&left_decimal_array, 10)?;
        let expect = create_decimal_array(&[Some(7), None, Some(7), Some(7)], 25, 3);
        assert_eq!(expect, result);

        Ok(())
    }
}
