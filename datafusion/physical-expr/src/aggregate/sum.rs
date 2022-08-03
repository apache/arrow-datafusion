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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::{AggregateExpr, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{
        ArrayRef, BasicDecimalArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateState};

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::array::Decimal128Array;
use arrow::compute::cast;
use datafusion_row::accessor::RowAccessor;

/// SUM aggregate expression
#[derive(Debug)]
pub struct Sum {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl Sum {
    /// Create a new SUM aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Sum {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "sum"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        matches!(
            self.data_type,
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        )
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(SumRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }
}

#[derive(Debug)]
struct SumAccumulator {
    sum: ScalarValue,
}

impl SumAccumulator {
    /// new sum accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(data_type)?,
        })
    }
}

// returns the new value after sum with the new values, taking nullability into account
macro_rules! typed_sum_delta_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let delta = compute::sum(array);
        ScalarValue::$SCALAR(delta)
    }};
}

// TODO implement this in arrow-rs with simd
// https://github.com/apache/arrow-rs/issues/1010
fn sum_decimal_batch(
    values: &ArrayRef,
    precision: &usize,
    scale: &usize,
) -> Result<ScalarValue> {
    let array = values.as_any().downcast_ref::<Decimal128Array>().unwrap();

    if array.null_count() == array.len() {
        return Ok(ScalarValue::Decimal128(None, *precision, *scale));
    }

    let mut result = 0_i128;
    for i in 0..array.len() {
        if array.is_valid(i) {
            result += array.value(i).as_i128();
        }
    }
    Ok(ScalarValue::Decimal128(Some(result), *precision, *scale))
}

// sums the array and returns a ScalarValue of its corresponding type.
pub(crate) fn sum_batch(values: &ArrayRef, sum_type: &DataType) -> Result<ScalarValue> {
    let values = &cast(values, sum_type)?;
    Ok(match values.data_type() {
        DataType::Decimal(precision, scale) => {
            sum_decimal_batch(values, precision, scale)?
        }
        DataType::Float64 => typed_sum_delta_batch!(values, Float64Array, Float64),
        DataType::Float32 => typed_sum_delta_batch!(values, Float32Array, Float32),
        DataType::Int64 => typed_sum_delta_batch!(values, Int64Array, Int64),
        DataType::Int32 => typed_sum_delta_batch!(values, Int32Array, Int32),
        DataType::Int16 => typed_sum_delta_batch!(values, Int16Array, Int16),
        DataType::Int8 => typed_sum_delta_batch!(values, Int8Array, Int8),
        DataType::UInt64 => typed_sum_delta_batch!(values, UInt64Array, UInt64),
        DataType::UInt32 => typed_sum_delta_batch!(values, UInt32Array, UInt32),
        DataType::UInt16 => typed_sum_delta_batch!(values, UInt16Array, UInt16),
        DataType::UInt8 => typed_sum_delta_batch!(values, UInt8Array, UInt8),
        e => {
            return Err(DataFusionError::Internal(format!(
                "Sum is not expected to receive the type {:?}",
                e
            )));
        }
    })
}

// returns the sum of two scalar values, including coercion into $TYPE.
macro_rules! typed_sum {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        ScalarValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some(a + (*b as $TYPE)),
        })
    }};
}

macro_rules! sum_row {
    ($INDEX:ident, $ACC:ident, $DELTA:expr, $TYPE:ident) => {{
        paste::item! {
            match $DELTA {
                None => {}
                Some(v) => $ACC.[<add_ $TYPE>]($INDEX, *v as $TYPE)
            }
        }
    }};
}

// TODO implement this in arrow-rs with simd
// https://github.com/apache/arrow-rs/issues/1010
fn sum_decimal(
    lhs: &Option<i128>,
    rhs: &Option<i128>,
    precision: &usize,
    scale: &usize,
) -> ScalarValue {
    match (lhs, rhs) {
        (None, None) => ScalarValue::Decimal128(None, *precision, *scale),
        (None, rhs) => ScalarValue::Decimal128(*rhs, *precision, *scale),
        (lhs, None) => ScalarValue::Decimal128(*lhs, *precision, *scale),
        (Some(lhs_value), Some(rhs_value)) => {
            ScalarValue::Decimal128(Some(lhs_value + rhs_value), *precision, *scale)
        }
    }
}

fn sum_decimal_with_diff_scale(
    lhs: &Option<i128>,
    rhs: &Option<i128>,
    precision: &usize,
    lhs_scale: &usize,
    rhs_scale: &usize,
) -> ScalarValue {
    // the lhs_scale must be greater or equal rhs_scale.
    match (lhs, rhs) {
        (None, None) => ScalarValue::Decimal128(None, *precision, *lhs_scale),
        (None, Some(rhs_value)) => {
            let new_value = rhs_value * 10_i128.pow((lhs_scale - rhs_scale) as u32);
            ScalarValue::Decimal128(Some(new_value), *precision, *lhs_scale)
        }
        (lhs, None) => ScalarValue::Decimal128(*lhs, *precision, *lhs_scale),
        (Some(lhs_value), Some(rhs_value)) => {
            let new_value =
                rhs_value * 10_i128.pow((lhs_scale - rhs_scale) as u32) + lhs_value;
            ScalarValue::Decimal128(Some(new_value), *precision, *lhs_scale)
        }
    }
}

pub(crate) fn sum(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    Ok(match (lhs, rhs) {
        (ScalarValue::Decimal128(v1, p1, s1), ScalarValue::Decimal128(v2, p2, s2)) => {
            let max_precision = p1.max(p2);
            if s1.eq(s2) {
                // s1 = s2
                sum_decimal(v1, v2, max_precision, s1)
            } else if s1.gt(s2) {
                // s1 > s2
                sum_decimal_with_diff_scale(v1, v2, max_precision, s1, s2)
            } else {
                // s1 < s2
                sum_decimal_with_diff_scale(v2, v1, max_precision, s2, s1)
            }
        }
        // float64 coerces everything to f64
        (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Float32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int16(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int8(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt16(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::UInt8(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        // float32 has no cast
        (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
            typed_sum!(lhs, rhs, Float32, f32)
        }
        // u64 coerces u* to u64
        (ScalarValue::UInt64(lhs), ScalarValue::UInt64(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt32(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt16(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt8(rhs)) => {
            typed_sum!(lhs, rhs, UInt64, u64)
        }
        // i64 coerces i* to i64
        (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int16(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int8(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::UInt32(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::UInt16(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::UInt8(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        e => {
            return Err(DataFusionError::Internal(format!(
                "Sum is not expected to receive a scalar {:?}",
                e
            )));
        }
    })
}

pub(crate) fn add_to_row(
    dt: &DataType,
    index: usize,
    accessor: &mut RowAccessor,
    s: &ScalarValue,
) -> Result<()> {
    match (dt, s) {
        // float64 coerces everything to f64
        (DataType::Float64, ScalarValue::Float64(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::Float32(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::Int64(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::Int32(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::Int16(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::Int8(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::UInt64(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::UInt32(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::UInt16(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        (DataType::Float64, ScalarValue::UInt8(rhs)) => {
            sum_row!(index, accessor, rhs, f64)
        }
        // float32 has no cast
        (DataType::Float32, ScalarValue::Float32(rhs)) => {
            sum_row!(index, accessor, rhs, f32)
        }
        // u64 coerces u* to u64
        (DataType::UInt64, ScalarValue::UInt64(rhs)) => {
            sum_row!(index, accessor, rhs, u64)
        }
        (DataType::UInt64, ScalarValue::UInt32(rhs)) => {
            sum_row!(index, accessor, rhs, u64)
        }
        (DataType::UInt64, ScalarValue::UInt16(rhs)) => {
            sum_row!(index, accessor, rhs, u64)
        }
        (DataType::UInt64, ScalarValue::UInt8(rhs)) => {
            sum_row!(index, accessor, rhs, u64)
        }
        // i64 coerces i* to i64
        (DataType::Int64, ScalarValue::Int64(rhs)) => {
            sum_row!(index, accessor, rhs, i64)
        }
        (DataType::Int64, ScalarValue::Int32(rhs)) => {
            sum_row!(index, accessor, rhs, i64)
        }
        (DataType::Int64, ScalarValue::Int16(rhs)) => {
            sum_row!(index, accessor, rhs, i64)
        }
        (DataType::Int64, ScalarValue::Int8(rhs)) => {
            sum_row!(index, accessor, rhs, i64)
        }
        e => {
            return Err(DataFusionError::Internal(format!(
                "Row sum updater is not expected to receive a scalar {:?}",
                e
            )));
        }
    }
    Ok(())
}

impl Accumulator for SumAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(self.sum.clone())])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.sum = sum(&self.sum, &sum_batch(values, &self.sum.get_datatype())?)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // sum(sum1, sum2, sum3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // TODO: add the checker for overflow
        // For the decimal(precision,_) data type, the absolute of value must be less than 10^precision.
        Ok(self.sum.clone())
    }
}

#[derive(Debug)]
struct SumRowAccumulator {
    index: usize,
    datatype: DataType,
}

impl SumRowAccumulator {
    pub fn new(index: usize, datatype: DataType) -> Self {
        Self { index, datatype }
    }
}

impl RowAccumulator for SumRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        add_to_row(
            &self.datatype,
            self.index,
            accessor,
            &sum_batch(values, &self.datatype)?,
        )?;
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.datatype, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;

    #[test]
    fn sum_decimal() -> Result<()> {
        // test sum
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        let result = sum(&left, &right)?;
        assert_eq!(ScalarValue::Decimal128(Some(123 + 124), 10, 2), result);
        // test sum decimal with diff scale
        let left = ScalarValue::Decimal128(Some(123), 10, 3);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        let result = sum(&left, &right)?;
        assert_eq!(
            ScalarValue::Decimal128(Some(123 + 124 * 10_i128.pow(1)), 10, 3),
            result
        );
        // diff precision and scale for decimal data type
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 11, 3);
        let result = sum(&left, &right);
        assert_eq!(
            ScalarValue::Decimal128(Some(123 * 10_i128.pow(3 - 2) + 124), 11, 3),
            result.unwrap()
        );

        // test sum batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal(10, 0))?;
        assert_eq!(ScalarValue::Decimal128(Some(15), 10, 0), result);

        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Sum,
            ScalarValue::Decimal128(Some(15), 20, 0),
            DataType::Decimal(20, 0)
        )
    }

    #[test]
    fn sum_decimal_with_nulls() -> Result<()> {
        // test sum
        let left = ScalarValue::Decimal128(None, 10, 2);
        let right = ScalarValue::Decimal128(Some(123), 10, 2);
        let result = sum(&left, &right)?;
        assert_eq!(ScalarValue::Decimal128(Some(123), 10, 2), result);

        // test with batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal(10, 0))?;
        assert_eq!(ScalarValue::Decimal128(Some(13), 10, 0), result);

        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(35, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(35, 0),
            Sum,
            ScalarValue::Decimal128(Some(13), 38, 0),
            DataType::Decimal(38, 0)
        )
    }

    #[test]
    fn sum_decimal_all_nulls() -> Result<()> {
        // test sum
        let left = ScalarValue::Decimal128(None, 10, 2);
        let right = ScalarValue::Decimal128(None, 10, 2);
        let result = sum(&left, &right)?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 2), result);

        // test with batch
        let array: ArrayRef = Arc::new(
            std::iter::repeat(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal(10, 0))?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // test agg
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Sum,
            ScalarValue::Decimal128(None, 20, 0),
            DataType::Decimal(20, 0)
        )
    }

    #[test]
    fn sum_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Sum,
            ScalarValue::from(15i64),
            DataType::Int64
        )
    }

    #[test]
    fn sum_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Sum,
            ScalarValue::from(13i64),
            DataType::Int64
        )
    }

    #[test]
    fn sum_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Sum,
            ScalarValue::Int64(None),
            DataType::Int64
        )
    }

    #[test]
    fn sum_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Sum,
            ScalarValue::from(15u64),
            DataType::UInt64
        )
    }

    #[test]
    fn sum_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Sum,
            ScalarValue::from(15_f32),
            DataType::Float32
        )
    }

    #[test]
    fn sum_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Sum,
            ScalarValue::from(15_f64),
            DataType::Float64
        )
    }
}
