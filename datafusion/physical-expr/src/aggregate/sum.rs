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
use datafusion_expr::Accumulator;

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::expressions::format_state_name;
use arrow::array::DecimalArray;
use arrow::array::{Array, Float16Array};
use arrow::compute::cast;
use datafusion_row::accessor::RowAccessor;
use std::any::type_name;

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
    let array = values.as_any().downcast_ref::<DecimalArray>().unwrap();
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

macro_rules! downcast_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to {}",
                $NAME,
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! union_arrays {
    ($LHS: expr, $RHS: expr, $DTYPE: expr, $ARR_DTYPE: ident, $NAME: expr) => {{
        let lhs_casted = &cast(&$LHS.to_array(), $DTYPE)?;
        let rhs_casted = &cast(&$RHS.to_array(), $DTYPE)?;
        let lhs_prim_array = downcast_arg!(lhs_casted, $NAME, $ARR_DTYPE);
        let rhs_prim_array = downcast_arg!(rhs_casted, $NAME, $ARR_DTYPE);

        let chained = lhs_prim_array
            .iter()
            .chain(rhs_prim_array.iter())
            .collect::<$ARR_DTYPE>();

        Arc::new(chained)
    }};
}

pub(crate) fn sum(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    let result = match (lhs.get_datatype(), rhs.get_datatype()) {
        (DataType::Decimal(p1, s1), DataType::Decimal(p2, s2)) => {
            let max_precision = p1.max(p2);

            match (lhs, rhs) {
                (
                    ScalarValue::Decimal128(v1, _, _),
                    ScalarValue::Decimal128(v2, _, _),
                ) => {
                    Ok(if s1.eq(&s2) {
                        // s1 = s2
                        sum_decimal(v1, v2, &max_precision, &s1)
                    } else if s1.gt(&s2) {
                        // s1 > s2
                        sum_decimal_with_diff_scale(v1, v2, &max_precision, &s1, &s2)
                    } else {
                        // s1 < s2
                        sum_decimal_with_diff_scale(v2, v1, &max_precision, &s2, &s1)
                    })
                }
                _ => Err(DataFusionError::Internal(
                    "Internal state error on sum decimals ".to_string(),
                )),
            }
        }
        (DataType::Float64, _) | (_, DataType::Float64) => {
            let data: ArrayRef =
                union_arrays!(lhs, rhs, &DataType::Float64, Float64Array, "f64");
            sum_batch(&data, &arrow::datatypes::DataType::Float64)
        }
        (DataType::Float32, _) | (_, DataType::Float32) => {
            let data: ArrayRef =
                union_arrays!(lhs, rhs, &DataType::Float32, Float32Array, "f32");
            sum_batch(&data, &arrow::datatypes::DataType::Float32)
        }
        (DataType::Float16, _) | (_, DataType::Float16) => {
            let data: ArrayRef =
                union_arrays!(lhs, rhs, &DataType::Float16, Float16Array, "f16");
            sum_batch(&data, &arrow::datatypes::DataType::Float16)
        }
        _ => {
            let data: ArrayRef =
                union_arrays!(lhs, rhs, &DataType::Int64, Int64Array, "i64");
            sum_batch(&data, &arrow::datatypes::DataType::Int64)
        }
    }?;

    Ok(result)
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
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.sum.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

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
                .collect::<DecimalArray>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal(10, 0))?;
        assert_eq!(ScalarValue::Decimal128(Some(15), 10, 0), result);

        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<DecimalArray>()
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
                .collect::<DecimalArray>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal(10, 0))?;
        assert_eq!(ScalarValue::Decimal128(Some(13), 10, 0), result);

        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<DecimalArray>()
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
                .collect::<DecimalArray>()
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
