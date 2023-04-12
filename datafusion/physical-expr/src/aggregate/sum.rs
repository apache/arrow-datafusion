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
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;

use crate::aggregate::row_accumulator::{
    is_row_accumulator_support_dtype, RowAccumulator,
};
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::array::Decimal128Array;
use arrow::compute::cast;
use datafusion_row::accessor::RowAccessor;

/// SUM aggregate expression
#[derive(Debug, Clone)]
pub struct Sum {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
    pub pre_cast_to_sum_type: bool,
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
            pre_cast_to_sum_type: false,
        }
    }

    pub fn new_with_pre_cast(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        pre_cast_to_sum_type: bool,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
            pre_cast_to_sum_type,
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
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "sum"),
                self.data_type.clone(),
                self.nullable,
            ),
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::UInt64,
                self.nullable,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        is_row_accumulator_support_dtype(&self.data_type)
    }

    fn supports_bounded_execution(&self) -> bool {
        true
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

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator::try_new(&self.data_type)?))
    }
}

#[derive(Debug)]
struct SumAccumulator {
    sum: ScalarValue,
    count: u64,
}

impl SumAccumulator {
    /// new sum accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(data_type)?,
            count: 0,
        })
    }
}

// returns the new value after sum with the new values, taking nullability into account
macro_rules! typed_sum_delta_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let delta = compute::sum(array);
        ScalarValue::$SCALAR(delta)
    }};
}

fn sum_decimal_batch(values: &ArrayRef, precision: u8, scale: i8) -> Result<ScalarValue> {
    let array = downcast_value!(values, Decimal128Array);
    let result = compute::sum(array);
    Ok(ScalarValue::Decimal128(result, precision, scale))
}

// sums the array and returns a ScalarValue of its corresponding type.
pub(crate) fn sum_batch(values: &ArrayRef, sum_type: &DataType) -> Result<ScalarValue> {
    // TODO refine the cast kernel in arrow-rs
    let cast_values = if values.data_type() != sum_type {
        Some(cast(values, sum_type)?)
    } else {
        None
    };
    let values = cast_values.as_ref().unwrap_or(values);
    Ok(match values.data_type() {
        DataType::Decimal128(precision, scale) => {
            sum_decimal_batch(values, *precision, *scale)?
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
                "Sum is not expected to receive the type {e:?}"
            )));
        }
    })
}

macro_rules! sum_row {
    ($INDEX:ident, $ACC:ident, $DELTA:expr, $TYPE:ident) => {{
        paste::item! {
            if let Some(v) = $DELTA {
                $ACC.[<add_ $TYPE>]($INDEX, *v)
            }
        }
    }};
}

pub(crate) fn add_to_row(
    index: usize,
    accessor: &mut RowAccessor,
    s: &ScalarValue,
) -> Result<()> {
    match s {
        ScalarValue::Float64(rhs) => {
            sum_row!(index, accessor, rhs, f64)
        }
        ScalarValue::Float32(rhs) => {
            sum_row!(index, accessor, rhs, f32)
        }
        ScalarValue::UInt64(rhs) => {
            sum_row!(index, accessor, rhs, u64)
        }
        ScalarValue::Int64(rhs) => {
            sum_row!(index, accessor, rhs, i64)
        }
        _ => {
            let msg =
                format!("Row sum updater is not expected to receive a scalar {s:?}");
            return Err(DataFusionError::Internal(msg));
        }
    }
    Ok(())
}

impl Accumulator for SumAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.sum.clone(), ScalarValue::from(self.count)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count += (values.len() - values.null_count()) as u64;
        let delta = sum_batch(values, &self.sum.get_datatype())?;
        self.sum = self.sum.add(&delta)?;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count -= (values.len() - values.null_count()) as u64;
        let delta = sum_batch(values, &self.sum.get_datatype())?;
        self.sum = self.sum.sub(&delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // sum(sum1, sum2, sum3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // TODO: add the checker for overflow
        // For the decimal(precision,_) data type, the absolute of value must be less than 10^precision.
        if self.count == 0 {
            ScalarValue::try_from(&self.sum.get_datatype())
        } else {
            Ok(self.sum.clone())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
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
        let delta = sum_batch(values, &self.datatype)?;
        add_to_row(self.index, accessor, &delta)?;
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
        // test sum batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal128(10, 0))?;
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
            DataType::Decimal128(10, 0),
            Sum,
            ScalarValue::Decimal128(Some(15), 20, 0)
        )
    }

    #[test]
    fn sum_decimal_with_nulls() -> Result<()> {
        // test with batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal128(10, 0))?;
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
            DataType::Decimal128(35, 0),
            Sum,
            ScalarValue::Decimal128(Some(13), 38, 0)
        )
    }

    #[test]
    fn sum_decimal_all_nulls() -> Result<()> {
        // test with batch
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = sum_batch(&array, &DataType::Decimal128(10, 0))?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // test agg
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Sum,
            ScalarValue::Decimal128(None, 20, 0)
        )
    }

    #[test]
    fn sum_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Sum, ScalarValue::from(15i32))
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
        generic_test_op!(a, DataType::Int32, Sum, ScalarValue::from(13i32))
    }

    #[test]
    fn sum_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, Sum, ScalarValue::Int32(None))
    }

    #[test]
    fn sum_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(a, DataType::UInt32, Sum, ScalarValue::from(15u32))
    }

    #[test]
    fn sum_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(a, DataType::Float32, Sum, ScalarValue::from(15_f32))
    }

    #[test]
    fn sum_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(a, DataType::Float64, Sum, ScalarValue::from(15_f64))
    }
}
