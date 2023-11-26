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

//! Defines `SUM` and `SUM DISTINCT` aggregate accumulators

use std::any::Any;
use std::sync::Arc;

use super::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, GroupsAccumulator, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::compute::sum;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Decimal256Type, Float64Type, Int64Type, UInt64Type,
};
use arrow_array::{Array, ArrowNativeTypeOp, ArrowNumericType};
use arrow_buffer::ArrowNativeType;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{
    internal_err, not_impl_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::type_coercion::aggregates::sum_return_type;
use datafusion_expr::Accumulator;

/// SUM aggregate expression
#[derive(Debug, Clone)]
pub struct Sum {
    name: String,
    // The DataType for the input expression
    data_type: DataType,
    // The DataType for the final sum
    return_type: DataType,
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
        let return_type = sum_return_type(&data_type).unwrap();
        Self {
            name: name.into(),
            data_type,
            return_type,
            expr,
            nullable: true,
        }
    }
}

/// Sum only supports a subset of numeric types, instead relying on type coercion
///
/// This macro is similar to [downcast_primitive](arrow_array::downcast_primitive)
///
/// `s` is a `Sum`, `helper` is a macro accepting (ArrowPrimitiveType, DataType)
macro_rules! downcast_sum {
    ($s:ident, $helper:ident) => {
        match $s.return_type {
            DataType::UInt64 => $helper!(UInt64Type, $s.return_type),
            DataType::Int64 => $helper!(Int64Type, $s.return_type),
            DataType::Float64 => $helper!(Float64Type, $s.return_type),
            DataType::Decimal128(_, _) => $helper!(Decimal128Type, $s.return_type),
            DataType::Decimal256(_, _) => $helper!(Decimal256Type, $s.return_type),
            _ => not_impl_err!("Sum not supported for {}: {}", $s.name, $s.return_type),
        }
    };
}
pub(crate) use downcast_sum;

impl AggregateExpr for Sum {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.return_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        if let DataType::List(field) = &self.data_type {
            match field.data_type() {
                DataType::Int64 => {
                    return Ok(Box::new(ArraySumAccumulator::<Int64Type>::new(
                        self.return_type.clone(),
                    )))
                }
                DataType::UInt64 => {
                    return Ok(Box::new(ArraySumAccumulator::<UInt64Type>::new(
                        self.return_type.clone(),
                    )))
                }
                DataType::Float64 => {
                    return Ok(Box::new(ArraySumAccumulator::<Float64Type>::new(
                        self.return_type.clone(),
                    )))
                }
                DataType::Decimal128(_, _) => {
                    return Ok(Box::new(ArraySumAccumulator::<Decimal128Type>::new(
                        self.return_type.clone(),
                    )))
                }
                DataType::Decimal256(_, _) => {
                    return Ok(Box::new(ArraySumAccumulator::<Decimal256Type>::new(
                        self.return_type.clone(),
                    )))
                }
                _ => {
                    return internal_err!(
                        "Sum not supported for {}: {}",
                        self.name,
                        self.data_type
                    )
                }
            }
        }

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(SumAccumulator::<$t>::new($dt.clone())))
            };
        }
        downcast_sum!(self, helper)
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "sum"),
            self.return_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PrimitiveGroupsAccumulator::<$t, _>::new(
                    &$dt,
                    |x, y| *x = x.add_wrapping(y),
                )))
            };
        }
        downcast_sum!(self, helper)
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(SlidingSumAccumulator::<$t>::new($dt.clone())))
            };
        }
        downcast_sum!(self, helper)
    }
}

impl PartialEq<dyn Any> for Sum {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

/// This accumulator computes SUM incrementally
struct SumAccumulator<T: ArrowNumericType> {
    sum: Option<T::Native>,
    data_type: DataType,
}

impl<T: ArrowNumericType> std::fmt::Debug for SumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> SumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            sum: None,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for SumAccumulator<T> {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert(T::Native::usize_as(0));
            *v = v.add_wrapping(x);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.sum, &self.data_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// This accumulator is specialized for `array_sum` or `array_aggregate('sum')`
struct ArraySumAccumulator<T: ArrowNumericType> {
    // Each element in `Vec` represents the partial sum of respective row
    sum: Vec<Option<T::Native>>,
    data_type: DataType,
}

impl<T: ArrowNumericType> std::fmt::Debug for ArraySumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArraySumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> ArraySumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            // Row number is unknown at the beginning, so we use an empty vector
            sum: vec![],
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for ArraySumAccumulator<T> {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    // There are two kinds of input, PrimitiveArray and ListArray
    // ListArray is for multiple-rows input, and PrimitiveArray is for single-row input
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Wrap single-row input into multiple-rows input and use the same logic as multiple-rows input
        let list_values = if let Some(list_arr) = values[0].as_list_opt::<i32>() {
            list_arr.to_owned()
        } else {
            array_into_list_array(values[0].clone())
        };

        let row_number = list_values.len();

        if self.sum.is_empty() {
            self.sum.resize(row_number, None);
        } else if self.sum.len() < row_number {
            return internal_err!("ArraySumAccumulator::update_batch only support consistent row number, got {} and {}", self.sum.len(), row_number);
        }

        for (i, values) in list_values.iter().enumerate() {
            if let Some(values) = values {
                let values = as_primitive_array::<T>(&values)?;
                if let Some(x) = sum(values) {
                    let v = self.sum[i].get_or_insert(T::Native::usize_as(0));
                    *v = v.add_wrapping(x);
                }
            } else {
                return internal_err!(
                    "ArraySumAccumulator::update_batch got null values"
                );
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        ScalarValue::new_primitives::<T>(self.sum.clone(), &self.data_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// This accumulator incrementally computes sums over a sliding window
///
/// This is separate from [`SumAccumulator`] as requires additional state
struct SlidingSumAccumulator<T: ArrowNumericType> {
    sum: T::Native,
    count: u64,
    data_type: DataType,
}

impl<T: ArrowNumericType> std::fmt::Debug for SlidingSumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlidingSumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> SlidingSumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            sum: T::Native::usize_as(0),
            count: 0,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for SlidingSumAccumulator<T> {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, self.count.into()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            self.sum = self.sum.add_wrapping(x)
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = states[0].as_primitive::<T>();
        if let Some(x) = sum(values) {
            self.sum = self.sum.add_wrapping(x)
        }
        if let Some(x) = sum(states[1].as_primitive::<UInt64Type>()) {
            self.count += x;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let v = (self.count != 0).then_some(self.sum);
        ScalarValue::new_primitive::<T>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if let Some(x) = sum(values) {
            self.sum = self.sum.sub_wrapping(x)
        }
        self.count -= (values.len() - values.null_count()) as u64;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::tests::assert_aggregate;
    use arrow_array::*;
    use datafusion_expr::AggregateFunction;

    #[test]
    fn sum_decimal() {
        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );

        assert_aggregate(
            array,
            AggregateFunction::Sum,
            false,
            ScalarValue::Decimal128(Some(15), 20, 0),
        );
    }

    #[test]
    fn sum_decimal_with_nulls() {
        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(35, 0)
                .unwrap(),
        );

        assert_aggregate(
            array,
            AggregateFunction::Sum,
            false,
            ScalarValue::Decimal128(Some(13), 38, 0),
        );
    }

    #[test]
    fn sum_decimal_all_nulls() {
        // test with batch
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );

        // test agg
        assert_aggregate(
            array,
            AggregateFunction::Sum,
            false,
            ScalarValue::Decimal128(None, 20, 0),
        );
    }

    #[test]
    fn sum_i32() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_aggregate(a, AggregateFunction::Sum, false, ScalarValue::from(15i64));
    }

    #[test]
    fn sum_i32_with_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        assert_aggregate(a, AggregateFunction::Sum, false, ScalarValue::from(13i64));
    }

    #[test]
    fn sum_i32_all_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        assert_aggregate(a, AggregateFunction::Sum, false, ScalarValue::Int64(None));
    }

    #[test]
    fn sum_u32() {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        assert_aggregate(a, AggregateFunction::Sum, false, ScalarValue::from(15u64));
    }

    #[test]
    fn sum_f32() {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        assert_aggregate(a, AggregateFunction::Sum, false, ScalarValue::from(15_f64));
    }

    #[test]
    fn sum_f64() {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        assert_aggregate(a, AggregateFunction::Sum, false, ScalarValue::from(15_f64));
    }
}
