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

use arrow::array::{AsArray, PrimitiveBuilder};
use log::debug;

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate::groups_accumulator::accumulate::NullState;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::compute::sum;
use arrow::datatypes::{DataType, Decimal128Type, Float64Type, UInt64Type};
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};
use arrow_array::types::{Decimal256Type, DecimalType};
use arrow_array::{
    Array, ArrowNativeTypeOp, ArrowNumericType, ArrowPrimitiveType, PrimitiveArray,
};
use arrow_buffer::{i256, ArrowNativeType};
use datafusion_common::{not_impl_err, Result, ScalarValue};
use datafusion_expr::type_coercion::aggregates::avg_return_type;
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};

use super::utils::DecimalAverager;

/// AVG aggregate expression
#[derive(Debug, Clone)]
pub struct Avg {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    input_data_type: DataType,
    result_data_type: DataType,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        let result_data_type = avg_return_type(&data_type).unwrap();

        Self {
            name: name.into(),
            expr,
            input_data_type: data_type,
            result_data_type,
        }
    }
}

impl AggregateExpr for Avg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.result_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        use DataType::*;
        // instantiate specialized accumulator based for the type
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
            (
                Decimal128(sum_precision, sum_scale),
                Decimal128(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type> {
                sum: None,
                count: 0,
                sum_scale: *sum_scale,
                sum_precision: *sum_precision,
                target_precision: *target_precision,
                target_scale: *target_scale,
            })),

            (
                Decimal256(sum_precision, sum_scale),
                Decimal256(target_precision, target_scale),
            ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type> {
                sum: None,
                count: 0,
                sum_scale: *sum_scale,
                sum_precision: *sum_precision,
                target_precision: *target_precision,
                target_scale: *target_scale,
            })),
            _ => not_impl_err!(
                "AvgAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "sum"),
                self.input_data_type.clone(),
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.create_accumulator()
    }

    fn groups_accumulator_supported(&self) -> bool {
        use DataType::*;

        matches!(&self.result_data_type, Float64 | Decimal128(_, _))
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        // instantiate specialized accumulator based for the type
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => {
                Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                    &self.input_data_type,
                    &self.result_data_type,
                    |sum: f64, count: u64| Ok(sum / count as f64),
                )))
            }
            (
                Decimal128(_sum_precision, sum_scale),
                Decimal128(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal128Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn =
                    move |sum: i128, count: u64| decimal_averager.avg(sum, count as i128);

                Ok(Box::new(AvgGroupsAccumulator::<Decimal128Type, _>::new(
                    &self.input_data_type,
                    &self.result_data_type,
                    avg_fn,
                )))
            }

            (
                Decimal256(_sum_precision, sum_scale),
                Decimal256(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal256Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i256, count: u64| {
                    decimal_averager.avg(sum, i256::from_usize(count as usize).unwrap())
                };

                Ok(Box::new(AvgGroupsAccumulator::<Decimal256Type, _>::new(
                    &self.input_data_type,
                    &self.result_data_type,
                    avg_fn,
                )))
            }

            _ => not_impl_err!(
                "AvgGroupsAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }
}

impl PartialEq<dyn Any> for Avg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.result_data_type == x.result_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

/// An accumulator to compute the average
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::Float64(self.sum),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            self.sum = Some(self.sum.unwrap() - x);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<Float64Type>()) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.sum.map(|f| f / self.count as f64),
        ))
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// An accumulator to compute the average for decimals
struct DecimalAvgAccumulator<T: DecimalType + ArrowNumericType> {
    sum: Option<T::Native>,
    count: u64,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType + ArrowNumericType> Debug for DecimalAvgAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecimalAvgAccumulator")
            .field("sum", &self.sum)
            .field("count", &self.count)
            .field("sum_scale", &self.sum_scale)
            .field("sum_precision", &self.sum_precision)
            .field("target_precision", &self.target_precision)
            .field("target_scale", &self.target_scale)
            .finish()
    }
}

impl<T: DecimalType + ArrowNumericType> Accumulator for DecimalAvgAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<T>(
                self.sum,
                &T::TYPE_CONSTRUCTOR(self.sum_precision, self.sum_scale),
            )?,
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();

        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert(T::Native::default());
            self.sum = Some(v.add_wrapping(x));
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            self.sum = Some(self.sum.unwrap().sub_wrapping(x));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<T>()) {
            let v = self.sum.get_or_insert(T::Native::default());
            self.sum = Some(v.add_wrapping(x));
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let v = self
            .sum
            .map(|v| {
                DecimalAverager::<T>::try_new(
                    self.sum_scale,
                    self.target_precision,
                    self.target_scale,
                )?
                .avg(v, T::Native::from_usize(self.count as usize).unwrap())
            })
            .transpose()?;

        ScalarValue::new_primitive::<T>(
            v,
            &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
        )
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calculates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    /// The type of the internal sum
    sum_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group (use u64 to make UInt64Array)
    counts: Vec<u64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the final average (value / count)
    avg_fn: F,
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    pub fn new(sum_data_type: &DataType, return_data_type: &DataType, avg_fn: F) -> Self {
        debug!(
            "AvgGroupsAccumulator ({}, sum type: {sum_data_type:?}) --> {return_data_type:?}",
            std::any::type_name::<T>()
        );

        Self {
            return_data_type: return_data_type.clone(),
            sum_data_type: sum_data_type.clone(),
            counts: vec![],
            sums: vec![],
            null_state: NullState::new(),
            avg_fn,
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
        _grouping_set: &[bool],
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let sum = &mut self.sums[group_index];
                *sum = sum.add_wrapping(new_value);

                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sums = values[1].as_primitive::<T>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                self.counts[group_index] += partial_count;
            },
        );

        // update sums
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, new_value: <T as ArrowPrimitiveType>::Native| {
                let sum = &mut self.sums[group_index];
                *sum = sum.add_wrapping(new_value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);

        assert_eq!(nulls.len(), sums.len());
        assert_eq!(counts.len(), sums.len());

        // don't evaluate averages with null inputs to avoid errors on null values

        let array: PrimitiveArray<T> = if nulls.null_count() > 0 {
            let mut builder = PrimitiveBuilder::<T>::with_capacity(nulls.len())
                .with_data_type(self.return_data_type.clone());
            let iter = sums.into_iter().zip(counts).zip(nulls.iter());

            for ((sum, count), is_valid) in iter {
                if is_valid {
                    builder.append_value((self.avg_fn)(sum, count)?)
                } else {
                    builder.append_null();
                }
            }
            builder.finish()
        } else {
            let averages: Vec<T::Native> = sums
                .into_iter()
                .zip(counts.into_iter())
                .map(|(sum, count)| (self.avg_fn)(sum, count))
                .collect::<Result<Vec<_>>>()?;
            PrimitiveArray::new(averages.into(), Some(nulls)) // no copy
                .with_data_type(self.return_data_type.clone())
        };

        Ok(Arc::new(array))
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build(emit_to);
        let nulls = Some(nulls);

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = UInt64Array::new(counts.into(), nulls.clone()); // zero copy

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), nulls) // zero copy
            .with_data_type(self.sum_data_type.clone());

        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<u64>()
            + self.sums.capacity() * std::mem::size_of::<T>()
    }
}
