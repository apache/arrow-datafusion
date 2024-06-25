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

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{ArrayRef, AsArray, ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, BooleanArray};
use arrow_schema::{IntervalUnit, TimeUnit};
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};
use datafusion_physical_expr_common::aggregate::groups_accumulator::accumulate::NullState;
use std::any::Any;
use std::sync::Arc;

/// ARRAY_AGG aggregate expression
#[derive(Debug)]
pub struct ArrayAgg {
    /// Column name
    name: String,
    /// The DataType for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
    /// If the input expression can have NULLs
    nullable: bool,
    // If the aggregate should ignore NULLs
    ignore_nulls: bool,
}

impl ArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type: data_type,
            expr,
            nullable,
            ignore_nulls,
        }
    }
}

impl AggregateExpr for ArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new_list(
            &self.name,
            // This should be the same as return type of AggregateFunction::ArrayAgg
            Field::new("item", self.input_data_type.clone(), true),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayAggAccumulator::try_new(
            &self.input_data_type,
            self.ignore_nulls,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "array_agg"),
            Field::new("item", self.input_data_type.clone(), true),
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
        self.input_data_type.is_primitive() || self.input_data_type == DataType::Utf8
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        match self.input_data_type {
            DataType::Int8 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int8Type>::new(
                &self.input_data_type,
                self.ignore_nulls,
            ))),
            DataType::Int16 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int16Type>::new(
                &self.input_data_type,
                self.ignore_nulls,
            ))),
            DataType::Int32 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int32Type>::new(
                &self.input_data_type,
                self.ignore_nulls,
            ))),
            DataType::Int64 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int64Type>::new(
                &self.input_data_type,
                self.ignore_nulls,
            ))),
            DataType::UInt8 => Ok(Box::new(ArrayAggGroupsAccumulator::<UInt8Type>::new(
                &self.input_data_type,
                self.ignore_nulls,
            ))),
            DataType::UInt16 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<UInt16Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::UInt32 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<UInt32Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::UInt64 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<UInt64Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Float32 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Float32Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Float64 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Float64Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Decimal128(_, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Decimal128Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Decimal256(_, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Decimal256Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Date32 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Date32Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Date64 => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Date64Type>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                )))
            }
            DataType::Timestamp(TimeUnit::Second, _) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<TimestampSecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<
                    TimestampMillisecondType,
                >::new(
                    &self.input_data_type, self.ignore_nulls
                )))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<
                    TimestampMicrosecondType,
                >::new(
                    &self.input_data_type, self.ignore_nulls
                )))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<TimestampNanosecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Time32(TimeUnit::Second) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time32SecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Time32(TimeUnit::Millisecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time32MillisecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Time64(TimeUnit::Microsecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time64MicrosecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Time64(TimeUnit::Nanosecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time64NanosecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Duration(TimeUnit::Second) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationSecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Duration(TimeUnit::Millisecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationMillisecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Duration(TimeUnit::Microsecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationMicrosecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Duration(TimeUnit::Nanosecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationNanosecondType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Interval(IntervalUnit::YearMonth) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<IntervalYearMonthType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Interval(IntervalUnit::DayTime) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<IntervalDayTimeType>::new(
                    &self.input_data_type,
                    self.ignore_nulls,
                ),
            )),
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<
                    IntervalMonthDayNanoType,
                >::new(
                    &self.input_data_type, self.ignore_nulls
                )))
            }
            DataType::Utf8 => Ok(Box::new(StringArrayAggGroupsAccumulator::new(
                self.ignore_nulls,
            ))),
            _ => Err(DataFusionError::Internal(format!(
                "ArrayAggGroupsAccumulator not supported for data type {:?}",
                self.input_data_type
            ))),
        }
    }
}

impl PartialEq<dyn Any> for ArrayAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct ArrayAggAccumulator {
    values: Vec<ArrayRef>,
    datatype: DataType,
    ignore_nulls: bool,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType, ignore_nulls: bool) -> Result<Self> {
        Ok(Self {
            values: vec![],
            datatype: datatype.clone(),
            ignore_nulls,
        })
    }
}

impl Accumulator for ArrayAggAccumulator {
    // Append value like Int64Array(1,2,3)
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        assert!(values.len() == 1, "array_agg can only take 1 param!");

        let val = values[0].clone();

        if self.ignore_nulls {
            if let Some(nulls) = val.logical_nulls() {
                let predicate = BooleanArray::from(nulls.inner().clone());
                let filtered = arrow::compute::filter(val.as_ref(), &predicate)?;
                self.values.push(filtered);
                return Ok(());
            }
        }

        self.values.push(val);
        Ok(())
    }

    // Append value like ListArray(Int64Array(1,2,3), Int64Array(4,5,6))
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert!(states.len() == 1, "array_agg states must be singleton!");

        let list_arr = as_list_array(&states[0])?;
        for arr in list_arr.iter().flatten() {
            self.values.push(arr);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Transform Vec<ListArr> to ListArr

        let element_arrays: Vec<&dyn Array> =
            self.values.iter().map(|a| a.as_ref()).collect();

        if element_arrays.is_empty() {
            let arr = ScalarValue::new_list(&[], &self.datatype);
            return Ok(ScalarValue::List(arr));
        }

        let concated_array = arrow::compute::concat(&element_arrays)?;
        let list_array = array_into_list_array(concated_array);

        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ArrayRef>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>()
            + self.datatype.size()
            - std::mem::size_of_val(&self.datatype)
    }
}

struct ArrayAggGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    values: Vec<PrimitiveBuilder<T>>,
    data_type: DataType,
    null_state: NullState,
    ignore_nulls: bool,
}

impl<T> ArrayAggGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    pub fn new(data_type: &DataType, ignore_nulls: bool) -> Self {
        Self {
            values: vec![],
            data_type: data_type.clone(),
            null_state: NullState::new(),
            ignore_nulls,
        }
    }
}

impl<T: ArrowPrimitiveType + Send> ArrayAggGroupsAccumulator<T> {
    fn build_list(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let arrays = emit_to.take_needed(&mut self.values);
        let (nulls, seen_nulls) = self.null_state.build_with_seen_nulls(emit_to);

        let len = nulls.len();
        assert_eq!(arrays.len(), len);

        let mut builder = ListBuilder::with_capacity(
            PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone()),
            len,
        );

        for ((is_valid, mut arr), seen_null) in nulls
            .iter()
            .zip(arrays.into_iter())
            .zip(seen_nulls.into_iter())
        {
            if is_valid {
                if seen_null && !self.ignore_nulls {
                    arr.append_null();
                }

                builder.append_value(arr.finish().into_iter());
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

impl<T> GroupsAccumulator for ArrayAggGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Sync,
{
    fn update_batch(
        &mut self,
        new_values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(new_values.len(), 1, "single argument to update_batch");
        let new_values = new_values[0].as_primitive::<T>();

        for _ in self.values.len()..total_num_groups {
            self.values.push(
                PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone()),
            );
        }

        self.null_state.accumulate(
            group_indices,
            new_values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                self.values[group_index].append_value(new_value);
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to merge_batch");
        let values = values[0].as_list();

        for _ in self.values.len()..total_num_groups {
            self.values.push(
                PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone()),
            );
        }

        self.null_state.accumulate_array(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value: ArrayRef| {
                let new_value = new_value.as_primitive::<T>();
                self.values[group_index].extend(new_value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.build_list(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.build_list(emit_to)?])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + std::mem::size_of::<PrimitiveBuilder<T>>() * self.values.capacity()
            + self.values.iter().map(|arr| arr.capacity()).sum::<usize>()
                * std::mem::size_of::<<T as ArrowPrimitiveType>::Native>()
            + self.null_state.size()
    }
}

struct StringArrayAggGroupsAccumulator {
    values: Vec<StringBuilder>,
    null_state: NullState,
    ignore_nulls: bool,
}

impl StringArrayAggGroupsAccumulator {
    pub fn new(ignore_nulls: bool) -> Self {
        Self {
            values: vec![],
            null_state: NullState::new(),
            ignore_nulls,
        }
    }
}

impl StringArrayAggGroupsAccumulator {
    fn build_list(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let array = emit_to.take_needed(&mut self.values);
        let (nulls, seen_nulls) = self.null_state.build_with_seen_nulls(emit_to);

        assert_eq!(array.len(), nulls.len());
        assert_eq!(array.len(), seen_nulls.len());

        let mut builder = ListBuilder::with_capacity(StringBuilder::new(), nulls.len());
        for (mut arr, (is_valid, seen_null)) in array
            .into_iter()
            .zip(nulls.into_iter().zip(seen_nulls.into_iter()))
        {
            if is_valid {
                if seen_null && !self.ignore_nulls {
                    arr.append_null();
                }
                builder.append_value(arr.finish().into_iter());
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

impl GroupsAccumulator for StringArrayAggGroupsAccumulator {
    fn update_batch(
        &mut self,
        new_values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(new_values.len(), 1, "single argument to update_batch");
        let new_values = new_values[0].as_string();

        for _ in self.values.len()..total_num_groups {
            self.values.push(StringBuilder::new());
        }

        self.null_state.accumulate_string(
            group_indices,
            new_values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                self.values[group_index].append_value(new_value);
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to merge_batch");
        let values = values[0].as_list();

        for _ in self.values.len()..total_num_groups {
            self.values.push(StringBuilder::new());
        }

        self.null_state.accumulate_array(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value: ArrayRef| {
                let new_value = new_value.as_string::<i32>();
                self.values[group_index]
                    .extend(new_value.into_iter().map(|s| s.map(|s| s.to_string())));
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.build_list(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.build_list(emit_to)?])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + std::mem::size_of::<StringBuilder>() * self.values.capacity()
            + self
                .values
                .iter()
                .map(|arr| {
                    std::mem::size_of_val(arr.values_slice())
                        + std::mem::size_of_val(arr.offsets_slice())
                        + arr.validity_slice().map(std::mem::size_of_val).unwrap_or(0)
                })
                .sum::<usize>()
            + self.null_state.size()
    }
}
