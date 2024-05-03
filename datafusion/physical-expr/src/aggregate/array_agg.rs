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

use crate::aggregate::groups_accumulator::accumulate::NullState;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::builder::{ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrowPrimitiveType, BooleanArray};
use arrow_schema::{IntervalUnit, TimeUnit};
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};
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
}

impl ArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type: data_type,
            expr,
            nullable,
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
            ))),
            DataType::Int16 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int16Type>::new(
                &self.input_data_type,
            ))),
            DataType::Int32 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int32Type>::new(
                &self.input_data_type,
            ))),
            DataType::Int64 => Ok(Box::new(ArrayAggGroupsAccumulator::<Int64Type>::new(
                &self.input_data_type,
            ))),
            DataType::UInt8 => Ok(Box::new(ArrayAggGroupsAccumulator::<UInt8Type>::new(
                &self.input_data_type,
            ))),
            DataType::UInt16 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<UInt16Type>::new(&self.input_data_type),
            )),
            DataType::UInt32 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<UInt32Type>::new(&self.input_data_type),
            )),
            DataType::UInt64 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<UInt64Type>::new(&self.input_data_type),
            )),
            DataType::Float32 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Float32Type>::new(&self.input_data_type),
            )),
            DataType::Float64 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Float64Type>::new(&self.input_data_type),
            )),
            DataType::Decimal128(_, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Decimal128Type>::new(
                    &self.input_data_type,
                )))
            }
            DataType::Decimal256(_, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<Decimal256Type>::new(
                    &self.input_data_type,
                )))
            }
            DataType::Date32 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Date32Type>::new(&self.input_data_type),
            )),
            DataType::Date64 => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Date64Type>::new(&self.input_data_type),
            )),
            DataType::Timestamp(TimeUnit::Second, _) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<TimestampSecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<
                    TimestampMillisecondType,
                >::new(&self.input_data_type)))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<
                    TimestampMicrosecondType,
                >::new(&self.input_data_type)))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<TimestampNanosecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Time32(TimeUnit::Second) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time32SecondType>::new(&self.input_data_type),
            )),
            DataType::Time32(TimeUnit::Millisecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time32MillisecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Time64(TimeUnit::Microsecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time64MicrosecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Time64(TimeUnit::Nanosecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<Time64NanosecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Duration(TimeUnit::Second) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationSecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Duration(TimeUnit::Millisecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationMillisecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Duration(TimeUnit::Microsecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationMicrosecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Duration(TimeUnit::Nanosecond) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<DurationNanosecondType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Interval(IntervalUnit::YearMonth) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<IntervalYearMonthType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Interval(IntervalUnit::DayTime) => Ok(Box::new(
                ArrayAggGroupsAccumulator::<IntervalDayTimeType>::new(
                    &self.input_data_type,
                ),
            )),
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Ok(Box::new(ArrayAggGroupsAccumulator::<
                    IntervalMonthDayNanoType,
                >::new(&self.input_data_type)))
            }
            DataType::Utf8 => Ok(Box::new(StringArrayAggGroupsAccumulator::new())),
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
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: vec![],
            datatype: datatype.clone(),
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
}

impl<T> ArrayAggGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            values: vec![],
            data_type: data_type.clone(),
            null_state: NullState::new(),
        }
    }
}

impl<T: ArrowPrimitiveType + Send> ArrayAggGroupsAccumulator<T> {
    fn build_list(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let arrays = emit_to.take_needed(&mut self.values);
        let nulls = self.null_state.build(emit_to);

        let len = nulls.len();
        assert_eq!(arrays.len(), len);

        let mut builder = ListBuilder::with_capacity(
            PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone()),
            len,
        );

        for (is_valid, mut arr) in nulls.iter().zip(arrays.into_iter()) {
            if is_valid {
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
}

impl StringArrayAggGroupsAccumulator {
    pub fn new() -> Self {
        Self {
            values: vec![],
            null_state: NullState::new(),
        }
    }
}

impl StringArrayAggGroupsAccumulator {
    fn build_list(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let array = emit_to.take_needed(&mut self.values);
        let nulls = self.null_state.build(emit_to);

        assert_eq!(array.len(), nulls.len());

        let mut builder = ListBuilder::with_capacity(StringBuilder::new(), nulls.len());
        for (is_valid, mut arr) in nulls.iter().zip(array.into_iter()) {
            if is_valid {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::{aggregate, aggregate_new};
    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use arrow_array::ListArray;
    use arrow_array::{Array, StringArray};
    use arrow_buffer::OffsetBuffer;
    use datafusion_common::Result;

    macro_rules! test_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr) => {
            test_op!($ARRAY, $DATATYPE, $OP, $EXPECTED, $EXPECTED.data_type())
        };
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
                true,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);
        }};
    }

    macro_rules! test_op_new {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr) => {
            generic_test_op_new!(
                $ARRAY,
                $DATATYPE,
                $OP,
                $EXPECTED,
                $EXPECTED.data_type().clone()
            )
        };
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
                true,
            ));
            let actual = aggregate_new(&batch, agg)?;
            assert_eq!($EXPECTED, &actual);
        }};
    }

    #[test]
    fn array_agg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        let expected = ScalarValue::List(Arc::new(list.clone()));

        test_op!(
            a.clone(),
            DataType::Int32,
            ArrayAgg,
            expected,
            DataType::Int32
        );

        let expected: ArrayRef = Arc::new(list);
        test_op_new!(a, DataType::Int32, ArrayAgg, &expected, DataType::Int32);

        Ok(())
    }

    #[test]
    fn array_agg_str() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"]));

        let mut list_builder = ListBuilder::with_capacity(StringBuilder::new(), 5);
        list_builder.values().append_value("1");
        list_builder.values().append_value("2");
        list_builder.values().append_value("3");
        list_builder.values().append_value("4");
        list_builder.values().append_value("5");
        list_builder.append(true);

        let list = list_builder.finish();
        let expected: ArrayRef = Arc::new(list);
        test_op_new!(a, DataType::Utf8, ArrayAgg, &expected, DataType::Utf8);

        Ok(())
    }

    #[test]
    fn array_agg_nested() -> Result<()> {
        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(4),
            Some(5),
        ])]);
        let l1 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([a1.len() + a2.len()]),
            arrow::compute::concat(&[&a1, &a2])?,
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(6)])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(7),
            Some(8),
        ])]);
        let l2 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([a1.len() + a2.len()]),
            arrow::compute::concat(&[&a1, &a2])?,
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(9)])]);
        let l3 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([a1.len()]),
            arrow::compute::concat(&[&a1])?,
            None,
        );

        let list = ListArray::new(
            Arc::new(Field::new("item", l1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([l1.len() + l2.len() + l3.len()]),
            arrow::compute::concat(&[&l1, &l2, &l3])?,
            None,
        );
        let list = ScalarValue::List(Arc::new(list));
        let l1 = ScalarValue::List(Arc::new(l1));
        let l2 = ScalarValue::List(Arc::new(l2));
        let l3 = ScalarValue::List(Arc::new(l3));

        let array = ScalarValue::iter_to_array(vec![l1, l2, l3]).unwrap();

        test_op!(
            array,
            DataType::List(Arc::new(Field::new_list(
                "item",
                Field::new("item", DataType::Int32, true),
                true,
            ))),
            ArrayAgg,
            list,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true,)))
        );

        Ok(())
    }
}
