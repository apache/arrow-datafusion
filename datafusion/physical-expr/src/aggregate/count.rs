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
use std::fmt::Debug;
use std::ops::BitAnd;
use std::sync::Arc;

use crate::aggregate::utils::down_cast_any_ref;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{Array, Int64Array};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{array::ArrayRef, datatypes::Field};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::PrimitiveArray;
use arrow_buffer::BooleanBuffer;
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};

use crate::expressions::format_state_name;

use super::groups_accumulator::accumulate::accumulate_indices;

/// COUNT aggregate expression
/// Returns the amount of non-null values of the given expression.
#[derive(Debug, Clone)]
pub struct Count {
    name: String,
    data_type: DataType,
    nullable: bool,
    /// Input exprs
    ///
    /// For `COUNT(c1)` this is `[c1]`
    /// For `COUNT(c1, c2)` this is `[c1, c2]`
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl Count {
    /// Create a new COUNT aggregate function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            exprs: vec![expr],
            data_type,
            nullable: true,
        }
    }

    pub fn new_with_multiple_exprs(
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            exprs,
            data_type,
            nullable: true,
        }
    }
}

/// An accumulator to compute the counts of [`PrimitiveArray<T>`].
/// Stores values as native types, and does overflow checking
///
/// Unlike most other accumulators, COUNT never produces NULLs. If no
/// non-null values are seen in any group the output is 0. Thus, this
/// accumulator has no additional null or seen filter tracking.
#[derive(Debug)]
struct CountGroupsAccumulator {
    /// Count per group.
    ///
    /// Note this is an i64 and not a u64 (or usize) because the
    /// output type of count is `DataType::Int64`. Thus by using `i64`
    /// for the counts, the output [`Int64Array`] can be created
    /// without copy.
    counts: Vec<i64>,
}

impl CountGroupsAccumulator {
    pub fn new() -> Self {
        Self { counts: vec![] }
    }
}

impl GroupsAccumulator for CountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
        _grouping_set: &[bool],
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = &values[0];

        // Add one to each group's counter for each non null, non
        // filtered value
        self.counts.resize(total_num_groups, 0);
        accumulate_indices(
            group_indices,
            values.logical_nulls().as_ref(),
            opt_filter,
            |group_index| {
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
        assert_eq!(values.len(), 1, "one argument to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<Int64Type>();

        // intermediate counts are always created as non null
        assert_eq!(partial_counts.null_count(), 0);
        let partial_counts = partial_counts.values();

        // Adds the counts with the partial counts
        self.counts.resize(total_num_groups, 0);
        match opt_filter {
            Some(filter) => filter
                .iter()
                .zip(group_indices.iter())
                .zip(partial_counts.iter())
                .for_each(|((filter_value, &group_index), partial_count)| {
                    if let Some(true) = filter_value {
                        self.counts[group_index] += partial_count;
                    }
                }),
            None => group_indices.iter().zip(partial_counts.iter()).for_each(
                |(&group_index, partial_count)| {
                    self.counts[group_index] += partial_count;
                },
            ),
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);

        // Count is always non null (null inputs just don't contribute to the overall values)
        let nulls = None;
        let array = PrimitiveArray::<Int64Type>::new(counts.into(), nulls);

        Ok(Arc::new(array))
    }

    // return arrays for counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let counts: PrimitiveArray<Int64Type> = Int64Array::from(counts); // zero copy, no nulls
        Ok(vec![Arc::new(counts) as ArrayRef])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<usize>()
    }
}

/// count null values for multiple columns
/// for each row if one column value is null, then null_count + 1
fn null_count_for_multiple_cols(values: &[ArrayRef]) -> usize {
    if values.len() > 1 {
        let result_bool_buf: Option<BooleanBuffer> = values
            .iter()
            .map(|a| a.logical_nulls())
            .fold(None, |acc, b| match (acc, b) {
                (Some(acc), Some(b)) => Some(acc.bitand(b.inner())),
                (Some(acc), None) => Some(acc),
                (None, Some(b)) => Some(b.into_inner()),
                _ => None,
            });
        result_bool_buf.map_or(0, |b| values[0].len() - b.count_set_bits())
    } else {
        values[0]
            .logical_nulls()
            .map_or(0, |nulls| nulls.null_count())
    }
}

impl AggregateExpr for Count {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Int64, self.nullable))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "count"),
            DataType::Int64,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        // groups accumulator only supports `COUNT(c1)`, not
        // `COUNT(c1, c2)`, etc
        self.exprs.len() == 1
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator
        Ok(Box::new(CountGroupsAccumulator::new()))
    }
}

impl PartialEq<dyn Any> for Count {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.exprs.len() == x.exprs.len()
                    && self
                        .exprs
                        .iter()
                        .zip(x.exprs.iter())
                        .all(|(expr1, expr2)| expr1.eq(expr2))
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - null_count_for_multiple_cols(values)) as i64;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count -= (array.len() - null_count_for_multiple_cols(values)) as i64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], Int64Array);
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
