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
use std::sync::Arc;

use crate::aggregate::groups_accumulator::accumulate::accumulate_indices;
use crate::aggregate::utils::down_cast_any_ref;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Int32Array, PrimitiveArray};
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};
use datafusion_physical_expr_common::expressions::column::Column;

use crate::expressions::format_state_name;

/// GROUPING aggregate expression
/// Returns the amount of non-null values of the given expression.
#[derive(Debug)]
pub struct Grouping {
    name: String,
    data_type: DataType,
    nullable: bool,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl Grouping {
    /// Create a new GROUPING aggregate function.
    pub fn new(
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

    /// Create a new GroupingGroupsAccumulator
    pub fn create_grouping_groups_accumulator(
        &self,
        group_by_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(GroupingGroupsAccumulator::new(
            &self.exprs,
            group_by_exprs,
        )?))
    }
}

impl AggregateExpr for Grouping {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Int32, self.nullable))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        not_impl_err!(
            "physical plan is not yet implemented for GROUPING aggregate function"
        )
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "grouping"),
            DataType::Int32,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Grouping {
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
struct GroupingGroupsAccumulator {
    /// Grouping columns' indices in grouping set
    indices: Vec<usize>,

    /// Mask per group.
    ///
    /// Note this is an i32 and not a u32 (or usize) because the
    /// output type of grouping is `DataType::Int32`. Thus by using `i32`
    /// for the grouping, the output [`Int32Array`] can be created
    /// without copy.
    masks: Vec<i32>,
}

impl GroupingGroupsAccumulator {
    pub fn new(
        grouping_exprs: &[Arc<dyn PhysicalExpr>],
        group_by_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    ) -> Result<Self> {
        // The PhysicalExprs of grouping_exprs must be Column PhysicalExpr. Because if
        // the group by PhysicalExpr in SQL is non-Column PhysicalExpr, then there is
        // a ProjectionExec before AggregateExec to convert the non-column PhysicalExpr
        // to Column PhysicalExpr.
        macro_rules! downcast_column {
            ($EXPR:expr) => {{
                if let Some(column) = $EXPR.as_any().downcast_ref::<Column>() {
                    column
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Grouping doesn't support expr: {}",
                        $EXPR
                    )));
                }
            }};
        }

        // collect column indices of group_by_exprs, only Column Expr
        let mut group_by_column_indices = Vec::with_capacity(group_by_exprs.len());
        for (group_by_expr, _) in group_by_exprs.iter() {
            let column = downcast_column!(group_by_expr);
            group_by_column_indices.push(column.index());
        }

        // collect grouping_exprs' indices in group_by_exprs list, eg:
        // SQL: SELECT c1, c2, grouping(c2, c1) FROM t GROUP BY ROLLUP(c1, c2);
        // group_by_exprs: [c1, c2]
        // grouping_exprs: [c2, c1]
        // indices: [1, 0]
        let mut indices = Vec::with_capacity(grouping_exprs.len());
        for grouping_expr in grouping_exprs {
            let column = downcast_column!(grouping_expr);
            indices.push(find_grouping_column_index(
                &group_by_column_indices,
                column.index(),
            )?);
        }

        Ok(Self {
            indices,
            masks: vec![],
        })
    }
}

fn find_grouping_column_index(
    group_by_column_indices: &[usize],
    grouping_column_index: usize,
) -> Result<usize> {
    for (i, group_by_column_index) in group_by_column_indices.iter().enumerate() {
        if grouping_column_index == *group_by_column_index {
            return Ok(i);
        }
    }
    Err(DataFusionError::Execution(format!(
        "Not found grouping column index {} in group by column indices {:?}",
        grouping_column_index, group_by_column_indices
    )))
}

fn compute_mask(indices: &[usize], grouping_set: &[bool]) -> i32 {
    let mut mask = 0;
    for (i, index) in indices.iter().rev().enumerate() {
        if grouping_set[*index] {
            mask |= 1 << i;
        }
    }
    mask
}

impl GroupsAccumulator for GroupingGroupsAccumulator {
    fn update_batch(
        &mut self,
        _values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
        grouping_set: &[bool],
    ) -> Result<()> {
        self.masks.resize(total_num_groups, 0);
        let mask = compute_mask(&self.indices, grouping_set);
        accumulate_indices(group_indices, None, opt_filter, |group_index| {
            self.masks[group_index] = mask;
        });
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let masks = emit_to.take_needed(&mut self.masks);

        // Mask is always non null (null inputs just don't contribute to the overall values)
        let nulls = None;
        let array = PrimitiveArray::<Int32Type>::new(masks.into(), nulls);

        Ok(Arc::new(array))
    }

    // return arrays for masks
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let masks = emit_to.take_needed(&mut self.masks);
        let masks: PrimitiveArray<Int32Type> = Int32Array::from(masks); // zero copy, no nulls
        Ok(vec![Arc::new(masks) as ArrayRef])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");
        let masks = values[0].as_primitive::<Int32Type>();

        // intermediate masks are always created as non null
        assert_eq!(masks.null_count(), 0);
        let masks = masks.values();

        self.masks.resize(total_num_groups, 0);
        match opt_filter {
            Some(filter) => filter
                .iter()
                .zip(group_indices.iter())
                .zip(masks.iter())
                .for_each(|((filter_value, &group_index), mask)| {
                    if let Some(true) = filter_value {
                        self.masks[group_index] = *mask;
                    }
                }),
            None => {
                group_indices
                    .iter()
                    .zip(masks.iter())
                    .for_each(|(&group_index, mask)| {
                        self.masks[group_index] = *mask;
                    })
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.masks.capacity() * std::mem::size_of::<usize>()
    }
}
