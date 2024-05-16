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

use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{ArrayRef, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use datafusion_common::Result;
use datafusion_expr::{EmitTo, GroupsAccumulator};

use super::accumulate::NullState;

/// An accumulator that implements a single operation over a
/// [`BooleanArray`] where the accumulated state is also boolean (such
/// as [`BitAndAssign`])
///
/// F: The function to apply to two elements. The first argument is
/// the existing value and should be updated with the second value
/// (e.g. [`BitAndAssign`] style).
///
/// [`BitAndAssign`]: std::ops::BitAndAssign
#[derive(Debug)]
pub struct BooleanGroupsAccumulator<F>
where
    F: Fn(bool, bool) -> bool + Send + Sync,
{
    /// values per group
    values: BooleanBufferBuilder,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the output
    bool_fn: F,
}

impl<F> BooleanGroupsAccumulator<F>
where
    F: Fn(bool, bool) -> bool + Send + Sync,
{
    pub fn new(bitop_fn: F) -> Self {
        Self {
            values: BooleanBufferBuilder::new(0),
            null_state: NullState::new(),
            bool_fn: bitop_fn,
        }
    }
}

impl<F> GroupsAccumulator for BooleanGroupsAccumulator<F>
where
    F: Fn(bool, bool) -> bool + Send + Sync,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        _grouping_set: &[bool],
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_boolean();

        if self.values.len() < total_num_groups {
            let new_groups = total_num_groups - self.values.len();
            self.values.append_n(new_groups, Default::default());
        }

        // NullState dispatches / handles tracking nulls and groups that saw no values
        self.null_state.accumulate_boolean(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let current_value = self.values.get_bit(group_index);
                let value = (self.bool_fn)(current_value, new_value);
                self.values.set_bit(group_index, value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = self.values.finish();

        let values = match emit_to {
            EmitTo::All => values,
            EmitTo::First(n) => {
                let first_n: BooleanBuffer = values.iter().take(n).collect();
                // put n+1 back into self.values
                for v in values.iter().skip(n) {
                    self.values.append(v);
                }
                first_n
            }
        };

        let nulls = self.null_state.build(emit_to);
        let values = BooleanArray::new(values, Some(nulls));
        Ok(Arc::new(values))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // update / merge are the same
        self.update_batch(values, group_indices, opt_filter, total_num_groups, &[])
    }

    fn size(&self) -> usize {
        // capacity is in bits, so convert to bytes
        self.values.capacity() / 8 + self.null_state.size()
    }
}
