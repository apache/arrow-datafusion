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

//! Vectorized [`GroupsAccumulator`]

use arrow_array::{ArrayRef, BooleanArray};
use datafusion_common::Result;

/// Describes how many rows should be emitted during grouping.
#[derive(Debug, Clone, Copy)]
pub enum EmitTo {
    /// Emit all groups
    All,
    /// Emit only the first `n` groups and shift all existing group
    /// indexes down by `n`.
    ///
    /// For example, if `n=10`, group_index `0, 1, ... 9` are emitted
    /// and group indexes '`10, 11, 12, ...` become `0, 1, 2, ...`.
    First(usize),
}

impl EmitTo {
    /// Removes the number of rows from `v` required to emit the right
    /// number of rows, returning a `Vec` with elements taken, and the
    /// remaining values in `v`.
    ///
    /// This avoids copying if Self::All
    pub fn take_needed<T>(&self, v: &mut Vec<T>) -> Vec<T> {
        match self {
            Self::All => {
                // Take the entire vector, leave new (empty) vector
                std::mem::take(v)
            }
            Self::First(n) => {
                // get end n+1,.. values into t
                let mut t = v.split_off(*n);
                // leave n+1,.. in v
                std::mem::swap(v, &mut t);
                t
            }
        }
    }
}

/// `GroupAccumulator` implements a single aggregate (e.g. AVG) and
/// stores the state for *all* groups internally.
///
/// # Notes on Implementing `GroupAccumulator`
///
/// All aggregates must first implement the simpler [`Accumulator`] trait, which
/// handles state for a single group. Implementing `GroupsAccumulator` is
/// optional and is harder to implement than `Accumulator`, but can be much
/// faster for queries with many group values.  See the [Aggregating Millions of
/// Groups Fast blog] for more background.
///
/// # Details
/// Each group is assigned a `group_index` by the hash table and each
/// accumulator manages the specific state, one per `group_index`.
///
/// `group_index`es are contiguous (there aren't gaps), and thus it is
/// expected that each `GroupAccumulator` will use something like `Vec<..>`
/// to store the group states.
///
/// [`Accumulator`]: crate::Accumulator
/// [Aggregating Millions of Groups Fast blog]: https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/
pub trait GroupsAccumulator: Send {
    /// Updates the accumulator's state from its arguments, encoded as
    /// a vector of [`ArrayRef`]s.
    ///
    /// * `values`: the input arguments to the accumulator
    ///
    /// * `group_indices`: To which groups do the rows in `values`
    /// belong, group id)
    ///
    /// * `opt_filter`: if present, only update aggregate state using
    /// `values[i]` if `opt_filter[i]` is true
    ///
    /// * `total_num_groups`: the number of groups (the largest
    /// group_index is thus `total_num_groups - 1`).
    ///
    /// * `grouping_set`: An indicator of whether the columns in the
    /// GroupingSet is null, typically used in GROUPING aggregate function.
    ///
    /// Note that subsequent calls to update_batch may have larger
    /// total_num_groups as new groups are seen.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        grouping_set: &[bool],
    ) -> Result<()>;

    /// Returns the final aggregate value for each group as a single
    /// `RecordBatch`, resetting the internal state.
    ///
    /// The rows returned *must* be in group_index order: The value
    /// for group_index 0, followed by 1, etc.  Any group_index that
    /// did not have values, should be null.
    ///
    /// For example, a `SUM` accumulator maintains a running sum for
    /// each group, and `evaluate` will produce that running sum as
    /// its output for all groups, in group_index order
    ///
    /// If `emit_to`` is [`EmitTo::All`], the accumulator should
    /// return all groups and release / reset its internal state
    /// equivalent to when it was first created.
    ///
    /// If `emit_to` is [`EmitTo::First`], only the first `n` groups
    /// should be emitted and the state for those first groups
    /// removed. State for the remaining groups must be retained for
    /// future use. The group_indices on subsequent calls to
    /// `update_batch` or `merge_batch` will be shifted down by
    /// `n`. See [`EmitTo::First`] for more details.
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef>;

    /// Returns the intermediate aggregate state for this accumulator,
    /// used for multi-phase grouping, resetting its internal state.
    ///
    /// For example, `AVG` might return two arrays: `SUM` and `COUNT`
    /// but the `MIN` aggregate would just return a single array.
    ///
    /// Note more sophisticated internal state can be passed as
    /// single `StructArray` rather than multiple arrays.
    ///
    /// See [`Self::evaluate`] for details on the required output
    /// order and  `emit_to`.
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Merges intermediate state (the output from [`Self::state`])
    /// into this accumulator's values.
    ///
    /// For some aggregates (such as `SUM`), `merge_batch` is the same
    /// as `update_batch`, but for some aggregates (such as `COUNT`,
    /// where the partial counts must be summed) the operations
    /// differ. See [`Self::state`] for more details on how state is
    /// used and merged.
    ///
    /// * `values`: arrays produced from calling `state` previously to the accumulator
    ///
    /// Other arguments are the same as for [`Self::update_batch`];
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Amount of memory used to store the state of this accumulator,
    /// in bytes. This function is called once per batch, so it should
    /// be `O(n)` to compute, not `O(num_groups)`
    fn size(&self) -> usize;
}
