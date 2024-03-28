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

//! Join related functionality used both on logical and physical plans

use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::future::Future;
use std::ops::{IndexMut, Range};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::usize;

use crate::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use crate::{ColumnStatistics, ExecutionPlan, Partitioning, Statistics};

use arrow::array::{
    downcast_array, new_null_array, Array, BooleanBufferBuilder, UInt32Array,
    UInt32BufferBuilder, UInt32Builder, UInt64Array, UInt64BufferBuilder,
};
use arrow::compute;
use arrow::datatypes::{Field, Schema, SchemaBuilder};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow_array::{ArrowPrimitiveType, NativeAdapter, PrimitiveArray};
use arrow_buffer::ArrowNativeType;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    plan_err, DataFusionError, JoinSide, JoinType, Result, SharedResult,
};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_physical_expr::equivalence::add_offset_to_expr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{collect_columns, merge_vectors};
use datafusion_physical_expr::{
    LexOrdering, LexOrderingRef, PhysicalExpr, PhysicalExprRef, PhysicalSortExpr,
};

use futures::future::{BoxFuture, Shared};
use futures::{ready, FutureExt};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;

/// Maps a `u64` hash value based on the build side ["on" values] to a list of indices with this key's value.
///
/// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the build side,
/// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
///
/// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
/// As the key is a hash value, we need to check possible hash collisions in the probe stage
/// During this stage it might be the case that a row is contained the same hashmap value,
/// but the values don't match. Those are checked in the [`equal_rows_arr`](crate::joins::hash_join::equal_rows_arr) method.
///
/// The indices (values) are stored in a separate chained list stored in the `Vec<u64>`.
///
/// The first value (+1) is stored in the hashmap, whereas the next value is stored in array at the position value.
///
/// The chain can be followed until the value "0" has been reached, meaning the end of the list.
/// Also see chapter 5.3 of [Balancing vectorized query execution with bandwidth-optimized storage](https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487)
///
/// # Example
///
/// ``` text
/// See the example below:
///
/// Insert (10,1)            <-- insert hash value 10 with row index 1
/// map:
/// ----------
/// | 10 | 2 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (20,2)
/// map:
/// ----------
/// | 10 | 2 |
/// | 20 | 3 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (10,3)           <-- collision! row index 3 has a hash value of 10 as well
/// map:
/// ----------
/// | 10 | 4 |
/// | 20 | 3 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 0 |  <--- hash value 10 maps to 4,2 (which means indices values 3,1)
/// ---------------------
/// Insert (10,4)          <-- another collision! row index 4 ALSO has a hash value of 10
/// map:
/// ---------
/// | 10 | 5 |
/// | 20 | 3 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 4 | <--- hash value 10 maps to 5,4,2 (which means indices values 4,3,1)
/// ---------------------
/// ```
pub struct JoinHashMap {
    // Stores hash value to last row index
    map: RawTable<(u64, u64)>,
    // Stores indices in chained list data structure
    next: Vec<u64>,
}

impl JoinHashMap {
    #[cfg(test)]
    pub(crate) fn new(map: RawTable<(u64, u64)>, next: Vec<u64>) -> Self {
        Self { map, next }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        JoinHashMap {
            map: RawTable::with_capacity(capacity),
            next: vec![0; capacity],
        }
    }
}

// Type of offsets for obtaining indices from JoinHashMap.
pub(crate) type JoinHashMapOffset = (usize, Option<u64>);

// Macro for traversing chained values with limit.
// Early returns in case of reacing output tuples limit.
macro_rules! chain_traverse {
    (
        $input_indices:ident, $match_indices:ident, $hash_values:ident, $next_chain:ident,
        $input_idx:ident, $chain_idx:ident, $deleted_offset:ident, $remaining_output:ident
    ) => {
        let mut i = $chain_idx - 1;
        loop {
            let match_row_idx = if let Some(offset) = $deleted_offset {
                // This arguments means that we prune the next index way before here.
                if i < offset as u64 {
                    // End of the list due to pruning
                    break;
                }
                i - offset as u64
            } else {
                i
            };
            $match_indices.append(match_row_idx);
            $input_indices.append($input_idx as u32);
            $remaining_output -= 1;
            // Follow the chain to get the next index value
            let next = $next_chain[match_row_idx as usize];

            if $remaining_output == 0 {
                // In case current input index is the last, and no more chain values left
                // returning None as whole input has been scanned
                let next_offset = if $input_idx == $hash_values.len() - 1 && next == 0 {
                    None
                } else {
                    Some(($input_idx, Some(next)))
                };
                return ($input_indices, $match_indices, next_offset);
            }
            if next == 0 {
                // end of list
                break;
            }
            i = next - 1;
        }
    };
}

// Trait defining methods that must be implemented by a hash map type to be used for joins.
pub trait JoinHashMapType {
    /// The type of list used to store the next list
    type NextType: IndexMut<usize, Output = u64>;
    /// Extend with zero
    fn extend_zero(&mut self, len: usize);
    /// Returns mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut RawTable<(u64, u64)>, &mut Self::NextType);
    /// Returns a reference to the hash map.
    fn get_map(&self) -> &RawTable<(u64, u64)>;
    /// Returns a reference to the next.
    fn get_list(&self) -> &Self::NextType;

    /// Updates hashmap from iterator of row indices & row hashes pairs.
    fn update_from_iter<'a>(
        &mut self,
        iter: impl Iterator<Item = (usize, &'a u64)>,
        deleted_offset: usize,
    ) {
        let (mut_map, mut_list) = self.get_mut();
        for (row, hash_value) in iter {
            let item = mut_map.get_mut(*hash_value, |(hash, _)| *hash_value == *hash);
            if let Some((_, index)) = item {
                // Already exists: add index to next array
                let prev_index = *index;
                // Store new value inside hashmap
                *index = (row + 1) as u64;
                // Update chained Vec at `row` with previous value
                mut_list[row - deleted_offset] = prev_index;
            } else {
                mut_map.insert(
                    *hash_value,
                    // store the value + 1 as 0 value reserved for end of list
                    (*hash_value, (row + 1) as u64),
                    |(hash, _)| *hash,
                );
                // chained list at `row` is already initialized with 0
                // meaning end of list
            }
        }
    }

    /// Returns all pairs of row indices matched by hash.
    ///
    /// This method only compares hashes, so additional further check for actual values
    /// equality may be required.
    fn get_matched_indices<'a>(
        &self,
        iter: impl Iterator<Item = (usize, &'a u64)>,
        deleted_offset: Option<usize>,
    ) -> (UInt32BufferBuilder, UInt64BufferBuilder) {
        let mut input_indices = UInt32BufferBuilder::new(0);
        let mut match_indices = UInt64BufferBuilder::new(0);

        let hash_map = self.get_map();
        let next_chain = self.get_list();
        for (row_idx, hash_value) in iter {
            // Get the hash and find it in the index
            if let Some((_, index)) =
                hash_map.get(*hash_value, |(hash, _)| *hash_value == *hash)
            {
                let mut i = *index - 1;
                loop {
                    let match_row_idx = if let Some(offset) = deleted_offset {
                        // This arguments means that we prune the next index way before here.
                        if i < offset as u64 {
                            // End of the list due to pruning
                            break;
                        }
                        i - offset as u64
                    } else {
                        i
                    };
                    match_indices.append(match_row_idx);
                    input_indices.append(row_idx as u32);
                    // Follow the chain to get the next index value
                    let next = next_chain[match_row_idx as usize];
                    if next == 0 {
                        // end of list
                        break;
                    }
                    i = next - 1;
                }
            }
        }

        (input_indices, match_indices)
    }

    /// Matches hashes with taking limit and offset into account.
    /// Returns pairs of matched indices along with the starting point for next
    /// matching iteration (`None` if limit has not been reached).
    ///
    /// This method only compares hashes, so additional further check for actual values
    /// equality may be required.
    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        deleted_offset: Option<usize>,
        limit: usize,
        offset: JoinHashMapOffset,
    ) -> (
        UInt32BufferBuilder,
        UInt64BufferBuilder,
        Option<JoinHashMapOffset>,
    ) {
        let mut input_indices = UInt32BufferBuilder::new(0);
        let mut match_indices = UInt64BufferBuilder::new(0);

        let mut remaining_output = limit;

        let hash_map: &RawTable<(u64, u64)> = self.get_map();
        let next_chain = self.get_list();

        // Calculate initial `hash_values` index before iterating
        let to_skip = match offset {
            // None `initial_next_idx` indicates that `initial_idx` processing has'n been started
            (initial_idx, None) => initial_idx,
            // Zero `initial_next_idx` indicates that `initial_idx` has been processed during
            // previous iteration, and it should be skipped
            (initial_idx, Some(0)) => initial_idx + 1,
            // Otherwise, process remaining `initial_idx` matches by traversing `next_chain`,
            // to start with the next index
            (initial_idx, Some(initial_next_idx)) => {
                chain_traverse!(
                    input_indices,
                    match_indices,
                    hash_values,
                    next_chain,
                    initial_idx,
                    initial_next_idx,
                    deleted_offset,
                    remaining_output
                );

                initial_idx + 1
            }
        };

        let mut row_idx = to_skip;
        for hash_value in &hash_values[to_skip..] {
            if let Some((_, index)) =
                hash_map.get(*hash_value, |(hash, _)| *hash_value == *hash)
            {
                chain_traverse!(
                    input_indices,
                    match_indices,
                    hash_values,
                    next_chain,
                    row_idx,
                    index,
                    deleted_offset,
                    remaining_output
                );
            }
            row_idx += 1;
        }

        (input_indices, match_indices, None)
    }
}

/// Implementation of `JoinHashMapType` for `JoinHashMap`.
impl JoinHashMapType for JoinHashMap {
    type NextType = Vec<u64>;

    // Void implementation
    fn extend_zero(&mut self, _: usize) {}

    /// Get mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut RawTable<(u64, u64)>, &mut Self::NextType) {
        (&mut self.map, &mut self.next)
    }

    /// Get a reference to the hash map.
    fn get_map(&self) -> &RawTable<(u64, u64)> {
        &self.map
    }

    /// Get a reference to the next.
    fn get_list(&self) -> &Self::NextType {
        &self.next
    }
}

impl fmt::Debug for JoinHashMap {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(PhysicalExprRef, PhysicalExprRef)>;
/// Reference for JoinOn.
pub type JoinOnRef<'a> = &'a [(PhysicalExprRef, PhysicalExprRef)];

/// Checks whether the schemas "left" and "right" and columns "on" represent a valid join.
/// They are valid whenever their columns' intersection equals the set `on`
pub fn check_join_is_valid(left: &Schema, right: &Schema, on: JoinOnRef) -> Result<()> {
    let left: HashSet<Column> = left
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();
    let right: HashSet<Column> = right
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();

    check_join_set_is_valid(&left, &right, on)
}

/// Checks whether the sets left, right and on compose a valid join.
/// They are valid whenever their intersection equals the set `on`
fn check_join_set_is_valid(
    left: &HashSet<Column>,
    right: &HashSet<Column>,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
) -> Result<()> {
    let on_left = &on
        .iter()
        .flat_map(|on| collect_columns(&on.0))
        .collect::<HashSet<_>>();
    let left_missing = on_left.difference(left).collect::<HashSet<_>>();

    let on_right = &on
        .iter()
        .flat_map(|on| collect_columns(&on.1))
        .collect::<HashSet<_>>();
    let right_missing = on_right.difference(right).collect::<HashSet<_>>();

    if !left_missing.is_empty() | !right_missing.is_empty() {
        return plan_err!(
            "The left or right side of the join does not have all columns on \"on\": \nMissing on the left: {left_missing:?}\nMissing on the right: {right_missing:?}"
        );
    };

    Ok(())
}

/// Calculate the OutputPartitioning for Partitioned Join
pub fn partitioned_join_output_partitioning(
    join_type: JoinType,
    left_partitioning: &Partitioning,
    right_partitioning: &Partitioning,
    left_columns_len: usize,
) -> Partitioning {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
            left_partitioning.clone()
        }
        JoinType::RightSemi | JoinType::RightAnti => right_partitioning.clone(),
        JoinType::Right => {
            adjust_right_output_partitioning(right_partitioning, left_columns_len)
        }
        JoinType::Full => {
            Partitioning::UnknownPartitioning(right_partitioning.partition_count())
        }
    }
}

/// Adjust the right out partitioning to new Column Index
pub fn adjust_right_output_partitioning(
    right_partitioning: &Partitioning,
    left_columns_len: usize,
) -> Partitioning {
    match right_partitioning {
        Partitioning::Hash(exprs, size) => {
            let new_exprs = exprs
                .iter()
                .map(|expr| add_offset_to_expr(expr.clone(), left_columns_len))
                .collect();
            Partitioning::Hash(new_exprs, *size)
        }
        result => result.clone(),
    }
}

/// Replaces the right column (first index in the `on_column` tuple) with
/// the left column (zeroth index in the tuple) inside `right_ordering`.
fn replace_on_columns_of_right_ordering(
    on_columns: &[(PhysicalExprRef, PhysicalExprRef)],
    right_ordering: &mut [PhysicalSortExpr],
) -> Result<()> {
    for (left_col, right_col) in on_columns {
        for item in right_ordering.iter_mut() {
            let new_expr = item
                .expr
                .clone()
                .transform(&|e| {
                    if e.eq(right_col) {
                        Ok(Transformed::yes(left_col.clone()))
                    } else {
                        Ok(Transformed::no(e))
                    }
                })
                .data()?;
            item.expr = new_expr;
        }
    }
    Ok(())
}

fn offset_ordering(
    ordering: LexOrderingRef,
    join_type: &JoinType,
    offset: usize,
) -> Vec<PhysicalSortExpr> {
    match join_type {
        // In the case below, right ordering should be offseted with the left
        // side length, since we append the right table to the left table.
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => ordering
            .iter()
            .map(|sort_expr| PhysicalSortExpr {
                expr: add_offset_to_expr(sort_expr.expr.clone(), offset),
                options: sort_expr.options,
            })
            .collect(),
        _ => ordering.to_vec(),
    }
}

/// Calculate the output ordering of a given join operation.
pub fn calculate_join_output_ordering(
    left_ordering: LexOrderingRef,
    right_ordering: LexOrderingRef,
    join_type: JoinType,
    on_columns: &[(PhysicalExprRef, PhysicalExprRef)],
    left_columns_len: usize,
    maintains_input_order: &[bool],
    probe_side: Option<JoinSide>,
) -> Option<LexOrdering> {
    let output_ordering = match maintains_input_order {
        [true, false] => {
            // Special case, we can prefix ordering of right side with the ordering of left side.
            if join_type == JoinType::Inner && probe_side == Some(JoinSide::Left) {
                replace_on_columns_of_right_ordering(
                    on_columns,
                    &mut right_ordering.to_vec(),
                )
                .ok()?;
                merge_vectors(
                    left_ordering,
                    &offset_ordering(right_ordering, &join_type, left_columns_len),
                )
            } else {
                left_ordering.to_vec()
            }
        }
        [false, true] => {
            // Special case, we can prefix ordering of left side with the ordering of right side.
            if join_type == JoinType::Inner && probe_side == Some(JoinSide::Right) {
                replace_on_columns_of_right_ordering(
                    on_columns,
                    &mut right_ordering.to_vec(),
                )
                .ok()?;
                merge_vectors(
                    &offset_ordering(right_ordering, &join_type, left_columns_len),
                    left_ordering,
                )
            } else {
                offset_ordering(right_ordering, &join_type, left_columns_len)
            }
        }
        // Doesn't maintain ordering, output ordering is None.
        [false, false] => return None,
        [true, true] => unreachable!("Cannot maintain ordering of both sides"),
        _ => unreachable!("Join operators can not have more than two children"),
    };
    (!output_ordering.is_empty()).then_some(output_ordering)
}

/// Information about the index and placement (left or right) of the columns
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnIndex {
    /// Index of the column
    pub index: usize,
    /// Whether the column is at the left or right side
    pub side: JoinSide,
}

/// Filter applied before join output
#[derive(Debug, Clone)]
pub struct JoinFilter {
    /// Filter expression
    expression: Arc<dyn PhysicalExpr>,
    /// Column indices required to construct intermediate batch for filtering
    column_indices: Vec<ColumnIndex>,
    /// Physical schema of intermediate batch
    schema: Schema,
}

impl JoinFilter {
    /// Creates new JoinFilter
    pub fn new(
        expression: Arc<dyn PhysicalExpr>,
        column_indices: Vec<ColumnIndex>,
        schema: Schema,
    ) -> JoinFilter {
        JoinFilter {
            expression,
            column_indices,
            schema,
        }
    }

    /// Helper for building ColumnIndex vector from left and right indices
    pub fn build_column_indices(
        left_indices: Vec<usize>,
        right_indices: Vec<usize>,
    ) -> Vec<ColumnIndex> {
        left_indices
            .into_iter()
            .map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Left,
            })
            .chain(right_indices.into_iter().map(|i| ColumnIndex {
                index: i,
                side: JoinSide::Right,
            }))
            .collect()
    }

    /// Filter expression
    pub fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expression
    }

    /// Column indices for intermediate batch creation
    pub fn column_indices(&self) -> &[ColumnIndex] {
        &self.column_indices
    }

    /// Intermediate batch schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Returns the output field given the input field. Outer joins may
/// insert nulls even if the input was not null
///
fn output_join_field(old_field: &Field, join_type: &JoinType, is_left: bool) -> Field {
    let force_nullable = match join_type {
        JoinType::Inner => false,
        JoinType::Left => !is_left, // right input is padded with nulls
        JoinType::Right => is_left, // left input is padded with nulls
        JoinType::Full => true,     // both inputs can be padded with nulls
        JoinType::LeftSemi => false, // doesn't introduce nulls
        JoinType::RightSemi => false, // doesn't introduce nulls
        JoinType::LeftAnti => false, // doesn't introduce nulls (or can it??)
        JoinType::RightAnti => false, // doesn't introduce nulls (or can it??)
    };

    if force_nullable {
        old_field.clone().with_nullable(true)
    } else {
        old_field.clone()
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    join_type: &JoinType,
) -> (Schema, Vec<ColumnIndex>) {
    let (fields, column_indices): (SchemaBuilder, Vec<ColumnIndex>) = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            let left_fields = left
                .fields()
                .iter()
                .map(|f| output_join_field(f, join_type, true))
                .enumerate()
                .map(|(index, f)| {
                    (
                        f,
                        ColumnIndex {
                            index,
                            side: JoinSide::Left,
                        },
                    )
                });
            let right_fields = right
                .fields()
                .iter()
                .map(|f| output_join_field(f, join_type, false))
                .enumerate()
                .map(|(index, f)| {
                    (
                        f,
                        ColumnIndex {
                            index,
                            side: JoinSide::Right,
                        },
                    )
                });

            // left then right
            left_fields.chain(right_fields).unzip()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => left
            .fields()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Left,
                    },
                )
            })
            .unzip(),
        JoinType::RightSemi | JoinType::RightAnti => right
            .fields()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Right,
                    },
                )
            })
            .unzip(),
    };

    (fields.finish(), column_indices)
}

/// A [`OnceAsync`] can be used to run an async closure once, with subsequent calls
/// to [`OnceAsync::once`] returning a [`OnceFut`] to the same asynchronous computation
///
/// This is useful for joins where the results of one child are buffered in memory
/// and shared across potentially multiple output partitions
pub(crate) struct OnceAsync<T> {
    fut: Mutex<Option<OnceFut<T>>>,
}

impl<T> Default for OnceAsync<T> {
    fn default() -> Self {
        Self {
            fut: Mutex::new(None),
        }
    }
}

impl<T> std::fmt::Debug for OnceAsync<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnceAsync")
    }
}

impl<T: 'static> OnceAsync<T> {
    /// If this is the first call to this function on this object, will invoke
    /// `f` to obtain a future and return a [`OnceFut`] referring to this
    ///
    /// If this is not the first call, will return a [`OnceFut`] referring
    /// to the same future as was returned by the first call
    pub(crate) fn once<F, Fut>(&self, f: F) -> OnceFut<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.fut
            .lock()
            .get_or_insert_with(|| OnceFut::new(f()))
            .clone()
    }
}

/// The shared future type used internally within [`OnceAsync`]
type OnceFutPending<T> = Shared<BoxFuture<'static, SharedResult<Arc<T>>>>;

/// A [`OnceFut`] represents a shared asynchronous computation, that will be evaluated
/// once for all [`Clone`]'s, with [`OnceFut::get`] providing a non-consuming interface
/// to drive the underlying [`Future`] to completion
pub(crate) struct OnceFut<T> {
    state: OnceFutState<T>,
}

impl<T> Clone for OnceFut<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// A shared state between statistic aggregators for a join
/// operation.
#[derive(Clone, Debug, Default)]
struct PartialJoinStatistics {
    pub num_rows: usize,
    pub column_statistics: Vec<ColumnStatistics>,
}

/// Estimate the statistics for the given join's output.
pub(crate) fn estimate_join_statistics(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: &JoinType,
    schema: &Schema,
) -> Result<Statistics> {
    let left_stats = left.statistics()?;
    let right_stats = right.statistics()?;

    let join_stats = estimate_join_cardinality(join_type, left_stats, right_stats, &on);
    let (num_rows, column_statistics) = match join_stats {
        Some(stats) => (Precision::Inexact(stats.num_rows), stats.column_statistics),
        None => (Precision::Absent, Statistics::unknown_column(schema)),
    };
    Ok(Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics,
    })
}

// Estimate the cardinality for the given join with input statistics.
fn estimate_join_cardinality(
    join_type: &JoinType,
    left_stats: Statistics,
    right_stats: Statistics,
    on: &JoinOn,
) -> Option<PartialJoinStatistics> {
    let (left_col_stats, right_col_stats) = on
        .iter()
        .map(|(left, right)| {
            match (
                left.as_any().downcast_ref::<Column>(),
                right.as_any().downcast_ref::<Column>(),
            ) {
                (Some(left), Some(right)) => (
                    left_stats.column_statistics[left.index()].clone(),
                    right_stats.column_statistics[right.index()].clone(),
                ),
                _ => (
                    ColumnStatistics::new_unknown(),
                    ColumnStatistics::new_unknown(),
                ),
            }
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();

    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let ij_cardinality = estimate_inner_join_cardinality(
                Statistics {
                    num_rows: left_stats.num_rows.clone(),
                    total_byte_size: Precision::Absent,
                    column_statistics: left_col_stats,
                },
                Statistics {
                    num_rows: right_stats.num_rows.clone(),
                    total_byte_size: Precision::Absent,
                    column_statistics: right_col_stats,
                },
            )?;

            // The cardinality for inner join can also be used to estimate
            // the cardinality of left/right/full outer joins as long as it
            // it is greater than the minimum cardinality constraints of these
            // joins (so that we don't underestimate the cardinality).
            let cardinality = match join_type {
                JoinType::Inner => ij_cardinality,
                JoinType::Left => ij_cardinality.max(&left_stats.num_rows),
                JoinType::Right => ij_cardinality.max(&right_stats.num_rows),
                JoinType::Full => ij_cardinality
                    .max(&left_stats.num_rows)
                    .add(&ij_cardinality.max(&right_stats.num_rows))
                    .sub(&ij_cardinality),
                _ => unreachable!(),
            };

            Some(PartialJoinStatistics {
                num_rows: *cardinality.get_value()?,
                // We don't do anything specific here, just combine the existing
                // statistics which might yield subpar results (although it is
                // true, esp regarding min/max). For a better estimation, we need
                // filter selectivity analysis first.
                column_statistics: left_stats
                    .column_statistics
                    .into_iter()
                    .chain(right_stats.column_statistics)
                    .collect(),
            })
        }

        // For SemiJoins estimation result is either zero, in cases when inputs
        // are non-overlapping according to statistics, or equal to number of rows
        // for outer input
        JoinType::LeftSemi | JoinType::RightSemi => {
            let (outer_stats, inner_stats) = match join_type {
                JoinType::LeftSemi => (left_stats, right_stats),
                _ => (right_stats, left_stats),
            };
            let cardinality = match estimate_disjoint_inputs(&outer_stats, &inner_stats) {
                Some(estimation) => *estimation.get_value()?,
                None => *outer_stats.num_rows.get_value()?,
            };

            Some(PartialJoinStatistics {
                num_rows: cardinality,
                column_statistics: outer_stats.column_statistics,
            })
        }

        // For AntiJoins estimation always equals to outer statistics, as
        // non-overlapping inputs won't affect estimation
        JoinType::LeftAnti | JoinType::RightAnti => {
            let outer_stats = match join_type {
                JoinType::LeftAnti => left_stats,
                _ => right_stats,
            };

            Some(PartialJoinStatistics {
                num_rows: *outer_stats.num_rows.get_value()?,
                column_statistics: outer_stats.column_statistics,
            })
        }
    }
}

/// Estimate the inner join cardinality by using the basic building blocks of
/// column-level statistics and the total row count. This is a very naive and
/// a very conservative implementation that can quickly give up if there is not
/// enough input statistics.
fn estimate_inner_join_cardinality(
    left_stats: Statistics,
    right_stats: Statistics,
) -> Option<Precision<usize>> {
    // Immediatedly return if inputs considered as non-overlapping
    if let Some(estimation) = estimate_disjoint_inputs(&left_stats, &right_stats) {
        return Some(estimation);
    };

    // The algorithm here is partly based on the non-histogram selectivity estimation
    // from Spark's Catalyst optimizer.
    let mut join_selectivity = Precision::Absent;
    for (left_stat, right_stat) in left_stats
        .column_statistics
        .iter()
        .zip(right_stats.column_statistics.iter())
    {
        // Break if any of statistics bounds are undefined
        if left_stat.min_value.get_value().is_none()
            || left_stat.max_value.get_value().is_none()
            || right_stat.min_value.get_value().is_none()
            || right_stat.max_value.get_value().is_none()
        {
            return None;
        }

        let left_max_distinct = max_distinct_count(&left_stats.num_rows, left_stat);
        let right_max_distinct = max_distinct_count(&right_stats.num_rows, right_stat);
        let max_distinct = left_max_distinct.max(&right_max_distinct);
        if max_distinct.get_value().is_some() {
            // Seems like there are a few implementations of this algorithm that implement
            // exponential decay for the selectivity (like Hive's Optiq Optimizer). Needs
            // further exploration.
            join_selectivity = max_distinct;
        }
    }

    // With the assumption that the smaller input's domain is generally represented in the bigger
    // input's domain, we can estimate the inner join's cardinality by taking the cartesian product
    // of the two inputs and normalizing it by the selectivity factor.
    let left_num_rows = left_stats.num_rows.get_value()?;
    let right_num_rows = right_stats.num_rows.get_value()?;
    match join_selectivity {
        Precision::Exact(value) if value > 0 => {
            Some(Precision::Exact((left_num_rows * right_num_rows) / value))
        }
        Precision::Inexact(value) if value > 0 => {
            Some(Precision::Inexact((left_num_rows * right_num_rows) / value))
        }
        // Since we don't have any information about the selectivity (which is derived
        // from the number of distinct rows information) we can give up here for now.
        // And let other passes handle this (otherwise we would need to produce an
        // overestimation using just the cartesian product).
        _ => None,
    }
}

/// Estimates if inputs are non-overlapping, using input statistics.
/// If inputs are disjoint, returns zero estimation, otherwise returns None
fn estimate_disjoint_inputs(
    left_stats: &Statistics,
    right_stats: &Statistics,
) -> Option<Precision<usize>> {
    for (left_stat, right_stat) in left_stats
        .column_statistics
        .iter()
        .zip(right_stats.column_statistics.iter())
    {
        // If there is no overlap in any of the join columns, this means the join
        // itself is disjoint and the cardinality is 0. Though we can only assume
        // this when the statistics are exact (since it is a very strong assumption).
        let left_min_val = left_stat.min_value.get_value();
        let right_max_val = right_stat.max_value.get_value();
        if left_min_val.is_some()
            && right_max_val.is_some()
            && left_min_val > right_max_val
        {
            return Some(
                if left_stat.min_value.is_exact().unwrap_or(false)
                    && right_stat.max_value.is_exact().unwrap_or(false)
                {
                    Precision::Exact(0)
                } else {
                    Precision::Inexact(0)
                },
            );
        }

        let left_max_val = left_stat.max_value.get_value();
        let right_min_val = right_stat.min_value.get_value();
        if left_max_val.is_some()
            && right_min_val.is_some()
            && left_max_val < right_min_val
        {
            return Some(
                if left_stat.max_value.is_exact().unwrap_or(false)
                    && right_stat.min_value.is_exact().unwrap_or(false)
                {
                    Precision::Exact(0)
                } else {
                    Precision::Inexact(0)
                },
            );
        }
    }

    None
}

/// Estimate the number of maximum distinct values that can be present in the
/// given column from its statistics. If distinct_count is available, uses it
/// directly. Otherwise, if the column is numeric and has min/max values, it
/// estimates the maximum distinct count from those.
fn max_distinct_count(
    num_rows: &Precision<usize>,
    stats: &ColumnStatistics,
) -> Precision<usize> {
    match &stats.distinct_count {
        dc @ (Precision::Exact(_) | Precision::Inexact(_)) => dc.clone(),
        _ => {
            // The number can never be greater than the number of rows we have
            // minus the nulls (since they don't count as distinct values).
            let result = match num_rows {
                Precision::Absent => Precision::Absent,
                Precision::Inexact(count) => {
                    // To safeguard against inexact number of rows (e.g. 0) being smaller than
                    // an exact null count we need to do a checked subtraction.
                    match count.checked_sub(*stats.null_count.get_value().unwrap_or(&0)) {
                        None => Precision::Inexact(0),
                        Some(non_null_count) => Precision::Inexact(non_null_count),
                    }
                }
                Precision::Exact(count) => {
                    let count = count - stats.null_count.get_value().unwrap_or(&0);
                    if stats.null_count.is_exact().unwrap_or(false) {
                        Precision::Exact(count)
                    } else {
                        Precision::Inexact(count)
                    }
                }
            };
            // Cap the estimate using the number of possible values:
            if let (Some(min), Some(max)) =
                (stats.min_value.get_value(), stats.max_value.get_value())
            {
                if let Some(range_dc) = Interval::try_new(min.clone(), max.clone())
                    .ok()
                    .and_then(|e| e.cardinality())
                {
                    let range_dc = range_dc as usize;
                    // Note that the `unwrap` calls in the below statement are safe.
                    return if matches!(result, Precision::Absent)
                        || &range_dc < result.get_value().unwrap()
                    {
                        if stats.min_value.is_exact().unwrap()
                            && stats.max_value.is_exact().unwrap()
                        {
                            Precision::Exact(range_dc)
                        } else {
                            Precision::Inexact(range_dc)
                        }
                    } else {
                        result
                    };
                }
            }

            result
        }
    }
}

enum OnceFutState<T> {
    Pending(OnceFutPending<T>),
    Ready(SharedResult<Arc<T>>),
}

impl<T> Clone for OnceFutState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending(p) => Self::Pending(p.clone()),
            Self::Ready(r) => Self::Ready(r.clone()),
        }
    }
}

impl<T: 'static> OnceFut<T> {
    /// Create a new [`OnceFut`] from a [`Future`]
    pub(crate) fn new<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        Self {
            state: OnceFutState::Pending(
                fut.map(|res| res.map(Arc::new).map_err(Arc::new))
                    .boxed()
                    .shared(),
            ),
        }
    }

    /// Get the result of the computation if it is ready, without consuming it
    pub(crate) fn get(&mut self, cx: &mut Context<'_>) -> Poll<Result<&T>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        }

        // Cannot use loop as this would trip up the borrow checker
        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => Poll::Ready(
                r.as_ref()
                    .map(|r| r.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e.clone()))),
            ),
        }
    }

    /// Get shared reference to the result of the computation if it is ready, without consuming it
    pub(crate) fn get_shared(&mut self, cx: &mut Context<'_>) -> Poll<Result<Arc<T>>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        }

        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => Poll::Ready(
                r.clone()
                    .map_err(|e| DataFusionError::External(Box::new(e))),
            ),
        }
    }
}

/// Some type `join_type` of join need to maintain the matched indices bit map for the left side, and
/// use the bit map to generate the part of result of the join.
///
/// For example of the `Left` join, in each iteration of right side, can get the matched result, but need
/// to maintain the matched indices bit map to get the unmatched row for the left side.
pub(crate) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::LeftAnti | JoinType::LeftSemi | JoinType::Full
    )
}

/// In the end of join execution, need to use bit map of the matched
/// indices to generate the final left and right indices.
///
/// For example:
///
/// 1. left_bit_map: `[true, false, true, true, false]`
/// 2. join_type: `Left`
///
/// The result is: `([1,4], [null, null])`
pub(crate) fn get_final_indices_from_bit_map(
    left_bit_map: &BooleanBufferBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let left_size = left_bit_map.len();
    let left_indices = if join_type == JoinType::LeftSemi {
        (0..left_size)
            .filter_map(|idx| (left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    } else {
        // just for `Left`, `LeftAnti` and `Full` join
        // `LeftAnti`, `Left` and `Full` will produce the unmatched left row finally
        (0..left_size)
            .filter_map(|idx| (!left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    };
    // right_indices
    // all the element in the right side is None
    let mut builder = UInt32Builder::with_capacity(left_indices.len());
    builder.append_nulls(left_indices.len());
    let right_indices = builder.finish();
    (left_indices, right_indices)
}

pub(crate) fn apply_join_filter_to_indices(
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
    filter: &JoinFilter,
    build_side: JoinSide,
) -> Result<(UInt64Array, UInt32Array)> {
    if build_indices.is_empty() && probe_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    };

    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        build_input_buffer,
        probe_batch,
        &build_indices,
        &probe_indices,
        filter.column_indices(),
        build_side,
    )?;
    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let mask = as_boolean_array(&filter_result)?;

    let left_filtered = compute::filter(&build_indices, mask)?;
    let right_filtered = compute::filter(&probe_indices, mask)?;
    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
pub(crate) fn build_batch_from_indices(
    schema: &Schema,
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: &UInt64Array,
    probe_indices: &UInt32Array,
    column_indices: &[ColumnIndex],
    build_side: JoinSide,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(build_indices.len()));

        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &options,
        )?);
    }

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.side == build_side {
            let array = build_input_buffer.column(column_index.index);
            if array.is_empty() || build_indices.null_count() == build_indices.len() {
                // Outer join would generate a null index when finding no match at our side.
                // Therefore, it's possible we are empty but need to populate an n-length null array,
                // where n is the length of the index array.
                assert_eq!(build_indices.null_count(), build_indices.len());
                new_null_array(array.data_type(), build_indices.len())
            } else {
                compute::take(array.as_ref(), build_indices, None)?
            }
        } else {
            let array = probe_batch.column(column_index.index);
            if array.is_empty() || probe_indices.null_count() == probe_indices.len() {
                assert_eq!(probe_indices.null_count(), probe_indices.len());
                new_null_array(array.data_type(), probe_indices.len())
            } else {
                compute::take(array.as_ref(), probe_indices, None)?
            }
        };
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// The input is the matched indices for left and right and
/// adjust the indices according to the join type
pub(crate) fn adjust_indices_by_join_type(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    adjust_range: Range<usize>,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    match join_type {
        JoinType::Inner => {
            // matched
            (left_indices, right_indices)
        }
        JoinType::Left => {
            // matched
            (left_indices, right_indices)
            // unmatched left row will be produced in the end of loop, and it has been set in the left visited bitmap
        }
        JoinType::Right | JoinType::Full => {
            // matched
            // unmatched right row will be produced in this batch
            let right_unmatched_indices = get_anti_indices(adjust_range, &right_indices);
            // combine the matched and unmatched right result together
            append_right_indices(left_indices, right_indices, right_unmatched_indices)
        }
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(adjust_range, &right_indices);
            // the left_indices will not be used later for the `right semi` join
            (left_indices, right_indices)
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(adjust_range, &right_indices);
            // the left_indices will not be used later for the `right anti` join
            (left_indices, right_indices)
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            (
                UInt64Array::from_iter_values(vec![]),
                UInt32Array::from_iter_values(vec![]),
            )
        }
    }
}

/// Appends the `right_unmatched_indices` to the `right_indices`,
/// and fills Null to tail of `left_indices` to
/// keep the length of `right_indices` and `left_indices` consistent.
pub(crate) fn append_right_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    right_unmatched_indices: UInt32Array,
) -> (UInt64Array, UInt32Array) {
    // left_indices, right_indices and right_unmatched_indices must not contain the null value
    if right_unmatched_indices.is_empty() {
        (left_indices, right_indices)
    } else {
        let unmatched_size = right_unmatched_indices.len();
        // the new left indices: left_indices + null array
        // the new right indices: right_indices + right_unmatched_indices
        let new_left_indices = left_indices
            .iter()
            .chain(std::iter::repeat(None).take(unmatched_size))
            .collect::<UInt64Array>();
        let new_right_indices = right_indices
            .iter()
            .chain(right_unmatched_indices.iter())
            .collect::<UInt32Array>();
        (new_left_indices, new_right_indices)
    }
}

/// Returns `range` indices which are not present in `input_indices`
pub(crate) fn get_anti_indices<T: ArrowPrimitiveType>(
    range: Range<usize>,
    input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .flatten()
        .map(|v| v.as_usize())
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the anti index
    (range)
        .filter_map(|idx| {
            (!bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx))
        })
        .collect::<PrimitiveArray<T>>()
}

/// Returns intersection of `range` and `input_indices` omitting duplicates
pub(crate) fn get_semi_indices<T: ArrowPrimitiveType>(
    range: Range<usize>,
    input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .flatten()
        .map(|v| v.as_usize())
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the semi index
    (range)
        .filter_map(|idx| {
            (bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx))
        })
        .collect::<PrimitiveArray<T>>()
}

/// Metrics for build & probe joins
#[derive(Clone, Debug)]
pub(crate) struct BuildProbeJoinMetrics {
    /// Total time for collecting build-side of join
    pub(crate) build_time: metrics::Time,
    /// Number of batches consumed by build-side
    pub(crate) build_input_batches: metrics::Count,
    /// Number of rows consumed by build-side
    pub(crate) build_input_rows: metrics::Count,
    /// Memory used by build-side in bytes
    pub(crate) build_mem_used: metrics::Gauge,
    /// Total time for joining probe-side batches to the build-side batches
    pub(crate) join_time: metrics::Time,
    /// Number of batches consumed by probe-side of this operator
    pub(crate) input_batches: metrics::Count,
    /// Number of rows consumed by probe-side this operator
    pub(crate) input_rows: metrics::Count,
    /// Number of batches produced by this operator
    pub(crate) output_batches: metrics::Count,
    /// Number of rows produced by this operator
    pub(crate) output_rows: metrics::Count,
}

impl BuildProbeJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);

        let build_time = MetricBuilder::new(metrics).subset_time("build_time", partition);

        let build_input_batches =
            MetricBuilder::new(metrics).counter("build_input_batches", partition);

        let build_input_rows =
            MetricBuilder::new(metrics).counter("build_input_rows", partition);

        let build_mem_used =
            MetricBuilder::new(metrics).gauge("build_mem_used", partition);

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            build_time,
            build_input_batches,
            build_input_rows,
            build_mem_used,
            join_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }
}

/// The `handle_state` macro is designed to process the result of a state-changing
/// operation, encountered e.g. in implementations of `EagerJoinStream`. It
/// operates on a `StatefulStreamResult` by matching its variants and executing
/// corresponding actions. This macro is used to streamline code that deals with
/// state transitions, reducing boilerplate and improving readability.
///
/// # Cases
///
/// - `Ok(StatefulStreamResult::Continue)`: Continues the loop, indicating the
///   stream join operation should proceed to the next step.
/// - `Ok(StatefulStreamResult::Ready(result))`: Returns a `Poll::Ready` with the
///   result, either yielding a value or indicating the stream is awaiting more
///   data.
/// - `Err(e)`: Returns a `Poll::Ready` containing an error, signaling an issue
///   during the stream join operation.
///
/// # Arguments
///
/// * `$match_case`: An expression that evaluates to a `Result<StatefulStreamResult<_>>`.
#[macro_export]
macro_rules! handle_state {
    ($match_case:expr) => {
        match $match_case {
            Ok(StatefulStreamResult::Continue) => continue,
            Ok(StatefulStreamResult::Ready(result)) => {
                Poll::Ready(Ok(result).transpose())
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    };
}

/// The `handle_async_state` macro adapts the `handle_state` macro for use in
/// asynchronous operations, particularly when dealing with `Poll` results within
/// async traits like `EagerJoinStream`. It polls the asynchronous state-changing
/// function using `poll_unpin` and then passes the result to `handle_state` for
/// further processing.
///
/// # Arguments
///
/// * `$state_func`: An async function or future that returns a
///   `Result<StatefulStreamResult<_>>`.
/// * `$cx`: The context to be passed for polling, usually of type `&mut Context`.
///
#[macro_export]
macro_rules! handle_async_state {
    ($state_func:expr, $cx:expr) => {
        $crate::handle_state!(ready!($state_func.poll_unpin($cx)))
    };
}

/// Represents the result of an operation on stateful join stream.
///
/// This enumueration indicates whether the state produced a result that is
/// ready for use (`Ready`) or if the operation requires continuation (`Continue`).
///
/// Variants:
/// - `Ready(T)`: Indicates that the operation is complete with a result of type `T`.
/// - `Continue`: Indicates that the operation is not yet complete and requires further
///   processing or more data. When this variant is returned, it typically means that the
///   current invocation of the state did not produce a final result, and the operation
///   should be invoked again later with more data and possibly with a different state.
pub enum StatefulStreamResult<T> {
    Ready(T),
    Continue,
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::*;

    use arrow::datatypes::{DataType, Fields};
    use arrow::error::{ArrowError, Result as ArrowResult};
    use arrow_schema::SortOptions;

    use datafusion_common::stats::Precision::{Absent, Exact, Inexact};
    use datafusion_common::{arrow_datafusion_err, arrow_err, ScalarValue};

    fn check(
        left: &[Column],
        right: &[Column],
        on: &[(PhysicalExprRef, PhysicalExprRef)],
    ) -> Result<()> {
        let left = left
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        let right = right
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        check_join_set_is_valid(&left, &right, on)
    }

    #[test]
    fn check_valid() -> Result<()> {
        let left = vec![Column::new("a", 0), Column::new("b1", 1)];
        let right = vec![Column::new("a", 0), Column::new("b2", 1)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("a", 0)) as _,
        )];

        check(&left, &right, on)?;
        Ok(())
    }

    #[test]
    fn check_not_in_right() {
        let left = vec![Column::new("a", 0), Column::new("b", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("a", 0)) as _,
        )];

        assert!(check(&left, &right, on).is_err());
    }

    #[tokio::test]
    async fn check_error_nesting() {
        let once_fut = OnceFut::<()>::new(async {
            arrow_err!(ArrowError::CsvError("some error".to_string()))
        });

        struct TestFut(OnceFut<()>);
        impl Future for TestFut {
            type Output = ArrowResult<()>;

            fn poll(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Self::Output> {
                match ready!(self.0.get(cx)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(Err(e.into())),
                }
            }
        }

        let res = TestFut(once_fut).await;
        let arrow_err_from_fut = res.expect_err("once_fut always return error");

        let wrapped_err = DataFusionError::from(arrow_err_from_fut);
        let root_err = wrapped_err.find_root();

        let _expected =
            arrow_datafusion_err!(ArrowError::CsvError("some error".to_owned()));

        assert!(matches!(root_err, _expected))
    }

    #[test]
    fn check_not_in_left() {
        let left = vec![Column::new("b", 0)];
        let right = vec![Column::new("a", 0)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("a", 0)) as _,
        )];

        assert!(check(&left, &right, on).is_err());
    }

    #[test]
    fn check_collision() {
        // column "a" would appear both in left and right
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("a", 0), Column::new("b", 1)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("b", 1)) as _,
        )];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn check_in_right() {
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("b", 0)) as _,
        )];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn test_join_schema() -> Result<()> {
        let a = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a_nulls = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let b = Schema::new(vec![Field::new("b", DataType::Int32, false)]);
        let b_nulls = Schema::new(vec![Field::new("b", DataType::Int32, true)]);

        let cases = vec![
            (&a, &b, JoinType::Inner, &a, &b),
            (&a, &b_nulls, JoinType::Inner, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Inner, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Inner, &a_nulls, &b_nulls),
            // right input of a `LEFT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Left, &a, &b_nulls),
            (&a, &b_nulls, JoinType::Left, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Left, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Left, &a_nulls, &b_nulls),
            // left input of a `RIGHT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Right, &a_nulls, &b),
            (&a, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Right, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            // Either input of a `FULL` join can be null
            (&a, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
        ];

        for (left_in, right_in, join_type, left_out, right_out) in cases {
            let (schema, _) = build_join_schema(left_in, right_in, &join_type);

            let expected_fields = left_out
                .fields()
                .iter()
                .cloned()
                .chain(right_out.fields().iter().cloned())
                .collect::<Fields>();

            let expected_schema = Schema::new(expected_fields);
            assert_eq!(
                schema,
                expected_schema,
                "Mismatch with left_in={}:{}, right_in={}:{}, join_type={:?}",
                left_in.fields()[0].name(),
                left_in.fields()[0].is_nullable(),
                right_in.fields()[0].name(),
                right_in.fields()[0].is_nullable(),
                join_type
            );
        }

        Ok(())
    }

    fn create_stats(
        num_rows: Option<usize>,
        column_stats: Vec<ColumnStatistics>,
        is_exact: bool,
    ) -> Statistics {
        Statistics {
            num_rows: if is_exact {
                num_rows.map(Precision::Exact)
            } else {
                num_rows.map(Precision::Inexact)
            }
            .unwrap_or(Precision::Absent),
            column_statistics: column_stats,
            total_byte_size: Precision::Absent,
        }
    }

    fn create_column_stats(
        min: Precision<i64>,
        max: Precision<i64>,
        distinct_count: Precision<usize>,
        null_count: Precision<usize>,
    ) -> ColumnStatistics {
        ColumnStatistics {
            distinct_count,
            min_value: min.map(ScalarValue::from),
            max_value: max.map(ScalarValue::from),
            null_count,
        }
    }

    type PartialStats = (
        usize,
        Precision<i64>,
        Precision<i64>,
        Precision<usize>,
        Precision<usize>,
    );

    // This is mainly for validating the all edge cases of the estimation, but
    // more advanced (and real world test cases) are below where we need some control
    // over the expected output (since it depends on join type to join type).
    #[test]
    fn test_inner_join_cardinality_single_column() -> Result<()> {
        let cases: Vec<(PartialStats, PartialStats, Option<Precision<usize>>)> = vec![
            // ------------------------------------------------
            // | left(rows, min, max, distinct, null_count),  |
            // | right(rows, min, max, distinct, null_count), |
            // | expected,                                    |
            // ------------------------------------------------

            // Cardinality computation
            // =======================
            //
            // distinct(left) == NaN, distinct(right) == NaN
            (
                (10, Inexact(1), Inexact(10), Absent, Absent),
                (10, Inexact(1), Inexact(10), Absent, Absent),
                Some(Inexact(10)),
            ),
            // range(left) > range(right)
            (
                (10, Inexact(6), Inexact(10), Absent, Absent),
                (10, Inexact(8), Inexact(10), Absent, Absent),
                Some(Inexact(20)),
            ),
            // range(right) > range(left)
            (
                (10, Inexact(8), Inexact(10), Absent, Absent),
                (10, Inexact(6), Inexact(10), Absent, Absent),
                Some(Inexact(20)),
            ),
            // range(left) > len(left), range(right) > len(right)
            (
                (10, Inexact(1), Inexact(15), Absent, Absent),
                (20, Inexact(1), Inexact(40), Absent, Absent),
                Some(Inexact(10)),
            ),
            // When we have distinct count.
            (
                (10, Inexact(1), Inexact(10), Inexact(10), Absent),
                (10, Inexact(1), Inexact(10), Inexact(10), Absent),
                Some(Inexact(10)),
            ),
            // distinct(left) > distinct(right)
            (
                (10, Inexact(1), Inexact(10), Inexact(5), Absent),
                (10, Inexact(1), Inexact(10), Inexact(2), Absent),
                Some(Inexact(20)),
            ),
            // distinct(right) > distinct(left)
            (
                (10, Inexact(1), Inexact(10), Inexact(2), Absent),
                (10, Inexact(1), Inexact(10), Inexact(5), Absent),
                Some(Inexact(20)),
            ),
            // min(left) < 0 (range(left) > range(right))
            (
                (10, Inexact(-5), Inexact(5), Absent, Absent),
                (10, Inexact(1), Inexact(5), Absent, Absent),
                Some(Inexact(10)),
            ),
            // min(right) < 0, max(right) < 0 (range(right) > range(left))
            (
                (10, Inexact(-25), Inexact(-20), Absent, Absent),
                (10, Inexact(-25), Inexact(-15), Absent, Absent),
                Some(Inexact(10)),
            ),
            // range(left) < 0, range(right) >= 0
            // (there isn't a case where both left and right ranges are negative
            //  so one of them is always going to work, this just proves negative
            //  ranges with bigger absolute values are not are not accidentally used).
            (
                (10, Inexact(-10), Inexact(0), Absent, Absent),
                (10, Inexact(0), Inexact(10), Inexact(5), Absent),
                Some(Inexact(10)),
            ),
            // range(left) = 1, range(right) = 1
            (
                (10, Inexact(1), Inexact(1), Absent, Absent),
                (10, Inexact(1), Inexact(1), Absent, Absent),
                Some(Inexact(100)),
            ),
            //
            // Edge cases
            // ==========
            //
            // No column level stats.
            (
                (10, Absent, Absent, Absent, Absent),
                (10, Absent, Absent, Absent, Absent),
                None,
            ),
            // No min or max (or both).
            (
                (10, Absent, Absent, Inexact(3), Absent),
                (10, Absent, Absent, Inexact(3), Absent),
                None,
            ),
            (
                (10, Inexact(2), Absent, Inexact(3), Absent),
                (10, Absent, Inexact(5), Inexact(3), Absent),
                None,
            ),
            (
                (10, Absent, Inexact(3), Inexact(3), Absent),
                (10, Inexact(1), Absent, Inexact(3), Absent),
                None,
            ),
            (
                (10, Absent, Inexact(3), Absent, Absent),
                (10, Inexact(1), Absent, Absent, Absent),
                None,
            ),
            // Non overlapping min/max (when exact=False).
            (
                (10, Absent, Inexact(4), Absent, Absent),
                (10, Inexact(5), Absent, Absent, Absent),
                Some(Inexact(0)),
            ),
            (
                (10, Inexact(0), Inexact(10), Absent, Absent),
                (10, Inexact(11), Inexact(20), Absent, Absent),
                Some(Inexact(0)),
            ),
            (
                (10, Inexact(11), Inexact(20), Absent, Absent),
                (10, Inexact(0), Inexact(10), Absent, Absent),
                Some(Inexact(0)),
            ),
            // distinct(left) = 0, distinct(right) = 0
            (
                (10, Inexact(1), Inexact(10), Inexact(0), Absent),
                (10, Inexact(1), Inexact(10), Inexact(0), Absent),
                None,
            ),
            // Inexact row count < exact null count with absent distinct count
            (
                (0, Inexact(1), Inexact(10), Absent, Exact(5)),
                (10, Inexact(1), Inexact(10), Absent, Absent),
                Some(Inexact(0)),
            ),
        ];

        for (left_info, right_info, expected_cardinality) in cases {
            let left_num_rows = left_info.0;
            let left_col_stats = vec![create_column_stats(
                left_info.1,
                left_info.2,
                left_info.3,
                left_info.4,
            )];

            let right_num_rows = right_info.0;
            let right_col_stats = vec![create_column_stats(
                right_info.1,
                right_info.2,
                right_info.3,
                right_info.4,
            )];

            assert_eq!(
                estimate_inner_join_cardinality(
                    Statistics {
                        num_rows: Inexact(left_num_rows),
                        total_byte_size: Absent,
                        column_statistics: left_col_stats.clone(),
                    },
                    Statistics {
                        num_rows: Inexact(right_num_rows),
                        total_byte_size: Absent,
                        column_statistics: right_col_stats.clone(),
                    },
                ),
                expected_cardinality.clone()
            );

            // We should also be able to use join_cardinality to get the same results
            let join_type = JoinType::Inner;
            let join_on = vec![(
                Arc::new(Column::new("a", 0)) as _,
                Arc::new(Column::new("b", 0)) as _,
            )];
            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(left_num_rows), left_col_stats.clone(), false),
                create_stats(Some(right_num_rows), right_col_stats.clone(), false),
                &join_on,
            );

            assert_eq!(
                partial_join_stats.clone().map(|s| Inexact(s.num_rows)),
                expected_cardinality.clone()
            );
            assert_eq!(
                partial_join_stats.map(|s| s.column_statistics),
                expected_cardinality
                    .clone()
                    .map(|_| [left_col_stats, right_col_stats].concat())
            );
        }
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_multiple_column() -> Result<()> {
        let left_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(100), Absent),
            create_column_stats(Inexact(100), Inexact(500), Inexact(150), Absent),
        ];

        let right_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(50), Absent),
            create_column_stats(Inexact(100), Inexact(500), Inexact(200), Absent),
        ];

        // We have statistics about 4 columns, where the highest distinct
        // count is 200, so we are going to pick it.
        assert_eq!(
            estimate_inner_join_cardinality(
                Statistics {
                    num_rows: Precision::Inexact(400),
                    total_byte_size: Precision::Absent,
                    column_statistics: left_col_stats,
                },
                Statistics {
                    num_rows: Precision::Inexact(400),
                    total_byte_size: Precision::Absent,
                    column_statistics: right_col_stats,
                },
            ),
            Some(Precision::Inexact((400 * 400) / 200))
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_decimal_range() -> Result<()> {
        let left_col_stats = vec![ColumnStatistics {
            distinct_count: Precision::Absent,
            min_value: Precision::Inexact(ScalarValue::Decimal128(Some(32500), 14, 4)),
            max_value: Precision::Inexact(ScalarValue::Decimal128(Some(35000), 14, 4)),
            ..Default::default()
        }];

        let right_col_stats = vec![ColumnStatistics {
            distinct_count: Precision::Absent,
            min_value: Precision::Inexact(ScalarValue::Decimal128(Some(33500), 14, 4)),
            max_value: Precision::Inexact(ScalarValue::Decimal128(Some(34000), 14, 4)),
            ..Default::default()
        }];

        assert_eq!(
            estimate_inner_join_cardinality(
                Statistics {
                    num_rows: Precision::Inexact(100),
                    total_byte_size: Precision::Absent,
                    column_statistics: left_col_stats,
                },
                Statistics {
                    num_rows: Precision::Inexact(100),
                    total_byte_size: Precision::Absent,
                    column_statistics: right_col_stats,
                },
            ),
            Some(Precision::Inexact(100))
        );
        Ok(())
    }

    #[test]
    fn test_join_cardinality() -> Result<()> {
        // Left table (rows=1000)
        //   a: min=0, max=100, distinct=100
        //   b: min=0, max=500, distinct=500
        //   x: min=1000, max=10000, distinct=None
        //
        // Right table (rows=2000)
        //   c: min=0, max=100, distinct=50
        //   d: min=0, max=2000, distinct=2500 (how? some inexact statistics)
        //   y: min=0, max=100, distinct=None
        //
        // Join on a=c, b=d (ignore x/y)
        let cases = vec![
            (JoinType::Inner, 800),
            (JoinType::Left, 1000),
            (JoinType::Right, 2000),
            (JoinType::Full, 2200),
        ];

        let left_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(100), Absent),
            create_column_stats(Inexact(0), Inexact(500), Inexact(500), Absent),
            create_column_stats(Inexact(1000), Inexact(10000), Absent, Absent),
        ];

        let right_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(50), Absent),
            create_column_stats(Inexact(0), Inexact(2000), Inexact(2500), Absent),
            create_column_stats(Inexact(0), Inexact(100), Absent, Absent),
        ];

        for (join_type, expected_num_rows) in cases {
            let join_on = vec![
                (
                    Arc::new(Column::new("a", 0)) as _,
                    Arc::new(Column::new("c", 0)) as _,
                ),
                (
                    Arc::new(Column::new("b", 1)) as _,
                    Arc::new(Column::new("d", 1)) as _,
                ),
            ];

            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(1000), left_col_stats.clone(), false),
                create_stats(Some(2000), right_col_stats.clone(), false),
                &join_on,
            )
            .unwrap();
            assert_eq!(partial_join_stats.num_rows, expected_num_rows);
            assert_eq!(
                partial_join_stats.column_statistics,
                [left_col_stats.clone(), right_col_stats.clone()].concat()
            );
        }

        Ok(())
    }

    #[test]
    fn test_join_cardinality_when_one_column_is_disjoint() -> Result<()> {
        // Left table (rows=1000)
        //   a: min=0, max=100, distinct=100
        //   b: min=0, max=500, distinct=500
        //   x: min=1000, max=10000, distinct=None
        //
        // Right table (rows=2000)
        //   c: min=0, max=100, distinct=50
        //   d: min=0, max=2000, distinct=2500 (how? some inexact statistics)
        //   y: min=0, max=100, distinct=None
        //
        // Join on a=c, x=y (ignores b/d) where x and y does not intersect

        let left_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(100), Absent),
            create_column_stats(Inexact(0), Inexact(500), Inexact(500), Absent),
            create_column_stats(Inexact(1000), Inexact(10000), Absent, Absent),
        ];

        let right_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(50), Absent),
            create_column_stats(Inexact(0), Inexact(2000), Inexact(2500), Absent),
            create_column_stats(Inexact(0), Inexact(100), Absent, Absent),
        ];

        let join_on = vec![
            (
                Arc::new(Column::new("a", 0)) as _,
                Arc::new(Column::new("c", 0)) as _,
            ),
            (
                Arc::new(Column::new("x", 2)) as _,
                Arc::new(Column::new("y", 2)) as _,
            ),
        ];

        let cases = vec![
            // Join type, expected cardinality
            //
            // When an inner join is disjoint, that means it won't
            // produce any rows.
            (JoinType::Inner, 0),
            // But left/right outer joins will produce at least
            // the amount of rows from the left/right side.
            (JoinType::Left, 1000),
            (JoinType::Right, 2000),
            // And a full outer join will produce at least the combination
            // of the rows above (minus the cardinality of the inner join, which
            // is 0).
            (JoinType::Full, 3000),
        ];

        for (join_type, expected_num_rows) in cases {
            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(1000), left_col_stats.clone(), true),
                create_stats(Some(2000), right_col_stats.clone(), true),
                &join_on,
            )
            .unwrap();
            assert_eq!(partial_join_stats.num_rows, expected_num_rows);
            assert_eq!(
                partial_join_stats.column_statistics,
                [left_col_stats.clone(), right_col_stats.clone()].concat()
            );
        }

        Ok(())
    }

    #[test]
    fn test_anti_semi_join_cardinality() -> Result<()> {
        let cases: Vec<(JoinType, PartialStats, PartialStats, Option<usize>)> = vec![
            // ------------------------------------------------
            // | join_type ,                                   |
            // | left(rows, min, max, distinct, null_count), |
            // | right(rows, min, max, distinct, null_count), |
            // | expected,                                    |
            // ------------------------------------------------

            // Cardinality computation
            // =======================
            (
                JoinType::LeftSemi,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::RightSemi,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftSemi,
                (10, Absent, Absent, Absent, Absent),
                (50, Absent, Absent, Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftSemi,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(30), Inexact(40), Absent, Absent),
                Some(0),
            ),
            (
                JoinType::LeftSemi,
                (50, Inexact(10), Absent, Absent, Absent),
                (10, Absent, Inexact(5), Absent, Absent),
                Some(0),
            ),
            (
                JoinType::LeftSemi,
                (50, Absent, Inexact(20), Absent, Absent),
                (10, Inexact(30), Absent, Absent, Absent),
                Some(0),
            ),
            (
                JoinType::LeftAnti,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::RightAnti,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftAnti,
                (10, Absent, Absent, Absent, Absent),
                (50, Absent, Absent, Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftAnti,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(30), Inexact(40), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::LeftAnti,
                (50, Inexact(10), Absent, Absent, Absent),
                (10, Absent, Inexact(5), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::LeftAnti,
                (50, Absent, Inexact(20), Absent, Absent),
                (10, Inexact(30), Absent, Absent, Absent),
                Some(50),
            ),
        ];

        let join_on = vec![(
            Arc::new(Column::new("l_col", 0)) as _,
            Arc::new(Column::new("r_col", 0)) as _,
        )];

        for (join_type, outer_info, inner_info, expected) in cases {
            let outer_num_rows = outer_info.0;
            let outer_col_stats = vec![create_column_stats(
                outer_info.1,
                outer_info.2,
                outer_info.3,
                outer_info.4,
            )];

            let inner_num_rows = inner_info.0;
            let inner_col_stats = vec![create_column_stats(
                inner_info.1,
                inner_info.2,
                inner_info.3,
                inner_info.4,
            )];

            let output_cardinality = estimate_join_cardinality(
                &join_type,
                Statistics {
                    num_rows: Inexact(outer_num_rows),
                    total_byte_size: Absent,
                    column_statistics: outer_col_stats,
                },
                Statistics {
                    num_rows: Inexact(inner_num_rows),
                    total_byte_size: Absent,
                    column_statistics: inner_col_stats,
                },
                &join_on,
            )
            .map(|cardinality| cardinality.num_rows);

            assert_eq!(
                output_cardinality, expected,
                "failure for join_type: {}",
                join_type
            );
        }

        Ok(())
    }

    #[test]
    fn test_semi_join_cardinality_absent_rows() -> Result<()> {
        let dummy_column_stats =
            vec![create_column_stats(Absent, Absent, Absent, Absent)];
        let join_on = vec![(
            Arc::new(Column::new("l_col", 0)) as _,
            Arc::new(Column::new("r_col", 0)) as _,
        )];

        let absent_outer_estimation = estimate_join_cardinality(
            &JoinType::LeftSemi,
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            Statistics {
                num_rows: Exact(10),
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            &join_on,
        );
        assert!(
            absent_outer_estimation.is_none(),
            "Expected \"None\" esimated SemiJoin cardinality for absent outer num_rows"
        );

        let absent_inner_estimation = estimate_join_cardinality(
            &JoinType::LeftSemi,
            Statistics {
                num_rows: Inexact(500),
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            &join_on,
        ).expect("Expected non-empty PartialJoinStatistics for SemiJoin with absent inner num_rows");

        assert_eq!(absent_inner_estimation.num_rows, 500, "Expected outer.num_rows esimated SemiJoin cardinality for absent inner num_rows");

        let absent_inner_estimation = estimate_join_cardinality(
            &JoinType::LeftSemi,
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            &join_on,
        );
        assert!(absent_inner_estimation.is_none(), "Expected \"None\" esimated SemiJoin cardinality for absent outer and inner num_rows");

        Ok(())
    }

    #[test]
    fn test_calculate_join_output_ordering() -> Result<()> {
        let options = SortOptions::default();
        let left_ordering = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("d", 3)),
                options,
            },
        ];
        let right_ordering = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("z", 2)),
                options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("y", 1)),
                options,
            },
        ];
        let join_type = JoinType::Inner;
        let on_columns = [(
            Arc::new(Column::new("b", 1)) as _,
            Arc::new(Column::new("x", 0)) as _,
        )];
        let left_columns_len = 5;
        let maintains_input_orders = [[true, false], [false, true]];
        let probe_sides = [Some(JoinSide::Left), Some(JoinSide::Right)];

        let expected = [
            Some(vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("c", 2)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("z", 7)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("y", 6)),
                    options,
                },
            ]),
            Some(vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("z", 7)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("y", 6)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("c", 2)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options,
                },
            ]),
        ];

        for (i, (maintains_input_order, probe_side)) in
            maintains_input_orders.iter().zip(probe_sides).enumerate()
        {
            assert_eq!(
                calculate_join_output_ordering(
                    &left_ordering,
                    &right_ordering,
                    join_type,
                    &on_columns,
                    left_columns_len,
                    maintains_input_order,
                    probe_side
                ),
                expected[i]
            );
        }

        Ok(())
    }
}
