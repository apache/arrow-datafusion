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

//! This file implements the symmetric hash join algorithm with range-based
//! data pruning to join two (potentially infinite) streams.
//!
//! A [`SymmetricHashJoinExec`] plan takes two children plan (with appropriate
//! output ordering) and produces the join output according to the given join
//! type and other options.
//!
//! This plan uses the [`OneSideHashJoiner`] object to facilitate join calculations
//! for both its children.

use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use crate::common::SharedMemoryReservation;
use crate::handle_state;
use crate::joins::hash_join::{equal_rows_arr, update_hash};
use crate::joins::stream_join_utils::{
    calculate_filter_expr_intervals, combine_two_batches,
    convert_sort_expr_with_filter_schema, get_pruning_anti_indices,
    get_pruning_semi_indices, prepare_sorted_exprs, record_visited_indices,
    PruningJoinHashMap, SortedFilterExpr, StreamJoinMetrics,
};
use crate::joins::utils::{
    apply_join_filter_to_indices, build_batch_from_indices, build_join_schema,
    check_join_is_valid, symmetric_join_output_partitioning, ColumnIndex, JoinFilter,
    JoinHashMapType, JoinOn, JoinOnRef, StatefulStreamResult,
};
use crate::{
    execution_mode_from_children,
    expressions::PhysicalSortExpr,
    joins::StreamJoinPartitionMode,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::array::{
    ArrowPrimitiveType, NativeAdapter, PrimitiveArray, PrimitiveBuilder, UInt32Array,
    UInt64Array,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::bisect;
use datafusion_common::{internal_err, plan_err, JoinSide, JoinType, Result};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::intervals::cp_solver::ExprIntervalGraph;
use datafusion_physical_expr::{PhysicalExprRef, PhysicalSortRequirement};

use ahash::RandomState;
use futures::{ready, Stream, StreamExt};
use hashbrown::HashSet;
use parking_lot::Mutex;

const HASHMAP_SHRINK_SCALE_FACTOR: usize = 4;

/// A symmetric hash join with range conditions is when both streams are hashed on the
/// join key and the resulting hash tables are used to join the streams.
/// The join is considered symmetric because the hash table is built on the join keys from both
/// streams, and the matching of rows is based on the values of the join keys in both streams.
/// This type of join is efficient in streaming context as it allows for fast lookups in the hash
/// table, rather than having to scan through one or both of the streams to find matching rows, also it
/// only considers the elements from the stream that fall within a certain sliding window (w/ range conditions),
/// making it more efficient and less likely to store stale data. This enables operating on unbounded streaming
/// data without any memory issues.
///
/// For each input stream, create a hash table.
///   - For each new [RecordBatch] in build side, hash and insert into inputs hash table. Update offsets.
///   - Test if input is equal to a predefined set of other inputs.
///   - If so record the visited rows. If the matched row results must be produced (INNER, LEFT), output the [RecordBatch].
///   - Try to prune other side (probe) with new [RecordBatch].
///   - If the join type indicates that the unmatched rows results must be produced (LEFT, FULL etc.),
/// output the [RecordBatch] when a pruning happens or at the end of the data.
///
///
/// ``` text
///                        +-------------------------+
///                        |                         |
///   left stream ---------|  Left OneSideHashJoiner |---+
///                        |                         |   |
///                        +-------------------------+   |
///                                                      |
///                                                      |--------- Joined output
///                                                      |
///                        +-------------------------+   |
///                        |                         |   |
///  right stream ---------| Right OneSideHashJoiner |---+
///                        |                         |
///                        +-------------------------+
///
/// Prune build side when the new RecordBatch comes to the probe side. We utilize interval arithmetic
/// on JoinFilter's sorted PhysicalExprs to calculate the joinable range.
///
///
///               PROBE SIDE          BUILD SIDE
///                 BUFFER              BUFFER
///             +-------------+     +------------+
///             |             |     |            |    Unjoinable
///             |             |     |            |    Range
///             |             |     |            |
///             |             |  |---------------------------------
///             |             |  |  |            |
///             |             |  |  |            |
///             |             | /   |            |
///             |             | |   |            |
///             |             | |   |            |
///             |             | |   |            |
///             |             | |   |            |
///             |             | |   |            |    Joinable
///             |             |/    |            |    Range
///             |             ||    |            |
///             |+-----------+||    |            |
///             || Record    ||     |            |
///             || Batch     ||     |            |
///             |+-----------+||    |            |
///             +-------------+\    +------------+
///                             |
///                             \
///                              |---------------------------------
///
///  This happens when range conditions are provided on sorted columns. E.g.
///
///        SELECT * FROM left_table, right_table
///        ON
///          left_key = right_key AND
///          left_time > right_time - INTERVAL 12 MINUTES AND left_time < right_time + INTERVAL 2 HOUR
///
/// or
///       SELECT * FROM left_table, right_table
///        ON
///          left_key = right_key AND
///          left_sorted > right_sorted - 3 AND left_sorted < right_sorted + 10
///
/// For general purpose, in the second scenario, when the new data comes to probe side, the conditions can be used to
/// determine a specific threshold for discarding rows from the inner buffer. For example, if the sort order the
/// two columns ("left_sorted" and "right_sorted") are ascending (it can be different in another scenarios)
/// and the join condition is "left_sorted > right_sorted - 3" and the latest value on the right input is 1234, meaning
/// that the left side buffer must only keep rows where "leftTime > rightTime - 3 > 1234 - 3 > 1231" ,
/// making the smallest value in 'left_sorted' 1231 and any rows below (since ascending)
/// than that can be dropped from the inner buffer.
/// ```
#[derive(Debug)]
pub struct SymmetricHashJoinExec {
    /// Left side stream
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// Right side stream
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub(crate) on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    /// Filters applied when finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// Shares the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    pub(crate) null_equals_null: bool,
    /// Left side sort expression(s)
    pub(crate) left_sort_exprs: Option<Vec<PhysicalSortExpr>>,
    /// Right side sort expression(s)
    pub(crate) right_sort_exprs: Option<Vec<PhysicalSortExpr>>,
    /// Partition Mode
    mode: StreamJoinPartitionMode,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl SymmetricHashJoinExec {
    /// Tries to create a new [SymmetricHashJoinExec].
    /// # Error
    /// This function errors when:
    /// - It is not possible to join the left and right sides on keys `on`, or
    /// - It fails to construct `SortedFilterExpr`s, or
    /// - It fails to create the [ExprIntervalGraph].
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        null_equals_null: bool,
        left_sort_exprs: Option<Vec<PhysicalSortExpr>>,
        right_sort_exprs: Option<Vec<PhysicalSortExpr>>,
        mode: StreamJoinPartitionMode,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        // Error out if no "on" contraints are given:
        if on.is_empty() {
            return plan_err!(
                "On constraints in SymmetricHashJoinExec should be non-empty"
            );
        }

        // Check if the join is valid with the given on constraints:
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        // Build the join schema from the left and right schemas:
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        // Initialize the random state for the join operation:
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let schema = Arc::new(schema);
        let cache =
            Self::compute_properties(&left, &right, Arc::clone(&schema), *join_type, &on);
        Ok(SymmetricHashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            random_state,
            metrics: ExecutionPlanMetricsSet::new(),
            column_indices,
            null_equals_null,
            left_sort_exprs,
            right_sort_exprs,
            mode,
            cache,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        join_on: JoinOnRef,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &[false, false],
            // Has alternating probe side
            None,
            join_on,
        );

        let output_partitioning =
            symmetric_join_output_partitioning(left, right, &join_type);

        // Determine execution mode:
        let mode = execution_mode_from_children([left, right]);

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }

    /// left stream
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right stream
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Filters applied before join output
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Get null_equals_null
    pub fn null_equals_null(&self) -> bool {
        self.null_equals_null
    }

    /// Get partition mode
    pub fn partition_mode(&self) -> StreamJoinPartitionMode {
        self.mode
    }

    /// Get left_sort_exprs
    pub fn left_sort_exprs(&self) -> Option<&[PhysicalSortExpr]> {
        self.left_sort_exprs.as_deref()
    }

    /// Get right_sort_exprs
    pub fn right_sort_exprs(&self) -> Option<&[PhysicalSortExpr]> {
        self.right_sort_exprs.as_deref()
    }

    /// Check if order information covers every column in the filter expression.
    pub fn check_if_order_information_available(&self) -> Result<bool> {
        if let Some(filter) = self.filter() {
            let left = self.left();
            if let Some(left_ordering) = left.output_ordering() {
                let right = self.right();
                if let Some(right_ordering) = right.output_ordering() {
                    let left_convertible = convert_sort_expr_with_filter_schema(
                        &JoinSide::Left,
                        filter,
                        &left.schema(),
                        &left_ordering[0],
                    )?
                    .is_some();
                    let right_convertible = convert_sort_expr_with_filter_schema(
                        &JoinSide::Right,
                        filter,
                        &right.schema(),
                        &right_ordering[0],
                    )?
                    .is_some();
                    return Ok(left_convertible && right_convertible);
                }
            }
        }
        Ok(false)
    }
}

impl DisplayAs for SymmetricHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "SymmetricHashJoinExec: mode={:?}, join_type={:?}, on=[{}]{}",
                    self.mode, self.join_type, on, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for SymmetricHashJoinExec {
    fn name(&self) -> &'static str {
        "SymmetricHashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            StreamJoinPartitionMode::Partitioned => {
                let (left_expr, right_expr) = self
                    .on
                    .iter()
                    .map(|(l, r)| (Arc::clone(l) as _, Arc::clone(r) as _))
                    .unzip();
                vec![
                    Distribution::HashPartitioned(left_expr),
                    Distribution::HashPartitioned(right_expr),
                ]
            }
            StreamJoinPartitionMode::SinglePartition => {
                vec![Distribution::SinglePartition, Distribution::SinglePartition]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![
            self.left_sort_exprs
                .as_ref()
                .map(PhysicalSortRequirement::from_sort_exprs),
            self.right_sort_exprs
                .as_ref()
                .map(PhysicalSortRequirement::from_sort_exprs),
        ]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SymmetricHashJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.null_equals_null,
            self.left_sort_exprs.clone(),
            self.right_sort_exprs.clone(),
            self.mode,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        if left_partitions != right_partitions {
            return internal_err!(
                "Invalid SymmetricHashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }
        // If `filter_state` and `filter` are both present, then calculate sorted filter expressions
        // for both sides, and build an expression graph.
        let (left_sorted_filter_expr, right_sorted_filter_expr, graph) =
            match (&self.left_sort_exprs, &self.right_sort_exprs, &self.filter) {
                (Some(left_sort_exprs), Some(right_sort_exprs), Some(filter)) => {
                    let (left, right, graph) = prepare_sorted_exprs(
                        filter,
                        &self.left,
                        &self.right,
                        left_sort_exprs,
                        right_sort_exprs,
                    )?;
                    (Some(left), Some(right), Some(graph))
                }
                // If `filter_state` or `filter` is not present, then return None for all three values:
                _ => (None, None, None),
            };

        let (on_left, on_right) = self.on.iter().cloned().unzip();

        let left_side_joiner =
            OneSideHashJoiner::new(JoinSide::Left, on_left, self.left.schema());
        let right_side_joiner =
            OneSideHashJoiner::new(JoinSide::Right, on_right, self.right.schema());

        let left_stream = self.left.execute(partition, Arc::clone(&context))?;

        let right_stream = self.right.execute(partition, Arc::clone(&context))?;

        let reservation = Arc::new(Mutex::new(
            MemoryConsumer::new(format!("SymmetricHashJoinStream[{partition}]"))
                .register(context.memory_pool()),
        ));
        if let Some(g) = graph.as_ref() {
            reservation.lock().try_grow(g.size())?;
        }

        Ok(Box::pin(SymmetricHashJoinStream {
            left_stream,
            right_stream,
            schema: self.schema(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            random_state: self.random_state.clone(),
            left: left_side_joiner,
            right: right_side_joiner,
            column_indices: self.column_indices.clone(),
            metrics: StreamJoinMetrics::new(partition, &self.metrics),
            graph,
            left_sorted_filter_expr,
            right_sorted_filter_expr,
            null_equals_null: self.null_equals_null,
            state: SHJStreamState::PullRight,
            reservation,
        }))
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct SymmetricHashJoinStream {
    /// Input streams
    left_stream: SendableRecordBatchStream,
    right_stream: SendableRecordBatchStream,
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    // left hash joiner
    left: OneSideHashJoiner,
    /// right hash joiner
    right: OneSideHashJoiner,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // Expression graph for range pruning.
    graph: Option<ExprIntervalGraph>,
    // Left globally sorted filter expr
    left_sorted_filter_expr: Option<SortedFilterExpr>,
    // Right globally sorted filter expr
    right_sorted_filter_expr: Option<SortedFilterExpr>,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// Metrics
    metrics: StreamJoinMetrics,
    /// Memory reservation
    reservation: SharedMemoryReservation,
    /// State machine for input execution
    state: SHJStreamState,
}

impl RecordBatchStream for SymmetricHashJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for SymmetricHashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

/// Determine the pruning length for `buffer`.
///
/// This function evaluates the build side filter expression, converts the
/// result into an array and determines the pruning length by performing a
/// binary search on the array.
///
/// # Arguments
///
/// * `buffer`: The record batch to be pruned.
/// * `build_side_filter_expr`: The filter expression on the build side used
/// to determine the pruning length.
///
/// # Returns
///
/// A [Result] object that contains the pruning length. The function will return
/// an error if
/// - there is an issue evaluating the build side filter expression;
/// - there is an issue converting the build side filter expression into an array
fn determine_prune_length(
    buffer: &RecordBatch,
    build_side_filter_expr: &SortedFilterExpr,
) -> Result<usize> {
    let origin_sorted_expr = build_side_filter_expr.origin_sorted_expr();
    let interval = build_side_filter_expr.interval();
    // Evaluate the build side filter expression and convert it into an array
    let batch_arr = origin_sorted_expr
        .expr
        .evaluate(buffer)?
        .into_array(buffer.num_rows())?;

    // Get the lower or upper interval based on the sort direction
    let target = if origin_sorted_expr.options.descending {
        interval.upper().clone()
    } else {
        interval.lower().clone()
    };

    // Perform binary search on the array to determine the length of the record batch to be pruned
    bisect::<true>(&[batch_arr], &[target], &[origin_sorted_expr.options])
}

/// This method determines if the result of the join should be produced in the final step or not.
///
/// # Arguments
///
/// * `build_side` - Enum indicating the side of the join used as the build side.
/// * `join_type` - Enum indicating the type of join to be performed.
///
/// # Returns
///
/// A boolean indicating whether the result of the join should be produced in the final step or not.
/// The result will be true if the build side is JoinSide::Left and the join type is one of
/// JoinType::Left, JoinType::LeftAnti, JoinType::Full or JoinType::LeftSemi.
/// If the build side is JoinSide::Right, the result will be true if the join type
/// is one of JoinType::Right, JoinType::RightAnti, JoinType::Full, or JoinType::RightSemi.
fn need_to_produce_result_in_final(build_side: JoinSide, join_type: JoinType) -> bool {
    if build_side == JoinSide::Left {
        matches!(
            join_type,
            JoinType::Left | JoinType::LeftAnti | JoinType::Full | JoinType::LeftSemi
        )
    } else {
        matches!(
            join_type,
            JoinType::Right | JoinType::RightAnti | JoinType::Full | JoinType::RightSemi
        )
    }
}

/// Calculate indices by join type.
///
/// This method returns a tuple of two arrays: build and probe indices.
/// The length of both arrays will be the same.
///
/// # Arguments
///
/// * `build_side`: Join side which defines the build side.
/// * `prune_length`: Length of the prune data.
/// * `visited_rows`: Hash set of visited rows of the build side.
/// * `deleted_offset`: Deleted offset of the build side.
/// * `join_type`: The type of join to be performed.
///
/// # Returns
///
/// A tuple of two arrays of primitive types representing the build and probe indices.
///
fn calculate_indices_by_join_type<L: ArrowPrimitiveType, R: ArrowPrimitiveType>(
    build_side: JoinSide,
    prune_length: usize,
    visited_rows: &HashSet<usize>,
    deleted_offset: usize,
    join_type: JoinType,
) -> Result<(PrimitiveArray<L>, PrimitiveArray<R>)>
where
    NativeAdapter<L>: From<<L as ArrowPrimitiveType>::Native>,
{
    // Store the result in a tuple
    let result = match (build_side, join_type) {
        // In the case of `Left` or `Right` join, or `Full` join, get the anti indices
        (JoinSide::Left, JoinType::Left | JoinType::LeftAnti)
        | (JoinSide::Right, JoinType::Right | JoinType::RightAnti)
        | (_, JoinType::Full) => {
            let build_unmatched_indices =
                get_pruning_anti_indices(prune_length, deleted_offset, visited_rows);
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        // In the case of `LeftSemi` or `RightSemi` join, get the semi indices
        (JoinSide::Left, JoinType::LeftSemi) | (JoinSide::Right, JoinType::RightSemi) => {
            let build_unmatched_indices =
                get_pruning_semi_indices(prune_length, deleted_offset, visited_rows);
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        // The case of other join types is not considered
        _ => unreachable!(),
    };
    Ok(result)
}

/// This function produces unmatched record results based on the build side,
/// join type and other parameters.
///
/// The method uses first `prune_length` rows from the build side input buffer
/// to produce results.
///
/// # Arguments
///
/// * `output_schema` - The schema of the final output record batch.
/// * `prune_length` - The length of the determined prune length.
/// * `probe_schema` - The schema of the probe [RecordBatch].
/// * `join_type` - The type of join to be performed.
/// * `column_indices` - Indices of columns that are being joined.
///
/// # Returns
///
/// * `Option<RecordBatch>` - The final output record batch if required, otherwise [None].
pub(crate) fn build_side_determined_results(
    build_hash_joiner: &OneSideHashJoiner,
    output_schema: &SchemaRef,
    prune_length: usize,
    probe_schema: SchemaRef,
    join_type: JoinType,
    column_indices: &[ColumnIndex],
) -> Result<Option<RecordBatch>> {
    // Check if we need to produce a result in the final output:
    if prune_length > 0
        && need_to_produce_result_in_final(build_hash_joiner.build_side, join_type)
    {
        // Calculate the indices for build and probe sides based on join type and build side:
        let (build_indices, probe_indices) = calculate_indices_by_join_type(
            build_hash_joiner.build_side,
            prune_length,
            &build_hash_joiner.visited_rows,
            build_hash_joiner.deleted_offset,
            join_type,
        )?;

        // Create an empty probe record batch:
        let empty_probe_batch = RecordBatch::new_empty(probe_schema);
        // Build the final result from the indices of build and probe sides:
        build_batch_from_indices(
            output_schema.as_ref(),
            &build_hash_joiner.input_buffer,
            &empty_probe_batch,
            &build_indices,
            &probe_indices,
            column_indices,
            build_hash_joiner.build_side,
        )
        .map(|batch| (batch.num_rows() > 0).then_some(batch))
    } else {
        // If we don't need to produce a result, return None
        Ok(None)
    }
}

/// This method performs a join between the build side input buffer and the probe side batch.
///
/// # Arguments
///
/// * `build_hash_joiner` - Build side hash joiner
/// * `probe_hash_joiner` - Probe side hash joiner
/// * `schema` - A reference to the schema of the output record batch.
/// * `join_type` - The type of join to be performed.
/// * `on_probe` - An array of columns on which the join will be performed. The columns are from the probe side of the join.
/// * `filter` - An optional filter on the join condition.
/// * `probe_batch` - The second record batch to be joined.
/// * `column_indices` - An array of columns to be selected for the result of the join.
/// * `random_state` - The random state for the join.
/// * `null_equals_null` - A boolean indicating whether NULL values should be treated as equal when joining.
///
/// # Returns
///
/// A [Result] containing an optional record batch if the join type is not one of `LeftAnti`, `RightAnti`, `LeftSemi` or `RightSemi`.
/// If the join type is one of the above four, the function will return [None].
#[allow(clippy::too_many_arguments)]
pub(crate) fn join_with_probe_batch(
    build_hash_joiner: &mut OneSideHashJoiner,
    probe_hash_joiner: &mut OneSideHashJoiner,
    schema: &SchemaRef,
    join_type: JoinType,
    filter: Option<&JoinFilter>,
    probe_batch: &RecordBatch,
    column_indices: &[ColumnIndex],
    random_state: &RandomState,
    null_equals_null: bool,
) -> Result<Option<RecordBatch>> {
    if build_hash_joiner.input_buffer.num_rows() == 0 || probe_batch.num_rows() == 0 {
        return Ok(None);
    }
    let (build_indices, probe_indices) = lookup_join_hashmap(
        &build_hash_joiner.hashmap,
        &build_hash_joiner.input_buffer,
        probe_batch,
        &build_hash_joiner.on,
        &probe_hash_joiner.on,
        random_state,
        null_equals_null,
        &mut build_hash_joiner.hashes_buffer,
        Some(build_hash_joiner.deleted_offset),
    )?;

    let (build_indices, probe_indices) = if let Some(filter) = filter {
        apply_join_filter_to_indices(
            &build_hash_joiner.input_buffer,
            probe_batch,
            build_indices,
            probe_indices,
            filter,
            build_hash_joiner.build_side,
        )?
    } else {
        (build_indices, probe_indices)
    };

    if need_to_produce_result_in_final(build_hash_joiner.build_side, join_type) {
        record_visited_indices(
            &mut build_hash_joiner.visited_rows,
            build_hash_joiner.deleted_offset,
            &build_indices,
        );
    }
    if need_to_produce_result_in_final(build_hash_joiner.build_side.negate(), join_type) {
        record_visited_indices(
            &mut probe_hash_joiner.visited_rows,
            probe_hash_joiner.offset,
            &probe_indices,
        );
    }
    if matches!(
        join_type,
        JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftSemi
            | JoinType::RightSemi
    ) {
        Ok(None)
    } else {
        build_batch_from_indices(
            schema,
            &build_hash_joiner.input_buffer,
            probe_batch,
            &build_indices,
            &probe_indices,
            column_indices,
            build_hash_joiner.build_side,
        )
        .map(|batch| (batch.num_rows() > 0).then_some(batch))
    }
}

/// This method performs lookups against JoinHashMap by hash values of join-key columns, and handles potential
/// hash collisions.
///
/// # Arguments
///
/// * `build_hashmap` - hashmap collected from build side data.
/// * `build_batch` - Build side record batch.
/// * `probe_batch` - Probe side record batch.
/// * `build_on` - An array of columns on which the join will be performed. The columns are from the build side of the join.
/// * `probe_on` - An array of columns on which the join will be performed. The columns are from the probe side of the join.
/// * `random_state` - The random state for the join.
/// * `null_equals_null` - A boolean indicating whether NULL values should be treated as equal when joining.
/// * `hashes_buffer` - Buffer used for probe side keys hash calculation.
/// * `deleted_offset` - deleted offset for build side data.
///
/// # Returns
///
/// A [Result] containing a tuple with two equal length arrays, representing indices of rows from build and probe side,
/// matched by join key columns.
#[allow(clippy::too_many_arguments)]
fn lookup_join_hashmap(
    build_hashmap: &PruningJoinHashMap,
    build_batch: &RecordBatch,
    probe_batch: &RecordBatch,
    build_on: &[PhysicalExprRef],
    probe_on: &[PhysicalExprRef],
    random_state: &RandomState,
    null_equals_null: bool,
    hashes_buffer: &mut Vec<u64>,
    deleted_offset: Option<usize>,
) -> Result<(UInt64Array, UInt32Array)> {
    let keys_values = probe_on
        .iter()
        .map(|c| c.evaluate(probe_batch)?.into_array(probe_batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;
    let build_join_values = build_on
        .iter()
        .map(|c| c.evaluate(build_batch)?.into_array(build_batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    hashes_buffer.clear();
    hashes_buffer.resize(probe_batch.num_rows(), 0);
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;

    // As SymmetricHashJoin uses LIFO JoinHashMap, the chained list algorithm
    // will return build indices for each probe row in a reverse order as such:
    // Build Indices: [5, 4, 3]
    // Probe Indices: [1, 1, 1]
    //
    // This affects the output sequence. Hypothetically, it's possible to preserve the lexicographic order on the build side.
    // Let's consider probe rows [0,1] as an example:
    //
    // When the probe iteration sequence is reversed, the following pairings can be derived:
    //
    // For probe row 1:
    //     (5, 1)
    //     (4, 1)
    //     (3, 1)
    //
    // For probe row 0:
    //     (5, 0)
    //     (4, 0)
    //     (3, 0)
    //
    // After reversing both sets of indices, we obtain reversed indices:
    //
    //     (3,0)
    //     (4,0)
    //     (5,0)
    //     (3,1)
    //     (4,1)
    //     (5,1)
    //
    // With this approach, the lexicographic order on both the probe side and the build side is preserved.
    let (mut matched_probe, mut matched_build) = build_hashmap
        .get_matched_indices(hash_values.iter().enumerate().rev(), deleted_offset);

    matched_probe.as_slice_mut().reverse();
    matched_build.as_slice_mut().reverse();

    let build_indices: UInt64Array =
        PrimitiveArray::new(matched_build.finish().into(), None);
    let probe_indices: UInt32Array =
        PrimitiveArray::new(matched_probe.finish().into(), None);

    let (build_indices, probe_indices) = equal_rows_arr(
        &build_indices,
        &probe_indices,
        &build_join_values,
        &keys_values,
        null_equals_null,
    )?;

    Ok((build_indices, probe_indices))
}

pub struct OneSideHashJoiner {
    /// Build side
    build_side: JoinSide,
    /// Input record batch buffer
    pub input_buffer: RecordBatch,
    /// Columns from the side
    pub(crate) on: Vec<PhysicalExprRef>,
    /// Hashmap
    pub(crate) hashmap: PruningJoinHashMap,
    /// Reuse the hashes buffer
    pub(crate) hashes_buffer: Vec<u64>,
    /// Matched rows
    pub(crate) visited_rows: HashSet<usize>,
    /// Offset
    pub(crate) offset: usize,
    /// Deleted offset
    pub(crate) deleted_offset: usize,
}

impl OneSideHashJoiner {
    pub fn size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of_val(self);
        size += std::mem::size_of_val(&self.build_side);
        size += self.input_buffer.get_array_memory_size();
        size += std::mem::size_of_val(&self.on);
        size += self.hashmap.size();
        size += self.hashes_buffer.capacity() * std::mem::size_of::<u64>();
        size += self.visited_rows.capacity() * std::mem::size_of::<usize>();
        size += std::mem::size_of_val(&self.offset);
        size += std::mem::size_of_val(&self.deleted_offset);
        size
    }
    pub fn new(
        build_side: JoinSide,
        on: Vec<PhysicalExprRef>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            build_side,
            input_buffer: RecordBatch::new_empty(schema),
            on,
            hashmap: PruningJoinHashMap::with_capacity(0),
            hashes_buffer: vec![],
            visited_rows: HashSet::new(),
            offset: 0,
            deleted_offset: 0,
        }
    }

    /// Updates the internal state of the [OneSideHashJoiner] with the incoming batch.
    ///
    /// # Arguments
    ///
    /// * `batch` - The incoming [RecordBatch] to be merged with the internal input buffer
    /// * `random_state` - The random state used to hash values
    ///
    /// # Returns
    ///
    /// Returns a [Result] encapsulating any intermediate errors.
    pub(crate) fn update_internal_state(
        &mut self,
        batch: &RecordBatch,
        random_state: &RandomState,
    ) -> Result<()> {
        // Merge the incoming batch with the existing input buffer:
        self.input_buffer = concat_batches(&batch.schema(), [&self.input_buffer, batch])?;
        // Resize the hashes buffer to the number of rows in the incoming batch:
        self.hashes_buffer.resize(batch.num_rows(), 0);
        // Get allocation_info before adding the item
        // Update the hashmap with the join key values and hashes of the incoming batch:
        update_hash(
            &self.on,
            batch,
            &mut self.hashmap,
            self.offset,
            random_state,
            &mut self.hashes_buffer,
            self.deleted_offset,
            false,
        )?;
        Ok(())
    }

    /// Calculate prune length.
    ///
    /// # Arguments
    ///
    /// * `build_side_sorted_filter_expr` - Build side mutable sorted filter expression..
    /// * `probe_side_sorted_filter_expr` - Probe side mutable sorted filter expression.
    /// * `graph` - A mutable reference to the physical expression graph.
    ///
    /// # Returns
    ///
    /// A Result object that contains the pruning length.
    pub(crate) fn calculate_prune_length_with_probe_batch(
        &mut self,
        build_side_sorted_filter_expr: &mut SortedFilterExpr,
        probe_side_sorted_filter_expr: &mut SortedFilterExpr,
        graph: &mut ExprIntervalGraph,
    ) -> Result<usize> {
        // Return early if the input buffer is empty:
        if self.input_buffer.num_rows() == 0 {
            return Ok(0);
        }
        // Process the build and probe side sorted filter expressions if both are present:
        // Collect the sorted filter expressions into a vector of (node_index, interval) tuples:
        let mut filter_intervals = vec![];
        for expr in [
            &build_side_sorted_filter_expr,
            &probe_side_sorted_filter_expr,
        ] {
            filter_intervals.push((expr.node_index(), expr.interval().clone()))
        }
        // Update the physical expression graph using the join filter intervals:
        graph.update_ranges(&mut filter_intervals, Interval::CERTAINLY_TRUE)?;
        // Extract the new join filter interval for the build side:
        let calculated_build_side_interval = filter_intervals.remove(0).1;
        // If the intervals have not changed, return early without pruning:
        if calculated_build_side_interval.eq(build_side_sorted_filter_expr.interval()) {
            return Ok(0);
        }
        // Update the build side interval and determine the pruning length:
        build_side_sorted_filter_expr.set_interval(calculated_build_side_interval);

        determine_prune_length(&self.input_buffer, build_side_sorted_filter_expr)
    }

    pub(crate) fn prune_internal_state(&mut self, prune_length: usize) -> Result<()> {
        // Prune the hash values:
        self.hashmap.prune_hash_values(
            prune_length,
            self.deleted_offset as u64,
            HASHMAP_SHRINK_SCALE_FACTOR,
        );
        // Remove pruned rows from the visited rows set:
        for row in self.deleted_offset..(self.deleted_offset + prune_length) {
            self.visited_rows.remove(&row);
        }
        // Update the input buffer after pruning:
        self.input_buffer = self
            .input_buffer
            .slice(prune_length, self.input_buffer.num_rows() - prune_length);
        // Increment the deleted offset:
        self.deleted_offset += prune_length;
        Ok(())
    }
}

/// `SymmetricHashJoinStream` manages incremental join operations between two
/// streams. Unlike traditional join approaches that need to scan one side of
/// the join fully before proceeding, `SymmetricHashJoinStream` facilitates
/// more dynamic join operations by working with streams as they emit data. This
/// approach allows for more efficient processing, particularly in scenarios
/// where waiting for complete data materialization is not feasible or optimal.
/// The trait provides a framework for handling various states of such a join
/// process, ensuring that join logic is efficiently executed as data becomes
/// available from either stream.
///
/// This implementation performs eager joins of data from two different asynchronous
/// streams, typically referred to as left and right streams. The implementation
/// provides a comprehensive set of methods to control and execute the join
/// process, leveraging the states defined in `SHJStreamState`. Methods are
/// primarily focused on asynchronously fetching data batches from each stream,
/// processing them, and managing transitions between various states of the join.
///
/// This implementations use a state machine approach to navigate different
/// stages of the join operation, handling data from both streams and determining
/// when the join completes.
///
/// State Transitions:
/// - From `PullLeft` to `PullRight` or `LeftExhausted`:
///   - In `fetch_next_from_left_stream`, when fetching a batch from the left stream:
///     - On success (`Some(Ok(batch))`), state transitions to `PullRight` for
///       processing the batch.
///     - On error (`Some(Err(e))`), the error is returned, and the state remains
///       unchanged.
///     - On no data (`None`), state changes to `LeftExhausted`, returning `Continue`
///       to proceed with the join process.
/// - From `PullRight` to `PullLeft` or `RightExhausted`:
///   - In `fetch_next_from_right_stream`, when fetching from the right stream:
///     - If a batch is available, state changes to `PullLeft` for processing.
///     - On error, the error is returned without changing the state.
///     - If right stream is exhausted (`None`), state transitions to `RightExhausted`,
///       with a `Continue` result.
/// - Handling `RightExhausted` and `LeftExhausted`:
///   - Methods `handle_right_stream_end` and `handle_left_stream_end` manage scenarios
///     when streams are exhausted:
///     - They attempt to continue processing with the other stream.
///     - If both streams are exhausted, state changes to `BothExhausted { final_result: false }`.
/// - Transition to `BothExhausted { final_result: true }`:
///   - Occurs in `prepare_for_final_results_after_exhaustion` when both streams are
///     exhausted, indicating completion of processing and availability of final results.
impl SymmetricHashJoinStream {
    /// Implements the main polling logic for the join stream.
    ///
    /// This method continuously checks the state of the join stream and
    /// acts accordingly by delegating the handling to appropriate sub-methods
    /// depending on the current state.
    ///
    /// # Arguments
    ///
    /// * `cx` - A context that facilitates cooperative non-blocking execution within a task.
    ///
    /// # Returns
    ///
    /// * `Poll<Option<Result<RecordBatch>>>` - A polled result, either a `RecordBatch` or None.
    fn poll_next_impl(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state() {
                SHJStreamState::PullRight => {
                    handle_state!(ready!(self.fetch_next_from_right_stream(cx)))
                }
                SHJStreamState::PullLeft => {
                    handle_state!(ready!(self.fetch_next_from_left_stream(cx)))
                }
                SHJStreamState::RightExhausted => {
                    handle_state!(ready!(self.handle_right_stream_end(cx)))
                }
                SHJStreamState::LeftExhausted => {
                    handle_state!(ready!(self.handle_left_stream_end(cx)))
                }
                SHJStreamState::BothExhausted {
                    final_result: false,
                } => {
                    handle_state!(self.prepare_for_final_results_after_exhaustion())
                }
                SHJStreamState::BothExhausted { final_result: true } => Poll::Ready(None),
            };
        }
    }
    /// Asynchronously pulls the next batch from the right stream.
    ///
    /// This default implementation checks for the next value in the right stream.
    /// If a batch is found, the state is switched to `PullLeft`, and the batch handling
    /// is delegated to `process_batch_from_right`. If the stream ends, the state is set to `RightExhausted`.
    ///
    /// # Returns
    ///
    /// * `Result<StatefulStreamResult<Option<RecordBatch>>>` - The state result after pulling the batch.
    fn fetch_next_from_right_stream(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.right_stream().poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(Ok(StatefulStreamResult::Continue));
                }
                self.set_state(SHJStreamState::PullLeft);
                Poll::Ready(self.process_batch_from_right(batch))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                self.set_state(SHJStreamState::RightExhausted);
                Poll::Ready(Ok(StatefulStreamResult::Continue))
            }
        }
    }

    /// Asynchronously pulls the next batch from the left stream.
    ///
    /// This default implementation checks for the next value in the left stream.
    /// If a batch is found, the state is switched to `PullRight`, and the batch handling
    /// is delegated to `process_batch_from_left`. If the stream ends, the state is set to `LeftExhausted`.
    ///
    /// # Returns
    ///
    /// * `Result<StatefulStreamResult<Option<RecordBatch>>>` - The state result after pulling the batch.
    fn fetch_next_from_left_stream(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.left_stream().poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(Ok(StatefulStreamResult::Continue));
                }
                self.set_state(SHJStreamState::PullRight);
                Poll::Ready(self.process_batch_from_left(batch))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                self.set_state(SHJStreamState::LeftExhausted);
                Poll::Ready(Ok(StatefulStreamResult::Continue))
            }
        }
    }

    /// Asynchronously handles the scenario when the right stream is exhausted.
    ///
    /// In this default implementation, when the right stream is exhausted, it attempts
    /// to pull from the left stream. If a batch is found in the left stream, it delegates
    /// the handling to `process_batch_from_left`. If both streams are exhausted, the state is set
    /// to indicate both streams are exhausted without final results yet.
    ///
    /// # Returns
    ///
    /// * `Result<StatefulStreamResult<Option<RecordBatch>>>` - The state result after checking the exhaustion state.
    fn handle_right_stream_end(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.left_stream().poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(Ok(StatefulStreamResult::Continue));
                }
                Poll::Ready(self.process_batch_after_right_end(batch))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                self.set_state(SHJStreamState::BothExhausted {
                    final_result: false,
                });
                Poll::Ready(Ok(StatefulStreamResult::Continue))
            }
        }
    }

    /// Asynchronously handles the scenario when the left stream is exhausted.
    ///
    /// When the left stream is exhausted, this default
    /// implementation tries to pull from the right stream and delegates the batch
    /// handling to `process_batch_after_left_end`. If both streams are exhausted, the state
    /// is updated to indicate so.
    ///
    /// # Returns
    ///
    /// * `Result<StatefulStreamResult<Option<RecordBatch>>>` - The state result after checking the exhaustion state.
    fn handle_left_stream_end(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.right_stream().poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(Ok(StatefulStreamResult::Continue));
                }
                Poll::Ready(self.process_batch_after_left_end(batch))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                self.set_state(SHJStreamState::BothExhausted {
                    final_result: false,
                });
                Poll::Ready(Ok(StatefulStreamResult::Continue))
            }
        }
    }

    /// Handles the state when both streams are exhausted and final results are yet to be produced.
    ///
    /// This default implementation switches the state to indicate both streams are
    /// exhausted with final results and then invokes the handling for this specific
    /// scenario via `process_batches_before_finalization`.
    ///
    /// # Returns
    ///
    /// * `Result<StatefulStreamResult<Option<RecordBatch>>>` - The state result after both streams are exhausted.
    fn prepare_for_final_results_after_exhaustion(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        self.set_state(SHJStreamState::BothExhausted { final_result: true });
        self.process_batches_before_finalization()
    }

    fn process_batch_from_right(
        &mut self,
        batch: RecordBatch,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        self.perform_join_for_given_side(batch, JoinSide::Right)
            .map(|maybe_batch| {
                if maybe_batch.is_some() {
                    StatefulStreamResult::Ready(maybe_batch)
                } else {
                    StatefulStreamResult::Continue
                }
            })
    }

    fn process_batch_from_left(
        &mut self,
        batch: RecordBatch,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        self.perform_join_for_given_side(batch, JoinSide::Left)
            .map(|maybe_batch| {
                if maybe_batch.is_some() {
                    StatefulStreamResult::Ready(maybe_batch)
                } else {
                    StatefulStreamResult::Continue
                }
            })
    }

    fn process_batch_after_left_end(
        &mut self,
        right_batch: RecordBatch,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        self.process_batch_from_right(right_batch)
    }

    fn process_batch_after_right_end(
        &mut self,
        left_batch: RecordBatch,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        self.process_batch_from_left(left_batch)
    }

    fn process_batches_before_finalization(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        // Get the left side results:
        let left_result = build_side_determined_results(
            &self.left,
            &self.schema,
            self.left.input_buffer.num_rows(),
            self.right.input_buffer.schema(),
            self.join_type,
            &self.column_indices,
        )?;
        // Get the right side results:
        let right_result = build_side_determined_results(
            &self.right,
            &self.schema,
            self.right.input_buffer.num_rows(),
            self.left.input_buffer.schema(),
            self.join_type,
            &self.column_indices,
        )?;

        // Combine the left and right results:
        let result = combine_two_batches(&self.schema, left_result, right_result)?;

        // Update the metrics and return the result:
        if let Some(batch) = &result {
            // Update the metrics:
            self.metrics.output_batches.add(1);
            self.metrics.output_rows.add(batch.num_rows());
            return Ok(StatefulStreamResult::Ready(result));
        }
        Ok(StatefulStreamResult::Continue)
    }

    fn right_stream(&mut self) -> &mut SendableRecordBatchStream {
        &mut self.right_stream
    }

    fn left_stream(&mut self) -> &mut SendableRecordBatchStream {
        &mut self.left_stream
    }

    fn set_state(&mut self, state: SHJStreamState) {
        self.state = state;
    }

    fn state(&mut self) -> SHJStreamState {
        self.state.clone()
    }

    fn size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of_val(&self.schema);
        size += std::mem::size_of_val(&self.filter);
        size += std::mem::size_of_val(&self.join_type);
        size += self.left.size();
        size += self.right.size();
        size += std::mem::size_of_val(&self.column_indices);
        size += self.graph.as_ref().map(|g| g.size()).unwrap_or(0);
        size += std::mem::size_of_val(&self.left_sorted_filter_expr);
        size += std::mem::size_of_val(&self.right_sorted_filter_expr);
        size += std::mem::size_of_val(&self.random_state);
        size += std::mem::size_of_val(&self.null_equals_null);
        size += std::mem::size_of_val(&self.metrics);
        size
    }

    /// Performs a join operation for the specified `probe_side` (either left or right).
    /// This function:
    /// 1. Determines which side is the probe and which is the build side.
    /// 2. Updates metrics based on the batch that was polled.
    /// 3. Executes the join with the given `probe_batch`.
    /// 4. Optionally computes anti-join results if all conditions are met.
    /// 5. Combines the results and returns a combined batch or `None` if no batch was produced.
    fn perform_join_for_given_side(
        &mut self,
        probe_batch: RecordBatch,
        probe_side: JoinSide,
    ) -> Result<Option<RecordBatch>> {
        let (
            probe_hash_joiner,
            build_hash_joiner,
            probe_side_sorted_filter_expr,
            build_side_sorted_filter_expr,
            probe_side_metrics,
        ) = if probe_side.eq(&JoinSide::Left) {
            (
                &mut self.left,
                &mut self.right,
                &mut self.left_sorted_filter_expr,
                &mut self.right_sorted_filter_expr,
                &mut self.metrics.left,
            )
        } else {
            (
                &mut self.right,
                &mut self.left,
                &mut self.right_sorted_filter_expr,
                &mut self.left_sorted_filter_expr,
                &mut self.metrics.right,
            )
        };
        // Update the metrics for the stream that was polled:
        probe_side_metrics.input_batches.add(1);
        probe_side_metrics.input_rows.add(probe_batch.num_rows());
        // Update the internal state of the hash joiner for the build side:
        probe_hash_joiner.update_internal_state(&probe_batch, &self.random_state)?;
        // Join the two sides:
        let equal_result = join_with_probe_batch(
            build_hash_joiner,
            probe_hash_joiner,
            &self.schema,
            self.join_type,
            self.filter.as_ref(),
            &probe_batch,
            &self.column_indices,
            &self.random_state,
            self.null_equals_null,
        )?;
        // Increment the offset for the probe hash joiner:
        probe_hash_joiner.offset += probe_batch.num_rows();

        let anti_result = if let (
            Some(build_side_sorted_filter_expr),
            Some(probe_side_sorted_filter_expr),
            Some(graph),
        ) = (
            build_side_sorted_filter_expr.as_mut(),
            probe_side_sorted_filter_expr.as_mut(),
            self.graph.as_mut(),
        ) {
            // Calculate filter intervals:
            calculate_filter_expr_intervals(
                &build_hash_joiner.input_buffer,
                build_side_sorted_filter_expr,
                &probe_batch,
                probe_side_sorted_filter_expr,
            )?;
            let prune_length = build_hash_joiner
                .calculate_prune_length_with_probe_batch(
                    build_side_sorted_filter_expr,
                    probe_side_sorted_filter_expr,
                    graph,
                )?;
            let result = build_side_determined_results(
                build_hash_joiner,
                &self.schema,
                prune_length,
                probe_batch.schema(),
                self.join_type,
                &self.column_indices,
            )?;
            build_hash_joiner.prune_internal_state(prune_length)?;
            result
        } else {
            None
        };

        // Combine results:
        let result = combine_two_batches(&self.schema, equal_result, anti_result)?;
        let capacity = self.size();
        self.metrics.stream_memory_usage.set(capacity);
        self.reservation.lock().try_resize(capacity)?;
        // Update the metrics if we have a batch; otherwise, continue the loop.
        if let Some(batch) = &result {
            self.metrics.output_batches.add(1);
            self.metrics.output_rows.add(batch.num_rows());
        }
        Ok(result)
    }
}

/// Represents the various states of an symmetric hash join stream operation.
///
/// This enum is used to track the current state of streaming during a join
/// operation. It provides indicators as to which side of the join needs to be
/// pulled next or if one (or both) sides have been exhausted. This allows
/// for efficient management of resources and optimal performance during the
/// join process.
#[derive(Clone, Debug)]
pub enum SHJStreamState {
    /// Indicates that the next step should pull from the right side of the join.
    PullRight,

    /// Indicates that the next step should pull from the left side of the join.
    PullLeft,

    /// State representing that the right side of the join has been fully processed.
    RightExhausted,

    /// State representing that the left side of the join has been fully processed.
    LeftExhausted,

    /// Represents a state where both sides of the join are exhausted.
    ///
    /// The `final_result` field indicates whether the join operation has
    /// produced a final result or not.
    BothExhausted { final_result: bool },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::joins::test_utils::{
        build_sides_record_batches, compare_batches, complicated_filter,
        create_memory_table, join_expr_tests_fixture_f64, join_expr_tests_fixture_i32,
        join_expr_tests_fixture_temporal, partitioned_hash_join_with_filter,
        partitioned_sym_join_with_filter, split_record_batches,
    };

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
    use datafusion_common::ScalarValue;
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col, lit, Column};

    use once_cell::sync::Lazy;
    use rstest::*;

    const TABLE_SIZE: i32 = 30;

    type TableKey = (i32, i32, usize); // (cardinality.0, cardinality.1, batch_size)
    type TableValue = (Vec<RecordBatch>, Vec<RecordBatch>); // (left, right)

    // Cache for storing tables
    static TABLE_CACHE: Lazy<Mutex<HashMap<TableKey, TableValue>>> =
        Lazy::new(|| Mutex::new(HashMap::new()));

    fn get_or_create_table(
        cardinality: (i32, i32),
        batch_size: usize,
    ) -> Result<TableValue> {
        {
            let cache = TABLE_CACHE.lock().unwrap();
            if let Some(table) = cache.get(&(cardinality.0, cardinality.1, batch_size)) {
                return Ok(table.clone());
            }
        }

        // If not, create the table
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;

        let (left_partition, right_partition) = (
            split_record_batches(&left_batch, batch_size)?,
            split_record_batches(&right_batch, batch_size)?,
        );

        // Lock the cache again and store the table
        let mut cache = TABLE_CACHE.lock().unwrap();

        // Store the table in the cache
        cache.insert(
            (cardinality.0, cardinality.1, batch_size),
            (left_partition.clone(), right_partition.clone()),
        );

        Ok((left_partition, right_partition))
    }

    pub async fn experiment(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        on: JoinOn,
        task_ctx: Arc<TaskContext>,
    ) -> Result<()> {
        let first_batches = partitioned_sym_join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter.clone(),
            &join_type,
            false,
            Arc::clone(&task_ctx),
        )
        .await?;
        let second_batches = partitioned_hash_join_with_filter(
            left, right, on, filter, &join_type, false, task_ctx,
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn complex_join_all_one_ascending_numeric(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
        (4, 5),
        (12, 17),
        )]
        cardinality: (i32, i32),
    ) -> Result<()> {
        // a + b > c + 10 AND a + b < c + 100
        let task_ctx = Arc::new(TaskContext::default());

        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();

        let left_sorted = vec![PhysicalSortExpr {
            expr: binary(
                col("la1", left_schema)?,
                Operator::Plus,
                col("la2", left_schema)?,
                left_schema,
            )?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            binary(
                col("lc1", left_schema)?,
                Operator::Plus,
                lit(ScalarValue::Int32(Some(1))),
                left_schema,
            )?,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&intermediate_schema)?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_all_one_ascending_numeric(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(0, 1, 2, 3, 4, 5)] case_expr: usize,
    ) -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let (left_partition, right_partition) = get_or_create_table((4, 5), 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();

        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1", left_schema)?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_without_sort_information(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(0, 1, 2, 3, 4, 5)] case_expr: usize,
    ) -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let (left_partition, right_partition) = get_or_create_table((4, 5), 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let (left, right) =
            create_memory_table(left_partition, right_partition, vec![], vec![])?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 5,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 5,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_without_filter(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
    ) -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let (left_partition, right_partition) = get_or_create_table((11, 21), 8)?;
        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let (left, right) =
            create_memory_table(left_partition, right_partition, vec![], vec![])?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];
        experiment(left, right, None, join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_all_one_descending_numeric_particular(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(0, 1, 2, 3, 4, 5)] case_expr: usize,
    ) -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let (left_partition, right_partition) = get_or_create_table((11, 21), 8)?;
        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1_des", left_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1_des", right_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 5,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 5,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_null_columns_first() -> Result<()> {
        let join_type = JoinType::Full;
        let case_expr = 1;
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table((10, 11), 8)?;
        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_asc_null_first", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_asc_null_first", right_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 6,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 6,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);
        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_null_columns_last() -> Result<()> {
        let join_type = JoinType::Full;
        let case_expr = 1;
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table((10, 11), 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_asc_null_last", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_asc_null_last", right_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 7,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 7,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_null_columns_first_descending() -> Result<()> {
        let join_type = JoinType::Full;
        let cardinality = (10, 11);
        let case_expr = 1;
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_desc_null_first", left_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_desc_null_first", right_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 8,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 8,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn complex_join_all_one_ascending_numeric_missing_stat() -> Result<()> {
        let cardinality = (3, 4);
        let join_type = JoinType::Full;

        // a + b > c + 10 AND a + b < c + 100
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1", left_schema)?,
            options: SortOptions::default(),
        }];

        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&intermediate_schema)?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn complex_join_all_one_ascending_equivalence() -> Result<()> {
        let cardinality = (3, 4);
        let join_type = JoinType::Full;

        // a + b > c + 10 AND a + b < c + 100
        let config = SessionConfig::new().with_repartition_joins(false);
        // let session_ctx = SessionContext::with_config(config);
        // let task_ctx = session_ctx.task_ctx();
        let task_ctx = Arc::new(TaskContext::default().with_session_config(config));
        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;
        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![
            vec![PhysicalSortExpr {
                expr: col("la1", left_schema)?,
                options: SortOptions::default(),
            }],
            vec![PhysicalSortExpr {
                expr: col("la2", left_schema)?,
                options: SortOptions::default(),
            }],
        ];

        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];

        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            left_sorted,
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&intermediate_schema)?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn testing_with_temporal_columns(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
            (4, 5),
            (12, 17),
        )]
        cardinality: (i32, i32),
        #[values(0, 1, 2)] case_expr: usize,
    ) -> Result<()> {
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("lt1", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("rt1", right_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;
        let intermediate_schema = Schema::new(vec![
            Field::new(
                "left",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "right",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);
        let filter_expr = join_expr_tests_fixture_temporal(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 3,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 3,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);
        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_interval_columns(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
            (4, 5),
            (12, 17),
        )]
        cardinality: (i32, i32),
    ) -> Result<()> {
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("li1", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ri1", right_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;
        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Interval(IntervalUnit::DayTime), false),
            Field::new("right", DataType::Interval(IntervalUnit::DayTime), false),
        ]);
        let filter_expr = join_expr_tests_fixture_temporal(
            0,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 9,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 9,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);
        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;

        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn testing_ascending_float_pruning(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
            (4, 5),
            (12, 17),
        )]
        cardinality: (i32, i32),
        #[values(0, 1, 2, 3, 4, 5)] case_expr: usize,
    ) -> Result<()> {
        let session_config = SessionConfig::new().with_repartition_joins(false);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);
        let (left_partition, right_partition) = get_or_create_table(cardinality, 8)?;

        let left_schema = &left_partition[0].schema();
        let right_schema = &right_partition[0].schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_float", left_schema)?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_float", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_partition,
            right_partition,
            vec![left_sorted],
            vec![right_sorted],
        )?;

        let on = vec![(
            Arc::new(Column::new_with_schema("lc1", left_schema)?) as _,
            Arc::new(Column::new_with_schema("rc1", right_schema)?) as _,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Float64, true),
            Field::new("right", DataType::Float64, true),
        ]);
        let filter_expr = join_expr_tests_fixture_f64(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 10, // l_float
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 10, // r_float
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }
}
