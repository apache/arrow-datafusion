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

//! [`HashJoinExec`] Partitioned Hash Join Operator

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::{any::Any, vec};

use super::utils::asymmetric_join_output_partitioning;
use super::{
    utils::{OnceAsync, OnceFut},
    PartitionMode,
};
use crate::ExecutionPlanProperties;
use crate::{
    coalesce_partitions::CoalescePartitionsExec,
    common::can_project,
    execution_mode_from_children, handle_state,
    hash_utils::create_hashes,
    joins::utils::{
        adjust_indices_by_join_type, apply_join_filter_to_indices,
        build_batch_from_indices, build_join_schema, check_join_is_valid,
        estimate_join_statistics, get_final_indices_from_bit_map,
        need_produce_result_in_final, symmetric_join_output_partitioning,
        BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinHashMap, JoinHashMapOffset,
        JoinHashMapType, JoinOn, JoinOnRef, StatefulStreamResult,
    },
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    DisplayAs, DisplayFormatType, Distribution, ExecutionMode, ExecutionPlan,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, PrimitiveArray, UInt32Array,
    UInt64Array,
};
use arrow::compute::kernels::cmp::{eq, not_distinct};
use arrow::compute::{and, concat_batches, take, FilterBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use arrow_array::cast::downcast_array;
use arrow_schema::ArrowError;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{
    internal_datafusion_err, internal_err, plan_err, project_schema, DataFusionError,
    JoinSide, JoinType, Result,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::{
    join_equivalence_properties, ProjectionMapping,
};
use datafusion_physical_expr::expressions::UnKnownColumn;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

use ahash::RandomState;
use datafusion_expr::Operator;
use datafusion_physical_expr_common::datum::compare_op_for_nested;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;

type SharedBitmapBuilder = Mutex<BooleanBufferBuilder>;

/// HashTable and input data for the left (build side) of a join
struct JoinLeftData {
    /// The hash table with indices into `batch`
    hash_map: JoinHashMap,
    /// The input rows for the build side
    batch: RecordBatch,
    /// Shared bitmap builder for visited left indices
    visited_indices_bitmap: Mutex<BooleanBufferBuilder>,
    /// Counter of running probe-threads, potentially
    /// able to update `visited_indices_bitmap`
    probe_threads_counter: AtomicUsize,
    /// Memory reservation that tracks memory used by `hash_map` hash table
    /// `batch`. Cleared on drop.
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl JoinLeftData {
    /// Create a new `JoinLeftData` from its parts
    fn new(
        hash_map: JoinHashMap,
        batch: RecordBatch,
        visited_indices_bitmap: SharedBitmapBuilder,
        probe_threads_counter: AtomicUsize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            hash_map,
            batch,
            visited_indices_bitmap,
            probe_threads_counter,
            reservation,
        }
    }

    /// return a reference to the hash map
    fn hash_map(&self) -> &JoinHashMap {
        &self.hash_map
    }

    /// returns a reference to the build side batch
    fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// returns a reference to the visited indices bitmap
    fn visited_indices_bitmap(&self) -> &SharedBitmapBuilder {
        &self.visited_indices_bitmap
    }

    /// Decrements the counter of running threads, and returns `true`
    /// if caller is the last running thread
    fn report_probe_completed(&self) -> bool {
        self.probe_threads_counter.fetch_sub(1, Ordering::Relaxed) == 1
    }
}

/// Join execution plan: Evaluates eqijoin predicates in parallel on multiple
/// partitions using a hash table and an optional filter list to apply post
/// join.
///
/// # Join Expressions
///
/// This implementation is optimized for evaluating eqijoin predicates  (
/// `<col1> = <col2>`) expressions, which are represented as a list of `Columns`
/// in [`Self::on`].
///
/// Non-equality predicates, which can not pushed down to a join inputs (e.g.
/// `<col1> != <col2>`) are known as "filter expressions" and are evaluated
/// after the equijoin predicates.
///
/// # "Build Side" vs "Probe Side"
///
/// HashJoin takes two inputs, which are referred to as the "build" and the
/// "probe". The build side is the first child, and the probe side is the second
/// child.
///
/// The two inputs are treated differently and it is VERY important that the
/// *smaller* input is placed on the build side to minimize the work of creating
/// the hash table.
///
/// ```text
///          ┌───────────┐
///          │ HashJoin  │
///          │           │
///          └───────────┘
///              │   │
///        ┌─────┘   └─────┐
///        ▼               ▼
/// ┌────────────┐  ┌─────────────┐
/// │   Input    │  │    Input    │
/// │    [0]     │  │     [1]     │
/// └────────────┘  └─────────────┘
///
///  "build side"    "probe side"
/// ```
///
/// Execution proceeds in 2 stages:
///
/// 1. the **build phase** creates a hash table from the tuples of the build side,
/// and single concatenated batch containing data from all fetched record batches.
/// Resulting hash table stores hashed join-key fields for each row as a key, and
/// indices of corresponding rows in concatenated batch.
///
/// Hash join uses LIFO data structure as a hash table, and in order to retain
/// original build-side input order while obtaining data during probe phase, hash
/// table is updated by iterating batch sequence in reverse order -- it allows to
/// keep rows with smaller indices "on the top" of hash table, and still maintain
/// correct indexing for concatenated build-side data batch.
///
/// Example of build phase for 3 record batches:
///
///
/// ```text
///
///  Original build-side data   Inserting build-side values into hashmap    Concatenated build-side batch
///                                                                         ┌───────────────────────────┐
///                             hasmap.insert(row-hash, row-idx + offset)   │                      idx  │
///            ┌───────┐                                                    │          ┌───────┐        │
///            │ Row 1 │        1) update_hash for batch 3 with offset 0    │          │ Row 6 │    0   │
///   Batch 1  │       │           - hashmap.insert(Row 7, idx 1)           │ Batch 3  │       │        │
///            │ Row 2 │           - hashmap.insert(Row 6, idx 0)           │          │ Row 7 │    1   │
///            └───────┘                                                    │          └───────┘        │
///                                                                         │                           │
///            ┌───────┐                                                    │          ┌───────┐        │
///            │ Row 3 │        2) update_hash for batch 2 with offset 2    │          │ Row 3 │    2   │
///            │       │           - hashmap.insert(Row 5, idx 4)           │          │       │        │
///   Batch 2  │ Row 4 │           - hashmap.insert(Row 4, idx 3)           │ Batch 2  │ Row 4 │    3   │
///            │       │           - hashmap.insert(Row 3, idx 2)           │          │       │        │
///            │ Row 5 │                                                    │          │ Row 5 │    4   │
///            └───────┘                                                    │          └───────┘        │
///                                                                         │                           │
///            ┌───────┐                                                    │          ┌───────┐        │
///            │ Row 6 │        3) update_hash for batch 1 with offset 5    │          │ Row 1 │    5   │
///   Batch 3  │       │           - hashmap.insert(Row 2, idx 5)           │ Batch 1  │       │        │
///            │ Row 7 │           - hashmap.insert(Row 1, idx 6)           │          │ Row 2 │    6   │
///            └───────┘                                                    │          └───────┘        │
///                                                                         │                           │
///                                                                         └───────────────────────────┘
///
/// ```
///
/// 2. the **probe phase** where the tuples of the probe side are streamed
/// through, checking for matches of the join keys in the hash table.
///
/// ```text
///                 ┌────────────────┐          ┌────────────────┐
///                 │ ┌─────────┐    │          │ ┌─────────┐    │
///                 │ │  Hash   │    │          │ │  Hash   │    │
///                 │ │  Table  │    │          │ │  Table  │    │
///                 │ │(keys are│    │          │ │(keys are│    │
///                 │ │equi join│    │          │ │equi join│    │  Stage 2: batches from
///  Stage 1: the   │ │columns) │    │          │ │columns) │    │    the probe side are
/// *entire* build  │ │         │    │          │ │         │    │  streamed through, and
///  side is read   │ └─────────┘    │          │ └─────────┘    │   checked against the
/// into the hash   │      ▲         │          │          ▲     │   contents of the hash
///     table       │       HashJoin │          │  HashJoin      │          table
///                 └──────┼─────────┘          └──────────┼─────┘
///             ─ ─ ─ ─ ─ ─                                 ─ ─ ─ ─ ─ ─ ─
///            │                                                         │
///
///            │                                                         │
///     ┌────────────┐                                            ┌────────────┐
///     │RecordBatch │                                            │RecordBatch │
///     └────────────┘                                            └────────────┘
///     ┌────────────┐                                            ┌────────────┐
///     │RecordBatch │                                            │RecordBatch │
///     └────────────┘                                            └────────────┘
///           ...                                                       ...
///     ┌────────────┐                                            ┌────────────┐
///     │RecordBatch │                                            │RecordBatch │
///     └────────────┘                                            └────────────┘
///
///        build side                                                probe side
///
/// ```
///
/// # Example "Optimal" Plans
///
/// The differences in the inputs means that for classic "Star Schema Query",
/// the optimal plan will be a **"Right Deep Tree"** . A Star Schema Query is
/// one where there is one large table and several smaller "dimension" tables,
/// joined on `Foreign Key = Primary Key` predicates.
///
/// A "Right Deep Tree" looks like this large table as the probe side on the
/// lowest join:
///
/// ```text
///             ┌───────────┐
///             │ HashJoin  │
///             │           │
///             └───────────┘
///                 │   │
///         ┌───────┘   └──────────┐
///         ▼                      ▼
/// ┌───────────────┐        ┌───────────┐
/// │ small table 1 │        │ HashJoin  │
/// │  "dimension"  │        │           │
/// └───────────────┘        └───┬───┬───┘
///                   ┌──────────┘   └───────┐
///                   │                      │
///                   ▼                      ▼
///           ┌───────────────┐        ┌───────────┐
///           │ small table 2 │        │ HashJoin  │
///           │  "dimension"  │        │           │
///           └───────────────┘        └───┬───┬───┘
///                               ┌────────┘   └────────┐
///                               │                     │
///                               ▼                     ▼
///                       ┌───────────────┐     ┌───────────────┐
///                       │ small table 3 │     │  large table  │
///                       │  "dimension"  │     │    "fact"     │
///                       └───────────────┘     └───────────────┘
/// ```
#[derive(Debug)]
pub struct HashJoinExec {
    /// left (build) side which gets hashed
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of equijoin columns from the relations: `(left_col, right_col)`
    pub on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The schema after join. Please be careful when using this schema,
    /// if there is a projection, the schema isn't the same as the output schema.
    join_schema: SchemaRef,
    /// Future that consumes left input and builds the hash table
    left_fut: OnceAsync<JoinLeftData>,
    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Partitioning mode to use
    pub mode: PartitionMode,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The projection indices of the columns in the output schema of join
    pub projection: Option<Vec<usize>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Null matching behavior: If `null_equals_null` is true, rows that have
    /// `null`s in both left and right equijoin columns will be matched.
    /// Otherwise, rows that have `null`s in the join columns will not be
    /// matched and thus will not appear in the output.
    pub null_equals_null: bool,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl HashJoinExec {
    /// Tries to create a new [HashJoinExec].
    ///
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
        partition_mode: PartitionMode,
        null_equals_null: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return plan_err!("On constraints in HashJoinExec should be non-empty");
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let join_schema = Arc::new(join_schema);

        //  check if the projection is valid
        can_project(&join_schema, projection.as_ref())?;

        let cache = Self::compute_properties(
            &left,
            &right,
            Arc::clone(&join_schema),
            *join_type,
            &on,
            partition_mode,
            projection.as_ref(),
        )?;

        Ok(HashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            join_schema,
            left_fut: Default::default(),
            random_state,
            mode: partition_mode,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
            column_indices,
            null_equals_null,
            cache,
        })
    }

    /// left (build) side which gets hashed
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right (probe) side which are filtered by the hash table
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

    /// The partitioning mode of this hash join
    pub fn partition_mode(&self) -> &PartitionMode {
        &self.mode
    }

    /// Get null_equals_null
    pub fn null_equals_null(&self) -> bool {
        self.null_equals_null
    }

    /// Calculate order preservation flags for this hash join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner | JoinType::RightAnti | JoinType::RightSemi
            ),
        ]
    }

    /// Get probe side information for the hash join.
    pub fn probe_side() -> JoinSide {
        // In current implementation right side is always probe side.
        JoinSide::Right
    }

    /// Return whether the join contains a projection
    pub fn contain_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// Return new instance of [HashJoinExec] with the given projection.
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        //  check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;
        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };
        Self::try_new(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            projection,
            self.mode,
            self.null_equals_null,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        on: JoinOnRef,
        mode: PartitionMode,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(&schema),
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side()),
            on,
        );

        let mut output_partitioning = match mode {
            PartitionMode::CollectLeft => {
                asymmetric_join_output_partitioning(left, right, &join_type)
            }
            PartitionMode::Auto => Partitioning::UnknownPartitioning(
                right.output_partitioning().partition_count(),
            ),
            PartitionMode::Partitioned => {
                symmetric_join_output_partitioning(left, right, &join_type)
            }
        };

        // Determine execution mode by checking whether this join is pipeline
        // breaking. This happens when the left side is unbounded, or the right
        // side is unbounded with `Left`, `Full`, `LeftAnti` or `LeftSemi` join types.
        let pipeline_breaking = left.execution_mode().is_unbounded()
            || (right.execution_mode().is_unbounded()
                && matches!(
                    join_type,
                    JoinType::Left
                        | JoinType::Full
                        | JoinType::LeftAnti
                        | JoinType::LeftSemi
                ));

        let mode = if pipeline_breaking {
            ExecutionMode::PipelineBreaking
        } else {
            execution_mode_from_children([left, right])
        };

        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            let projection_exprs = project_index_to_exprs(projection, &schema);
            // construct a map from the input expressions to the output expression of the Projection
            let projection_mapping =
                ProjectionMapping::try_new(&projection_exprs, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
            if let Partitioning::Hash(exprs, part) = output_partitioning {
                let normalized_exprs = exprs
                    .iter()
                    .map(|expr| {
                        eq_properties
                            .project_expr(expr, &projection_mapping)
                            .unwrap_or_else(|| {
                                Arc::new(UnKnownColumn::new(&expr.to_string()))
                            })
                    })
                    .collect();
                output_partitioning = Partitioning::Hash(normalized_exprs, part);
            }
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            mode,
        ))
    }
}

impl DisplayAs for HashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let display_projections = if self.contain_projection() {
                    format!(
                        ", projection=[{}]",
                        self.projection
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.join_schema.fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "HashJoinExec: mode={:?}, join_type={:?}, on=[{}]{}{}",
                    self.mode, self.join_type, on, display_filter, display_projections
                )
            }
        }
    }
}

fn project_index_to_exprs(
    projection_index: &[usize],
    schema: &SchemaRef,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    projection_index
        .iter()
        .map(|index| {
            let field = schema.field(*index);
            (
                Arc::new(datafusion_physical_expr::expressions::Column::new(
                    field.name(),
                    *index,
                )) as Arc<dyn PhysicalExpr>,
                field.name().to_owned(),
            )
        })
        .collect::<Vec<_>>()
}

impl ExecutionPlan for HashJoinExec {
    fn name(&self) -> &'static str {
        "HashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            PartitionMode::CollectLeft => vec![
                Distribution::SinglePartition,
                Distribution::UnspecifiedDistribution,
            ],
            PartitionMode::Partitioned => {
                let (left_expr, right_expr) = self
                    .on
                    .iter()
                    .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
                    .unzip();
                vec![
                    Distribution::HashPartitioned(left_expr),
                    Distribution::HashPartitioned(right_expr),
                ]
            }
            PartitionMode::Auto => vec![
                Distribution::UnspecifiedDistribution,
                Distribution::UnspecifiedDistribution,
            ],
        }
    }

    // For [JoinType::Inner] and [JoinType::RightSemi] in hash joins, the probe phase initiates by
    // applying the hash function to convert the join key(s) in each row into a hash value from the
    // probe side table in the order they're arranged. The hash value is used to look up corresponding
    // entries in the hash table that was constructed from the build side table during the build phase.
    //
    // Because of the immediate generation of result rows once a match is found,
    // the output of the join tends to follow the order in which the rows were read from
    // the probe side table. This is simply due to the sequence in which the rows were processed.
    // Hence, it appears that the hash join is preserving the order of the probe side.
    //
    // Meanwhile, in the case of a [JoinType::RightAnti] hash join,
    // the unmatched rows from the probe side are also kept in order.
    // This is because the **`RightAnti`** join is designed to return rows from the right
    // (probe side) table that have no match in the left (build side) table. Because the rows
    // are processed sequentially in the probe phase, and unmatched rows are directly output
    // as results, these results tend to retain the order of the probe side table.
    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HashJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.projection.clone(),
            self.mode,
            self.null_equals_null,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_left = self
            .on
            .iter()
            .map(|on| Arc::clone(&on.0))
            .collect::<Vec<_>>();
        let on_right = self
            .on
            .iter()
            .map(|on| Arc::clone(&on.1))
            .collect::<Vec<_>>();
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();

        if self.mode == PartitionMode::Partitioned && left_partitions != right_partitions
        {
            return internal_err!(
                "Invalid HashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);
        let left_fut = match self.mode {
            PartitionMode::CollectLeft => self.left_fut.once(|| {
                let reservation =
                    MemoryConsumer::new("HashJoinInput").register(context.memory_pool());
                collect_left_input(
                    None,
                    self.random_state.clone(),
                    Arc::clone(&self.left),
                    on_left.clone(),
                    Arc::clone(&context),
                    join_metrics.clone(),
                    reservation,
                    need_produce_result_in_final(self.join_type),
                    self.right().output_partitioning().partition_count(),
                )
            }),
            PartitionMode::Partitioned => {
                let reservation =
                    MemoryConsumer::new(format!("HashJoinInput[{partition}]"))
                        .register(context.memory_pool());

                OnceFut::new(collect_left_input(
                    Some(partition),
                    self.random_state.clone(),
                    Arc::clone(&self.left),
                    on_left.clone(),
                    Arc::clone(&context),
                    join_metrics.clone(),
                    reservation,
                    need_produce_result_in_final(self.join_type),
                    1,
                ))
            }
            PartitionMode::Auto => {
                return plan_err!(
                    "Invalid HashJoinExec, unsupported PartitionMode {:?} in execute()",
                    PartitionMode::Auto
                );
            }
        };

        let batch_size = context.session_config().batch_size();

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.
        let right_stream = self.right.execute(partition, context)?;

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
        };

        Ok(Box::pin(HashJoinStream {
            schema: self.schema(),
            on_left,
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            right: right_stream,
            column_indices: column_indices_after_projection,
            random_state: self.random_state.clone(),
            join_metrics,
            null_equals_null: self.null_equals_null,
            state: HashJoinStreamState::WaitBuildSide,
            build_side: BuildSide::Initial(BuildSideInitialState { left_fut }),
            batch_size,
            hashes_buffer: vec![],
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        let mut stats = estimate_join_statistics(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.on.clone(),
            &self.join_type,
            &self.join_schema,
        )?;
        // Project statistics if there is a projection
        if let Some(projection) = &self.projection {
            stats.column_statistics = stats
                .column_statistics
                .into_iter()
                .enumerate()
                .filter(|(i, _)| projection.contains(i))
                .map(|(_, s)| s)
                .collect();
        }
        Ok(stats)
    }
}

/// Reads the left (build) side of the input, buffering it in memory, to build a
/// hash table (`LeftJoinData`)
#[allow(clippy::too_many_arguments)]
async fn collect_left_input(
    partition: Option<usize>,
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<PhysicalExprRef>,
    context: Arc<TaskContext>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    with_visited_indices_bitmap: bool,
    probe_threads_count: usize,
) -> Result<JoinLeftData> {
    let schema = left.schema();

    let (left_input, left_input_partition) = if let Some(partition) = partition {
        (left, partition)
    } else if left.output_partitioning().partition_count() != 1 {
        (Arc::new(CoalescePartitionsExec::new(left)) as _, 0)
    } else {
        (left, 0)
    };

    // Depending on partition argument load single partition or whole left side in memory
    let stream = left_input.execute(left_input_partition, Arc::clone(&context))?;

    // This operation performs 2 steps at once:
    // 1. creates a [JoinHashMap] of all batches from the stream
    // 2. stores the batches in a vector.
    let initial = (Vec::new(), 0, metrics, reservation);
    let (batches, num_rows, metrics, mut reservation) = stream
        .try_fold(initial, |mut acc, batch| async {
            let batch_size = batch.get_array_memory_size();
            // Reserve memory for incoming batch
            acc.3.try_grow(batch_size)?;
            // Update metrics
            acc.2.build_mem_used.add(batch_size);
            acc.2.build_input_batches.add(1);
            acc.2.build_input_rows.add(batch.num_rows());
            // Update rowcount
            acc.1 += batch.num_rows();
            // Push batch to output
            acc.0.push(batch);
            Ok(acc)
        })
        .await?;

    // Estimation of memory size, required for hashtable, prior to allocation.
    // Final result can be verified using `RawTable.allocation_info()`
    let fixed_size = std::mem::size_of::<JoinHashMap>();
    let estimated_hashtable_size =
        estimate_memory_size::<(u64, u64)>(num_rows, fixed_size)?;

    reservation.try_grow(estimated_hashtable_size)?;
    metrics.build_mem_used.add(estimated_hashtable_size);

    let mut hashmap = JoinHashMap::with_capacity(num_rows);
    let mut hashes_buffer = Vec::new();
    let mut offset = 0;

    // Updating hashmap starting from the last batch
    let batches_iter = batches.iter().rev();
    for batch in batches_iter.clone() {
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hash(
            &on_left,
            batch,
            &mut hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
            0,
            true,
        )?;
        offset += batch.num_rows();
    }
    // Merge all batches into a single batch, so we can directly index into the arrays
    let single_batch = concat_batches(&schema, batches_iter)?;

    // Reserve additional memory for visited indices bitmap and create shared builder
    let visited_indices_bitmap = if with_visited_indices_bitmap {
        let bitmap_size = bit_util::ceil(single_batch.num_rows(), 8);
        reservation.try_grow(bitmap_size)?;
        metrics.build_mem_used.add(bitmap_size);

        let mut bitmap_buffer = BooleanBufferBuilder::new(single_batch.num_rows());
        bitmap_buffer.append_n(num_rows, false);
        bitmap_buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    let data = JoinLeftData::new(
        hashmap,
        single_batch,
        Mutex::new(visited_indices_bitmap),
        AtomicUsize::new(probe_threads_count),
        reservation,
    );

    Ok(data)
}

/// Updates `hash_map` with new entries from `batch` evaluated against the expressions `on`
/// using `offset` as a start value for `batch` row indices.
///
/// `fifo_hashmap` sets the order of iteration over `batch` rows while updating hashmap,
/// which allows to keep either first (if set to true) or last (if set to false) row index
/// as a chain head for rows with equal hash values.
#[allow(clippy::too_many_arguments)]
pub fn update_hash<T>(
    on: &[PhysicalExprRef],
    batch: &RecordBatch,
    hash_map: &mut T,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
    deleted_offset: usize,
    fifo_hashmap: bool,
) -> Result<()>
where
    T: JoinHashMapType,
{
    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|c| c.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    // calculate the hash values
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;

    // For usual JoinHashmap, the implementation is void.
    hash_map.extend_zero(batch.num_rows());

    // Updating JoinHashMap from hash values iterator
    let hash_values_iter = hash_values
        .iter()
        .enumerate()
        .map(|(i, val)| (i + offset, val));

    if fifo_hashmap {
        hash_map.update_from_iter(hash_values_iter.rev(), deleted_offset);
    } else {
        hash_map.update_from_iter(hash_values_iter, deleted_offset);
    }

    Ok(())
}

/// Represents build-side of hash join.
enum BuildSide {
    /// Indicates that build-side not collected yet
    Initial(BuildSideInitialState),
    /// Indicates that build-side data has been collected
    Ready(BuildSideReadyState),
}

/// Container for BuildSide::Initial related data
struct BuildSideInitialState {
    /// Future for building hash table from build-side input
    left_fut: OnceFut<JoinLeftData>,
}

/// Container for BuildSide::Ready related data
struct BuildSideReadyState {
    /// Collected build-side data
    left_data: Arc<JoinLeftData>,
}

impl BuildSide {
    /// Tries to extract BuildSideInitialState from BuildSide enum.
    /// Returns an error if state is not Initial.
    fn try_as_initial_mut(&mut self) -> Result<&mut BuildSideInitialState> {
        match self {
            BuildSide::Initial(state) => Ok(state),
            _ => internal_err!("Expected build side in initial state"),
        }
    }

    /// Tries to extract BuildSideReadyState from BuildSide enum.
    /// Returns an error if state is not Ready.
    fn try_as_ready(&self) -> Result<&BuildSideReadyState> {
        match self {
            BuildSide::Ready(state) => Ok(state),
            _ => internal_err!("Expected build side in ready state"),
        }
    }

    /// Tries to extract BuildSideReadyState from BuildSide enum.
    /// Returns an error if state is not Ready.
    fn try_as_ready_mut(&mut self) -> Result<&mut BuildSideReadyState> {
        match self {
            BuildSide::Ready(state) => Ok(state),
            _ => internal_err!("Expected build side in ready state"),
        }
    }
}

/// Represents state of HashJoinStream
///
/// Expected state transitions performed by HashJoinStream are:
///
/// ```text
///
///       WaitBuildSide
///             │
///             ▼
///  ┌─► FetchProbeBatch ───► ExhaustedProbeSide ───► Completed
///  │          │
///  │          ▼
///  └─ ProcessProbeBatch
///
/// ```
enum HashJoinStreamState {
    /// Initial state for HashJoinStream indicating that build-side data not collected yet
    WaitBuildSide,
    /// Indicates that build-side has been collected, and stream is ready for fetching probe-side
    FetchProbeBatch,
    /// Indicates that non-empty batch has been fetched from probe-side, and is ready to be processed
    ProcessProbeBatch(ProcessProbeBatchState),
    /// Indicates that probe-side has been fully processed
    ExhaustedProbeSide,
    /// Indicates that HashJoinStream execution is completed
    Completed,
}

impl HashJoinStreamState {
    /// Tries to extract ProcessProbeBatchState from HashJoinStreamState enum.
    /// Returns an error if state is not ProcessProbeBatchState.
    fn try_as_process_probe_batch_mut(&mut self) -> Result<&mut ProcessProbeBatchState> {
        match self {
            HashJoinStreamState::ProcessProbeBatch(state) => Ok(state),
            _ => internal_err!("Expected hash join stream in ProcessProbeBatch state"),
        }
    }
}

/// Container for HashJoinStreamState::ProcessProbeBatch related data
struct ProcessProbeBatchState {
    /// Current probe-side batch
    batch: RecordBatch,
    /// Starting offset for JoinHashMap lookups
    offset: JoinHashMapOffset,
    /// Max joined probe-side index from current batch
    joined_probe_idx: Option<usize>,
}

impl ProcessProbeBatchState {
    fn advance(&mut self, offset: JoinHashMapOffset, joined_probe_idx: Option<usize>) {
        self.offset = offset;
        if joined_probe_idx.is_some() {
            self.joined_probe_idx = joined_probe_idx;
        }
    }
}

/// [`Stream`] for [`HashJoinExec`] that does the actual join.
///
/// This stream:
///
/// 1. Reads the entire left input (build) and constructs a hash table
///
/// 2. Streams [RecordBatch]es as they arrive from the right input (probe) and joins
/// them with the contents of the hash table
struct HashJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// equijoin columns from the left (build side)
    on_left: Vec<PhysicalExprRef>,
    /// equijoin columns from the right (probe side)
    on_right: Vec<PhysicalExprRef>,
    /// optional join filter
    filter: Option<JoinFilter>,
    /// type of the join (left, right, semi, etc)
    join_type: JoinType,
    /// right (probe) input
    right: SendableRecordBatchStream,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// Metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// State of the stream
    state: HashJoinStreamState,
    /// Build side
    build_side: BuildSide,
    /// Maximum output batch size
    batch_size: usize,
    /// Scratch space for computing hashes
    hashes_buffer: Vec<u64>,
}

impl RecordBatchStream for HashJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Executes lookups by hash against JoinHashMap and resolves potential
/// hash collisions.
/// Returns build/probe indices satisfying the equality condition, along with
/// (optional) starting point for next iteration.
///
/// # Example
///
/// For `LEFT.b1 = RIGHT.b2`:
/// LEFT (build) Table:
/// ```text
///  a1  b1  c1
///  1   1   10
///  3   3   30
///  5   5   50
///  7   7   70
///  9   8   90
///  11  8   110
///  13   10  130
/// ```
///
/// RIGHT (probe) Table:
/// ```text
///  a2   b2  c2
///  2    2   20
///  4    4   40
///  6    6   60
///  8    8   80
/// 10   10  100
/// 12   10  120
/// ```
///
/// The result is
/// ```text
/// "+----+----+-----+----+----+-----+",
/// "| a1 | b1 | c1  | a2 | b2 | c2  |",
/// "+----+----+-----+----+----+-----+",
/// "| 9  | 8  | 90  | 8  | 8  | 80  |",
/// "| 11 | 8  | 110 | 8  | 8  | 80  |",
/// "| 13 | 10 | 130 | 10 | 10 | 100 |",
/// "| 13 | 10 | 130 | 12 | 10 | 120 |",
/// "+----+----+-----+----+----+-----+"
/// ```
///
/// And the result of build and probe indices are:
/// ```text
/// Build indices: 4, 5, 6, 6
/// Probe indices: 3, 3, 4, 5
/// ```
#[allow(clippy::too_many_arguments)]
fn lookup_join_hashmap(
    build_hashmap: &JoinHashMap,
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_on: &[PhysicalExprRef],
    probe_on: &[PhysicalExprRef],
    null_equals_null: bool,
    hashes_buffer: &[u64],
    limit: usize,
    offset: JoinHashMapOffset,
) -> Result<(UInt64Array, UInt32Array, Option<JoinHashMapOffset>)> {
    let keys_values = probe_on
        .iter()
        .map(|c| c.evaluate(probe_batch)?.into_array(probe_batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;
    let build_join_values = build_on
        .iter()
        .map(|c| {
            c.evaluate(build_input_buffer)?
                .into_array(build_input_buffer.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    let (mut probe_builder, mut build_builder, next_offset) = build_hashmap
        .get_matched_indices_with_limit_offset(hashes_buffer, None, limit, offset);

    let build_indices: UInt64Array =
        PrimitiveArray::new(build_builder.finish().into(), None);
    let probe_indices: UInt32Array =
        PrimitiveArray::new(probe_builder.finish().into(), None);

    let (build_indices, probe_indices) = equal_rows_arr(
        &build_indices,
        &probe_indices,
        &build_join_values,
        &keys_values,
        null_equals_null,
    )?;

    Ok((build_indices, probe_indices, next_offset))
}

// version of eq_dyn supporting equality on null arrays
fn eq_dyn_null(
    left: &dyn Array,
    right: &dyn Array,
    null_equals_null: bool,
) -> Result<BooleanArray, ArrowError> {
    // Nested datatypes cannot use the underlying not_distinct function and must use a special
    // implementation
    // <https://github.com/apache/datafusion/issues/10749>
    if left.data_type().is_nested() && null_equals_null {
        return Ok(compare_op_for_nested(&Operator::Eq, &left, &right)?);
    }
    match (left.data_type(), right.data_type()) {
        _ if null_equals_null => not_distinct(&left, &right),
        _ => eq(&left, &right),
    }
}

pub fn equal_rows_arr(
    indices_left: &UInt64Array,
    indices_right: &UInt32Array,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    null_equals_null: bool,
) -> Result<(UInt64Array, UInt32Array)> {
    let mut iter = left_arrays.iter().zip(right_arrays.iter());

    let (first_left, first_right) = iter.next().ok_or_else(|| {
        DataFusionError::Internal(
            "At least one array should be provided for both left and right".to_string(),
        )
    })?;

    let arr_left = take(first_left.as_ref(), indices_left, None)?;
    let arr_right = take(first_right.as_ref(), indices_right, None)?;

    let mut equal: BooleanArray = eq_dyn_null(&arr_left, &arr_right, null_equals_null)?;

    // Use map and try_fold to iterate over the remaining pairs of arrays.
    // In each iteration, take is used on the pair of arrays and their equality is determined.
    // The results are then folded (combined) using the and function to get a final equality result.
    equal = iter
        .map(|(left, right)| {
            let arr_left = take(left.as_ref(), indices_left, None)?;
            let arr_right = take(right.as_ref(), indices_right, None)?;
            eq_dyn_null(arr_left.as_ref(), arr_right.as_ref(), null_equals_null)
        })
        .try_fold(equal, |acc, equal2| and(&acc, &equal2?))?;

    let filter_builder = FilterBuilder::new(&equal).optimize().build();

    let left_filtered = filter_builder.filter(indices_left)?;
    let right_filtered = filter_builder.filter(indices_right)?;

    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

fn get_final_indices_from_shared_bitmap(
    shared_bitmap: &SharedBitmapBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let bitmap = shared_bitmap.lock();
    get_final_indices_from_bit_map(&bitmap, join_type)
}

impl HashJoinStream {
    /// Separate implementation function that unpins the [`HashJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                HashJoinStreamState::WaitBuildSide => {
                    handle_state!(ready!(self.collect_build_side(cx)))
                }
                HashJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                HashJoinStreamState::ProcessProbeBatch(_) => {
                    handle_state!(self.process_probe_batch())
                }
                HashJoinStreamState::ExhaustedProbeSide => {
                    handle_state!(self.process_unmatched_build_batch())
                }
                HashJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    /// Collects build-side data by polling `OnceFut` future from initialized build-side
    ///
    /// Updates build-side to `Ready`, and state to `FetchProbeSide`
    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        // build hash table from left (build) side, if not yet done
        let left_data = ready!(self
            .build_side
            .try_as_initial_mut()?
            .left_fut
            .get_shared(cx))?;
        build_timer.done();

        self.state = HashJoinStreamState::FetchProbeBatch;
        self.build_side = BuildSide::Ready(BuildSideReadyState { left_data });

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Fetches next batch from probe-side
    ///
    /// If non-empty batch has been fetched, updates state to `ProcessProbeBatchState`,
    /// otherwise updates state to `ExhaustedProbeSide`
    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.right.poll_next_unpin(cx)) {
            None => {
                self.state = HashJoinStreamState::ExhaustedProbeSide;
            }
            Some(Ok(batch)) => {
                // Precalculate hash values for fetched batch
                let keys_values = self
                    .on_right
                    .iter()
                    .map(|c| c.evaluate(&batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()?;

                self.hashes_buffer.clear();
                self.hashes_buffer.resize(batch.num_rows(), 0);
                create_hashes(&keys_values, &self.random_state, &mut self.hashes_buffer)?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                self.state =
                    HashJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState {
                        batch,
                        offset: (0, None),
                        joined_probe_idx: None,
                    });
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Joins current probe batch with build-side data and produces batch with matched output
    ///
    /// Updates state to `FetchProbeBatch`
    fn process_probe_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let state = self.state.try_as_process_probe_batch_mut()?;
        let build_side = self.build_side.try_as_ready_mut()?;

        let timer = self.join_metrics.join_time.timer();

        // get the matched by join keys indices
        let (left_indices, right_indices, next_offset) = lookup_join_hashmap(
            build_side.left_data.hash_map(),
            build_side.left_data.batch(),
            &state.batch,
            &self.on_left,
            &self.on_right,
            self.null_equals_null,
            &self.hashes_buffer,
            self.batch_size,
            state.offset,
        )?;

        // apply join filter if exists
        let (left_indices, right_indices) = if let Some(filter) = &self.filter {
            apply_join_filter_to_indices(
                build_side.left_data.batch(),
                &state.batch,
                left_indices,
                right_indices,
                filter,
                JoinSide::Left,
            )?
        } else {
            (left_indices, right_indices)
        };

        // mark joined left-side indices as visited, if required by join type
        if need_produce_result_in_final(self.join_type) {
            let mut bitmap = build_side.left_data.visited_indices_bitmap().lock();
            left_indices.iter().flatten().for_each(|x| {
                bitmap.set_bit(x as usize, true);
            });
        }

        // The goals of index alignment for different join types are:
        //
        // 1) Right & FullJoin -- to append all missing probe-side indices between
        //    previous (excluding) and current joined indices.
        // 2) SemiJoin -- deduplicate probe indices in range between previous
        //    (excluding) and current joined indices.
        // 3) AntiJoin -- return only missing indices in range between
        //    previous and current joined indices.
        //    Inclusion/exclusion of the indices themselves don't matter
        //
        // As a summary -- alignment range can be produced based only on
        // joined (matched with filters applied) probe side indices, excluding starting one
        // (left from previous iteration).

        // if any rows have been joined -- get last joined probe-side (right) row
        // it's important that index counts as "joined" after hash collisions checks
        // and join filters applied.
        let last_joined_right_idx = match right_indices.len() {
            0 => None,
            n => Some(right_indices.value(n - 1) as usize),
        };

        // Calculate range and perform alignment.
        // In case probe batch has been processed -- align all remaining rows.
        let index_alignment_range_start = state.joined_probe_idx.map_or(0, |v| v + 1);
        let index_alignment_range_end = if next_offset.is_none() {
            state.batch.num_rows()
        } else {
            last_joined_right_idx.map_or(0, |v| v + 1)
        };

        let (left_indices, right_indices) = adjust_indices_by_join_type(
            left_indices,
            right_indices,
            index_alignment_range_start..index_alignment_range_end,
            self.join_type,
        );

        let result = build_batch_from_indices(
            &self.schema,
            build_side.left_data.batch(),
            &state.batch,
            &left_indices,
            &right_indices,
            &self.column_indices,
            JoinSide::Left,
        )?;

        self.join_metrics.output_batches.add(1);
        self.join_metrics.output_rows.add(result.num_rows());
        timer.done();

        if next_offset.is_none() {
            self.state = HashJoinStreamState::FetchProbeBatch;
        } else {
            state.advance(
                next_offset
                    .ok_or_else(|| internal_datafusion_err!("unexpected None offset"))?,
                last_joined_right_idx,
            )
        };

        Ok(StatefulStreamResult::Ready(Some(result)))
    }

    /// Processes unmatched build-side rows for certain join types and produces output batch
    ///
    /// Updates state to `Completed`
    fn process_unmatched_build_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let timer = self.join_metrics.join_time.timer();

        if !need_produce_result_in_final(self.join_type) {
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        let build_side = self.build_side.try_as_ready()?;
        if !build_side.left_data.report_probe_completed() {
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        // use the global left bitmap to produce the left indices and right indices
        let (left_side, right_side) = get_final_indices_from_shared_bitmap(
            build_side.left_data.visited_indices_bitmap(),
            self.join_type,
        );
        let empty_right_batch = RecordBatch::new_empty(self.right.schema());
        // use the left and right indices to produce the batch result
        let result = build_batch_from_indices(
            &self.schema,
            build_side.left_data.batch(),
            &empty_right_batch,
            &left_side,
            &right_side,
            &self.column_indices,
            JoinSide::Left,
        );

        if let Ok(ref batch) = result {
            self.join_metrics.input_batches.add(1);
            self.join_metrics.input_rows.add(batch.num_rows());

            self.join_metrics.output_batches.add(1);
            self.join_metrics.output_rows.add(batch.num_rows());
        }
        timer.done();

        self.state = HashJoinStreamState::Completed;

        Ok(StatefulStreamResult::Ready(Some(result?)))
    }
}

impl Stream for HashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        common, expressions::Column, memory::MemoryExec, repartition::RepartitionExec,
        test::build_table_i32, test::exec::MockExec,
    };

    use arrow::array::{Date32Array, Int32Array, UInt32Builder, UInt64Builder};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{
        assert_batches_eq, assert_batches_sorted_eq, assert_contains, exec_err,
        ScalarValue,
    };
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};

    use hashbrown::raw::RawTable;
    use rstest::*;
    use rstest_reuse::*;

    fn div_ceil(a: usize, b: usize) -> usize {
        (a + b - 1) / b
    }

    #[template]
    #[rstest]
    fn batch_sizes(#[values(8192, 10, 5, 2, 1)] batch_size: usize) {}

    fn prepare_task_ctx(batch_size: usize) -> Arc<TaskContext> {
        let session_config = SessionConfig::default().with_batch_size(batch_size);
        Arc::new(TaskContext::default().with_session_config(session_config))
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equals_null: bool,
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            join_type,
            None,
            PartitionMode::CollectLeft,
            null_equals_null,
        )
    }

    fn join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equals_null: bool,
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(
            left,
            right,
            on,
            Some(filter),
            join_type,
            None,
            PartitionMode::CollectLeft,
            null_equals_null,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let join = join(left, right, on, join_type, null_equals_null)?;
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context)?;
        let batches = common::collect(stream).await?;

        Ok((columns_header, batches))
    }

    async fn partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        join_collect_with_partition_mode(
            left,
            right,
            on,
            join_type,
            PartitionMode::Partitioned,
            null_equals_null,
            context,
        )
        .await
    }

    async fn join_collect_with_partition_mode(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        partition_mode: PartitionMode,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let partition_count = 4;

        let (left_expr, right_expr) = on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();

        let left_repartitioned: Arc<dyn ExecutionPlan> = match partition_mode {
            PartitionMode::CollectLeft => Arc::new(CoalescePartitionsExec::new(left)),
            PartitionMode::Partitioned => Arc::new(RepartitionExec::try_new(
                left,
                Partitioning::Hash(left_expr, partition_count),
            )?),
            PartitionMode::Auto => {
                return internal_err!("Unexpected PartitionMode::Auto in join tests")
            }
        };

        let right_repartitioned: Arc<dyn ExecutionPlan> = match partition_mode {
            PartitionMode::CollectLeft => {
                let partition_column_name = right.schema().field(0).name().clone();
                let partition_expr = vec![Arc::new(Column::new_with_schema(
                    &partition_column_name,
                    &right.schema(),
                )?) as _];
                Arc::new(RepartitionExec::try_new(
                    right,
                    Partitioning::Hash(partition_expr, partition_count),
                )?) as _
            }
            PartitionMode::Partitioned => Arc::new(RepartitionExec::try_new(
                right,
                Partitioning::Hash(right_expr, partition_count),
            )?),
            PartitionMode::Auto => {
                return internal_err!("Unexpected PartitionMode::Auto in join tests")
            }
        };

        let join = HashJoinExec::try_new(
            left_repartitioned,
            right_repartitioned,
            on,
            None,
            join_type,
            None,
            partition_mode,
            null_equals_null,
        )?;

        let columns = columns(&join.schema());

        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = join.execute(i, Arc::clone(&context))?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }

        Ok((columns, batches))
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Inner,
            false,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_inner_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches) = partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Inner,
            false,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_one_no_shared_column_names() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_one_randomly_ordered() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a1", &vec![0, 3, 2, 1]),
            ("b1", &vec![4, 5, 5, 4]),
            ("c1", &vec![6, 9, 8, 7]),
        );
        let right = build_table(
            ("a2", &vec![20, 30, 10]),
            ("b2", &vec![5, 6, 4]),
            ("c2", &vec![80, 90, 70]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 0  | 4  | 6  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_two(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b2", "c1", "a1", "b2", "c2"]);

        // expected joined records = 3
        // in case batch_size is 1 - additional empty batch for remaining 3-2 row
        let mut expected_batch_count = div_ceil(3, batch_size);
        if batch_size == 1 {
            expected_batch_count += 1;
        }
        assert_eq!(batches.len(), expected_batch_count);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    /// Test where the left has 2 parts, the right with 1 part => 1 part
    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_one_two_parts_left(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let batch1 = build_table_i32(
            ("a1", &vec![1, 2]),
            ("b2", &vec![1, 2]),
            ("c1", &vec![7, 8]),
        );
        let batch2 =
            build_table_i32(("a1", &vec![2]), ("b2", &vec![2]), ("c1", &vec![9]));
        let schema = batch1.schema();
        let left = Arc::new(
            MemoryExec::try_new(&[vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b2", "c1", "a1", "b2", "c2"]);

        // expected joined records = 3
        // in case batch_size is 1 - additional empty batch for remaining 3-2 row
        let mut expected_batch_count = div_ceil(3, batch_size);
        if batch_size == 1 {
            expected_batch_count += 1;
        }
        assert_eq!(batches.len(), expected_batch_count);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_one_two_parts_left_randomly_ordered() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let batch1 = build_table_i32(
            ("a1", &vec![0, 3]),
            ("b1", &vec![4, 5]),
            ("c1", &vec![6, 9]),
        );
        let batch2 = build_table_i32(
            ("a1", &vec![2, 1]),
            ("b1", &vec![5, 4]),
            ("c1", &vec![8, 7]),
        );
        let schema = batch1.schema();

        let left = Arc::new(
            MemoryExec::try_new(&[vec![batch1], vec![batch2]], schema, None).unwrap(),
        );
        let right = build_table(
            ("a2", &vec![20, 30, 10]),
            ("b2", &vec![5, 6, 4]),
            ("c2", &vec![80, 90, 70]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 0  | 4  | 6  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    /// Test where the left has 1 part, the right has 2 parts => 2 parts
    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_one_two_parts_right(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        let batch1 = build_table_i32(
            ("a2", &vec![10, 20]),
            ("b1", &vec![4, 6]),
            ("c2", &vec![70, 80]),
        );
        let batch2 =
            build_table_i32(("a2", &vec![30]), ("b1", &vec![5]), ("c2", &vec![90]));
        let schema = batch1.schema();
        let right = Arc::new(
            MemoryExec::try_new(&[vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let join = join(left, right, on, &JoinType::Inner, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        // first part
        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        // expected joined records = 1 (first right batch)
        // and additional empty batch for non-joined 20-6-80
        let mut expected_batch_count = div_ceil(1, batch_size);
        if batch_size == 1 {
            expected_batch_count += 1;
        }
        assert_eq!(batches.len(), expected_batch_count);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        // second part
        let stream = join.execute(1, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        // expected joined records = 2 (second right batch)
        let expected_batch_count = div_ceil(2, batch_size);
        assert_eq!(batches.len(), expected_batch_count);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 5  | 8  | 30 | 5  | 90 |",
            "| 3  | 5  | 9  | 30 | 5  | 90 |",
            "+----+----+----+----+----+----+",
        ];

        // Inner join output is expected to preserve both inputs order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    fn build_table_two_batches(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(
            MemoryExec::try_new(&[vec![batch.clone(), batch]], schema, None).unwrap(),
        )
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_multi_batch(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_two_batches(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        )];

        let join = join(left, right, on, &JoinType::Left, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_multi_batch(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        // create two identical batches for the right side
        let right = build_table_two_batches(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join = join(left, right, on, &JoinType::Full, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_empty_right(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        )];
        let schema = right.schema();
        let right = Arc::new(MemoryExec::try_new(&[vec![right]], schema, None).unwrap());
        let join = join(left, right, on, &JoinType::Left, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  |    |    |    |",
            "| 2  | 5  | 8  |    |    |    |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_empty_right(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_i32(("a2", &vec![]), ("b2", &vec![]), ("c2", &vec![]));
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];
        let schema = right.schema();
        let right = Arc::new(MemoryExec::try_new(&[vec![right]], schema, None).unwrap());
        let join = join(left, right, on, &JoinType::Full, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  |    |    |    |",
            "| 2  | 5  | 8  |    |    |    |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Left,
            false,
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_left_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches) = partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Left,
            false,
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    fn build_semi_anti_left_table() -> Arc<dyn ExecutionPlan> {
        // just two line match
        // b1 = 10
        build_table(
            ("a1", &vec![1, 3, 5, 7, 9, 11, 13]),
            ("b1", &vec![1, 3, 5, 7, 8, 8, 10]),
            ("c1", &vec![10, 30, 50, 70, 90, 110, 130]),
        )
    }

    fn build_semi_anti_right_table() -> Arc<dyn ExecutionPlan> {
        // just two line match
        // b2 = 10
        build_table(
            ("a2", &vec![8, 12, 6, 2, 10, 4]),
            ("b2", &vec![8, 10, 6, 2, 10, 4]),
            ("c2", &vec![20, 40, 60, 80, 100, 120]),
        )
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_semi(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left semi join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(left, right, on, &JoinType::LeftSemi, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // ignore the order
        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 13 | 10 | 130 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_semi_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table left semi join right_table on left_table.b1 = right_table.b2 and right_table.a2 != 10
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::LeftSemi,
            false,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header.clone(), vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 13 | 10 | 130 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // left_table left semi join right_table on left_table.b1 = right_table.b2 and right_table.a2 > 10
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join = join_with_filter(left, right, on, filter, &JoinType::LeftSemi, false)?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 13 | 10 | 130 |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_semi(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table right semi join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(left, right, on, &JoinType::RightSemi, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 8  | 8  | 20  |",
            "| 12 | 10 | 40  |",
            "| 10 | 10 | 100 |",
            "+----+----+-----+",
        ];

        // RightSemi join output is expected to preserve right input order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_semi_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table right semi join right_table on left_table.b1 = right_table.b2 on left_table.a1!=9
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(9)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::RightSemi,
            false,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 8  | 8  | 20  |",
            "| 12 | 10 | 40  |",
            "| 10 | 10 | 100 |",
            "+----+----+-----+",
        ];

        // RightSemi join output is expected to preserve right input order
        assert_batches_eq!(expected, &batches);

        // left_table right semi join right_table on left_table.b1 = right_table.b2 on left_table.a1!=9
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(11)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join =
            join_with_filter(left, right, on, filter, &JoinType::RightSemi, false)?;
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 12 | 10 | 40  |",
            "| 10 | 10 | 100 |",
            "+----+----+-----+",
        ];

        // RightSemi join output is expected to preserve right input order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_anti(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left anti join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(left, right, on, &JoinType::LeftAnti, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 1  | 10 |",
            "| 3  | 3  | 30 |",
            "| 5  | 5  | 50 |",
            "| 7  | 7  | 70 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_anti_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left anti join right_table on left_table.b1 = right_table.b2 and right_table.a2!=8
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::LeftAnti,
            false,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 1  | 1  | 10  |",
            "| 11 | 8  | 110 |",
            "| 3  | 3  | 30  |",
            "| 5  | 5  | 50  |",
            "| 7  | 7  | 70  |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // left_table left anti join right_table on left_table.b1 = right_table.b2 and right_table.a2 != 13
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join = join_with_filter(left, right, on, filter, &JoinType::LeftAnti, false)?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 1  | 1  | 10  |",
            "| 11 | 8  | 110 |",
            "| 3  | 3  | 30  |",
            "| 5  | 5  | 50  |",
            "| 7  | 7  | 70  |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_anti(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(left, right, on, &JoinType::RightAnti, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 6  | 6  | 60  |",
            "| 2  | 2  | 80  |",
            "| 4  | 4  | 120 |",
            "+----+----+-----+",
        ];

        // RightAnti join output is expected to preserve right input order
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_anti_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table right anti join right_table on left_table.b1 = right_table.b2 and left_table.a1!=13
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(13)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices,
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::RightAnti,
            false,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 12 | 10 | 40  |",
            "| 6  | 6  | 60  |",
            "| 2  | 2  | 80  |",
            "| 10 | 10 | 100 |",
            "| 4  | 4  | 120 |",
            "+----+----+-----+",
        ];

        // RightAnti join output is expected to preserve right input order
        assert_batches_eq!(expected, &batches);

        // left_table right anti join right_table on left_table.b1 = right_table.b2 and right_table.b2!=8
        let column_indices = vec![ColumnIndex {
            index: 1,
            side: JoinSide::Right,
        }];
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join =
            join_with_filter(left, right, on, filter, &JoinType::RightAnti, false)?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 8  | 8  | 20  |",
            "| 6  | 6  | 60  |",
            "| 2  | 2  | 80  |",
            "| 4  | 4  | 120 |",
            "+----+----+-----+",
        ];

        // RightAnti join output is expected to preserve right input order
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Right, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_right_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches) =
            partitioned_join_collect(left, right, on, &JoinType::Right, false, task_ctx)
                .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join = join(left, right, on, &JoinType::Full, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[test]
    fn join_with_hash_collision() -> Result<()> {
        let mut hashmap_left = RawTable::with_capacity(2);
        let left = build_table_i32(
            ("a", &vec![10, 20]),
            ("x", &vec![100, 200]),
            ("y", &vec![200, 300]),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; left.num_rows()];
        let hashes = create_hashes(
            &[Arc::clone(&left.columns()[0])],
            &random_state,
            hashes_buff,
        )?;

        // Create hash collisions (same hashes)
        hashmap_left.insert(hashes[0], (hashes[0], 1), |(h, _)| *h);
        hashmap_left.insert(hashes[1], (hashes[1], 1), |(h, _)| *h);

        let next = vec![2, 0];

        let right = build_table_i32(
            ("a", &vec![10, 20]),
            ("b", &vec![0, 0]),
            ("c", &vec![30, 40]),
        );

        // Join key column for both join sides
        let key_column: PhysicalExprRef = Arc::new(Column::new("a", 0)) as _;

        let join_hash_map = JoinHashMap::new(hashmap_left, next);

        let right_keys_values =
            key_column.evaluate(&right)?.into_array(right.num_rows())?;
        let mut hashes_buffer = vec![0; right.num_rows()];
        create_hashes(&[right_keys_values], &random_state, &mut hashes_buffer)?;

        let (l, r, _) = lookup_join_hashmap(
            &join_hash_map,
            &left,
            &right,
            &[Arc::clone(&key_column)],
            &[key_column],
            false,
            &hashes_buffer,
            8192,
            (0, None),
        )?;

        let mut left_ids = UInt64Builder::with_capacity(0);
        left_ids.append_value(0);
        left_ids.append_value(1);

        let mut right_ids = UInt32Builder::with_capacity(0);
        right_ids.append_value(0);
        right_ids.append_value(1);

        assert_eq!(left_ids.finish(), l);

        assert_eq!(right_ids.finish(), r);

        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];

        let join = join(left, right, on, &JoinType::Inner, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+---+---+---+----+---+----+",
            "| a | b | c | a  | b | c  |",
            "+---+---+---+----+---+----+",
            "| 1 | 4 | 7 | 10 | 1 | 70 |",
            "| 2 | 5 | 8 | 20 | 2 | 80 |",
            "+---+---+---+----+---+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    fn prepare_join_filter() -> JoinFilter {
        let column_indices = vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("c", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c", 0)),
            Operator::Gt,
            Arc::new(Column::new("c", 1)),
        )) as Arc<dyn PhysicalExpr>;

        JoinFilter::new(filter_expression, column_indices, intermediate_schema)
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Inner, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Left, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "| 0 | 4 | 7 |    |   |   |",
            "| 1 | 5 | 8 |    |   |   |",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "| 2 | 8 | 1 |    |   |   |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Right, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "|   |   |   | 30 | 3 | 6 |",
            "|   |   |   | 40 | 4 | 4 |",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Full, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "|   |   |   | 30 | 3 | 6 |",
            "|   |   |   | 40 | 4 | 4 |",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "| 0 | 4 | 7 |    |   |   |",
            "| 1 | 5 | 8 |    |   |   |",
            "| 2 | 8 | 1 |    |   |   |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    /// Test for parallelised HashJoinExec with PartitionMode::CollectLeft
    #[tokio::test]
    async fn test_collect_left_multiple_partitions_join() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let expected_inner = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        let expected_left = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        let expected_right = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        let expected_full = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        let expected_left_semi = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        let expected_left_anti = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "+----+----+----+",
        ];
        let expected_right_semi = vec![
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 10 | 4  | 70 |",
            "| 20 | 5  | 80 |",
            "+----+----+----+",
        ];
        let expected_right_anti = vec![
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 30 | 6  | 90 |",
            "+----+----+----+",
        ];

        let test_cases = vec![
            (JoinType::Inner, expected_inner),
            (JoinType::Left, expected_left),
            (JoinType::Right, expected_right),
            (JoinType::Full, expected_full),
            (JoinType::LeftSemi, expected_left_semi),
            (JoinType::LeftAnti, expected_left_anti),
            (JoinType::RightSemi, expected_right_semi),
            (JoinType::RightAnti, expected_right_anti),
        ];

        for (join_type, expected) in test_cases {
            let (_, batches) = join_collect_with_partition_mode(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                &join_type,
                PartitionMode::CollectLeft,
                false,
                Arc::clone(&task_ctx),
            )
            .await?;
            assert_batches_sorted_eq!(expected, &batches);
        }

        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("n", DataType::Int32, false),
        ]));

        let dates: ArrayRef = Arc::new(Date32Array::from(vec![19107, 19108, 19109]));
        let n: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dates, n])?;
        let left = Arc::new(
            MemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None).unwrap(),
        );

        let dates: ArrayRef = Arc::new(Date32Array::from(vec![19108, 19108, 19109]));
        let n: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dates, n])?;
        let right = Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap());

        let on = vec![(
            Arc::new(Column::new_with_schema("date", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("date", &right.schema()).unwrap()) as _,
        )];

        let join = join(left, right, on, &JoinType::Inner, false)?;

        let task_ctx = Arc::new(TaskContext::default());
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
            "+------------+---+------------+---+",
            "| date       | n | date       | n |",
            "+------------+---+------------+---+",
            "| 2022-04-26 | 2 | 2022-04-26 | 4 |",
            "| 2022-04-26 | 2 | 2022-04-26 | 5 |",
            "| 2022-04-27 | 3 | 2022-04-27 | 6 |",
            "+------------+---+------------+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_with_error_right() {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );

        // right input stream returns one good batch and then one error.
        // The error should be returned.
        let err = exec_err!("bad data error");
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        )];
        let schema = right.schema();
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));
        let right_input = Arc::new(MockExec::new(vec![Ok(right), err], schema));

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let join = join(
                Arc::clone(&left),
                Arc::clone(&right_input) as Arc<dyn ExecutionPlan>,
                on.clone(),
                &join_type,
                false,
            )
            .unwrap();
            let task_ctx = Arc::new(TaskContext::default());

            let stream = join.execute(0, task_ctx).unwrap();

            // Expect that an error is returned
            let result_string = crate::common::collect(stream)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                result_string.contains("bad data error"),
                "actual: {result_string}"
            );
        }
    }

    #[tokio::test]
    async fn join_splitted_batch() {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4]),
            ("b1", &vec![1, 1, 1, 1]),
            ("c1", &vec![0, 0, 0, 0]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40, 50]),
            ("b2", &vec![1, 1, 1, 1, 1]),
            ("c2", &vec![0, 0, 0, 0, 0]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];
        let expected_resultset_records = 20;
        let common_result = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 0  | 10 | 1  | 0  |",
            "| 2  | 1  | 0  | 10 | 1  | 0  |",
            "| 3  | 1  | 0  | 10 | 1  | 0  |",
            "| 4  | 1  | 0  | 10 | 1  | 0  |",
            "| 1  | 1  | 0  | 20 | 1  | 0  |",
            "| 2  | 1  | 0  | 20 | 1  | 0  |",
            "| 3  | 1  | 0  | 20 | 1  | 0  |",
            "| 4  | 1  | 0  | 20 | 1  | 0  |",
            "| 1  | 1  | 0  | 30 | 1  | 0  |",
            "| 2  | 1  | 0  | 30 | 1  | 0  |",
            "| 3  | 1  | 0  | 30 | 1  | 0  |",
            "| 4  | 1  | 0  | 30 | 1  | 0  |",
            "| 1  | 1  | 0  | 40 | 1  | 0  |",
            "| 2  | 1  | 0  | 40 | 1  | 0  |",
            "| 3  | 1  | 0  | 40 | 1  | 0  |",
            "| 4  | 1  | 0  | 40 | 1  | 0  |",
            "| 1  | 1  | 0  | 50 | 1  | 0  |",
            "| 2  | 1  | 0  | 50 | 1  | 0  |",
            "| 3  | 1  | 0  | 50 | 1  | 0  |",
            "| 4  | 1  | 0  | 50 | 1  | 0  |",
            "+----+----+----+----+----+----+",
        ];
        let left_batch = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 1  | 0  |",
            "| 2  | 1  | 0  |",
            "| 3  | 1  | 0  |",
            "| 4  | 1  | 0  |",
            "+----+----+----+",
        ];
        let right_batch = [
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 10 | 1  | 0  |",
            "| 20 | 1  | 0  |",
            "| 30 | 1  | 0  |",
            "| 40 | 1  | 0  |",
            "| 50 | 1  | 0  |",
            "+----+----+----+",
        ];
        let right_empty = [
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "+----+----+----+",
        ];
        let left_empty = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "+----+----+----+",
        ];

        // validation of partial join results output for different batch_size setting
        for join_type in join_types {
            for batch_size in (1..21).rev() {
                let task_ctx = prepare_task_ctx(batch_size);

                let join = join(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    &join_type,
                    false,
                )
                .unwrap();

                let stream = join.execute(0, task_ctx).unwrap();
                let batches = common::collect(stream).await.unwrap();

                // For inner/right join expected batch count equals dev_ceil result,
                // as there is no need to append non-joined build side data.
                // For other join types it'll be div_ceil + 1 -- for additional batch
                // containing not visited build side rows (empty in this test case).
                let expected_batch_count = match join_type {
                    JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti => {
                        (expected_resultset_records + batch_size - 1) / batch_size
                    }
                    _ => (expected_resultset_records + batch_size - 1) / batch_size + 1,
                };
                assert_eq!(
                    batches.len(),
                    expected_batch_count,
                    "expected {} output batches for {} join with batch_size = {}",
                    expected_batch_count,
                    join_type,
                    batch_size
                );

                let expected = match join_type {
                    JoinType::RightSemi => right_batch.to_vec(),
                    JoinType::RightAnti => right_empty.to_vec(),
                    JoinType::LeftSemi => left_batch.to_vec(),
                    JoinType::LeftAnti => left_empty.to_vec(),
                    _ => common_result.to_vec(),
                };
                assert_batches_eq!(expected, &batches);
            }
        }
    }

    #[tokio::test]
    async fn single_partition_join_overallocation() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let right = build_table(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("a1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = join(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                &join_type,
                false,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(
                err.to_string(),
                "External error: Resources exhausted: Failed to allocate additional"
            );

            // Asserting that operator-level reservation attempting to overallocate
            assert_contains!(err.to_string(), "HashJoinInput");
        }

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_overallocation() -> Result<()> {
        // Prepare partitioned inputs for HashJoinExec
        // No need to adjust partitioning, as execution should fail with `Resources exhausted` error
        let left_batch = build_table_i32(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let left = Arc::new(
            MemoryExec::try_new(
                &[vec![left_batch.clone()], vec![left_batch.clone()]],
                left_batch.schema(),
                None,
            )
            .unwrap(),
        );
        let right_batch = build_table_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );
        let right = Arc::new(
            MemoryExec::try_new(
                &[vec![right_batch.clone()], vec![right_batch.clone()]],
                right_batch.schema(),
                None,
            )
            .unwrap(),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left_batch.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right_batch.schema())?) as _,
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let session_config = SessionConfig::default().with_batch_size(50);
            let task_ctx = TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = HashJoinExec::try_new(
                Arc::clone(&left) as Arc<dyn ExecutionPlan>,
                Arc::clone(&right) as Arc<dyn ExecutionPlan>,
                on.clone(),
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                false,
            )?;

            let stream = join.execute(1, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(
                err.to_string(),
                "External error: Resources exhausted: Failed to allocate additional"
            );

            // Asserting that stream-level reservation attempting to overallocate
            assert_contains!(err.to_string(), "HashJoinInput[1]");
        }

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
