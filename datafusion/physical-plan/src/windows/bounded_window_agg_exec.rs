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

//! Stream and channel implementations for window function expressions.
//! The executor given here uses bounded memory (does not maintain all
//! the input data seen so far), which makes it appropriate when processing
//! infinite inputs.

use std::any::Any;
use std::cmp::{min, Ordering};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::expressions::PhysicalSortExpr;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_equivalence_properties,
};
use crate::{
    ColumnStatistics, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, InputOrderMode, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics, WindowExpr,
};

use arrow::{
    array::{Array, ArrayRef, RecordBatchOptions, UInt32Builder},
    compute::{concat, concat_batches, sort_to_indices},
    datatypes::{Schema, SchemaBuilder, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::stats::Precision;
use datafusion_common::utils::{
    evaluate_partition_ranges, get_arrayref_at_indices, get_at_indices,
    get_record_batch_at_indices, get_row_at_idx,
};
use datafusion_common::{arrow_datafusion_err, exec_err, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::window_state::{PartitionBatchState, WindowAggState};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::window::{
    PartitionBatches, PartitionKey, PartitionWindowAggStates, WindowState,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortRequirement};

use ahash::RandomState;
use futures::stream::Stream;
use futures::{ready, StreamExt};
use hashbrown::raw::RawTable;
use indexmap::IndexMap;
use log::debug;

/// Window execution plan
#[derive(Debug)]
pub struct BoundedWindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Partition Keys
    pub partition_keys: Vec<Arc<dyn PhysicalExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Describes how the input is ordered relative to the partition keys
    pub input_order_mode: InputOrderMode,
    /// Partition by indices that define ordering
    // For example, if input ordering is ORDER BY a, b and window expression
    // contains PARTITION BY b, a; `ordered_partition_by_indices` would be 1, 0.
    // Similarly, if window expression contains PARTITION BY a, b; then
    // `ordered_partition_by_indices` would be 0, 1.
    // See `get_ordered_partition_by_indices` for more details.
    ordered_partition_by_indices: Vec<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl BoundedWindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        partition_keys: Vec<Arc<dyn PhysicalExpr>>,
        input_order_mode: InputOrderMode,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);
        let partition_by_exprs = window_expr[0].partition_by();
        let ordered_partition_by_indices = match &input_order_mode {
            InputOrderMode::Sorted => {
                let indices = get_ordered_partition_by_indices(
                    window_expr[0].partition_by(),
                    &input,
                );
                if indices.len() == partition_by_exprs.len() {
                    indices
                } else {
                    (0..partition_by_exprs.len()).collect::<Vec<_>>()
                }
            }
            InputOrderMode::PartiallySorted(ordered_indices) => ordered_indices.clone(),
            InputOrderMode::Linear => {
                vec![]
            }
        };
        let cache = Self::compute_properties(&input, &schema, &window_expr);
        Ok(Self {
            input,
            window_expr,
            schema,
            partition_keys,
            metrics: ExecutionPlanMetricsSet::new(),
            input_order_mode,
            ordered_partition_by_indices,
            cache,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Return the output sort order of partition keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column a
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let partition_by = self.window_expr()[0].partition_by();
        get_partition_by_sort_exprs(
            &self.input,
            partition_by,
            &self.ordered_partition_by_indices,
        )
    }

    /// Initializes the appropriate [`PartitionSearcher`] implementation from
    /// the state.
    fn get_search_algo(&self) -> Result<Box<dyn PartitionSearcher>> {
        let partition_by_sort_keys = self.partition_by_sort_keys()?;
        let ordered_partition_by_indices = self.ordered_partition_by_indices.clone();
        let input_schema = self.input().schema();
        Ok(match &self.input_order_mode {
            InputOrderMode::Sorted => {
                // In Sorted mode, all partition by columns should be ordered.
                if self.window_expr()[0].partition_by().len()
                    != ordered_partition_by_indices.len()
                {
                    return exec_err!("All partition by columns should have an ordering in Sorted mode.");
                }
                Box::new(SortedSearch {
                    partition_by_sort_keys,
                    ordered_partition_by_indices,
                    input_schema,
                })
            }
            InputOrderMode::Linear | InputOrderMode::PartiallySorted(_) => Box::new(
                LinearSearch::new(ordered_partition_by_indices, input_schema),
            ),
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(schema, input, window_expr);

        // As we can have repartitioning using the partition keys, this can
        // be either one or more than one, depending on the presence of
        // repartitioning.
        let output_partitioning = input.output_partitioning().clone();

        // Construct properties cache
        PlanProperties::new(
            eq_properties,          // Equivalence Properties
            output_partitioning,    // Output Partitioning
            input.execution_mode(), // Execution Mode
        )
    }
}

impl DisplayAs for BoundedWindowAggExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BoundedWindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
                    .collect();
                let mode = &self.input_order_mode;
                write!(f, "wdw=[{}], mode=[{:?}]", g.join(", "), mode)?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for BoundedWindowAggExec {
    fn name(&self) -> &'static str {
        "BoundedWindowAggExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        let partition_bys = self.window_expr()[0].partition_by();
        let order_keys = self.window_expr()[0].order_by();
        if self.input_order_mode != InputOrderMode::Sorted
            || self.ordered_partition_by_indices.len() >= partition_bys.len()
        {
            let partition_bys = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| &partition_bys[*idx]);
            vec![calc_requirements(partition_bys, order_keys)]
        } else {
            vec![calc_requirements(partition_bys, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys.is_empty() {
            debug!("No partition defined for BoundedWindowAggExec!!!");
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::HashPartitioned(self.partition_keys.clone())]
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            self.window_expr.clone(),
            Arc::clone(&children[0]),
            self.partition_keys.clone(),
            self.input_order_mode.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let search_mode = self.get_search_algo()?;
        let stream = Box::pin(BoundedWindowAggStream::new(
            Arc::clone(&self.schema),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            search_mode,
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let input_stat = self.input.statistics()?;
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        })
    }
}

/// Trait that specifies how we search for (or calculate) partitions. It has two
/// implementations: [`SortedSearch`] and [`LinearSearch`].
trait PartitionSearcher: Send {
    /// This method constructs output columns using the result of each window expression
    /// (each entry in the output vector comes from a window expression).
    /// Executor when producing output concatenates `input_buffer` (corresponding section), and
    /// result of this function to generate output `RecordBatch`. `input_buffer` is used to determine
    /// which sections of the window expression results should be used to generate output.
    /// `partition_buffers` contains corresponding section of the `RecordBatch` for each partition.
    /// `window_agg_states` stores per partition state for each window expression.
    /// None case means that no result is generated
    /// `Some(Vec<ArrayRef>)` is the result of each window expression.
    fn calculate_out_columns(
        &mut self,
        input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>>;

    /// Determine whether `[InputOrderMode]` is `[InputOrderMode::Linear]` or not.
    fn is_mode_linear(&self) -> bool {
        false
    }

    // Constructs corresponding batches for each partition for the record_batch.
    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>>;

    /// Prunes the state.
    fn prune(&mut self, _n_out: usize) {}

    /// Marks the partition as done if we are sure that corresponding partition
    /// cannot receive any more values.
    fn mark_partition_end(&self, partition_buffers: &mut PartitionBatches);

    /// Updates `input_buffer` and `partition_buffers` with the new `record_batch`.
    fn update_partition_batch(
        &mut self,
        input_buffer: &mut RecordBatch,
        record_batch: RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
        partition_buffers: &mut PartitionBatches,
    ) -> Result<()> {
        if record_batch.num_rows() == 0 {
            return Ok(());
        }
        let partition_batches =
            self.evaluate_partition_batches(&record_batch, window_expr)?;
        for (partition_row, partition_batch) in partition_batches {
            let partition_batch_state = partition_buffers
                .entry(partition_row)
                // Use input_schema for the buffer schema, not `record_batch.schema()`
                // as it may not have the "correct" schema in terms of output
                // nullability constraints. For details, see the following issue:
                // https://github.com/apache/datafusion/issues/9320
                .or_insert_with(|| {
                    PartitionBatchState::new(Arc::clone(self.input_schema()))
                });
            partition_batch_state.extend(&partition_batch)?;
        }

        if self.is_mode_linear() {
            // In `Linear` mode, it is guaranteed that the first ORDER BY column
            // is sorted across partitions. Note that only the first ORDER BY
            // column is guaranteed to be ordered. As a counter example, consider
            // the case, `PARTITION BY b, ORDER BY a, c` when the input is sorted
            // by `[a, b, c]`. In this case, `BoundedWindowAggExec` mode will be
            // `Linear`. However, we cannot guarantee that the last row of the
            // input data will be the "last" data in terms of the ordering requirement
            // `[a, c]` -- it will be the "last" data in terms of `[a, b, c]`.
            // Hence, only column `a` should be used as a guarantee of the "last"
            // data across partitions. For other modes (`Sorted`, `PartiallySorted`),
            // we do not need to keep track of the most recent row guarantee across
            // partitions. Since leading ordering separates partitions, guaranteed
            // by the most recent row, already prune the previous partitions completely.
            let last_row = get_last_row_batch(&record_batch)?;
            for (_, partition_batch) in partition_buffers.iter_mut() {
                partition_batch.set_most_recent_row(last_row.clone());
            }
        }
        self.mark_partition_end(partition_buffers);

        *input_buffer = if input_buffer.num_rows() == 0 {
            record_batch
        } else {
            concat_batches(self.input_schema(), [input_buffer, &record_batch])?
        };

        Ok(())
    }

    fn input_schema(&self) -> &SchemaRef;
}

/// This object encapsulates the algorithm state for a simple linear scan
/// algorithm for computing partitions.
pub struct LinearSearch {
    /// Keeps the hash of input buffer calculated from PARTITION BY columns.
    /// Its length is equal to the `input_buffer` length.
    input_buffer_hashes: VecDeque<u64>,
    /// Used during hash value calculation.
    random_state: RandomState,
    /// Input ordering and partition by key ordering need not be the same, so
    /// this vector stores the mapping between them. For instance, if the input
    /// is ordered by a, b and the window expression contains a PARTITION BY b, a
    /// clause, this attribute stores [1, 0].
    ordered_partition_by_indices: Vec<usize>,
    /// We use this [`RawTable`] to calculate unique partitions for each new
    /// RecordBatch. First entry in the tuple is the hash value, the second
    /// entry is the unique ID for each partition (increments from 0 to n).
    row_map_batch: RawTable<(u64, usize)>,
    /// We use this [`RawTable`] to calculate the output columns that we can
    /// produce at each cycle. First entry in the tuple is the hash value, the
    /// second entry is the unique ID for each partition (increments from 0 to n).
    /// The third entry stores how many new outputs are calculated for the
    /// corresponding partition.
    row_map_out: RawTable<(u64, usize, usize)>,
    input_schema: SchemaRef,
}

impl PartitionSearcher for LinearSearch {
    /// This method constructs output columns using the result of each window expression.
    // Assume input buffer is         |      Partition Buffers would be (Where each partition and its data is seperated)
    // a, 2                           |      a, 2
    // b, 2                           |      a, 2
    // a, 2                           |      a, 2
    // b, 2                           |
    // a, 2                           |      b, 2
    // b, 2                           |      b, 2
    // b, 2                           |      b, 2
    //                                |      b, 2
    // Also assume we happen to calculate 2 new values for a, and 3 for b (To be calculate missing values we may need to consider future values).
    // Partition buffers effectively will be
    // a, 2, 1
    // a, 2, 2
    // a, 2, (missing)
    //
    // b, 2, 1
    // b, 2, 2
    // b, 2, 3
    // b, 2, (missing)
    // When partition buffers are mapped back to the original record batch. Result becomes
    // a, 2, 1
    // b, 2, 1
    // a, 2, 2
    // b, 2, 2
    // a, 2, (missing)
    // b, 2, 3
    // b, 2, (missing)
    // This function calculates the column result of window expression(s) (First 4 entry of 3rd column in the above section.)
    // 1
    // 1
    // 2
    // 2
    // Above section corresponds to calculated result which can be emitted without breaking input buffer ordering.
    fn calculate_out_columns(
        &mut self,
        input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>> {
        let partition_output_indices = self.calc_partition_output_indices(
            input_buffer,
            window_agg_states,
            window_expr,
        )?;

        let n_window_col = window_agg_states.len();
        let mut new_columns = vec![vec![]; n_window_col];
        // Size of all_indices can be at most input_buffer.num_rows():
        let mut all_indices = UInt32Builder::with_capacity(input_buffer.num_rows());
        for (row, indices) in partition_output_indices {
            let length = indices.len();
            for (idx, window_agg_state) in window_agg_states.iter().enumerate() {
                let partition = &window_agg_state[&row];
                let values = Arc::clone(&partition.state.out_col.slice(0, length));
                new_columns[idx].push(values);
            }
            let partition_batch_state = &mut partition_buffers[&row];
            // Store how many rows are generated for each partition
            partition_batch_state.n_out_row = length;
            // For each row keep corresponding index in the input record batch
            all_indices.append_slice(&indices);
        }
        let all_indices = all_indices.finish();
        if all_indices.is_empty() {
            // We couldn't generate any new value, return early:
            return Ok(None);
        }

        // Concatenate results for each column by converting `Vec<Vec<ArrayRef>>`
        // to Vec<ArrayRef> where inner `Vec<ArrayRef>`s are converted to `ArrayRef`s.
        let new_columns = new_columns
            .iter()
            .map(|items| {
                concat(&items.iter().map(|e| e.as_ref()).collect::<Vec<_>>())
                    .map_err(|e| arrow_datafusion_err!(e))
            })
            .collect::<Result<Vec<_>>>()?;
        // We should emit columns according to row index ordering.
        let sorted_indices = sort_to_indices(&all_indices, None, None)?;
        // Construct new column according to row ordering. This fixes ordering
        get_arrayref_at_indices(&new_columns, &sorted_indices).map(Some)
    }

    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let partition_bys =
            self.evaluate_partition_by_column_values(record_batch, window_expr)?;
        // NOTE: In Linear or PartiallySorted modes, we are sure that
        //       `partition_bys` are not empty.
        // Calculate indices for each partition and construct a new record
        // batch from the rows at these indices for each partition:
        self.get_per_partition_indices(&partition_bys, record_batch)?
            .into_iter()
            .map(|(row, indices)| {
                let mut new_indices = UInt32Builder::with_capacity(indices.len());
                new_indices.append_slice(&indices);
                let indices = new_indices.finish();
                Ok((row, get_record_batch_at_indices(record_batch, &indices)?))
            })
            .collect()
    }

    fn prune(&mut self, n_out: usize) {
        // Delete hashes for the rows that are outputted.
        self.input_buffer_hashes.drain(0..n_out);
    }

    fn mark_partition_end(&self, partition_buffers: &mut PartitionBatches) {
        // We should be in the `PartiallySorted` case, otherwise we can not
        // tell when we are at the end of a given partition.
        if !self.ordered_partition_by_indices.is_empty() {
            if let Some((last_row, _)) = partition_buffers.last() {
                let last_sorted_cols = self
                    .ordered_partition_by_indices
                    .iter()
                    .map(|idx| last_row[*idx].clone())
                    .collect::<Vec<_>>();
                for (row, partition_batch_state) in partition_buffers.iter_mut() {
                    let sorted_cols = self
                        .ordered_partition_by_indices
                        .iter()
                        .map(|idx| &row[*idx]);
                    // All the partitions other than `last_sorted_cols` are done.
                    // We are sure that we will no longer receive values for these
                    // partitions (arrival of a new value would violate ordering).
                    partition_batch_state.is_end = !sorted_cols.eq(&last_sorted_cols);
                }
            }
        }
    }

    fn is_mode_linear(&self) -> bool {
        self.ordered_partition_by_indices.is_empty()
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

impl LinearSearch {
    /// Initialize a new [`LinearSearch`] partition searcher.
    fn new(ordered_partition_by_indices: Vec<usize>, input_schema: SchemaRef) -> Self {
        LinearSearch {
            input_buffer_hashes: VecDeque::new(),
            random_state: Default::default(),
            ordered_partition_by_indices,
            row_map_batch: RawTable::with_capacity(256),
            row_map_out: RawTable::with_capacity(256),
            input_schema,
        }
    }

    /// Calculates partition by expression results for each window expression
    /// on `record_batch`.
    fn evaluate_partition_by_column_values(
        &self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<ArrayRef>> {
        window_expr[0]
            .partition_by()
            .iter()
            .map(|item| match item.evaluate(record_batch)? {
                ColumnarValue::Array(array) => Ok(array),
                ColumnarValue::Scalar(scalar) => {
                    scalar.to_array_of_size(record_batch.num_rows())
                }
            })
            .collect()
    }

    /// Calculate indices of each partition (according to PARTITION BY expression)
    /// `columns` contain partition by expression results.
    fn get_per_partition_indices(
        &mut self,
        columns: &[ArrayRef],
        batch: &RecordBatch,
    ) -> Result<Vec<(PartitionKey, Vec<u32>)>> {
        let mut batch_hashes = vec![0; batch.num_rows()];
        create_hashes(columns, &self.random_state, &mut batch_hashes)?;
        self.input_buffer_hashes.extend(&batch_hashes);
        // reset row_map for new calculation
        self.row_map_batch.clear();
        // res stores PartitionKey and row indices (indices where these partition occurs in the `batch`) for each partition.
        let mut result: Vec<(PartitionKey, Vec<u32>)> = vec![];
        for (hash, row_idx) in batch_hashes.into_iter().zip(0u32..) {
            let entry = self.row_map_batch.get_mut(hash, |(_, group_idx)| {
                // We can safely get the first index of the partition indices
                // since partition indices has one element during initialization.
                let row = get_row_at_idx(columns, row_idx as usize).unwrap();
                // Handle hash collusions with an equality check:
                row.eq(&result[*group_idx].0)
            });
            if let Some((_, group_idx)) = entry {
                result[*group_idx].1.push(row_idx)
            } else {
                self.row_map_batch
                    .insert(hash, (hash, result.len()), |(hash, _)| *hash);
                let row = get_row_at_idx(columns, row_idx as usize)?;
                // This is a new partition its only index is row_idx for now.
                result.push((row, vec![row_idx]));
            }
        }
        Ok(result)
    }

    /// Calculates partition keys and result indices for each partition.
    /// The return value is a vector of tuples where the first entry stores
    /// the partition key (unique for each partition) and the second entry
    /// stores indices of the rows for which the partition is constructed.
    fn calc_partition_output_indices(
        &mut self,
        input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, Vec<u32>)>> {
        let partition_by_columns =
            self.evaluate_partition_by_column_values(input_buffer, window_expr)?;
        // Reset the row_map state:
        self.row_map_out.clear();
        let mut partition_indices: Vec<(PartitionKey, Vec<u32>)> = vec![];
        for (hash, row_idx) in self.input_buffer_hashes.iter().zip(0u32..) {
            let entry = self.row_map_out.get_mut(*hash, |(_, group_idx, _)| {
                let row =
                    get_row_at_idx(&partition_by_columns, row_idx as usize).unwrap();
                row == partition_indices[*group_idx].0
            });
            if let Some((_, group_idx, n_out)) = entry {
                let (_, indices) = &mut partition_indices[*group_idx];
                if indices.len() >= *n_out {
                    break;
                }
                indices.push(row_idx);
            } else {
                let row = get_row_at_idx(&partition_by_columns, row_idx as usize)?;
                let min_out = window_agg_states
                    .iter()
                    .map(|window_agg_state| {
                        window_agg_state
                            .get(&row)
                            .map(|partition| partition.state.out_col.len())
                            .unwrap_or(0)
                    })
                    .min()
                    .unwrap_or(0);
                if min_out == 0 {
                    break;
                }
                self.row_map_out.insert(
                    *hash,
                    (*hash, partition_indices.len(), min_out),
                    |(hash, _, _)| *hash,
                );
                partition_indices.push((row, vec![row_idx]));
            }
        }
        Ok(partition_indices)
    }
}

/// This object encapsulates the algorithm state for sorted searching
/// when computing partitions.
pub struct SortedSearch {
    /// Stores partition by columns and their ordering information
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    /// Input ordering and partition by key ordering need not be the same, so
    /// this vector stores the mapping between them. For instance, if the input
    /// is ordered by a, b and the window expression contains a PARTITION BY b, a
    /// clause, this attribute stores [1, 0].
    ordered_partition_by_indices: Vec<usize>,
    input_schema: SchemaRef,
}

impl PartitionSearcher for SortedSearch {
    /// This method constructs new output columns using the result of each window expression.
    fn calculate_out_columns(
        &mut self,
        _input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        _window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>> {
        let n_out = self.calculate_n_out_row(window_agg_states, partition_buffers);
        if n_out == 0 {
            Ok(None)
        } else {
            window_agg_states
                .iter()
                .map(|map| get_aggregate_result_out_column(map, n_out).map(Some))
                .collect()
        }
    }

    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        _window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let num_rows = record_batch.num_rows();
        // Calculate result of partition by column expressions
        let partition_columns = self
            .partition_by_sort_keys
            .iter()
            .map(|elem| elem.evaluate_to_sort_column(record_batch))
            .collect::<Result<Vec<_>>>()?;
        // Reorder `partition_columns` such that its ordering matches input ordering.
        let partition_columns_ordered =
            get_at_indices(&partition_columns, &self.ordered_partition_by_indices)?;
        let partition_points =
            evaluate_partition_ranges(num_rows, &partition_columns_ordered)?;
        let partition_bys = partition_columns
            .into_iter()
            .map(|arr| arr.values)
            .collect::<Vec<ArrayRef>>();

        partition_points
            .iter()
            .map(|range| {
                let row = get_row_at_idx(&partition_bys, range.start)?;
                let len = range.end - range.start;
                let slice = record_batch.slice(range.start, len);
                Ok((row, slice))
            })
            .collect::<Result<Vec<_>>>()
    }

    fn mark_partition_end(&self, partition_buffers: &mut PartitionBatches) {
        // In Sorted case. We can mark all partitions besides last partition as ended.
        // We are sure that those partitions will never receive any values.
        // (Otherwise ordering invariant is violated.)
        let n_partitions = partition_buffers.len();
        for (idx, (_, partition_batch_state)) in partition_buffers.iter_mut().enumerate()
        {
            partition_batch_state.is_end |= idx < n_partitions - 1;
        }
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

impl SortedSearch {
    /// Calculates how many rows we can output.
    fn calculate_n_out_row(
        &mut self,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
    ) -> usize {
        // Different window aggregators may produce results at different rates.
        // We produce the overall batch result only as fast as the slowest one.
        let mut counts = vec![];
        let out_col_counts = window_agg_states.iter().map(|window_agg_state| {
            // Store how many elements are generated for the current
            // window expression:
            let mut cur_window_expr_out_result_len = 0;
            // We iterate over `window_agg_state`, which is an IndexMap.
            // Iterations follow the insertion order, hence we preserve
            // sorting when partition columns are sorted.
            let mut per_partition_out_results = HashMap::new();
            for (row, WindowState { state, .. }) in window_agg_state.iter() {
                cur_window_expr_out_result_len += state.out_col.len();
                let count = per_partition_out_results.entry(row).or_insert(0);
                if *count < state.out_col.len() {
                    *count = state.out_col.len();
                }
                // If we do not generate all results for the current
                // partition, we do not generate results for next
                // partition --  otherwise we will lose input ordering.
                if state.n_row_result_missing > 0 {
                    break;
                }
            }
            counts.push(per_partition_out_results);
            cur_window_expr_out_result_len
        });
        argmin(out_col_counts).map_or(0, |(min_idx, minima)| {
            for (row, count) in counts.swap_remove(min_idx).into_iter() {
                let partition_batch = &mut partition_buffers[row];
                partition_batch.n_out_row = count;
            }
            minima
        })
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let capacity = input_schema.fields().len() + window_expr.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields.iter().cloned());
    // append results to the schema
    for expr in window_expr {
        builder.push(expr.field()?);
    }
    Ok(builder.finish())
}

/// Stream for the bounded window aggregation plan.
pub struct BoundedWindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    /// The record batch executor receives as input (i.e. the columns needed
    /// while calculating aggregation results).
    input_buffer: RecordBatch,
    /// We separate `input_buffer` based on partitions (as
    /// determined by PARTITION BY columns) and store them per partition
    /// in `partition_batches`. We use this variable when calculating results
    /// for each window expression. This enables us to use the same batch for
    /// different window expressions without copying.
    // Note that we could keep record batches for each window expression in
    // `PartitionWindowAggStates`. However, this would use more memory (as
    // many times as the number of window expressions).
    partition_buffers: PartitionBatches,
    /// An executor can run multiple window expressions if the PARTITION BY
    /// and ORDER BY sections are same. We keep state of the each window
    /// expression inside `window_agg_states`.
    window_agg_states: Vec<PartitionWindowAggStates>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    baseline_metrics: BaselineMetrics,
    /// Search mode for partition columns. This determines the algorithm with
    /// which we group each partition.
    search_mode: Box<dyn PartitionSearcher>,
}

impl BoundedWindowAggStream {
    /// Prunes sections of the state that are no longer needed when calculating
    /// results (as determined by window frame boundaries and number of results generated).
    // For instance, if first `n` (not necessarily same with `n_out`) elements are no longer needed to
    // calculate window expression result (outside the window frame boundary) we retract first `n` elements
    // from `self.partition_batches` in corresponding partition.
    // For instance, if `n_out` number of rows are calculated, we can remove
    // first `n_out` rows from `self.input_buffer`.
    fn prune_state(&mut self, n_out: usize) -> Result<()> {
        // Prune `self.window_agg_states`:
        self.prune_out_columns();
        // Prune `self.partition_batches`:
        self.prune_partition_batches();
        // Prune `self.input_buffer`:
        self.prune_input_batch(n_out)?;
        // Prune internal state of search algorithm.
        self.search_mode.prune(n_out);
        Ok(())
    }
}

impl Stream for BoundedWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl BoundedWindowAggStream {
    /// Create a new BoundedWindowAggStream
    fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        search_mode: Box<dyn PartitionSearcher>,
    ) -> Result<Self> {
        let state = window_expr.iter().map(|_| IndexMap::new()).collect();
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        Ok(Self {
            schema,
            input,
            input_buffer: empty_batch,
            partition_buffers: IndexMap::new(),
            window_agg_states: state,
            finished: false,
            window_expr,
            baseline_metrics,
            search_mode,
        })
    }

    fn compute_aggregates(&mut self) -> Result<RecordBatch> {
        // calculate window cols
        for (cur_window_expr, state) in
            self.window_expr.iter().zip(&mut self.window_agg_states)
        {
            cur_window_expr.evaluate_stateful(&self.partition_buffers, state)?;
        }

        let schema = Arc::clone(&self.schema);
        let window_expr_out = self.search_mode.calculate_out_columns(
            &self.input_buffer,
            &self.window_agg_states,
            &mut self.partition_buffers,
            &self.window_expr,
        )?;
        if let Some(window_expr_out) = window_expr_out {
            let n_out = window_expr_out[0].len();
            // right append new columns to corresponding section in the original input buffer.
            let columns_to_show = self
                .input_buffer
                .columns()
                .iter()
                .map(|elem| elem.slice(0, n_out))
                .chain(window_expr_out)
                .collect::<Vec<_>>();
            let n_generated = columns_to_show[0].len();
            self.prune_state(n_generated)?;
            Ok(RecordBatch::try_new(schema, columns_to_show)?)
        } else {
            Ok(RecordBatch::new_empty(schema))
        }
    }

    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let result = match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                self.search_mode.update_partition_batch(
                    &mut self.input_buffer,
                    batch,
                    &self.window_expr,
                    &mut self.partition_buffers,
                )?;
                self.compute_aggregates()
            }
            Some(Err(e)) => Err(e),
            None => {
                self.finished = true;
                for (_, partition_batch_state) in self.partition_buffers.iter_mut() {
                    partition_batch_state.is_end = true;
                }
                self.compute_aggregates()
            }
        };
        Poll::Ready(Some(result))
    }

    /// Prunes the sections of the record batch (for each partition)
    /// that we no longer need to calculate the window function result.
    fn prune_partition_batches(&mut self) {
        // Remove partitions which we know already ended (is_end flag is true).
        // Since the retain method preserves insertion order, we still have
        // ordering in between partitions after removal.
        self.partition_buffers
            .retain(|_, partition_batch_state| !partition_batch_state.is_end);

        // The data in `self.partition_batches` is used by all window expressions.
        // Therefore, when removing from `self.partition_batches`, we need to remove
        // from the earliest range boundary among all window expressions. Variable
        // `n_prune_each_partition` fill the earliest range boundary information for
        // each partition. This way, we can delete the no-longer-needed sections from
        // `self.partition_batches`.
        // For instance, if window frame one uses [10, 20] and window frame two uses
        // [5, 15]; we only prune the first 5 elements from the corresponding record
        // batch in `self.partition_batches`.

        // Calculate how many elements to prune for each partition batch
        let mut n_prune_each_partition = HashMap::new();
        for window_agg_state in self.window_agg_states.iter_mut() {
            window_agg_state.retain(|_, WindowState { state, .. }| !state.is_end);
            for (partition_row, WindowState { state: value, .. }) in window_agg_state {
                let n_prune =
                    min(value.window_frame_range.start, value.last_calculated_index);
                if let Some(current) = n_prune_each_partition.get_mut(partition_row) {
                    if n_prune < *current {
                        *current = n_prune;
                    }
                } else {
                    n_prune_each_partition.insert(partition_row.clone(), n_prune);
                }
            }
        }

        // Retract no longer needed parts during window calculations from partition batch:
        for (partition_row, n_prune) in n_prune_each_partition.iter() {
            let pb_state = &mut self.partition_buffers[partition_row];

            let batch = &pb_state.record_batch;
            pb_state.record_batch = batch.slice(*n_prune, batch.num_rows() - n_prune);
            pb_state.n_out_row = 0;

            // Update state indices since we have pruned some rows from the beginning:
            for window_agg_state in self.window_agg_states.iter_mut() {
                window_agg_state[partition_row].state.prune_state(*n_prune);
            }
        }
    }

    /// Prunes the section of the input batch whose aggregate results
    /// are calculated and emitted.
    fn prune_input_batch(&mut self, n_out: usize) -> Result<()> {
        // Prune first n_out rows from the input_buffer
        let n_to_keep = self.input_buffer.num_rows() - n_out;
        let batch_to_keep = self
            .input_buffer
            .columns()
            .iter()
            .map(|elem| elem.slice(n_out, n_to_keep))
            .collect::<Vec<_>>();
        self.input_buffer = RecordBatch::try_new_with_options(
            self.input_buffer.schema(),
            batch_to_keep,
            &RecordBatchOptions::new().with_row_count(Some(n_to_keep)),
        )?;
        Ok(())
    }

    /// Prunes emitted parts from WindowAggState `out_col` field.
    fn prune_out_columns(&mut self) {
        // We store generated columns for each window expression in the `out_col`
        // field of `WindowAggState`. Given how many rows are emitted, we remove
        // these sections from state.
        for partition_window_agg_states in self.window_agg_states.iter_mut() {
            // Remove `n_out` entries from the `out_col` field of `WindowAggState`.
            // `n_out` is stored in `self.partition_buffers` for each partition.
            // If `is_end` is set, directly remove them; this shrinks the hash map.
            partition_window_agg_states
                .retain(|_, partition_batch_state| !partition_batch_state.state.is_end);
            for (
                partition_key,
                WindowState {
                    state: WindowAggState { out_col, .. },
                    ..
                },
            ) in partition_window_agg_states
            {
                let partition_batch = &mut self.partition_buffers[partition_key];
                let n_to_del = partition_batch.n_out_row;
                let n_to_keep = out_col.len() - n_to_del;
                *out_col = out_col.slice(n_to_del, n_to_keep);
            }
        }
    }
}

impl RecordBatchStream for BoundedWindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// Gets the index of minimum entry, returns None if empty.
fn argmin<T: PartialOrd>(data: impl Iterator<Item = T>) -> Option<(usize, T)> {
    data.enumerate()
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
}

/// Calculates the section we can show results for expression
fn get_aggregate_result_out_column(
    partition_window_agg_states: &PartitionWindowAggStates,
    len_to_show: usize,
) -> Result<ArrayRef> {
    let mut result = None;
    let mut running_length = 0;
    // We assume that iteration order is according to insertion order
    for (
        _,
        WindowState {
            state: WindowAggState { out_col, .. },
            ..
        },
    ) in partition_window_agg_states
    {
        if running_length < len_to_show {
            let n_to_use = min(len_to_show - running_length, out_col.len());
            let slice_to_use = out_col.slice(0, n_to_use);
            result = Some(match result {
                Some(arr) => concat(&[&arr, &slice_to_use])?,
                None => slice_to_use,
            });
            running_length += n_to_use;
        } else {
            break;
        }
    }
    if running_length != len_to_show {
        return exec_err!(
            "Generated row number should be {len_to_show}, it is {running_length}"
        );
    }
    result
        .ok_or_else(|| DataFusionError::Execution("Should contain something".to_string()))
}

/// Constructs a batch from the last row of batch in the argument.
pub(crate) fn get_last_row_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return exec_err!("Latest batch should have at least 1 row");
    }
    Ok(batch.slice(batch.num_rows() - 1, 1))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use crate::common::collect;
    use crate::memory::MemoryExec;
    use crate::projection::ProjectionExec;
    use crate::streaming::{PartitionStream, StreamingTableExec};
    use crate::windows::{create_window_expr, BoundedWindowAggExec, InputOrderMode};
    use crate::{execute_stream, get_plan_string, ExecutionPlan};

    use arrow_array::builder::{Int64Builder, UInt64Builder};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
    use datafusion_common::{
        assert_batches_eq, exec_datafusion_err, Result, ScalarValue,
    };
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::{
        RecordBatchStream, SendableRecordBatchStream, TaskContext,
    };
    use datafusion_expr::{
        Expr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::expressions::{col, Column, NthValue};
    use datafusion_physical_expr::window::{
        BuiltInWindowExpr, BuiltInWindowFunctionExpr,
    };
    use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};

    use futures::future::Shared;
    use futures::{pin_mut, ready, FutureExt, Stream, StreamExt};
    use itertools::Itertools;
    use tokio::time::timeout;

    #[derive(Debug, Clone)]
    struct TestStreamPartition {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        idx: usize,
        state: PolingState,
        sleep_duration: Duration,
        send_exit: bool,
    }

    impl PartitionStream for TestStreamPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            // We create an iterator from the record batches and map them into Ok values,
            // converting the iterator into a futures::stream::Stream
            Box::pin(self.clone())
        }
    }

    impl Stream for TestStreamPartition {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.poll_next_inner(cx)
        }
    }

    #[derive(Debug, Clone)]
    enum PolingState {
        Sleep(Shared<futures::future::BoxFuture<'static, ()>>),
        BatchReturn,
    }

    impl TestStreamPartition {
        fn poll_next_inner(
            self: &mut Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<RecordBatch>>> {
            loop {
                match &mut self.state {
                    PolingState::BatchReturn => {
                        // Wait for self.sleep_duration before sending any new data
                        let f = tokio::time::sleep(self.sleep_duration).boxed().shared();
                        self.state = PolingState::Sleep(f);
                        let input_batch = if let Some(batch) =
                            self.batches.clone().get(self.idx)
                        {
                            batch.clone()
                        } else if self.send_exit {
                            // Send None to signal end of data
                            return Poll::Ready(None);
                        } else {
                            // Go to sleep mode
                            let f =
                                tokio::time::sleep(self.sleep_duration).boxed().shared();
                            self.state = PolingState::Sleep(f);
                            continue;
                        };
                        self.idx += 1;
                        return Poll::Ready(Some(Ok(input_batch)));
                    }
                    PolingState::Sleep(future) => {
                        pin_mut!(future);
                        ready!(future.poll_unpin(cx));
                        self.state = PolingState::BatchReturn;
                    }
                }
            }
        }
    }

    impl RecordBatchStream for TestStreamPartition {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    fn bounded_window_exec_pb_latent_range(
        input: Arc<dyn ExecutionPlan>,
        n_future_range: usize,
        hash: &str,
        order_by: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let window_fn = WindowFunctionDefinition::AggregateUDF(count_udaf());
        let col_expr =
            Arc::new(Column::new(schema.fields[0].name(), 0)) as Arc<dyn PhysicalExpr>;
        let log_expr =
            Expr::Column(datafusion_common::Column::from(schema.fields[0].name()));
        let args = vec![col_expr];
        let log_args = vec![log_expr];
        let partitionby_exprs = vec![col(hash, &schema)?];
        let orderby_exprs = vec![PhysicalSortExpr {
            expr: col(order_by, &schema)?,
            options: SortOptions::default(),
        }];
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n_future_range as u64))),
        );
        let fn_name = format!(
            "{}({:?}) PARTITION BY: [{:?}], ORDER BY: [{:?}]",
            window_fn, args, partitionby_exprs, orderby_exprs
        );
        let input_order_mode = InputOrderMode::Linear;
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                &window_fn,
                fn_name,
                &args,
                &log_args,
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame.clone()),
                &input.schema(),
                false,
            )?],
            input,
            partitionby_exprs,
            input_order_mode,
        )?))
    }

    fn projection_exec(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let exprs = input
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let name = if field.name().len() > 20 {
                    format!("col_{idx}")
                } else {
                    field.name().clone()
                };
                let expr = col(field.name(), &schema).unwrap();
                (expr, name)
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
    }

    fn task_context_helper() -> TaskContext {
        let task_ctx = TaskContext::default();
        // Create session context with config
        let session_config = SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(2)
            .with_round_robin_repartition(false);
        task_ctx.with_session_config(session_config)
    }

    fn task_context() -> Arc<TaskContext> {
        Arc::new(task_context_helper())
    }

    pub async fn collect_stream(
        mut stream: SendableRecordBatchStream,
        results: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        while let Some(item) = stream.next().await {
            results.push(item?);
        }
        Ok(())
    }

    /// Execute the [ExecutionPlan] and collect the results in memory
    pub async fn collect_with_timeout(
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        timeout_duration: Duration,
    ) -> Result<Vec<RecordBatch>> {
        let stream = execute_stream(plan, context)?;
        let mut results = vec![];

        // Execute the asynchronous operation with a timeout
        if timeout(timeout_duration, collect_stream(stream, &mut results))
            .await
            .is_ok()
        {
            return Err(exec_datafusion_err!("shouldn't have completed"));
        };

        Ok(results)
    }

    /// Execute the [ExecutionPlan] and collect the results in memory
    #[allow(dead_code)]
    pub async fn collect_bonafide(
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let stream = execute_stream(plan, context)?;
        let mut results = vec![];

        collect_stream(stream, &mut results).await?;

        Ok(results)
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("sn", DataType::UInt64, true),
            Field::new("hash", DataType::Int64, true),
        ]))
    }

    fn schema_orders(schema: &SchemaRef) -> Result<Vec<LexOrdering>> {
        let orderings = vec![vec![PhysicalSortExpr {
            expr: col("sn", schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]];
        Ok(orderings)
    }

    fn is_integer_division_safe(lhs: usize, rhs: usize) -> bool {
        let res = lhs / rhs;
        res * rhs == lhs
    }
    fn generate_batches(
        schema: &SchemaRef,
        n_row: usize,
        n_chunk: usize,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = vec![];
        assert!(n_row > 0);
        assert!(n_chunk > 0);
        assert!(is_integer_division_safe(n_row, n_chunk));
        let hash_replicate = 4;

        let chunks = (0..n_row)
            .chunks(n_chunk)
            .into_iter()
            .map(|elem| elem.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        // Send 2 RecordBatches at the source
        for sn_values in chunks {
            let mut sn1_array = UInt64Builder::with_capacity(sn_values.len());
            let mut hash_array = Int64Builder::with_capacity(sn_values.len());

            for sn in sn_values {
                sn1_array.append_value(sn as u64);
                let hash_value = (2 - (sn / hash_replicate)) as i64;
                hash_array.append_value(hash_value);
            }

            let batch = RecordBatch::try_new(
                Arc::clone(schema),
                vec![Arc::new(sn1_array.finish()), Arc::new(hash_array.finish())],
            )?;
            batches.push(batch);
        }
        Ok(batches)
    }

    fn generate_never_ending_source(
        n_rows: usize,
        chunk_length: usize,
        n_partition: usize,
        is_infinite: bool,
        send_exit: bool,
        per_batch_wait_duration_in_millis: u64,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(n_partition > 0);

        // We use same hash value in the table. This makes sure that
        // After hashing computation will continue in only in one of the output partitions
        // In this case, data flow should still continue
        let schema = test_schema();
        let orderings = schema_orders(&schema)?;

        // Source waits per_batch_wait_duration_in_millis ms before sending other batch
        let per_batch_wait_duration =
            Duration::from_millis(per_batch_wait_duration_in_millis);

        let batches = generate_batches(&schema, n_rows, chunk_length)?;

        // Source has 2 partitions
        let partitions = vec![
            Arc::new(TestStreamPartition {
                schema: Arc::clone(&schema),
                batches: batches.clone(),
                idx: 0,
                state: PolingState::BatchReturn,
                sleep_duration: per_batch_wait_duration,
                send_exit,
            }) as _;
            n_partition
        ];
        let source = Arc::new(StreamingTableExec::try_new(
            Arc::clone(&schema),
            partitions,
            None,
            orderings,
            is_infinite,
            None,
        )?) as _;
        Ok(source)
    }

    // Tests NTH_VALUE(negative index) with memoize feature.
    // To be able to trigger memoize feature for NTH_VALUE we need to
    // - feed BoundedWindowAggExec with batch stream data.
    // - Window frame should contain UNBOUNDED PRECEDING.
    // It hard to ensure these conditions are met, from the sql query.
    #[tokio::test]
    async fn test_window_nth_value_bounded_memoize() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(1);
        let task_ctx = Arc::new(TaskContext::default().with_session_config(config));

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )?;

        let memory_exec = MemoryExec::try_new(
            &[vec![batch.clone(), batch.clone(), batch.clone()]],
            Arc::clone(&schema),
            None,
        )
        .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;
        let col_a = col("a", &schema)?;
        let nth_value_func1 = NthValue::nth(
            "nth_value(-1)",
            Arc::clone(&col_a),
            DataType::Int32,
            1,
            false,
        )?
        .reverse_expr()
        .unwrap();
        let nth_value_func2 = NthValue::nth(
            "nth_value(-2)",
            Arc::clone(&col_a),
            DataType::Int32,
            2,
            false,
        )?
        .reverse_expr()
        .unwrap();
        let last_value_func = Arc::new(NthValue::last(
            "last",
            Arc::clone(&col_a),
            DataType::Int32,
            false,
        )) as _;
        let window_exprs = vec![
            // LAST_VALUE(a)
            Arc::new(BuiltInWindowExpr::new(
                last_value_func,
                &[],
                &[],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    WindowFrameBound::CurrentRow,
                )),
            )) as _,
            // NTH_VALUE(a, -1)
            Arc::new(BuiltInWindowExpr::new(
                nth_value_func1,
                &[],
                &[],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    WindowFrameBound::CurrentRow,
                )),
            )) as _,
            // NTH_VALUE(a, -2)
            Arc::new(BuiltInWindowExpr::new(
                nth_value_func2,
                &[],
                &[],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    WindowFrameBound::CurrentRow,
                )),
            )) as _,
        ];
        let physical_plan = BoundedWindowAggExec::try_new(
            window_exprs,
            memory_exec,
            vec![],
            InputOrderMode::Sorted,
        )
        .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;

        let batches = collect(physical_plan.execute(0, task_ctx)?).await?;

        let expected = vec![
            "BoundedWindowAggExec: wdw=[last: Ok(Field { name: \"last\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: true }, nth_value(-1): Ok(Field { name: \"nth_value(-1)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: true }, nth_value(-2): Ok(Field { name: \"nth_value(-2)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
            "  MemoryExec: partitions=1, partition_sizes=[3]",
        ];
        // Get string representation of the plan
        let actual = get_plan_string(&physical_plan);
        assert_eq!(
            expected, actual,
            "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let expected = [
            "+---+------+---------------+---------------+",
            "| a | last | nth_value(-1) | nth_value(-2) |",
            "+---+------+---------------+---------------+",
            "| 1 | 1    | 1             |               |",
            "| 2 | 2    | 2             | 1             |",
            "| 3 | 3    | 3             | 2             |",
            "| 1 | 1    | 1             | 3             |",
            "| 2 | 2    | 2             | 1             |",
            "| 3 | 3    | 3             | 2             |",
            "| 1 | 1    | 1             | 3             |",
            "| 2 | 2    | 2             | 1             |",
            "| 3 | 3    | 3             | 2             |",
            "+---+------+---------------+---------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    // This test, tests whether most recent row guarantee by the input batch of the `BoundedWindowAggExec`
    // helps `BoundedWindowAggExec` to generate low latency result in the `Linear` mode.
    // Input data generated at the source is
    //       "+----+------+",
    //       "| sn | hash |",
    //       "+----+------+",
    //       "| 0  | 2    |",
    //       "| 1  | 2    |",
    //       "| 2  | 2    |",
    //       "| 3  | 2    |",
    //       "| 4  | 1    |",
    //       "| 5  | 1    |",
    //       "| 6  | 1    |",
    //       "| 7  | 1    |",
    //       "| 8  | 0    |",
    //       "| 9  | 0    |",
    //       "+----+------+",
    //
    // Effectively following query is run on this data
    //
    //   SELECT *, count(*) OVER(PARTITION BY duplicated_hash ORDER BY sn RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)
    //   FROM test;
    //
    // partition `duplicated_hash=2` receives following data from the input
    //
    //       "+----+------+",
    //       "| sn | hash |",
    //       "+----+------+",
    //       "| 0  | 2    |",
    //       "| 1  | 2    |",
    //       "| 2  | 2    |",
    //       "| 3  | 2    |",
    //       "+----+------+",
    // normally `BoundedWindowExec` can only generate following result from the input above
    //
    //       "+----+------+---------+",
    //       "| sn | hash |  count  |",
    //       "+----+------+---------+",
    //       "| 0  | 2    |  2      |",
    //       "| 1  | 2    |  2      |",
    //       "| 2  | 2    |<not yet>|",
    //       "| 3  | 2    |<not yet>|",
    //       "+----+------+---------+",
    // where result of last 2 row is missing. Since window frame end is not may change with future data
    // since window frame end is determined by 1 following (To generate result for row=3[where sn=2] we
    // need to received sn=4 to make sure window frame end bound won't change with future data).
    //
    // With the ability of different partitions to use global ordering at the input (where most up-to date
    //   row is
    //      "| 9  | 0    |",
    //   )
    //
    // `BoundedWindowExec` should be able to generate following result in the test
    //
    //       "+----+------+-------+",
    //       "| sn | hash | col_2 |",
    //       "+----+------+-------+",
    //       "| 0  | 2    | 2     |",
    //       "| 1  | 2    | 2     |",
    //       "| 2  | 2    | 2     |",
    //       "| 3  | 2    | 1     |",
    //       "| 4  | 1    | 2     |",
    //       "| 5  | 1    | 2     |",
    //       "| 6  | 1    | 2     |",
    //       "| 7  | 1    | 1     |",
    //       "+----+------+-------+",
    //
    // where result for all rows except last 2 is calculated (To calculate result for row 9 where sn=8
    //   we need to receive sn=10 value to calculate it result.).
    // In this test, out aim is to test for which portion of the input data `BoundedWindowExec` can generate
    // a result. To test this behaviour, we generated the data at the source infinitely (no `None` signal
    //    is sent to output from source). After, row:
    //
    //       "| 9  | 0    |",
    //
    // is sent. Source stops sending data to output. We collect, result emitted by the `BoundedWindowExec` at the
    // end of the pipeline with a timeout (Since no `None` is sent from source. Collection never ends otherwise).
    #[tokio::test]
    async fn bounded_window_exec_linear_mode_range_information() -> Result<()> {
        let n_rows = 10;
        let chunk_length = 2;
        let n_future_range = 1;

        let timeout_duration = Duration::from_millis(2000);

        let source =
            generate_never_ending_source(n_rows, chunk_length, 1, true, false, 5)?;

        let window =
            bounded_window_exec_pb_latent_range(source, n_future_range, "hash", "sn")?;

        let plan = projection_exec(window)?;

        let expected_plan = vec![
            "ProjectionExec: expr=[sn@0 as sn, hash@1 as hash, count([Column { name: \"sn\", index: 0 }]) PARTITION BY: [[Column { name: \"hash\", index: 1 }]], ORDER BY: [[PhysicalSortExpr { expr: Column { name: \"sn\", index: 0 }, options: SortOptions { descending: false, nulls_first: true } }]]@2 as col_2]",
            "  BoundedWindowAggExec: wdw=[count([Column { name: \"sn\", index: 0 }]) PARTITION BY: [[Column { name: \"hash\", index: 1 }]], ORDER BY: [[PhysicalSortExpr { expr: Column { name: \"sn\", index: 0 }, options: SortOptions { descending: false, nulls_first: true } }]]: Ok(Field { name: \"count([Column { name: \\\"sn\\\", index: 0 }]) PARTITION BY: [[Column { name: \\\"hash\\\", index: 1 }]], ORDER BY: [[PhysicalSortExpr { expr: Column { name: \\\"sn\\\", index: 0 }, options: SortOptions { descending: false, nulls_first: true } }]]\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(UInt64(1)), is_causal: false }], mode=[Linear]",
            "    StreamingTableExec: partition_sizes=1, projection=[sn, hash], infinite_source=true, output_ordering=[sn@0 ASC NULLS LAST]",
        ];

        // Get string representation of the plan
        let actual = get_plan_string(&plan);
        assert_eq!(
            expected_plan, actual,
            "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_plan:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let task_ctx = task_context();
        let batches = collect_with_timeout(plan, task_ctx, timeout_duration).await?;

        let expected = [
            "+----+------+-------+",
            "| sn | hash | col_2 |",
            "+----+------+-------+",
            "| 0  | 2    | 2     |",
            "| 1  | 2    | 2     |",
            "| 2  | 2    | 2     |",
            "| 3  | 2    | 1     |",
            "| 4  | 1    | 2     |",
            "| 5  | 1    | 2     |",
            "| 6  | 1    | 2     |",
            "| 7  | 1    | 1     |",
            "+----+------+-------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }
}
