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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! The Union operator combines multiple inputs with the same schema

use std::borrow::Borrow;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{any::Any, sync::Arc};

use super::{
    execution_mode_from_children,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::metrics::BaselineMetrics;
use crate::stream::ObservedStream;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{ConstExpr, EquivalenceProperties};

use futures::Stream;
use itertools::Itertools;
use log::{debug, trace, warn};
use tokio::macros::support::thread_rng_n;

/// `UnionExec`: `UNION ALL` execution plan.
///
/// `UnionExec` combines multiple inputs with the same schema by
/// concatenating the partitions.  It does not mix or copy data within
/// or across partitions. Thus if the input partitions are sorted, the
/// output partitions of the union are also sorted.
///
/// For example, given a `UnionExec` of two inputs, with `N`
/// partitions, and `M` partitions, there will be `N+M` output
/// partitions. The first `N` output partitions are from Input 1
/// partitions, and then next `M` output partitions are from Input 2.
///
/// ```text
///                       ▲       ▲           ▲         ▲
///                       │       │           │         │
///     Output            │  ...  │           │         │
///   Partitions          │0      │N-1        │ N       │N+M-1
///(passes through   ┌────┴───────┴───────────┴─────────┴───┐
/// the N+M input    │              UnionExec               │
///  partitions)     │                                      │
///                  └──────────────────────────────────────┘
///                                      ▲
///                                      │
///                                      │
///       Input           ┌────────┬─────┴────┬──────────┐
///     Partitions        │ ...    │          │     ...  │
///                    0  │        │ N-1      │ 0        │  M-1
///                  ┌────┴────────┴───┐  ┌───┴──────────┴───┐
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │Input 1          │  │Input 2           │
///                  └─────────────────┘  └──────────────────┘
/// ```
#[derive(Debug)]
pub struct UnionExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl UnionExec {
    /// Create a new UnionExec
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let schema = union_schema(&inputs);
        let cache = Self::compute_properties(&inputs, schema);
        UnionExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        inputs: &[Arc<dyn ExecutionPlan>],
        schema: SchemaRef,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let children_eqs = inputs
            .iter()
            .map(|child| child.equivalence_properties())
            .collect::<Vec<_>>();
        let eq_properties = calculate_union_eq_properties(&children_eqs, schema);

        // Calculate output partitioning; i.e. sum output partitions of the inputs.
        let num_partitions = inputs
            .iter()
            .map(|plan| plan.output_partitioning().partition_count())
            .sum();
        let output_partitioning = Partitioning::UnknownPartitioning(num_partitions);

        // Determine execution mode:
        let mode = execution_mode_from_children(inputs.iter());

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}
/// Calculate `EquivalenceProperties` for `UnionExec` from the `EquivalenceProperties`
/// of its children.
fn calculate_union_eq_properties(
    children_eqs: &[&EquivalenceProperties],
    schema: SchemaRef,
) -> EquivalenceProperties {
    // Calculate equivalence properties:
    // TODO: In some cases, we should be able to preserve some equivalence
    //       classes and constants. Add support for such cases.
    let mut eq_properties = EquivalenceProperties::new(schema);
    // Use the ordering equivalence class of the first child as the seed:
    let mut meets = children_eqs[0]
        .oeq_class()
        .iter()
        .map(|item| item.to_vec())
        .collect::<Vec<_>>();
    // Iterate over all the children:
    for child_eqs in &children_eqs[1..] {
        // Compute meet orderings of the current meets and the new ordering
        // equivalence class.
        let mut idx = 0;
        while idx < meets.len() {
            // Find all the meets of `current_meet` with this child's orderings:
            let valid_meets = child_eqs.oeq_class().iter().filter_map(|ordering| {
                child_eqs.get_meet_ordering(ordering, &meets[idx])
            });
            // Use the longest of these meets as others are redundant:
            if let Some(next_meet) = valid_meets.max_by_key(|m| m.len()) {
                meets[idx] = next_meet;
                idx += 1;
            } else {
                meets.swap_remove(idx);
            }
        }
    }
    // We know have all the valid orderings after union, remove redundant
    // entries (implicitly) and return:
    eq_properties.add_new_orderings(meets);

    let mut meet_constants = children_eqs[0].constants().to_vec();
    // Iterate over all the children:
    for child_eqs in &children_eqs[1..] {
        let constants = child_eqs.constants();
        meet_constants = meet_constants
            .into_iter()
            .filter_map(|meet_constant| {
                for const_expr in constants {
                    if const_expr.expr().eq(meet_constant.expr()) {
                        // TODO: Check whether constant expressions evaluates the same value or not for each partition
                        let across_partitions = false;
                        return Some(
                            ConstExpr::new(meet_constant.owned_expr())
                                .with_across_partitions(across_partitions),
                        );
                    }
                }
                None
            })
            .collect::<Vec<_>>();
    }
    eq_properties.add_constants(meet_constants)
}

impl DisplayAs for UnionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnionExec")
            }
        }
    }
}

impl ExecutionPlan for UnionExec {
    fn name(&self) -> &'static str {
        "UnionExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // If the Union has an output ordering, it maintains at least one
        // child's ordering (i.e. the meet).
        // For instance, assume that the first child is SortExpr('a','b','c'),
        // the second child is SortExpr('a','b') and the third child is
        // SortExpr('a','b'). The output ordering would be SortExpr('a','b'),
        // which is the "meet" of all input orderings. In this example, this
        // function will return vec![false, true, true], indicating that we
        // preserve the orderings for the 2nd and the 3rd children.
        if let Some(output_ordering) = self.properties().output_ordering() {
            self.inputs()
                .iter()
                .map(|child| {
                    if let Some(child_ordering) = child.output_ordering() {
                        output_ordering.len() == child_ordering.len()
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            vec![false; self.inputs().len()]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnionExec::new(children)))
    }

    fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start UnionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        // find partition to execute
        for input in self.inputs.iter() {
            // Calculate whether partition belongs to the current partition
            if partition < input.output_partitioning().partition_count() {
                let stream = input.execute(partition, context)?;
                debug!("Found a Union partition to execute");
                return Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)));
            } else {
                partition -= input.output_partitioning().partition_count();
            }
        }

        warn!("Error in Union: Partition {} not found", partition);

        exec_err!("Partition {partition} not found in Union")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let stats = self
            .inputs
            .iter()
            .map(|stat| stat.statistics())
            .collect::<Result<Vec<_>>>()?;

        Ok(stats
            .into_iter()
            .reduce(stats_union)
            .unwrap_or_else(|| Statistics::new_unknown(&self.schema())))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }
}

/// Combines multiple input streams by interleaving them.
///
/// This only works if all inputs have the same hash-partitioning.
///
/// # Data Flow
/// ```text
/// +---------+
/// |         |---+
/// | Input 1 |   |
/// |         |-------------+
/// +---------+   |         |
///               |         |         +---------+
///               +------------------>|         |
///                 +---------------->| Combine |-->
///                 | +-------------->|         |
///                 | |     |         +---------+
/// +---------+     | |     |
/// |         |-----+ |     |
/// | Input 2 |       |     |
/// |         |---------------+
/// +---------+       |     | |
///                   |     | |       +---------+
///                   |     +-------->|         |
///                   |       +------>| Combine |-->
///                   |         +---->|         |
///                   |         |     +---------+
/// +---------+       |         |
/// |         |-------+         |
/// | Input 3 |                 |
/// |         |-----------------+
/// +---------+
/// ```
#[derive(Debug)]
pub struct InterleaveExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl InterleaveExec {
    /// Create a new InterleaveExec
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        if !can_interleave(inputs.iter()) {
            return internal_err!(
                "Not all InterleaveExec children have a consistent hash partitioning"
            );
        }
        let cache = Self::compute_properties(&inputs);
        Ok(InterleaveExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(inputs: &[Arc<dyn ExecutionPlan>]) -> PlanProperties {
        let schema = union_schema(inputs);
        let eq_properties = EquivalenceProperties::new(schema);
        // Get output partitioning:
        let output_partitioning = inputs[0].output_partitioning().clone();
        // Determine execution mode:
        let mode = execution_mode_from_children(inputs.iter());

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl DisplayAs for InterleaveExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InterleaveExec")
            }
        }
    }
}

impl ExecutionPlan for InterleaveExec {
    fn name(&self) -> &'static str {
        "InterleaveExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.inputs().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(InterleaveExec::try_new(children)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start InterleaveExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        let mut input_stream_vec = vec![];
        for input in self.inputs.iter() {
            if partition < input.output_partitioning().partition_count() {
                input_stream_vec.push(input.execute(partition, context.clone())?);
            } else {
                // Do not find a partition to execute
                break;
            }
        }
        if input_stream_vec.len() == self.inputs.len() {
            let stream = Box::pin(CombinedRecordBatchStream::new(
                self.schema(),
                input_stream_vec,
            ));
            return Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)));
        }

        warn!("Error in InterleaveExec: Partition {} not found", partition);

        exec_err!("Partition {partition} not found in InterleaveExec")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let stats = self
            .inputs
            .iter()
            .map(|stat| stat.statistics())
            .collect::<Result<Vec<_>>>()?;

        Ok(stats
            .into_iter()
            .reduce(stats_union)
            .unwrap_or_else(|| Statistics::new_unknown(&self.schema())))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }
}

/// If all the input partitions have the same Hash partition spec with the first_input_partition
/// The InterleaveExec is partition aware.
///
/// It might be too strict here in the case that the input partition specs are compatible but not exactly the same.
/// For example one input partition has the partition spec Hash('a','b','c') and
/// other has the partition spec Hash('a'), It is safe to derive the out partition with the spec Hash('a','b','c').
pub fn can_interleave<T: Borrow<Arc<dyn ExecutionPlan>>>(
    mut inputs: impl Iterator<Item = T>,
) -> bool {
    let Some(first) = inputs.next() else {
        return false;
    };

    let reference = first.borrow().output_partitioning();
    matches!(reference, Partitioning::Hash(_, _))
        && inputs
            .map(|plan| plan.borrow().output_partitioning().clone())
            .all(|partition| partition == *reference)
}

fn union_schema(inputs: &[Arc<dyn ExecutionPlan>]) -> SchemaRef {
    let fields: Vec<Field> = (0..inputs[0].schema().fields().len())
        .map(|i| {
            inputs
                .iter()
                .filter_map(|input| {
                    if input.schema().fields().len() > i {
                        Some(input.schema().field(i).clone())
                    } else {
                        None
                    }
                })
                .find_or_first(|f| f.is_nullable())
                .unwrap()
        })
        .collect();

    Arc::new(Schema::new_with_metadata(
        fields,
        inputs[0].schema().metadata().clone(),
    ))
}

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(schema: SchemaRef, entries: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, entries }
    }
}

impl RecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Ready(Some(val)),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
    }
}

fn col_stats_union(
    mut left: ColumnStatistics,
    right: ColumnStatistics,
) -> ColumnStatistics {
    left.distinct_count = Precision::Absent;
    left.min_value = left.min_value.min(&right.min_value);
    left.max_value = left.max_value.max(&right.max_value);
    left.null_count = left.null_count.add(&right.null_count);

    left
}

fn stats_union(mut left: Statistics, right: Statistics) -> Statistics {
    left.num_rows = left.num_rows.add(&right.num_rows);
    left.total_byte_size = left.total_byte_size.add(&right.total_byte_size);
    left.column_statistics = left
        .column_statistics
        .into_iter()
        .zip(right.column_statistics)
        .map(|(a, b)| col_stats_union(a, b))
        .collect::<Vec<_>>();
    left
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::memory::MemoryExec;
    use crate::test;

    use arrow_schema::{DataType, SortOptions};
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

    // Generate a schema which consists of 7 columns (a, b, c, d, e, f, g)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let g = Field::new("g", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f, g]));

        Ok(schema)
    }

    // Convert each tuple to PhysicalSortExpr
    fn convert_to_sort_exprs(
        in_data: &[(&Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> Vec<PhysicalSortExpr> {
        in_data
            .iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: (*expr).clone(),
                options: *options,
            })
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn test_union_partitions() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Create inputs with different partitioning
        let csv = test::scan_partitioned(4);
        let csv2 = test::scan_partitioned(5);

        let union_exec = Arc::new(UnionExec::new(vec![csv, csv2]));

        // Should have 9 partitions and 9 output batches
        assert_eq!(
            union_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            9
        );

        let result: Vec<RecordBatch> = collect(union_exec, task_ctx).await?;
        assert_eq!(result.len(), 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_union() {
        let left = Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    null_count: Precision::Exact(3),
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Float32(Some(1.1))),
                    min_value: Precision::Exact(ScalarValue::Float32(Some(0.1))),
                    null_count: Precision::Absent,
                },
            ],
        };

        let right = Statistics {
            num_rows: Precision::Exact(7),
            total_byte_size: Precision::Exact(29),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(34))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    null_count: Precision::Exact(1),
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::from("c")),
                    min_value: Precision::Exact(ScalarValue::from("b")),
                    null_count: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    null_count: Precision::Absent,
                },
            ],
        };

        let result = stats_union(left, right);
        let expected = Statistics {
            num_rows: Precision::Exact(12),
            total_byte_size: Precision::Exact(52),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Int64(Some(34))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    null_count: Precision::Exact(1),
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    null_count: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    null_count: Precision::Absent,
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_union_equivalence_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_f = &col("f", &schema)?;
        let options = SortOptions::default();
        let test_cases = [
            //-----------TEST CASE 1----------//
            (
                // First child orderings
                vec![
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                ],
                // Second child orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, options), (col_b, options), (col_c, options)],
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                ],
                // Union output orderings
                vec![
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                ],
            ),
            //-----------TEST CASE 2----------//
            (
                // First child orderings
                vec![
                    // [a ASC, b ASC, f ASC]
                    vec![(col_a, options), (col_b, options), (col_f, options)],
                    // d ASC
                    vec![(col_d, options)],
                ],
                // Second child orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, options), (col_b, options), (col_c, options)],
                    // [e ASC]
                    vec![(col_e, options)],
                ],
                // Union output orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, options), (col_b, options)],
                ],
            ),
        ];

        for (
            test_idx,
            (first_child_orderings, second_child_orderings, union_orderings),
        ) in test_cases.iter().enumerate()
        {
            let first_orderings = first_child_orderings
                .iter()
                .map(|ordering| convert_to_sort_exprs(ordering))
                .collect::<Vec<_>>();
            let second_orderings = second_child_orderings
                .iter()
                .map(|ordering| convert_to_sort_exprs(ordering))
                .collect::<Vec<_>>();
            let union_expected_orderings = union_orderings
                .iter()
                .map(|ordering| convert_to_sort_exprs(ordering))
                .collect::<Vec<_>>();
            let child1 = Arc::new(
                MemoryExec::try_new(&[], schema.clone(), None)?
                    .with_sort_information(first_orderings),
            );
            let child2 = Arc::new(
                MemoryExec::try_new(&[], schema.clone(), None)?
                    .with_sort_information(second_orderings),
            );

            let union = UnionExec::new(vec![child1, child2]);
            let union_eq_properties = union.properties().equivalence_properties();
            let union_actual_orderings = union_eq_properties.oeq_class();
            let err_msg = format!(
                "Error in test id: {:?}, test case: {:?}",
                test_idx, test_cases[test_idx]
            );
            assert_eq!(
                union_actual_orderings.len(),
                union_expected_orderings.len(),
                "{}",
                err_msg
            );
            for expected in &union_expected_orderings {
                assert!(union_actual_orderings.contains(expected), "{}", err_msg);
            }
        }
        Ok(())
    }
}
