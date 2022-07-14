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

//! Defines the execution plan for the hash aggregate operation

use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use futures::{
    stream::{Stream, StreamExt},
    Future,
};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::hash_utils::create_hashes;
use crate::physical_plan::{
    Accumulator, AggregateExpr, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, PhysicalExpr,
};
use crate::scalar::ScalarValue;

use crate::record_batch::RecordBatch;
use arrow::array::UInt32Array;
use arrow::compute::{concatenate, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::{
    array::Array,
    error::{Error as ArrowError, Result as ArrowResult},
};
use arrow::{array::ArrayRef, compute::cast};
use hashbrown::raw::RawTable;
use pin_project_lite::pin_project;

use crate::execution::runtime_env::RuntimeEnv;
use async_trait::async_trait;
use datafusion_common::field_util::{FieldExt, SchemaExt};
use datafusion_physical_expr::expressions::DEFAULT_DATAFUSION_CAST_OPTIONS;

use super::common::AbortOnDropSingle;
use super::expressions::PhysicalSortExpr;
use super::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput,
};
use super::Statistics;
use super::{expressions::Column, RecordBatchStream, SendableRecordBatchStream};

/// Hash aggregate modes
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggregateMode {
    /// Partial aggregate that can be applied in parallel across input partitions
    Partial,
    /// Final aggregate that produces a single partition of output
    Final,
    /// Final aggregate that works on pre-partitioned data.
    ///
    /// This requires the invariant that all rows with a particular
    /// grouping key are in the same partitions, such as is the case
    /// with Hash repartitioning on the group keys. If a group key is
    /// duplicated, duplicate groups would be produced
    FinalPartitioned,
}

/// Hash aggregate execution plan
#[derive(Debug)]
pub struct HashAggregateExec {
    /// Aggregation mode (full, partial)
    mode: AggregateMode,
    /// Grouping expressions
    group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied
    schema: SchemaRef,
    /// Input schema before any aggregation is applied. For partial aggregate this will be the
    /// same as input.schema() but for the final aggregate it will be the same as the input
    /// to the partial aggregate
    input_schema: SchemaRef,
    /// Execution Metrics
    metrics: ExecutionPlanMetricsSet,
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            expr.nullable(input_schema)?,
        ))
    }

    match mode {
        AggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned())
            }
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field()?)
            }
        }
    }

    Ok(Schema::new(fields))
}

impl HashAggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &group_expr, &aggr_expr, mode)?;

        let schema = Arc::new(schema);

        Ok(HashAggregateExec {
            mode,
            group_expr,
            aggr_expr,
            input,
            schema,
            input_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Aggregation mode (full, partial)
    pub fn mode(&self) -> &AggregateMode {
        &self.mode
    }

    /// Grouping expressions
    pub fn group_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.group_expr
    }

    /// Grouping expressions as they occur in the output schema
    pub fn output_group_expr(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        // Update column indices. Since the group by columns come first in the output schema, their
        // indices are simply 0..self.group_expr(len).
        self.group_expr
            .iter()
            .enumerate()
            .map(|(index, (_col, name))| {
                Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>
            })
            .collect()
    }

    /// Aggregate expressions
    pub fn aggr_expr(&self) -> &[Arc<dyn AggregateExpr>] {
        &self.aggr_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

#[async_trait]
impl ExecutionPlan for HashAggregateExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_child_distribution(&self) -> Distribution {
        match &self.mode {
            AggregateMode::Partial => Distribution::UnspecifiedDistribution,
            AggregateMode::FinalPartitioned => Distribution::HashPartitioned(
                self.group_expr.iter().map(|x| x.0.clone()).collect(),
            ),
            AggregateMode::Final => Distribution::SinglePartition,
        }
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, runtime).await?;
        let group_expr = self.group_expr.iter().map(|x| x.0.clone()).collect();

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        if self.group_expr.is_empty() {
            Ok(Box::pin(HashAggregateStream::new(
                self.mode,
                self.schema.clone(),
                self.aggr_expr.clone(),
                input,
                baseline_metrics,
            )))
        } else {
            Ok(Box::pin(GroupedHashAggregateStream::new(
                self.mode,
                self.schema.clone(),
                group_expr,
                self.aggr_expr.clone(),
                input,
                baseline_metrics,
            )))
        }
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(HashAggregateExec::try_new(
                self.mode,
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                children[0].clone(),
                self.input_schema.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "HashAggregateExec wrong number of children".to_string(),
            )),
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "HashAggregateExec: mode={:?}", self.mode)?;
                let g: Vec<String> = self
                    .group_expr
                    .iter()
                    .map(|(e, alias)| {
                        let e = e.to_string();
                        if &e != alias {
                            format!("{} as {}", e, alias)
                        } else {
                            e
                        }
                    })
                    .collect();
                write!(f, ", gby=[{}]", g.join(", "))?;

                let a: Vec<String> = self
                    .aggr_expr
                    .iter()
                    .map(|agg| agg.name().to_string())
                    .collect();
                write!(f, ", aggr=[{}]", a.join(", "))?;
            }
        }
        Ok(())
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: group expressions:
        // - once expressions will be able to compute their own stats, use it here
        // - case where we group by on a column for which with have the `distinct` stat
        // TODO stats: aggr expression:
        // - aggregations somtimes also preserve invariants such as min, max...
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned
                if self.group_expr.is_empty() =>
            {
                Statistics {
                    num_rows: Some(1),
                    is_exact: true,
                    ..Default::default()
                }
            }
            _ => Statistics::default(),
        }
    }
}

/*
The architecture is the following:

1. An accumulator has state that is updated on each batch.
2. At the end of the aggregation (e.g. end of batches in a partition), the accumulator converts its state to a RecordBatch of a single row
3. The RecordBatches of all accumulators are merged (`concatenate` in `rust/arrow`) together to a single RecordBatch.
4. The state's RecordBatch is `merge`d to a new state
5. The state is mapped to the final value

Why:

* Accumulators' state can be statically typed, but it is more efficient to transmit data from the accumulators via `Array`
* The `merge` operation must have access to the state of the aggregators because it uses it to correctly merge
* It uses Arrow's native dynamically typed object, `Array`.
* Arrow shines in batch operations and both `merge` and `concatenate` of uniform types are very performant.

Example: average

* the state is `n: u32` and `sum: f64`
* For every batch, we update them accordingly.
* At the end of the accumulation (of a partition), we convert `n` and `sum` to a RecordBatch of 1 row and two columns: `[n, sum]`
* The RecordBatch is (sent back / transmitted over network)
* Once all N record batches arrive, `merge` is performed, which builds a RecordBatch with N rows and 2 columns.
* Finally, `get_value` returns an array with one entry computed from the state
*/
pin_project! {
    struct GroupedHashAggregateStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<RecordBatch>>,
        finished: bool,
        drop_helper: AbortOnDropSingle<()>,
    }
}

fn group_aggregate_batch(
    mode: &AggregateMode,
    random_state: &RandomState,
    group_expr: &[Arc<dyn PhysicalExpr>],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    batch: RecordBatch,
    mut accumulators: Accumulators,
    aggregate_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<Accumulators> {
    // evaluate the grouping expressions
    let group_values = evaluate(group_expr, &batch)?;

    // evaluate the aggregation expressions.
    // We could evaluate them after the `take`, but since we need to evaluate all
    // of them anyways, it is more performant to do it while they are together.
    let aggr_input_values = evaluate_many(aggregate_expressions, &batch)?;

    // 1.1 construct the key from the group values
    // 1.2 construct the mapping key if it does not exist
    // 1.3 add the row' index to `indices`

    // track which entries in `accumulators` have rows in this batch to aggregate
    let mut groups_with_rows = vec![];

    // 1.1 Calculate the group keys for the group values
    let mut batch_hashes = vec![0; batch.num_rows()];
    create_hashes(&group_values, random_state, &mut batch_hashes)?;

    for (row, hash) in batch_hashes.into_iter().enumerate() {
        let Accumulators { map, group_states } = &mut accumulators;

        let entry = map.get_mut(hash, |(_hash, group_idx)| {
            // verify that a group that we are inserting with hash is
            // actually the same key value as the group in
            // existing_idx  (aka group_values @ row)
            let group_state = &group_states[*group_idx];
            group_values
                .iter()
                .zip(group_state.group_by_values.iter())
                .all(|(array, scalar)| scalar.eq_array(array, row))
        });

        match entry {
            // Existing entry for this group value
            Some((_hash, group_idx)) => {
                let group_state = &mut group_states[*group_idx];
                // 1.3
                if group_state.indices.is_empty() {
                    groups_with_rows.push(*group_idx);
                };
                group_state.indices.push(row as u32); // remember this row
            }
            //  1.2 Need to create new entry
            None => {
                let accumulator_set = create_accumulators(aggr_expr)?;

                // Copy group values out of arrays into `ScalarValue`s
                let group_by_values = group_values
                    .iter()
                    .map(|col| ScalarValue::try_from_array(col, row))
                    .collect::<Result<Vec<_>>>()?;

                // Add new entry to group_states and save newly created index
                let group_state = GroupState {
                    group_by_values: group_by_values.into_boxed_slice(),
                    accumulator_set,
                    indices: vec![row as u32], // 1.3
                };
                let group_idx = group_states.len();
                group_states.push(group_state);
                groups_with_rows.push(group_idx);

                // for hasher function, use precomputed hash value
                map.insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);
            }
        };
    }

    // Collect all indices + offsets based on keys in this vec
    let mut batch_indices = Vec::<u32>::new();
    let mut offsets = vec![0];
    let mut offset_so_far = 0;
    for group_idx in groups_with_rows.iter() {
        let indices = &accumulators.group_states[*group_idx].indices;
        batch_indices.extend_from_slice(indices);
        offset_so_far += indices.len();
        offsets.push(offset_so_far);
    }
    let batch_indices =
        UInt32Array::from_data(DataType::UInt32, batch_indices.into(), None);

    // `Take` all values based on indices into Arrays
    let values: Vec<Vec<Arc<dyn Array>>> = aggr_input_values
        .iter()
        .map(|array| {
            array
                .iter()
                .map(|array| take::take(array.as_ref(), &batch_indices).unwrap().into())
                .collect()
            // 2.3
        })
        .collect();

    // 2.1 for each key in this batch
    // 2.2 for each aggregation
    // 2.3 `slice` from each of its arrays the keys' values
    // 2.4 update / merge the accumulator with the values
    // 2.5 clear indices
    groups_with_rows
        .iter()
        .zip(offsets.windows(2))
        .try_for_each(|(group_idx, offsets)| {
            let group_state = &mut accumulators.group_states[*group_idx];
            // 2.2
            group_state
                .accumulator_set
                .iter_mut()
                .zip(values.iter())
                .map(|(accumulator, aggr_array)| {
                    (
                        accumulator,
                        aggr_array
                            .iter()
                            .map(|array| {
                                // 2.3
                                array.slice(offsets[0], offsets[1] - offsets[0]).into()
                            })
                            .collect::<Vec<ArrayRef>>(),
                    )
                })
                .try_for_each(|(accumulator, values)| match mode {
                    AggregateMode::Partial => accumulator.update_batch(&values),
                    AggregateMode::FinalPartitioned | AggregateMode::Final => {
                        // note: the aggregation here is over states, not values, thus the merge
                        accumulator.merge_batch(&values)
                    }
                })
                // 2.5
                .and({
                    group_state.indices.clear();
                    Ok(())
                })
        })?;

    Ok(accumulators)
}

async fn compute_grouped_hash_aggregate(
    mode: AggregateMode,
    schema: SchemaRef,
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    mut input: SendableRecordBatchStream,
    elapsed_compute: metrics::Time,
) -> ArrowResult<RecordBatch> {
    let timer = elapsed_compute.timer();
    // The expressions to evaluate the batch, one vec of expressions per aggregation.
    // Assume create_schema() always put group columns in front of aggr columns, we set
    // col_idx_base to group expression count.
    let aggregate_expressions =
        aggregate_expressions(&aggr_expr, &mode, group_expr.len())?;

    let random_state = RandomState::new();

    // iterate over all input batches and update the accumulators
    let mut accumulators = Accumulators::default();
    timer.done();
    while let Some(batch) = input.next().await {
        let batch = batch?;
        let timer = elapsed_compute.timer();
        accumulators = group_aggregate_batch(
            &mode,
            &random_state,
            &group_expr,
            &aggr_expr,
            batch,
            accumulators,
            &aggregate_expressions,
        )?;
        timer.done();
    }

    let timer = elapsed_compute.timer();
    let batch = create_batch_from_map(&mode, &accumulators, group_expr.len(), &schema);
    timer.done();
    batch
}

impl GroupedHashAggregateStream {
    /// Create a new HashAggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();

        let schema_clone = schema.clone();
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();

        let join_handle = tokio::spawn(async move {
            let result = compute_grouped_hash_aggregate(
                mode,
                schema_clone,
                group_expr,
                aggr_expr,
                input,
                elapsed_compute,
            )
            .await
            .record_output(&baseline_metrics);

            // failing here is OK, the receiver is gone and does not care about the result
            tx.send(result).ok();
        });

        GroupedHashAggregateStream {
            schema,
            output: rx,
            finished: false,
            drop_helper: AbortOnDropSingle::new(join_handle),
        }
    }
}

type AccumulatorItem = Box<dyn Accumulator>;

/// The state that is built for each output group.
#[derive(Debug)]
struct GroupState {
    /// The actual group by values, one for each group column
    group_by_values: Box<[ScalarValue]>,

    // Accumulator state, one for each aggregate
    accumulator_set: Vec<AccumulatorItem>,

    /// scratch space used to collect indices for input rows in a
    /// bach that have values to aggregate. Reset on each batch
    indices: Vec<u32>,
}

/// The state of all the groups
#[derive(Default)]
struct Accumulators {
    /// Logically maps group values to an index in `group_states`
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, index into `group_states`)
    map: RawTable<(u64, usize)>,

    /// State for each group
    group_states: Vec<GroupState>,
}

impl std::fmt::Debug for Accumulators {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // hashes are not store inline, so could only get values
        let map_string = "RawTable";
        f.debug_struct("Accumulators")
            .field("map", &map_string)
            .field("group_states", &self.group_states)
            .finish()
    }
}

impl Stream for GroupedHashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;

                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Err(ArrowError::External("".to_string(), Box::new(e))), // error receiving
                    Ok(result) => result,
                };

                Poll::Ready(Some(result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(
    expr: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| expr.evaluate(batch))
        .map(|r| r.map(|v| v.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()
}

/// Evaluates expressions against a record batch.
fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter()
        .map(|expr| evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()
}

/// uses `state_fields` to build a vec of physical column expressions required to merge the
/// AggregateExpr' accumulator's state.
///
/// `index_base` is the starting physical column index for the next expanded state field.
fn merge_expressions(
    index_base: usize,
    expr: &Arc<dyn AggregateExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    Ok(expr
        .state_fields()?
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            Arc::new(Column::new(f.name(), index_base + idx)) as Arc<dyn PhysicalExpr>
        })
        .collect::<Vec<_>>())
}

/// returns physical expressions to evaluate against a batch
/// The expressions are different depending on `mode`:
/// * Partial: AggregateExpr::expressions
/// * Final: columns of `AggregateExpr::state_fields()`
fn aggregate_expressions(
    aggr_expr: &[Arc<dyn AggregateExpr>],
    mode: &AggregateMode,
    col_idx_base: usize,
) -> Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    match mode {
        AggregateMode::Partial => {
            Ok(aggr_expr.iter().map(|agg| agg.expressions()).collect())
        }
        // in this mode, we build the merge expressions of the aggregation
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let mut col_idx_base = col_idx_base;
            Ok(aggr_expr
                .iter()
                .map(|agg| {
                    let exprs = merge_expressions(col_idx_base, agg)?;
                    col_idx_base += exprs.len();
                    Ok(exprs)
                })
                .collect::<Result<Vec<_>>>()?)
        }
    }
}

pin_project! {
    struct HashAggregateStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<RecordBatch>>,
        finished: bool,
        drop_helper: AbortOnDropSingle<()>,
    }
}

/// Special case aggregate with no groups
async fn compute_hash_aggregate(
    mode: AggregateMode,
    schema: SchemaRef,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    mut input: SendableRecordBatchStream,
    elapsed_compute: metrics::Time,
) -> ArrowResult<RecordBatch> {
    let timer = elapsed_compute.timer();
    let mut accumulators = create_accumulators(&aggr_expr)?;
    let expressions = aggregate_expressions(&aggr_expr, &mode, 0)?;
    let expressions = Arc::new(expressions);
    timer.done();

    // 1 for each batch, update / merge accumulators with the expressions' values
    // future is ready when all batches are computed
    while let Some(batch) = input.next().await {
        let batch = batch?;
        let timer = elapsed_compute.timer();
        aggregate_batch(&mode, &batch, &mut accumulators, &expressions)?;
        timer.done();
    }

    // 2. convert values to a record batch
    let timer = elapsed_compute.timer();
    let batch = finalize_aggregation(&accumulators, &mode)
        .map(|columns| RecordBatch::try_new(schema.clone(), columns))?;
    timer.done();
    batch
}

impl HashAggregateStream {
    /// Create a new HashAggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();

        let schema_clone = schema.clone();
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let join_handle = tokio::spawn(async move {
            let result = compute_hash_aggregate(
                mode,
                schema_clone,
                aggr_expr,
                input,
                elapsed_compute,
            )
            .await
            .record_output(&baseline_metrics);

            // failing here is OK, the receiver is gone and does not care about the result
            tx.send(result).ok();
        });

        HashAggregateStream {
            schema,
            output: rx,
            finished: false,
            drop_helper: AbortOnDropSingle::new(join_handle),
        }
    }
}

fn aggregate_batch(
    mode: &AggregateMode,
    batch: &RecordBatch,
    accumulators: &mut [AccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<()> {
    // 1.1 iterate accumulators and respective expressions together
    // 1.2 evaluate expressions
    // 1.3 update / merge accumulators with the expressions' values

    // 1.1
    accumulators
        .iter_mut()
        .zip(expressions)
        .try_for_each(|(accum, expr)| {
            // 1.2
            let values = &expr
                .iter()
                .map(|e| e.evaluate(batch))
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            // 1.3
            match mode {
                AggregateMode::Partial => accum.update_batch(values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    accum.merge_batch(values)
                }
            }
        })
}

impl Stream for HashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;

                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Err(ArrowError::External("".to_string(), Box::new(e))), // error receiving
                    Ok(result) => result,
                };

                Poll::Ready(Some(result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for HashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Given Vec<Vec<ArrayRef>>, concatenates the inners `Vec<ArrayRef>` into `ArrayRef`, returning `Vec<ArrayRef>`
/// This assumes that `arrays` is not empty.
#[allow(dead_code)]
fn concatenate(arrays: Vec<Vec<ArrayRef>>) -> ArrowResult<Vec<ArrayRef>> {
    (0..arrays[0].len())
        .map(|column| {
            let array_list = arrays
                .iter()
                .map(|a| a[column].as_ref())
                .collect::<Vec<_>>();
            Ok(concatenate::concatenate(&array_list)?.into())
        })
        .collect::<ArrowResult<Vec<_>>>()
}

/// Create a RecordBatch with all group keys and accumulator' states or values.
fn create_batch_from_map(
    mode: &AggregateMode,
    accumulators: &Accumulators,
    num_group_expr: usize,
    output_schema: &Schema,
) -> ArrowResult<RecordBatch> {
    if accumulators.group_states.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(output_schema.to_owned())));
    }
    let accs = &accumulators.group_states[0].accumulator_set;
    let mut acc_data_types: Vec<usize> = vec![];

    // Calculate number/shape of state arrays
    match mode {
        AggregateMode::Partial => {
            for acc in accs.iter() {
                let state = acc.state()?;
                acc_data_types.push(state.len());
            }
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            acc_data_types = vec![1; accs.len()];
        }
    }

    let mut columns = (0..num_group_expr)
        .map(|i| {
            ScalarValue::iter_to_array(
                accumulators
                    .group_states
                    .iter()
                    .map(|group_state| group_state.group_by_values[i].clone()),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // add state / evaluated arrays
    for (x, &state_len) in acc_data_types.iter().enumerate() {
        for y in 0..state_len {
            match mode {
                AggregateMode::Partial => {
                    let res = ScalarValue::iter_to_array(
                        accumulators.group_states.iter().map(|group_state| {
                            let x = group_state.accumulator_set[x].state().unwrap();
                            x[y].clone()
                        }),
                    )?;

                    columns.push(res);
                }
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    let res = ScalarValue::iter_to_array(
                        accumulators.group_states.iter().map(|group_state| {
                            group_state.accumulator_set[x].evaluate().unwrap()
                        }),
                    )?;
                    columns.push(res);
                }
            }
        }
    }

    // cast output if needed (e.g. for types like Dictionary where
    // the intermediate GroupByScalar type was not the same as the
    // output
    let columns = columns
        .iter()
        .zip(output_schema.fields().iter())
        .map(|(col, desired_field)| {
            cast::cast(
                col.as_ref(),
                desired_field.data_type(),
                DEFAULT_DATAFUSION_CAST_OPTIONS,
            )
            .map(|b| Arc::from(b))
        })
        .collect::<ArrowResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(output_schema.to_owned()), columns)
}

fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final) or states (mode = Partial)
fn finalize_aggregation(
    accumulators: &[AccumulatorItem],
    mode: &AggregateMode,
) -> Result<Vec<ArrayRef>> {
    match mode {
        AggregateMode::Partial => {
            // build the vector of states
            let a = accumulators
                .iter()
                .map(|accumulator| accumulator.state())
                .map(|value| {
                    value.map(|e| {
                        e.iter().map(|v| v.to_array()).collect::<Vec<ArrayRef>>()
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(a.iter().flatten().cloned().collect::<Vec<_>>())
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            // merge the state to the final value
            accumulators
                .iter()
                .map(|accumulator| accumulator.evaluate().map(|v| v.to_array()))
                .collect::<Result<Vec<ArrayRef>>>()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::{col, Avg};
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::{assert_batches_sorted_eq, physical_plan::common};
    use arrow::array::{Float64Array, UInt32Array};
    use arrow::datatypes::DataType;
    use futures::FutureExt;

    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    /// some mock data to aggregates
    fn some_data() -> (Arc<Schema>, Vec<RecordBatch>) {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // define data.
        (
            schema.clone(),
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from_slice(&[2, 3, 4, 4])),
                        Arc::new(Float64Array::from_slice(&[1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt32Array::from_slice(&[2, 3, 3, 4])),
                        Arc::new(Float64Array::from_slice(&[1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
        )
    }

    /// build the aggregates on the data from some_data() and check the results
    async fn check_aggregates(input: Arc<dyn ExecutionPlan>) -> Result<()> {
        let input_schema = input.schema();

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &input_schema)?, "a".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &input_schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let runtime = Arc::new(RuntimeEnv::default());

        let partial_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            input,
            input_schema.clone(),
        )?);

        let result =
            common::collect(partial_aggregate.execute(0, runtime.clone()).await?).await?;

        let expected = vec![
            "+---+---------------+-------------+",
            "| a | AVG(b)[count] | AVG(b)[sum] |",
            "+---+---------------+-------------+",
            "| 2 | 2             | 2           |",
            "| 3 | 3             | 7           |",
            "| 4 | 3             | 11          |",
            "+---+---------------+-------------+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_group: Vec<Arc<dyn PhysicalExpr>> = (0..groups.len())
            .map(|i| col(&groups[i].1, &input_schema))
            .collect::<Result<_>>()?;

        let merged_aggregate = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            final_group
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                .collect(),
            aggregates,
            merge,
            input_schema,
        )?);

        let result =
            common::collect(merged_aggregate.execute(0, runtime.clone()).await?).await?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let expected = vec![
            "+---+--------------------+",
            "| a | AVG(b)             |",
            "+---+--------------------+",
            "| 2 | 1                  |",
            "| 3 | 2.3333333333333335 |", // 3, (2 + 3 + 2) / 3
            "| 4 | 3.6666666666666665 |", // 4, (3 + 4 + 4) / 3
            "+---+--------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &result);

        let metrics = merged_aggregate.metrics().unwrap();
        let output_rows = metrics.output_rows().unwrap();
        assert_eq!(3, output_rows);

        Ok(())
    }

    /// Define a test source that can yield back to runtime before returning its first item ///

    #[derive(Debug)]
    struct TestYieldingExec {
        /// True if this exec should yield back to runtime the first time it is polled
        pub yield_first: bool,
    }

    #[async_trait]
    impl ExecutionPlan for TestYieldingExec {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            some_data().0
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn with_new_children(
            &self,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }

        async fn execute(
            &self,
            _partition: usize,
            _runtime: Arc<RuntimeEnv>,
        ) -> Result<SendableRecordBatchStream> {
            let stream = if self.yield_first {
                TestYieldingStream::New
            } else {
                TestYieldingStream::Yielded
            };
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            let (_, batches) = some_data();
            common::compute_record_batch_statistics(&[batches], &self.schema(), None)
        }
    }

    /// A stream using the demo data. If inited as new, it will first yield to runtime before returning records
    enum TestYieldingStream {
        New,
        Yielded,
        ReturnedBatch1,
        ReturnedBatch2,
    }

    impl Stream for TestYieldingStream {
        type Item = ArrowResult<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match &*self {
                TestYieldingStream::New => {
                    *(self.as_mut()) = TestYieldingStream::Yielded;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                TestYieldingStream::Yielded => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch1;
                    Poll::Ready(Some(Ok(some_data().1[0].clone())))
                }
                TestYieldingStream::ReturnedBatch1 => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch2;
                    Poll::Ready(Some(Ok(some_data().1[1].clone())))
                }
                TestYieldingStream::ReturnedBatch2 => Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for TestYieldingStream {
        fn schema(&self) -> SchemaRef {
            some_data().0
        }
    }

    //// Tests ////

    #[tokio::test]
    async fn aggregate_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_aggregates(input).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_aggregates(input).await
    }

    #[tokio::test]
    async fn test_drop_cancel_without_groups() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let groups = vec![];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("a", &schema)?,
            "AVG(a)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let hash_aggregate_exec = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            blocking_exec,
            schema,
        )?);

        let fut = crate::physical_plan::collect(hash_aggregate_exec, runtime);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel_with_groups() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "a".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let hash_aggregate_exec = Arc::new(HashAggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            blocking_exec,
            schema,
        )?);

        let fut = crate::physical_plan::collect(hash_aggregate_exec, runtime);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
