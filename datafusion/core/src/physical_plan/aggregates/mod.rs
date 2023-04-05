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

//! Aggregates functionalities

use crate::execution::context::TaskContext;
use crate::physical_plan::aggregates::no_grouping::AggregateStream;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    expressions, AggregateExpr, PhysicalExpr, PhysicalSortExpr,
};
use std::any::Any;
use std::collections::HashMap;

use std::sync::Arc;

mod no_grouping;
mod row_hash;

use crate::physical_plan::aggregates::row_hash::GroupedHashAggregateStream;
use crate::physical_plan::EquivalenceProperties;
pub use datafusion_expr::AggregateFunction;
use datafusion_physical_expr::aggregate::row_accumulator::RowAccumulator;
use datafusion_physical_expr::equivalence::project_equivalence_properties;
pub use datafusion_physical_expr::expressions::create_aggregate_expr;
use datafusion_physical_expr::normalize_out_expr_with_alias_schema;

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

/// Represents `GROUP BY` clause in the plan (including the more general GROUPING SET)
/// In the case of a simple `GROUP BY a, b` clause, this will contain the expression [a, b]
/// and a single group [false, false].
/// In the case of `GROUP BY GROUPING SET/CUBE/ROLLUP` the planner will expand the expression
/// into multiple groups, using null expressions to align each group.
/// For example, with a group by clause `GROUP BY GROUPING SET ((a,b),(a),(b))` the planner should
/// create a `PhysicalGroupBy` like
/// PhysicalGroupBy {
///     expr: [(col(a), a), (col(b), b)],
///     null_expr: [(NULL, a), (NULL, b)],
///     groups: [
///         [false, false], // (a,b)
///         [false, true],  // (a) <=> (a, NULL)
///         [true, false]   // (b) <=> (NULL, b)
///     ]
/// }
#[derive(Clone, Debug, Default)]
pub struct PhysicalGroupBy {
    /// Distinct (Physical Expr, Alias) in the grouping set
    grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Hidden grouping set expr in the grouping set
    hidden_grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Distinct result expr for the grouping set, used to generate output schema
    result_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Corresponding NULL expressions for expr
    null_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Null mask for each group in this grouping set. Each group is
    /// composed of either one of the group expressions in expr or a null
    /// expression in null_expr. If `groups[i][j]` is true, then the the
    /// j-th expression in the i-th group is NULL, otherwise it is `expr[j]`.
    groups: Vec<Vec<bool>>,
}

impl PhysicalGroupBy {
    /// Create a new `PhysicalGroupBy`
    pub fn new(
        grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        hidden_grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        result_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        null_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        groups: Vec<Vec<bool>>,
    ) -> Self {
        Self {
            grouping_set_expr,
            hidden_grouping_set_expr,
            result_expr,
            null_expr,
            groups,
        }
    }

    /// Create a GROUPING SET with only a single group. This is the "standard"
    /// case when building a plan from an expression such as `GROUP BY a,b,c`
    pub fn new_single(expr: Vec<(Arc<dyn PhysicalExpr>, String)>) -> Self {
        let num_exprs = expr.len();
        Self {
            grouping_set_expr: expr.clone(),
            hidden_grouping_set_expr: vec![],
            result_expr: expr,
            null_expr: vec![],
            groups: vec![vec![false; num_exprs]],
        }
    }

    /// Returns true if this GROUP BY contains NULL expressions
    pub fn contains_null(&self) -> bool {
        self.groups.iter().flatten().any(|is_null| *is_null)
    }

    /// Returns the group expressions
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.grouping_set_expr
    }

    /// Returns the group result expressions
    pub fn result_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.result_expr
    }

    /// Returns the null expressions
    pub fn null_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.null_expr
    }

    /// Returns the hidden grouping set expressions
    pub fn hidden_grouping_set_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.hidden_grouping_set_expr
    }

    /// Returns the group null masks
    pub fn groups(&self) -> &[Vec<bool>] {
        &self.groups
    }

    /// Returns true if this `PhysicalGroupBy` has no group expressions
    pub fn is_empty(&self) -> bool {
        self.grouping_set_expr.is_empty()
    }
}

enum StreamType {
    AggregateStream(AggregateStream),
    GroupedHashAggregateStream(GroupedHashAggregateStream),
}

impl From<StreamType> for SendableRecordBatchStream {
    fn from(stream: StreamType) -> Self {
        match stream {
            StreamType::AggregateStream(stream) => Box::pin(stream),
            StreamType::GroupedHashAggregateStream(stream) => Box::pin(stream),
        }
    }
}

/// Hash aggregate execution plan
#[derive(Debug)]
pub struct AggregateExec {
    /// Aggregation mode (full, partial)
    pub(crate) mode: AggregateMode,
    /// Group by expressions
    pub(crate) group_by: PhysicalGroupBy,
    /// Aggregate expressions
    pub(crate) aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied
    schema: SchemaRef,
    /// Input schema before any aggregation is applied. For partial aggregate this will be the
    /// same as input.schema() but for the final aggregate it will be the same as the input
    /// to the partial aggregate
    pub(crate) input_schema: SchemaRef,
    /// The alias map used to normalize out expressions like Partitioning and PhysicalSortExpr
    /// The key is the column from the input schema and the values are the columns from the output schema
    alias_map: HashMap<Column, Vec<Column>>,
    /// Execution Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl AggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(
            &input.schema(),
            group_by.result_expr(),
            &aggr_expr,
            group_by.contains_null(),
            mode,
        )?;

        let schema = Arc::new(schema);

        let mut alias_map: HashMap<Column, Vec<Column>> = HashMap::new();
        for (expression, name) in group_by.result_expr().iter() {
            if let Some(column) = expression.as_any().downcast_ref::<Column>() {
                let new_col_idx = schema.index_of(name)?;
                // When the column name is the same, but index does not equal, treat it as Alias
                if (column.name() != name) || (column.index() != new_col_idx) {
                    let entry = alias_map.entry(column.clone()).or_insert_with(Vec::new);
                    entry.push(Column::new(name, new_col_idx));
                }
            };
        }

        Ok(AggregateExec {
            mode,
            group_by,
            aggr_expr,
            input,
            schema,
            input_schema,
            alias_map,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Aggregation mode (full, partial)
    pub fn mode(&self) -> &AggregateMode {
        &self.mode
    }

    /// Grouping expressions
    pub fn group_expr(&self) -> &PhysicalGroupBy {
        &self.group_by
    }

    /// Grouping expressions as they occur in the output schema
    pub fn output_group_expr(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        // Update column indices. Since the group by columns come first in the output schema, their
        // indices are simply 0..self.group_expr(len).
        self.group_by
            .result_expr()
            .iter()
            .enumerate()
            .map(|(index, (_col, name))| {
                Arc::new(expressions::Column::new(name, index)) as Arc<dyn PhysicalExpr>
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

    fn execute_typed(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<StreamType> {
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        if self.group_by.result_expr().is_empty() {
            Ok(StreamType::AggregateStream(AggregateStream::new(
                self.mode,
                self.schema.clone(),
                self.aggr_expr.clone(),
                input,
                baseline_metrics,
                context,
                partition,
            )?))
        } else {
            Ok(StreamType::GroupedHashAggregateStream(
                GroupedHashAggregateStream::new(
                    self.mode,
                    self.schema.clone(),
                    self.group_by.clone(),
                    self.aggr_expr.clone(),
                    input,
                    baseline_metrics,
                    batch_size,
                    context,
                    partition,
                )?,
            ))
        }
    }
}

impl ExecutionPlan for AggregateExec {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        match &self.mode {
            AggregateMode::Partial => {
                // Partial Aggregation will not change the output partitioning but need to respect the Alias
                let input_partition = self.input.output_partitioning();
                match input_partition {
                    Partitioning::Hash(exprs, part) => {
                        let normalized_exprs = exprs
                            .into_iter()
                            .map(|expr| {
                                normalize_out_expr_with_alias_schema(
                                    expr,
                                    &self.alias_map,
                                    &self.schema,
                                )
                            })
                            .collect::<Vec<_>>();
                        Partitioning::Hash(normalized_exprs, part)
                    }
                    _ => input_partition,
                }
            }
            // Final Aggregation's output partitioning is the same as its real input
            _ => self.input.output_partitioning(),
        }
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but it its input(s) are
    /// infinite, returns an error to indicate this.    
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        if children[0] {
            Err(DataFusionError::Plan(
                "Aggregate Error: `GROUP BY` clause (including the more general GROUPING SET) is not supported for unbounded inputs.".to_string(),
            ))
        } else {
            Ok(false)
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match &self.mode {
            AggregateMode::Partial => vec![Distribution::UnspecifiedDistribution],
            AggregateMode::FinalPartitioned => {
                vec![Distribution::HashPartitioned(self.output_group_expr())]
            }
            AggregateMode::Final => vec![Distribution::SinglePartition],
        }
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let mut new_properties = EquivalenceProperties::new(self.schema());
        project_equivalence_properties(
            self.input.equivalence_properties(),
            &self.alias_map,
            &mut new_properties,
        );
        new_properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AggregateExec::try_new(
            self.mode,
            self.group_by.clone(),
            self.aggr_expr.clone(),
            children[0].clone(),
            self.input_schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.execute_typed(partition, context)
            .map(|stream| stream.into())
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
                write!(f, "AggregateExec: mode={:?}", self.mode)?;
                let g: Vec<String> = if self.group_by.groups.len() == 1 {
                    self.group_by
                        .grouping_set_expr
                        .iter()
                        .map(|(e, alias)| {
                            let e = e.to_string();
                            if &e != alias {
                                format!("{e} as {alias}")
                            } else {
                                e
                            }
                        })
                        .collect()
                } else {
                    self.group_by
                        .groups
                        .iter()
                        .map(|group| {
                            let terms = group
                                .iter()
                                .enumerate()
                                .map(|(idx, is_null)| {
                                    if *is_null {
                                        let (e, alias) = &self.group_by.null_expr[idx];
                                        let e = e.to_string();
                                        if &e != alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    } else {
                                        let (e, alias) =
                                            &self.group_by.grouping_set_expr[idx];
                                        let e = e.to_string();
                                        if &e != alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    }
                                })
                                .collect::<Vec<String>>()
                                .join(", ");
                            format!("({terms})")
                        })
                        .collect()
                };

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
                if self.group_by.result_expr().is_empty() =>
            {
                Statistics {
                    num_rows: Some(1),
                    is_exact: true,
                    ..Default::default()
                }
            }
            _ => Statistics {
                // the output row count is surely not larger than its input row count
                num_rows: self.input.statistics().num_rows,
                is_exact: false,
                ..Default::default()
            },
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    contains_null_expr: bool,
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            // In cases where we have multiple grouping sets, we will use NULL expressions in
            // order to align the grouping sets. So the field must be nullable even if the underlying
            // schema field is not.
            contains_null_expr || expr.nullable(input_schema)?,
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

fn group_schema(schema: &Schema, group_count: usize) -> SchemaRef {
    let group_fields = schema.fields()[0..group_count].to_vec();
    Arc::new(Schema::new(group_fields))
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

pub(crate) type AccumulatorItem = Box<dyn Accumulator>;
pub(crate) type RowAccumulatorItem = Box<dyn RowAccumulator>;

fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

fn create_row_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<RowAccumulatorItem>> {
    let mut state_index = 0;
    aggr_expr
        .iter()
        .map(|expr| {
            let result = expr.create_row_accumulator(state_index);
            state_index += expr.state_fields().unwrap().len();
            result
        })
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

fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    let exprs_value: Vec<ArrayRef> = group_by
        .grouping_set_expr
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            Ok(value.into_array(batch.num_rows()))
        })
        .collect::<Result<Vec<_>>>()?;

    let null_exprs_value: Vec<ArrayRef> = group_by
        .null_expr
        .iter()
        .map(|(expr, _)| {
            let value = expr.evaluate(batch)?;
            Ok(value.into_array(batch.num_rows()))
        })
        .collect::<Result<Vec<_>>>()?;

    if !group_by.hidden_grouping_set_expr().is_empty() {
        let hidden_exprs_value: Vec<ArrayRef> = group_by
            .hidden_grouping_set_expr
            .iter()
            .map(|(expr, _)| {
                let value = expr.evaluate(batch)?;
                Ok(value.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        let chunk_size = hidden_exprs_value.len() / group_by.groups.len();
        let hidden_expr_value_chunks =
            hidden_exprs_value.chunks(chunk_size).collect::<Vec<_>>();

        Ok(group_by
            .groups
            .iter()
            .enumerate()
            .map(|(groud_id, group)| {
                let mut group_data = group
                    .iter()
                    .enumerate()
                    .map(|(idx, is_null)| {
                        if *is_null {
                            null_exprs_value[idx].clone()
                        } else {
                            exprs_value[idx].clone()
                        }
                    })
                    .collect::<Vec<_>>();
                for data in hidden_expr_value_chunks[groud_id] {
                    group_data.push(data.clone());
                }
                group_data
            })
            .collect())
    } else {
        Ok(group_by
            .groups
            .iter()
            .map(|group| {
                group
                    .iter()
                    .enumerate()
                    .map(|(idx, is_null)| {
                        if *is_null {
                            null_exprs_value[idx].clone()
                        } else {
                            exprs_value[idx].clone()
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::context::{SessionConfig, TaskContext};
    use crate::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use crate::from_slice::FromSlice;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::{col, Avg};
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::{assert_batches_sorted_eq, physical_plan::common};
    use arrow::array::{Float64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use datafusion_physical_expr::expressions::{lit, ApproxDistinct, Count, Median};
    use datafusion_physical_expr::{AggregateExpr, PhysicalExpr, PhysicalSortExpr};
    use futures::{FutureExt, Stream};
    use std::any::Any;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use super::StreamType;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::{
        ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
        Statistics,
    };
    use crate::prelude::SessionContext;

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
                        Arc::new(UInt32Array::from_slice([2, 3, 4, 4])),
                        Arc::new(Float64Array::from_slice([1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt32Array::from_slice([2, 3, 3, 4])),
                        Arc::new(Float64Array::from_slice([1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
        )
    }

    async fn check_grouping_sets(input: Arc<dyn ExecutionPlan>) -> Result<()> {
        let input_schema = input.schema();

        let grouping_set = PhysicalGroupBy {
            grouping_set_expr: vec![
                (col("a", &input_schema)?, "a".to_string()),
                (col("b", &input_schema)?, "b".to_string()),
            ],
            hidden_grouping_set_expr: vec![],
            result_expr: vec![
                (col("a", &input_schema)?, "a".to_string()),
                (col("b", &input_schema)?, "b".to_string()),
            ],
            null_expr: vec![
                (lit(ScalarValue::UInt32(None)), "a".to_string()),
                (lit(ScalarValue::Float64(None)), "b".to_string()),
            ],
            groups: vec![
                vec![false, true],  // (a, NULL)
                vec![true, false],  // (NULL, b)
                vec![false, false], // (a,b)
            ],
        };

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        ))];

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            grouping_set.clone(),
            aggregates.clone(),
            input,
            input_schema.clone(),
        )?);

        let result =
            common::collect(partial_aggregate.execute(0, task_ctx.clone())?).await?;

        let expected = vec![
            "+---+-----+-----------------+",
            "| a | b   | COUNT(1)[count] |",
            "+---+-----+-----------------+",
            "|   | 1.0 | 2               |",
            "|   | 2.0 | 2               |",
            "|   | 3.0 | 2               |",
            "|   | 4.0 | 2               |",
            "| 2 |     | 2               |",
            "| 2 | 1.0 | 2               |",
            "| 3 |     | 3               |",
            "| 3 | 2.0 | 2               |",
            "| 3 | 3.0 | 1               |",
            "| 4 |     | 3               |",
            "| 4 | 3.0 | 1               |",
            "| 4 | 4.0 | 2               |",
            "+---+-----+-----------------+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let groups = partial_aggregate.group_expr().expr().to_vec();

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_group: Vec<(Arc<dyn PhysicalExpr>, String)> = groups
            .iter()
            .map(|(_expr, name)| Ok((col(name, &input_schema)?, name.clone())))
            .collect::<Result<_>>()?;

        let final_grouping_set = PhysicalGroupBy::new_single(final_group);

        let merged_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            final_grouping_set,
            aggregates,
            merge,
            input_schema,
        )?);

        let result =
            common::collect(merged_aggregate.execute(0, task_ctx.clone())?).await?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 12);

        let expected = vec![
            "+---+-----+----------+",
            "| a | b   | COUNT(1) |",
            "+---+-----+----------+",
            "|   | 1.0 | 2        |",
            "|   | 2.0 | 2        |",
            "|   | 3.0 | 2        |",
            "|   | 4.0 | 2        |",
            "| 2 |     | 2        |",
            "| 2 | 1.0 | 2        |",
            "| 3 |     | 3        |",
            "| 3 | 2.0 | 2        |",
            "| 3 | 3.0 | 1        |",
            "| 4 |     | 3        |",
            "| 4 | 3.0 | 1        |",
            "| 4 | 4.0 | 2        |",
            "+---+-----+----------+",
        ];

        assert_batches_sorted_eq!(&expected, &result);

        let metrics = merged_aggregate.metrics().unwrap();
        let output_rows = metrics.output_rows().unwrap();
        assert_eq!(12, output_rows);

        Ok(())
    }

    /// build the aggregates on the data from some_data() and check the results
    async fn check_aggregates(input: Arc<dyn ExecutionPlan>) -> Result<()> {
        let input_schema = input.schema();

        let grouping_set = PhysicalGroupBy {
            grouping_set_expr: vec![(col("a", &input_schema)?, "a".to_string())],
            hidden_grouping_set_expr: vec![],
            null_expr: vec![],
            groups: vec![vec![false]],
            result_expr: vec![(col("a", &input_schema)?, "a".to_string())],
        };

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &input_schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            grouping_set.clone(),
            aggregates.clone(),
            input,
            input_schema.clone(),
        )?);

        let result =
            common::collect(partial_aggregate.execute(0, task_ctx.clone())?).await?;

        let expected = vec![
            "+---+---------------+-------------+",
            "| a | AVG(b)[count] | AVG(b)[sum] |",
            "+---+---------------+-------------+",
            "| 2 | 2             | 2.0         |",
            "| 3 | 3             | 7.0         |",
            "| 4 | 3             | 11.0        |",
            "+---+---------------+-------------+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_group: Vec<(Arc<dyn PhysicalExpr>, String)> = grouping_set
            .result_expr()
            .iter()
            .map(|(_expr, name)| Ok((col(name, &input_schema)?, name.clone())))
            .collect::<Result<_>>()?;

        let final_grouping_set = PhysicalGroupBy::new_single(final_group);

        let merged_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            final_grouping_set,
            aggregates,
            merge,
            input_schema,
        )?);

        let result =
            common::collect(merged_aggregate.execute(0, task_ctx.clone())?).await?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let expected = vec![
            "+---+--------------------+",
            "| a | AVG(b)             |",
            "+---+--------------------+",
            "| 2 | 1.0                |",
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

    impl ExecutionPlan for TestYieldingExec {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            some_data().0
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {self:?}"
            )))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
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
        type Item = Result<RecordBatch>;

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
    async fn aggregate_grouping_sets_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_grouping_sets(input).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_aggregates(input).await
    }

    #[tokio::test]
    async fn aggregate_grouping_sets_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_grouping_sets(input).await
    }

    #[tokio::test]
    async fn test_oom() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });
        let input_schema = input.schema();

        let session_ctx = SessionContext::with_config_rt(
            SessionConfig::default(),
            Arc::new(
                RuntimeEnv::new(RuntimeConfig::default().with_memory_limit(1, 1.0))
                    .unwrap(),
            ),
        );
        let task_ctx = session_ctx.task_ctx();

        let groups_none = PhysicalGroupBy::default();
        let groups_some = PhysicalGroupBy {
            grouping_set_expr: vec![(col("a", &input_schema)?, "a".to_string())],
            hidden_grouping_set_expr: vec![],
            null_expr: vec![],
            groups: vec![vec![false]],
            result_expr: vec![(col("a", &input_schema)?, "a".to_string())],
        };

        // something that allocates within the aggregator
        let aggregates_v0: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Median::new(
            col("a", &input_schema)?,
            "MEDIAN(a)".to_string(),
            DataType::UInt32,
        ))];

        // use slow-path in `hash.rs`
        let aggregates_v1: Vec<Arc<dyn AggregateExpr>> =
            vec![Arc::new(ApproxDistinct::new(
                col("a", &input_schema)?,
                "APPROX_DISTINCT(a)".to_string(),
                DataType::UInt32,
            ))];

        // use fast-path in `row_hash.rs`.
        let aggregates_v2: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &input_schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        for (version, groups, aggregates) in [
            (0, groups_none, aggregates_v0),
            (1, groups_some.clone(), aggregates_v1),
            (2, groups_some, aggregates_v2),
        ] {
            let partial_aggregate = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                groups,
                aggregates,
                input.clone(),
                input_schema.clone(),
            )?);

            let stream = partial_aggregate.execute_typed(0, task_ctx.clone())?;

            // ensure that we really got the version we wanted
            match version {
                0 => {
                    assert!(matches!(stream, StreamType::AggregateStream(_)));
                }
                1 => {
                    assert!(matches!(stream, StreamType::GroupedHashAggregateStream(_)));
                }
                2 => {
                    assert!(matches!(stream, StreamType::GroupedHashAggregateStream(_)));
                }
                _ => panic!("Unknown version: {version}"),
            }

            let stream: SendableRecordBatchStream = stream.into();
            let err = common::collect(stream).await.unwrap_err();

            // error root cause traversal is a bit complicated, see #4172.
            let err = err.find_root();
            assert!(
                matches!(err, DataFusionError::ResourcesExhausted(_)),
                "Wrong error type: {err}",
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel_without_groups() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let groups = PhysicalGroupBy::default();

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("a", &schema)?,
            "AVG(a)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            blocking_exec,
            schema,
        )?);

        let fut = crate::physical_plan::collect(aggregate_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel_with_groups() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let groups =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups,
            aggregates.clone(),
            blocking_exec,
            schema,
        )?);

        let fut = crate::physical_plan::collect(aggregate_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
