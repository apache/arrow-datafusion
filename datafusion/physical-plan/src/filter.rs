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

//! FilterExec evaluates a boolean predicate against all input batches to determine which rows to
//! include in its output batches.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{
    ColumnStatistics, DisplayAs, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    DisplayFormatType, ExecutionPlan,
};

use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::stats::Precision;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::intervals::utils::check_support;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{
    analyze, split_conjunction, AnalysisContext, ConstExpr, ExprBoundaries, PhysicalExpr,
};

use futures::stream::{Stream, StreamExt};
use log::trace;

/// FilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Selectivity for statistics. 0 = no rows, 100 all rows
    default_selectivity: u8,
    cache: PlanProperties,
}

impl FilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => {
                let default_selectivity = 20;
                let cache =
                    Self::compute_properties(&input, &predicate, default_selectivity)?;
                Ok(Self {
                    predicate,
                    input: input.clone(),
                    metrics: ExecutionPlanMetricsSet::new(),
                    default_selectivity,
                    cache,
                })
            }
            other => {
                plan_err!("Filter predicate must return boolean values, not {other:?}")
            }
        }
    }

    pub fn with_default_selectivity(
        mut self,
        default_selectivity: u8,
    ) -> Result<Self, DataFusionError> {
        if default_selectivity > 100 {
            return plan_err!("Default filter selectivity needs to be less than 100");
        }
        self.default_selectivity = default_selectivity;
        Ok(self)
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The default selectivity
    pub fn default_selectivity(&self) -> u8 {
        self.default_selectivity
    }

    /// Calculates `Statistics` for `FilterExec`, by applying selectivity (either default, or estimated) to input statistics.
    fn statistics_helper(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
    ) -> Result<Statistics> {
        let input_stats = input.statistics()?;
        let schema = input.schema();
        if !check_support(predicate, &schema) {
            let selectivity = default_selectivity as f64 / 100.0;
            let mut stats = input_stats.into_inexact();
            stats.num_rows = stats.num_rows.with_estimated_selectivity(selectivity);
            stats.total_byte_size = stats
                .total_byte_size
                .with_estimated_selectivity(selectivity);
            return Ok(stats);
        }

        let num_rows = input_stats.num_rows;
        let total_byte_size = input_stats.total_byte_size;
        let input_analysis_ctx = AnalysisContext::try_from_statistics(
            &input.schema(),
            &input_stats.column_statistics,
        )?;

        let analysis_ctx = analyze(predicate, input_analysis_ctx, &schema)?;

        // Estimate (inexact) selectivity of predicate
        let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
        let num_rows = num_rows.with_estimated_selectivity(selectivity);
        let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

        let column_statistics = collect_new_statistics(
            &input_stats.column_statistics,
            analysis_ctx.boundaries,
        );
        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    fn extend_constants(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
    ) -> Vec<ConstExpr> {
        let mut res_constants = Vec::new();
        let input_eqs = input.equivalence_properties();

        let conjunctions = split_conjunction(predicate);
        for conjunction in conjunctions {
            if let Some(binary) = conjunction.as_any().downcast_ref::<BinaryExpr>() {
                if binary.op() == &Operator::Eq {
                    // Filter evaluates to single value for all partitions
                    if input_eqs.is_expr_constant(binary.left()) {
                        res_constants.push(
                            ConstExpr::new(binary.right().clone())
                                .with_across_partitions(true),
                        )
                    } else if input_eqs.is_expr_constant(binary.right()) {
                        res_constants.push(
                            ConstExpr::new(binary.left().clone())
                                .with_across_partitions(true),
                        )
                    }
                }
            }
        }
        res_constants
    }
    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
    ) -> Result<PlanProperties> {
        // Combine the equal predicates with the input equivalence properties
        // to construct the equivalence properties:
        let stats = Self::statistics_helper(input, predicate, default_selectivity)?;
        let mut eq_properties = input.equivalence_properties().clone();
        let (equal_pairs, _) = collect_columns_from_predicate(predicate);
        for (lhs, rhs) in equal_pairs {
            eq_properties.add_equal_conditions(lhs, rhs)?
        }
        // Add the columns that have only one viable value (singleton) after
        // filtering to constants.
        let constants = collect_columns(predicate)
            .into_iter()
            .filter(|column| stats.column_statistics[column.index()].is_singleton())
            .map(|column| {
                let expr = Arc::new(column) as _;
                ConstExpr::new(expr).with_across_partitions(true)
            });
        // this is for statistics
        eq_properties = eq_properties.add_constants(constants);
        // this is for logical constant (for example: a = '1', then a could be marked as a constant)
        // to do: how to deal with multiple situation to represent = (for example c1 between 0 and 0)
        eq_properties =
            eq_properties.add_constants(Self::extend_constants(input, predicate));
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for FilterExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FilterExec: {}", self.predicate)
            }
        }
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &'static str {
        "FilterExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        FilterExec::try_new(self.predicate.clone(), children.swap_remove(0))
            .and_then(|e| {
                let selectivity = e.default_selectivity();
                e.with_default_selectivity(selectivity)
            })
            .map(|e| Arc::new(e) as _)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(FilterExecStream {
            schema: self.input.schema(),
            predicate: self.predicate.clone(),
            input: self.input.execute(partition, context)?,
            baseline_metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The output statistics of a filtering operation can be estimated if the
    /// predicate's selectivity value can be determined for the incoming data.
    fn statistics(&self) -> Result<Statistics> {
        Self::statistics_helper(&self.input, self.predicate(), self.default_selectivity)
    }
}

/// This function ensures that all bounds in the `ExprBoundaries` vector are
/// converted to closed bounds. If a lower/upper bound is initially open, it
/// is adjusted by using the next/previous value for its data type to convert
/// it into a closed bound.
fn collect_new_statistics(
    input_column_stats: &[ColumnStatistics],
    analysis_boundaries: Vec<ExprBoundaries>,
) -> Vec<ColumnStatistics> {
    analysis_boundaries
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                ExprBoundaries {
                    interval,
                    distinct_count,
                    ..
                },
            )| {
                let (lower, upper) = interval.into_bounds();
                let (min_value, max_value) = if lower.eq(&upper) {
                    (Precision::Exact(lower), Precision::Exact(upper))
                } else {
                    (Precision::Inexact(lower), Precision::Inexact(upper))
                };
                ColumnStatistics {
                    null_count: input_column_stats[idx].null_count.clone().to_inexact(),
                    max_value,
                    min_value,
                    distinct_count: distinct_count.to_inexact(),
                }
            },
        )
        .collect()
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct FilterExecStream {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

pub(crate) fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(batch, filter_array)?))
        })
}

impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    Some(Ok(batch)) => {
                        let timer = self.baseline_metrics.elapsed_compute().timer();
                        let filtered_batch = batch_filter(&batch, &self.predicate)?;
                        // skip entirely filtered batches
                        if filtered_batch.num_rows() == 0 {
                            continue;
                        }
                        timer.done();
                        poll = Poll::Ready(Some(Ok(filtered_batch)));
                        break;
                    }
                    _ => {
                        poll = Poll::Ready(value);
                        break;
                    }
                },
                Poll::Pending => {
                    poll = Poll::Pending;
                    break;
                }
            }
        }
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for FilterExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Return the equals Column-Pairs and Non-equals Column-Pairs
fn collect_columns_from_predicate(predicate: &Arc<dyn PhysicalExpr>) -> EqualAndNonEqual {
    let mut eq_predicate_columns = Vec::<PhysicalExprPairRef>::new();
    let mut ne_predicate_columns = Vec::<PhysicalExprPairRef>::new();

    let predicates = split_conjunction(predicate);
    predicates.into_iter().for_each(|p| {
        if let Some(binary) = p.as_any().downcast_ref::<BinaryExpr>() {
            match binary.op() {
                Operator::Eq => {
                    eq_predicate_columns.push((binary.left(), binary.right()))
                }
                Operator::NotEq => {
                    ne_predicate_columns.push((binary.left(), binary.right()))
                }
                _ => {}
            }
        }
    });

    (eq_predicate_columns, ne_predicate_columns)
}

/// Pair of `Arc<dyn PhysicalExpr>`s
pub type PhysicalExprPairRef<'a> = (&'a Arc<dyn PhysicalExpr>, &'a Arc<dyn PhysicalExpr>);

/// The equals Column-Pairs and Non-equals Column-Pairs in the Predicates
pub type EqualAndNonEqual<'a> =
    (Vec<PhysicalExprPairRef<'a>>, Vec<PhysicalExprPairRef<'a>>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty::EmptyExec;
    use crate::expressions::*;
    use crate::test;
    use crate::test::exec::StatisticsExec;

    use arrow::datatypes::{Field, Schema};
    use arrow_schema::{UnionFields, UnionMode};
    use datafusion_common::ScalarValue;

    #[tokio::test]
    async fn collect_columns_predicates() -> Result<()> {
        let schema = test::aggr_test_schema();
        let predicate: Arc<dyn PhysicalExpr> = binary(
            binary(
                binary(col("c2", &schema)?, Operator::GtEq, lit(1u32), &schema)?,
                Operator::And,
                binary(col("c2", &schema)?, Operator::Eq, lit(4u32), &schema)?,
                &schema,
            )?,
            Operator::And,
            binary(
                binary(
                    col("c2", &schema)?,
                    Operator::Eq,
                    col("c9", &schema)?,
                    &schema,
                )?,
                Operator::And,
                binary(
                    col("c1", &schema)?,
                    Operator::NotEq,
                    col("c13", &schema)?,
                    &schema,
                )?,
                &schema,
            )?,
            &schema,
        )?;

        let (equal_pairs, ne_pairs) = collect_columns_from_predicate(&predicate);
        assert_eq!(2, equal_pairs.len());
        assert!(equal_pairs[0].0.eq(&col("c2", &schema)?));
        assert!(equal_pairs[0].1.eq(&lit(4u32)));

        assert!(equal_pairs[1].0.eq(&col("c2", &schema)?));
        assert!(equal_pairs[1].1.eq(&col("c9", &schema)?));

        assert_eq!(1, ne_pairs.len());
        assert!(ne_pairs[0].0.eq(&col("c1", &schema)?));
        assert!(ne_pairs[0].1.eq(&col("c13", &schema)?));

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_basic_expr() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let bytes_per_row = 4;
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(100 * bytes_per_row),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }],
            },
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.statistics()?;
        assert_eq!(statistics.num_rows, Precision::Inexact(25));
        assert_eq!(
            statistics.total_byte_size,
            Precision::Inexact(25 * bytes_per_row)
        );
        assert_eq!(
            statistics.column_statistics,
            vec![ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(25))),
                ..Default::default()
            }]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_nested() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                column_statistics: vec![ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }],
                total_byte_size: Precision::Absent,
            },
            schema.clone(),
        ));

        // WHERE a <= 25
        let sub_filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?,
            input,
        )?);

        // Nested filters (two separate physical plans, instead of AND chain in the expr)
        // WHERE a >= 10
        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            sub_filter,
        )?);

        let statistics = filter.statistics()?;
        assert_eq!(statistics.num_rows, Precision::Inexact(16));
        assert_eq!(
            statistics.column_statistics,
            vec![ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(25))),
                ..Default::default()
            }]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_nested_multiple() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=50
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(50))),
                        ..Default::default()
                    },
                ],
                total_byte_size: Precision::Absent,
            },
            schema.clone(),
        ));

        // WHERE a <= 25
        let a_lte_25: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?,
            input,
        )?);

        // WHERE b > 45
        let b_gt_5: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("b", &schema)?, Operator::Gt, lit(45i32), &schema)?,
            a_lte_25,
        )?);

        // WHERE a >= 10
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            b_gt_5,
        )?);
        let statistics = filter.statistics()?;
        // On a uniform distribution, only fifteen rows will satisfy the
        // filter that 'a' proposed (a >= 10 AND a <= 25) (15/100) and only
        // 5 rows will satisfy the filter that 'b' proposed (b > 45) (5/50).
        //
        // Which would result with a selectivity of  '15/100 * 5/50' or 0.015
        // and that means about %1.5 of the all rows (rounded up to 2 rows).
        assert_eq!(statistics.num_rows, Precision::Inexact(2));
        assert_eq!(
            statistics.column_statistics,
            vec![
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(25))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(46))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(50))),
                    ..Default::default()
                }
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_when_input_stats_missing() -> Result<()> {
        // Table:
        //      a: min=???, max=??? (missing)
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.statistics()?;
        assert_eq!(statistics.num_rows, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_multiple_columns() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=3
        //      c: min=1000.0  max=1100.0
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Float32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Float32(Some(1000.0))),
                        max_value: Precision::Inexact(ScalarValue::Float32(Some(1100.0))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a<=53 AND (b=3 AND (c<=1075.0 AND a>b))
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(53)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("b", 1)),
                    Operator::Eq,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::LtEq,
                        Arc::new(Literal::new(ScalarValue::Float32(Some(1075.0)))),
                    )),
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a", 0)),
                        Operator::Gt,
                        Arc::new(Column::new("b", 1)),
                    )),
                )),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.statistics()?;
        // 0.5 (from a) * 0.333333... (from b) * 0.798387... (from c) ≈ 0.1330...
        // num_rows after ceil => 133.0... => 134
        // total_byte_size after ceil => 532.0... => 533
        assert_eq!(statistics.num_rows, Precision::Inexact(134));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(533));
        let exp_col_stats = vec![
            ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(4))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(53))),
                ..Default::default()
            },
            ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                ..Default::default()
            },
            ColumnStatistics {
                min_value: Precision::Inexact(ScalarValue::Float32(Some(1000.0))),
                max_value: Precision::Inexact(ScalarValue::Float32(Some(1075.0))),
                ..Default::default()
            },
        ];
        let _ = exp_col_stats
            .into_iter()
            .zip(statistics.column_statistics)
            .map(|(expected, actual)| {
                if let Some(val) = actual.min_value.get_value() {
                    if val.data_type().is_floating() {
                        // Windows rounds arithmetic operation results differently for floating point numbers.
                        // Therefore, we check if the actual values are in an epsilon range.
                        let actual_min = actual.min_value.get_value().unwrap();
                        let actual_max = actual.max_value.get_value().unwrap();
                        let expected_min = expected.min_value.get_value().unwrap();
                        let expected_max = expected.max_value.get_value().unwrap();
                        let eps = ScalarValue::Float32(Some(1e-6));

                        assert!(actual_min.sub(expected_min).unwrap() < eps);
                        assert!(actual_min.sub(expected_min).unwrap() < eps);

                        assert!(actual_max.sub(expected_max).unwrap() < eps);
                        assert!(actual_max.sub(expected_max).unwrap() < eps);
                    } else {
                        assert_eq!(actual, expected);
                    }
                } else {
                    assert_eq!(actual, expected);
                }
            });

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_full_selective() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=3
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a<200 AND 1<=b
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(200)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::LtEq,
                Arc::new(Column::new("b", 1)),
            )),
        ));
        // Since filter predicate passes all entries, statistics after filter shouldn't change.
        let expected = input.statistics()?.column_statistics;
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.statistics()?;

        assert_eq!(statistics.num_rows, Precision::Inexact(1000));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(4000));
        assert_eq!(statistics.column_statistics, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_zero_selective() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=3
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a>200 AND 1<=b
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(200)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::LtEq,
                Arc::new(Column::new("b", 1)),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.statistics()?;

        assert_eq!(statistics.num_rows, Precision::Inexact(0));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(0));
        assert_eq!(
            statistics.column_statistics,
            vec![
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(3))),
                    ..Default::default()
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_more_inputs() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                        max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                ],
            },
            schema,
        ));
        // WHERE a<50
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(50)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let statistics = filter.statistics()?;

        assert_eq!(statistics.num_rows, Precision::Inexact(490));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(1960));
        assert_eq!(
            statistics.column_statistics,
            vec![
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(49))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_input_statistics() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ));
        // WHERE a <= 10 AND 0 <= a - 5
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
                Operator::LtEq,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Minus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
                )),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let filter_statistics = filter.statistics()?;

        let expected_filter_statistics = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                min_value: Precision::Inexact(ScalarValue::Int32(Some(5))),
                max_value: Precision::Inexact(ScalarValue::Int32(Some(10))),
                distinct_count: Precision::Absent,
            }],
        };

        assert_eq!(filter_statistics, expected_filter_statistics);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_with_constant_column() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ));
        // WHERE a = 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);
        let filter_statistics = filter.statistics()?;
        // First column is "a", and it is a column with only one value after the filter.
        assert!(filter_statistics.column_statistics[0].is_singleton());

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_filter_selectivity() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics::new_unknown(&schema),
            schema,
        ));
        // WHERE a = 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));
        let filter = FilterExec::try_new(predicate, input)?;
        assert!(filter.with_default_selectivity(120).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_custom_filter_selectivity() -> Result<()> {
        // Need a decimal to trigger inexact selectivity
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(2, 3), false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                total_byte_size: Precision::Inexact(4000),
                column_statistics: vec![ColumnStatistics {
                    ..Default::default()
                }],
            },
            schema,
        ));
        // WHERE a = 10
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Decimal128(Some(10), 10, 10))),
        ));
        let filter = FilterExec::try_new(predicate, input)?;
        let statistics = filter.statistics()?;
        assert_eq!(statistics.num_rows, Precision::Inexact(200));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(800));
        let filter = filter.with_default_selectivity(40)?;
        let statistics = filter.statistics()?;
        assert_eq!(statistics.num_rows, Precision::Inexact(400));
        assert_eq!(statistics.total_byte_size, Precision::Inexact(1600));
        Ok(())
    }

    #[test]
    fn test_equivalence_properties_union_type() -> Result<()> {
        let union_type = DataType::Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("f1", DataType::Int32, true),
                    Field::new("f2", DataType::Utf8, true),
                ],
            ),
            UnionMode::Sparse,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", union_type, true),
        ]));

        let exec = FilterExec::try_new(
            binary(
                binary(col("c1", &schema)?, Operator::GtEq, lit(1i32), &schema)?,
                Operator::And,
                binary(col("c1", &schema)?, Operator::LtEq, lit(4i32), &schema)?,
                &schema,
            )?,
            Arc::new(EmptyExec::new(schema.clone())),
        )?;

        exec.statistics().unwrap();

        Ok(())
    }
}
