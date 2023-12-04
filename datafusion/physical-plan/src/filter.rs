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

use super::expressions::PhysicalSortExpr;
use super::{
    ColumnStatistics, DisplayAs, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    Column, DisplayFormatType, ExecutionPlan, Partitioning,
};

use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::stats::Precision;
use datafusion_common::{plan_err, project_schema, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::intervals::utils::check_support;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{
    analyze, split_conjunction, AnalysisContext, EquivalenceProperties, ExprBoundaries,
    PhysicalExpr,
};

use futures::stream::{Stream, StreamExt};
use log::trace;

/// FilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl FilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        projection: Option<Vec<usize>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&input.schema(), projection.as_ref())?;
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => Ok(Self {
                predicate,
                projection,
                projected_schema,
                input: input.clone(),
                metrics: ExecutionPlanMetricsSet::new(),
            }),
            other => {
                plan_err!("Filter predicate must return boolean values, not {other:?}")
            }
        }
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
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
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let stats = self.statistics().unwrap();
        // Combine the equal predicates with the input equivalence properties
        let mut result = self.input.equivalence_properties().project(
            &ProjectionMapping::try_new(&[], &self.input().schema()).unwrap(),
            self.schema(),
        );

        let (equal_pairs, _) =
            collect_columns_from_predicate(&self.predicate, self.schema());
        for (lhs, rhs) in equal_pairs {
            let lhs_expr = Arc::new(lhs.clone()) as _;
            let rhs_expr = Arc::new(rhs.clone()) as _;
            result.add_equal_conditions(&lhs_expr, &rhs_expr)
        }
        // Add the columns that have only one value (singleton) after filtering to constants.
        let constants = collect_columns(self.predicate())
            .into_iter()
            .filter(|x| self.schema().column_with_name(x.name()).is_some())
            .filter(|column| {
                stats.column_statistics[self.schema().index_of(column.name()).unwrap()]
                    .is_singleton()
            })
            .map(|column| Arc::new(column) as _);
        result.add_constants(constants)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        FilterExec::try_new(
            self.predicate.clone(),
            self.projection.clone(),
            children.swap_remove(0),
        )
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
            schema: self.schema(),
            predicate: self.predicate.clone(),
            projection: self.projection.clone(),
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
        let predicate = self.predicate();

        let input_stats = self.input.statistics()?.clone();
        let schema = self.input.schema();
        let projected_stats = self
            .projection
            .as_ref()
            .map(|x| input_stats.clone().project(&x, &self.input().schema()))
            .unwrap_or(input_stats);
        if !check_support(predicate, &schema) {
            // assume filter selects 20% of rows if we cannot do anything smarter
            // tracking issue for making this configurable:
            // https://github.com/apache/arrow-datafusion/issues/8133
            let selectivity = 0.2_f64;
            let mut stats = projected_stats.into_inexact();
            stats.num_rows = stats.num_rows.with_estimated_selectivity(selectivity);
            stats.total_byte_size = stats
                .total_byte_size
                .with_estimated_selectivity(selectivity);
            return Ok(stats);
        }

        let num_rows = projected_stats.num_rows;
        let total_byte_size = projected_stats.total_byte_size;
        let input_analysis_ctx = AnalysisContext::try_from_statistics(
            &self.input.schema(),
            &projected_stats.column_statistics,
        )?;

        let analysis_ctx = analyze(predicate, input_analysis_ctx, &self.schema())?;

        // Estimate (inexact) selectivity of predicate
        let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
        let num_rows = num_rows.with_estimated_selectivity(selectivity);
        // TODO: projection alters the estimated size of the output
        let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

        let column_statistics = collect_new_statistics(
            &projected_stats.column_statistics,
            analysis_ctx.boundaries,
        );
        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
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
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

pub(crate) fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projection: &Option<Vec<usize>>,
) -> Result<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            //load just the columns requested
            let batch = match projection.as_ref() {
                Some(columns) => batch.project(columns)?,
                None => batch.clone(),
            };
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(&batch, filter_array)?))
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
                        let timer: crate::metrics::ScopedTimerGuard<'_> =
                            self.baseline_metrics.elapsed_compute().timer();
                        let filtered_batch =
                            batch_filter(&batch, &self.predicate, &self.projection)?;
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
fn collect_columns_from_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
) -> EqualAndNonEqual {
    let mut eq_predicate_columns = Vec::<(&Column, &Column)>::new();
    let mut ne_predicate_columns = Vec::<(&Column, &Column)>::new();

    let predicates = split_conjunction(predicate);
    predicates.into_iter().for_each(|p| {
        if let Some(binary) = p.as_any().downcast_ref::<BinaryExpr>() {
            if let (Some(left_column), Some(right_column)) = (
                binary.left().as_any().downcast_ref::<Column>(),
                binary.right().as_any().downcast_ref::<Column>(),
            ) {
                if schema.column_with_name(left_column.name()).is_none()
                    || schema.column_with_name(right_column.name()).is_none()
                {
                    // skip columns not in target schema
                    return;
                }
                match binary.op() {
                    Operator::Eq => {
                        eq_predicate_columns.push((left_column, right_column))
                    }
                    Operator::NotEq => {
                        ne_predicate_columns.push((left_column, right_column))
                    }
                    _ => {}
                }
            }
        }
    });

    (eq_predicate_columns, ne_predicate_columns)
}
/// The equals Column-Pairs and Non-equals Column-Pairs in the Predicates
pub type EqualAndNonEqual<'a> =
    (Vec<(&'a Column, &'a Column)>, Vec<(&'a Column, &'a Column)>);

#[cfg(test)]
mod tests {
    use std::iter::Iterator;
    use std::sync::Arc;

    use super::*;
    use crate::expressions::*;
    use crate::test;
    use crate::test::exec::StatisticsExec;
    use crate::ExecutionPlan;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ColumnStatistics, ScalarValue};
    use datafusion_expr::Operator;

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

        let (equal_pairs, ne_pairs) = collect_columns_from_predicate(&predicate, schema);

        assert_eq!(1, equal_pairs.len());
        assert_eq!(equal_pairs[0].0.name(), "c2");
        assert_eq!(equal_pairs[0].1.name(), "c9");

        assert_eq!(1, ne_pairs.len());
        assert_eq!(ne_pairs[0].0.name(), "c1");
        assert_eq!(ne_pairs[0].1.name(), "c13");

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
            Arc::new(FilterExec::try_new(predicate, None, input)?);

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
            None,
            input,
        )?);

        // Nested filters (two separate physical plans, instead of AND chain in the expr)
        // WHERE a >= 10
        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            None,
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
            None,
            input,
        )?);

        // WHERE b > 45
        let b_gt_5: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("b", &schema)?, Operator::Gt, lit(45i32), &schema)?,
            None,
            a_lte_25,
        )?);

        // WHERE a >= 10
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            None,
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
            Arc::new(FilterExec::try_new(predicate, None, input)?);

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
            Arc::new(FilterExec::try_new(predicate, None, input)?);
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
            Arc::new(FilterExec::try_new(predicate, None, input)?);
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
            Arc::new(FilterExec::try_new(predicate, None, input)?);
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
            Arc::new(FilterExec::try_new(predicate, None, input)?);
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
            Arc::new(FilterExec::try_new(predicate, None, input)?);
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
            Arc::new(FilterExec::try_new(predicate, None, input)?);
        let filter_statistics = filter.statistics()?;
        // First column is "a", and it is a column with only one value after the filter.
        assert!(filter_statistics.column_statistics[0].is_singleton());

        Ok(())
    }
}
