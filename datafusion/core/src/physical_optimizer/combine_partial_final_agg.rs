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

//! CombinePartialFinalAggregate optimizer rule checks the adjacent Partial and Final AggregateExecs
//! and try to combine them if necessary

use std::sync::Arc;

use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::ExecutionPlan;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::{physical_exprs_equal, AggregateExpr, PhysicalExpr};

/// CombinePartialFinalAggregate optimizer rule combines the adjacent Partial and Final AggregateExecs
/// into a Single AggregateExec if their grouping exprs and aggregate exprs equal.
///
/// This rule should be applied after the EnforceDistribution and EnforceSorting rules
///
#[derive(Default)]
pub struct CombinePartialFinalAggregate {}

impl CombinePartialFinalAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for CombinePartialFinalAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let transformed =
                plan.as_any()
                    .downcast_ref::<AggregateExec>()
                    .and_then(|agg_exec| {
                        if matches!(
                            agg_exec.mode(),
                            AggregateMode::Final | AggregateMode::FinalPartitioned
                        ) {
                            agg_exec
                                .input()
                                .as_any()
                                .downcast_ref::<AggregateExec>()
                                .and_then(|input_agg_exec| {
                                    if matches!(
                                        input_agg_exec.mode(),
                                        AggregateMode::Partial
                                    ) && can_combine(
                                        (
                                            agg_exec.group_expr(),
                                            agg_exec.aggr_expr(),
                                            agg_exec.filter_expr(),
                                        ),
                                        (
                                            input_agg_exec.group_expr(),
                                            input_agg_exec.aggr_expr(),
                                            input_agg_exec.filter_expr(),
                                        ),
                                    ) {
                                        let mode =
                                            if agg_exec.mode() == &AggregateMode::Final {
                                                AggregateMode::Single
                                            } else {
                                                AggregateMode::SinglePartitioned
                                            };
                                        AggregateExec::try_new(
                                            mode,
                                            input_agg_exec.group_expr().clone(),
                                            input_agg_exec.aggr_expr().to_vec(),
                                            input_agg_exec.filter_expr().to_vec(),
                                            Arc::clone(input_agg_exec.input()),
                                            input_agg_exec.input_schema(),
                                        )
                                        .map(|combined_agg| {
                                            combined_agg.with_limit(agg_exec.limit())
                                        })
                                        .ok()
                                        .map(Arc::new)
                                    } else {
                                        None
                                    }
                                })
                        } else {
                            None
                        }
                    });

            Ok(if let Some(transformed) = transformed {
                Transformed::yes(transformed)
            } else {
                Transformed::no(plan)
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "CombinePartialFinalAggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

type GroupExprsRef<'a> = (
    &'a PhysicalGroupBy,
    &'a [Arc<dyn AggregateExpr>],
    &'a [Option<Arc<dyn PhysicalExpr>>],
);

fn can_combine(final_agg: GroupExprsRef, partial_agg: GroupExprsRef) -> bool {
    let (final_group_by, final_aggr_expr, final_filter_expr) = final_agg;
    let (input_group_by, input_aggr_expr, input_filter_expr) = partial_agg;

    // Compare output expressions of the partial, and input expressions of the final operator.
    physical_exprs_equal(
        &input_group_by.output_exprs(),
        &final_group_by.input_exprs(),
    ) && input_group_by.groups() == final_group_by.groups()
        && input_group_by.null_expr().len() == final_group_by.null_expr().len()
        && input_group_by
            .null_expr()
            .iter()
            .zip(final_group_by.null_expr().iter())
            .all(|((lhs_expr, lhs_str), (rhs_expr, rhs_str))| {
                lhs_expr.eq(rhs_expr) && lhs_str == rhs_str
            })
        && final_aggr_expr.len() == input_aggr_expr.len()
        && final_aggr_expr
            .iter()
            .zip(input_aggr_expr.iter())
            .all(|(final_expr, partial_expr)| final_expr.eq(partial_expr))
        && final_filter_expr.len() == input_filter_expr.len()
        && final_filter_expr.iter().zip(input_filter_expr.iter()).all(
            |(final_expr, partial_expr)| match (final_expr, partial_expr) {
                (Some(l), Some(r)) => l.eq(r),
                (None, None) => true,
                _ => false,
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use crate::physical_plan::expressions::lit;
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::{displayable, Partitioning};

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_plan::udaf::create_aggregate_expr;

    /// Runs the CombinePartialFinalAggregate optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            // run optimizer
            let optimizer = CombinePartialFinalAggregate {};
            let config = ConfigOptions::new();
            let optimized = optimizer.optimize($PLAN, &config)?;
            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    }

    fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
        ParquetExec::builder(
            FileScanConfig::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                schema.clone(),
            )
            .with_file(PartitionedFile::new("x".to_string(), 100)),
        )
        .build_arc()
    }

    fn partial_aggregate_exec(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let n_aggr = aggr_expr.len();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by,
                aggr_expr,
                vec![None; n_aggr],
                input,
                schema,
            )
            .unwrap(),
        )
    }

    fn final_aggregate_exec(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let n_aggr = aggr_expr.len();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                group_by,
                aggr_expr,
                vec![None; n_aggr],
                input,
                schema,
            )
            .unwrap(),
        )
    }

    fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap(),
        )
    }

    // Return appropriate expr depending if COUNT is for col or table (*)
    fn count_expr(
        expr: Arc<dyn PhysicalExpr>,
        name: &str,
        schema: &Schema,
    ) -> Arc<dyn AggregateExpr> {
        create_aggregate_expr(
            &count_udaf(),
            &[expr],
            &[],
            &[],
            &[],
            schema,
            name,
            false,
            false,
        )
        .unwrap()
    }

    #[test]
    fn aggregations_not_combined() -> Result<()> {
        let schema = schema();

        let aggr_expr = vec![count_expr(lit(1i8), "COUNT(1)", &schema)];

        let plan = final_aggregate_exec(
            repartition_exec(partial_aggregate_exec(
                parquet_exec(&schema),
                PhysicalGroupBy::default(),
                aggr_expr.clone(),
            )),
            PhysicalGroupBy::default(),
            aggr_expr,
        );
        // should not combine the Partial/Final AggregateExecs
        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[COUNT(1)]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "AggregateExec: mode=Partial, gby=[], aggr=[COUNT(1)]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
        ];
        assert_optimized!(expected, plan);

        let aggr_expr1 = vec![count_expr(lit(1i8), "COUNT(1)", &schema)];
        let aggr_expr2 = vec![count_expr(lit(1i8), "COUNT(2)", &schema)];

        let plan = final_aggregate_exec(
            partial_aggregate_exec(
                parquet_exec(&schema),
                PhysicalGroupBy::default(),
                aggr_expr1,
            ),
            PhysicalGroupBy::default(),
            aggr_expr2,
        );
        // should not combine the Partial/Final AggregateExecs
        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[COUNT(2)]",
            "AggregateExec: mode=Partial, gby=[], aggr=[COUNT(1)]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
        ];

        assert_optimized!(expected, plan);

        Ok(())
    }

    #[test]
    fn aggregations_combined() -> Result<()> {
        let schema = schema();
        let aggr_expr = vec![count_expr(lit(1i8), "COUNT(1)", &schema)];

        let plan = final_aggregate_exec(
            partial_aggregate_exec(
                parquet_exec(&schema),
                PhysicalGroupBy::default(),
                aggr_expr.clone(),
            ),
            PhysicalGroupBy::default(),
            aggr_expr,
        );
        // should combine the Partial/Final AggregateExecs to tne Single AggregateExec
        let expected = &[
            "AggregateExec: mode=Single, gby=[], aggr=[COUNT(1)]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn aggregations_with_group_combined() -> Result<()> {
        let schema = schema();

        let aggr_expr = vec![create_aggregate_expr(
            &sum_udaf(),
            &[col("b", &schema)?],
            &[],
            &[],
            &[],
            &schema,
            "Sum(b)",
            false,
            false,
        )?];
        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("c", &schema)?, "c".to_string())];

        let partial_group_by = PhysicalGroupBy::new_single(groups);
        let partial_agg = partial_aggregate_exec(
            parquet_exec(&schema),
            partial_group_by,
            aggr_expr.clone(),
        );

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("c", &partial_agg.schema())?, "c".to_string())];
        let final_group_by = PhysicalGroupBy::new_single(groups);

        let plan = final_aggregate_exec(partial_agg, final_group_by, aggr_expr);
        // should combine the Partial/Final AggregateExecs to tne Single AggregateExec
        let expected = &[
            "AggregateExec: mode=Single, gby=[c@2 as c], aggr=[Sum(b)]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn aggregations_with_limit_combined() -> Result<()> {
        let schema = schema();
        let aggr_expr = vec![];

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("c", &schema)?, "c".to_string())];

        let partial_group_by = PhysicalGroupBy::new_single(groups);
        let partial_agg = partial_aggregate_exec(
            parquet_exec(&schema),
            partial_group_by,
            aggr_expr.clone(),
        );

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("c", &partial_agg.schema())?, "c".to_string())];
        let final_group_by = PhysicalGroupBy::new_single(groups);

        let schema = partial_agg.schema();
        let final_agg = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                final_group_by,
                aggr_expr,
                vec![],
                partial_agg,
                schema,
            )
            .unwrap()
            .with_limit(Some(5)),
        );
        let plan: Arc<dyn ExecutionPlan> = final_agg;
        // should combine the Partial/Final AggregateExecs to a Single AggregateExec
        // with the final limit preserved
        let expected = &[
            "AggregateExec: mode=Single, gby=[c@2 as c], aggr=[], lim=[5]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }
}
