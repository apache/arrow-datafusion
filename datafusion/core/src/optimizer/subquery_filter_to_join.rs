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

//! Optimizer rule for rewriting subquery filters to joins
//!
//! It handles standalone parts of logical conjunction expressions, i.e.
//! ```text
//!   WHERE t1.f IN (SELECT f FROM t2) AND t2.f = 'x'
//! ```
//! will be rewritten, but
//! ```text
//!   WHERE t1.f IN (SELECT f FROM t2) OR t2.f = 'x'
//! ```
//!
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use crate::execution::context::ExecutionProps;
use crate::logical_plan::plan::{Filter, Join, Projection};
use crate::logical_plan::{
    build_join_schema, Expr, JoinConstraint, JoinType, LogicalPlan, Operator,
};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct SubqueryFilterToJoin {}

impl SubqueryFilterToJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    fn rewrite_correlated_subquery_as_join(
        &self,
        outer_plan: LogicalPlan,
        subquery_expr: &Expr,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        match subquery_expr {
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let mut correlated_join_columns = vec![];
                let subquery_ref = &*subquery.subquery;
                let right_decorrelated_plan = match subquery_ref {
                    // NOTE: We only pattern match against Projection(Filter(..)). We will have another optimization rule
                    // which tries to pull up all correlated predicates in an InSubquery into a Projection(Filter(..))
                    // at the root node of the InSubquery's subquery. The Projection at the root must have as its expression
                    // a single Column.
                    LogicalPlan::Projection(Projection { input, expr, .. }) => {
                        if expr.len() != 1 {
                            return Err(DataFusionError::Plan(
                                "Only single column allowed in InSubquery".to_string(),
                            ));
                        };
                        match (&expr[0], &**input) {
                            (
                                Expr::Column(right_key),
                                LogicalPlan::Filter(Filter { predicate, input }),
                            ) => {
                                // Extract correlated columns as join columns from the filter predicate
                                let non_correlated_predicate =
                                    utils::extract_correlated_as_join_columns(
                                        predicate,
                                        outer_plan.schema(),
                                        &mut correlated_join_columns,
                                    );

                                // Strip the projection away and use its input for the semi/anti-join
                                // Note that this rule is quite quirky. But a removing a projection below a semi
                                // or anti join is inconsequential if it is a Column projection.
                                let plan =
                                    if let Some(predicate) = non_correlated_predicate {
                                        LogicalPlan::Filter(Filter {
                                            input: input.clone(),
                                            predicate,
                                        })
                                    } else {
                                        (**input).clone()
                                    };
                                Some((plan, right_key.clone()))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                };

                // optimize the subquery and obtain the appropriate IN join key
                let (right_input, right_key) =
                    if let Some((plan, key)) = right_decorrelated_plan {
                        let right_input = self.optimize(&plan, execution_props)?;
                        (right_input, key)
                    } else {
                        // If we were unable to decorrelate the subquery by matching against
                        // the pattern, we assume the subquery itself is not correlated
                        // and we run the semi/anti join on its output column
                        let right_input = self.optimize(subquery_ref, execution_props)?;
                        let right_schema = right_input.schema();
                        if right_schema.fields().len() != 1 {
                            return Err(DataFusionError::Plan(
                                "Only single column allowed in InSubquery".to_string(),
                            ));
                        }
                        let right_key = right_schema.field(0).qualified_column();

                        (right_input, right_key)
                    };

                let left_key = match *expr.clone() {
                    Expr::Column(col) => col,
                    _ => {
                        return Err(DataFusionError::NotImplemented(
                            "Filtering by expression not implemented for InSubquery"
                                .to_string(),
                        ))
                    }
                };
                correlated_join_columns.push((left_key, right_key));

                let join_type = if *negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                };

                let schema = build_join_schema(
                    outer_plan.schema(),
                    right_input.schema(),
                    &join_type,
                )?;

                Ok(LogicalPlan::Join(Join {
                    left: Arc::new(outer_plan),
                    right: Arc::new(right_input),
                    on: correlated_join_columns,
                    join_type,
                    join_constraint: JoinConstraint::On,
                    schema: Arc::new(schema),
                    null_equals_null: false,
                }))
            }
            Expr::Exists { subquery, negated } => {
                // NOTE: We only pattern match against Filter(..). We will have another optimization rule
                // which tries to pull up all correlated predicates in an Exists into a Filter(..)
                // at the root node of the Exists's subquery
                let mut correlated_join_columns = vec![];
                let right_input = match &*subquery.subquery {
                    LogicalPlan::Filter(Filter { predicate, input }) => {
                        let non_correlated_predicate =
                            utils::extract_correlated_as_join_columns(
                                predicate,
                                outer_plan.schema(),
                                &mut correlated_join_columns,
                            );
                        if let Some(predicate) = non_correlated_predicate {
                            Arc::new(LogicalPlan::Filter(Filter {
                                input: input.clone(),
                                predicate,
                            }))
                        } else {
                            input.clone()
                        }
                    }
                    _ => subquery.subquery.clone(),
                };

                let right_input = self.optimize(&right_input, execution_props)?;

                let join_type = if *negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                };

                let schema = build_join_schema(
                    outer_plan.schema(),
                    right_input.schema(),
                    &join_type,
                )?;

                Ok(LogicalPlan::Join(Join {
                    left: Arc::new(outer_plan),
                    right: Arc::new(right_input),
                    on: correlated_join_columns,
                    join_type,
                    join_constraint: JoinConstraint::On,
                    schema: Arc::new(schema),
                    null_equals_null: false,
                }))
            }
            _ => Err(DataFusionError::Plan(
                "Unknown expression while rewriting subquery to joins".to_string(),
            )),
        }
    }
}

impl OptimizerRule for SubqueryFilterToJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        match plan {
            // Pattern match on all plans of the form
            // Filter: Exists(Filter(..)) AND InSubquery(Project(Filter(..))) AND ...
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, execution_props)?;

                // Splitting filter expression into components by AND
                let mut filters = vec![];
                utils::split_conjunction(predicate, &mut filters);

                // Searching for subquery-based filters
                let (subquery_filters, remainder): (Vec<&Expr>, Vec<&Expr>) =
                    filters.into_iter().partition(|&e| {
                        matches!(e, Expr::InSubquery { .. } | Expr::Exists { .. })
                    });

                let remaining_predicate = utils::combine_conjunctive(&remainder);

                if let Some(predicate) = remaining_predicate {
                    // Since we are unable to simplify the correlated subquery,
                    // we must do a row scan against the outer plan anyway, so we abort
                    //
                    // TODO: complex expressions which are disjunctive with our subquery expressions
                    // can be rewritten as unions (without deduplication...)?
                    if utils::contains_joinable_subquery(&predicate)? {
                        return Ok(LogicalPlan::Filter(Filter {
                            predicate,
                            input: Arc::new(optimized_input),
                        }));
                    }
                }

                // Add subquery joins to optimized_input
                let new_input = subquery_filters.iter().try_fold(
                    optimized_input,
                    |outer_plan, &subquery_expr| {
                        self.rewrite_correlated_subquery_as_join(
                            outer_plan,
                            subquery_expr,
                            execution_props,
                        )
                    },
                )?;

                // Apply filters to join output if any
                Ok(utils::filter_by_all(new_input, &remainder))
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, execution_props)
            }
        }
    }

    fn name(&self) -> &str {
        "subquery_filter_to_join"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{
        and, binary_expr, col, exists, in_subquery, lit, not_exists, not_in_subquery, or,
        LogicalPlanBuilder, Operator,
    };
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SubqueryFilterToJoin::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

    fn test_subquery_with_name(name: &str) -> Result<Arc<LogicalPlan>> {
        let table_scan = test_table_scan_with_name(name)?;
        Ok(Arc::new(
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("c")])?
                .build()?,
        ))
    }

    /// Test for single IN subquery filter
    #[test]
    fn in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq.c [c:UInt32]\
        \n      TableScan: sq projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for single NOT IN subquery filter
    #[test]
    fn not_in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Anti Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq.c [c:UInt32]\
        \n      TableScan: sq projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for several IN subquery expressions
    #[test]
    fn in_subquery_multiple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(
                in_subquery(col("c"), test_subquery_with_name("sq_1")?),
                in_subquery(col("b"), test_subquery_with_name("sq_2")?),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.b = #sq_2.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    Semi Join: #test.c = #sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n      Projection: #sq_1.c [c:UInt32]\
        \n        TableScan: sq_1 projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq_2.c [c:UInt32]\
        \n      TableScan: sq_2 projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for IN subquery with additional AND filter
    #[test]
    fn in_subquery_with_and_filters() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(
                in_subquery(col("c"), test_subquery_with_name("sq")?),
                and(
                    binary_expr(col("a"), Operator::Eq, lit(1_u32)),
                    binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                ),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Filter: #test.a = UInt32(1) AND #test.b < UInt32(30) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Semi Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n      Projection: #sq.c [c:UInt32]\
        \n        TableScan: sq projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for IN subquery with additional OR filter
    /// filter expression not modified
    #[test]
    fn in_subquery_with_or_filters() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(or(
                and(
                    binary_expr(col("a"), Operator::Eq, lit(1_u32)),
                    binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                ),
                in_subquery(col("c"), test_subquery_with_name("sq")?),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Filter: #test.a = UInt32(1) AND #test.b < UInt32(30) OR #test.c IN (\
        Subquery: Projection: #sq.c\
        \n  TableScan: sq projection=None) [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for nested IN subqueries
    #[test]
    fn in_subquery_nested() -> Result<()> {
        let table_scan = test_table_scan()?;

        let subquery = LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
            .filter(in_subquery(col("a"), test_subquery_with_name("sq_nested")?))?
            .project(vec![col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("b"), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.b = #sq.a [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n    Semi Join: #sq.a = #sq_nested.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: sq projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n      Projection: #sq_nested.c [c:UInt32]\
        \n        TableScan: sq_nested projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for IN subquery with additional correlated (dependent) predicate
    #[test]
    fn in_subquery_with_correlated_filters() -> Result<()> {
        let table_a = test_table_scan_with_name("table_a")?;
        let table_b = test_table_scan_with_name("table_b")?;

        let subquery = LogicalPlanBuilder::from(table_b)
            .filter(col("table_a.a").eq(col("table_b.a")))?
            .project(vec![col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_a)
            .filter(in_subquery(col("c"), Arc::new(subquery)))?
            .project(vec![col("table_a.b")])?
            .build()?;

        let expected = "\
            Projection: #table_a.b [b:UInt32]\
            \n  Semi Join: #table_a.a = #table_b.a, #table_a.c = #table_b.c [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_a projection=None [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_b projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for filter input modification in case filter not supported
    /// Outer filter expression not modified while inner converted to join
    #[test]
    fn in_subquery_input_modified() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq_inner")?))?
            .project_with_alias(vec![col("b"), col("c")], Some("wrapped".to_string()))?
            .filter(or(
                binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                in_subquery(col("c"), test_subquery_with_name("sq_outer")?),
            ))?
            .project(vec![col("b")])?
            .build()?;

        let expected = "Projection: #wrapped.b [b:UInt32]\
        \n  Filter: #wrapped.b < UInt32(30) OR #wrapped.c IN (\
        Subquery: Projection: #sq_outer.c\
        \n  TableScan: sq_outer projection=None) [b:UInt32, c:UInt32]\
        \n    Projection: #test.b, #test.c, alias=wrapped [b:UInt32, c:UInt32]\
        \n      Semi Join: #test.c = #sq_inner.c [a:UInt32, b:UInt32, c:UInt32]\
        \n        TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]\
        \n        Projection: #sq_inner.c [c:UInt32]\
        \n          TableScan: sq_inner projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_exists_simple() -> Result<()> {
        let table_a = test_table_scan_with_name("table_a")?;
        let table_b = test_table_scan_with_name("table_b")?;
        let subquery = LogicalPlanBuilder::from(table_b)
            .filter(col("table_a.a").eq(col("table_b.a")))?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_a)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let expected = "\
            Projection: #table_a.a, #table_a.b [a:UInt32, b:UInt32]\
            \n  Semi Join: #table_a.a = #table_b.a [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_a projection=None [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_b projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn test_exists_multiple_correlated_filters() -> Result<()> {
        let table_a = test_table_scan_with_name("table_a")?;
        let table_b = test_table_scan_with_name("table_b")?;

        // Test AND and nested filters will be extracted as join columns
        let subquery = LogicalPlanBuilder::from(table_b)
            .filter(
                (col("table_a.c").eq(col("table_b.c"))).and(
                    (col("table_a.a").eq(col("table_b.a")))
                        .and(col("table_a.b").eq(col("table_b.b"))),
                ),
            )?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_a)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let expected = "\
            Projection: #table_a.a, #table_a.b [a:UInt32, b:UInt32]\
            \n  Semi Join: #table_a.c = #table_b.c, #table_a.a = #table_b.a, #table_a.b = #table_b.b [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_a projection=None [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_b projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn test_exists_with_non_correlated_filter() -> Result<()> {
        let table_a = test_table_scan_with_name("table_a")?;
        let table_b = test_table_scan_with_name("table_b")?;
        let subquery = LogicalPlanBuilder::from(table_b)
            .filter(
                (col("table_a.a").eq(col("table_b.a")))
                    .and(col("table_b.b").gt(lit("5"))),
            )?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_a)
            .project(vec![col("a"), col("b")])?
            .filter(exists(Arc::new(subquery)))?
            .build()?;
        let expected = "\
            Semi Join: #table_a.a = #table_b.a [a:UInt32, b:UInt32]\
            \n  Projection: #table_a.a, #table_a.b [a:UInt32, b:UInt32]\
            \n    TableScan: table_a projection=None [a:UInt32, b:UInt32, c:UInt32]\
            \n  Filter: #table_b.b > Utf8(\"5\") [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_b projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    // We only test not exists for the simplest case since all the other code paths
    // are covered by exists
    #[test]
    fn test_not_exists_simple() -> Result<()> {
        let table_a = test_table_scan_with_name("table_a")?;
        let table_b = test_table_scan_with_name("table_b")?;
        let subquery = LogicalPlanBuilder::from(table_b)
            .filter(col("table_a.a").eq(col("table_b.a")))?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_a)
            .filter(not_exists(Arc::new(subquery)))?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let expected = "\
            Projection: #table_a.a, #table_a.b [a:UInt32, b:UInt32]\
            \n  Anti Join: #table_a.a = #table_b.a [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_a projection=None [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: table_b projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
