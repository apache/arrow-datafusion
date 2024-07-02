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

//! [`SingleDistinctToGroupBy`] replaces `AGG(DISTINCT ..)` with `AGG(..) GROUP BY ..`

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{
    internal_err, qualified_name, tree_node::Transformed, DataFusionError, Result,
};
use datafusion_expr::builder::project;
use datafusion_expr::expr::AggregateFunctionDefinition;
use datafusion_expr::{
    aggregate_function::AggregateFunction::{Max, Min},
    col,
    expr::AggregateFunction,
    logical_plan::{Aggregate, LogicalPlan},
    Expr,
};

use hashbrown::HashSet;

/// single distinct to group by optimizer rule
///  ```text
///    Before:
///    SELECT a, count(DINSTINCT b), sum(c)
///    FROM t
///    GROUP BY a
///
///    After:
///    SELECT a, count(alias1), sum(alias2)
///    FROM (
///      SELECT a, b as alias1, sum(c) as alias2
///      FROM t
///      GROUP BY a, b
///    )
///    GROUP BY a
///  ```
#[derive(Default)]
pub struct SingleDistinctToGroupBy {}

const SINGLE_DISTINCT_ALIAS: &str = "alias1";

impl SingleDistinctToGroupBy {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Check whether all aggregate exprs are distinct on a single field.
fn is_single_distinct_agg(aggr_expr: &[Expr]) -> Result<bool> {
    let mut fields_set = HashSet::new();
    let mut aggregate_count = 0;
    for expr in aggr_expr {
        if let Expr::AggregateFunction(AggregateFunction {
            func_def: AggregateFunctionDefinition::BuiltIn(fun),
            distinct,
            args,
            filter,
            order_by,
            null_treatment: _,
        }) = expr
        {
            if filter.is_some() || order_by.is_some() {
                return Ok(false);
            }
            aggregate_count += 1;
            if *distinct {
                for e in args {
                    fields_set.insert(e);
                }
            } else if !matches!(fun, Min | Max) {
                return Ok(false);
            }
        } else if let Expr::AggregateFunction(AggregateFunction {
            func_def: AggregateFunctionDefinition::UDF(fun),
            distinct,
            args,
            filter,
            order_by,
            null_treatment: _,
        }) = expr
        {
            if filter.is_some() || order_by.is_some() {
                return Ok(false);
            }
            aggregate_count += 1;
            if *distinct {
                for e in args {
                    fields_set.insert(e);
                }
            } else if fun.name() != "sum" && fun.name() != "MIN" && fun.name() != "MAX" {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }
    Ok(aggregate_count == aggr_expr.len() && fields_set.len() == 1)
}

/// Check if the first expr is [Expr::GroupingSet].
fn contains_grouping_set(expr: &[Expr]) -> bool {
    matches!(expr.first(), Some(Expr::GroupingSet(_)))
}

impl OptimizerRule for SingleDistinctToGroupBy {
    fn name(&self) -> &str {
        "single_distinct_aggregation_to_group_by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                schema,
                group_expr,
                ..
            }) if is_single_distinct_agg(&aggr_expr)?
                && !contains_grouping_set(&group_expr) =>
            {
                let group_size = group_expr.len();
                // alias all original group_by exprs
                let (mut inner_group_exprs, out_group_expr_with_alias): (
                    Vec<Expr>,
                    Vec<(Expr, Option<String>)>,
                ) = group_expr
                    .into_iter()
                    .enumerate()
                    .map(|(i, group_expr)| {
                        if let Expr::Column(_) = group_expr {
                            // For Column expressions we can use existing expression as is.
                            (group_expr.clone(), (group_expr, None))
                        } else {
                            // For complex expression write is as alias, to be able to refer
                            // if from parent operators successfully.
                            // Consider plan below.
                            //
                            // Aggregate: groupBy=[[group_alias_0]], aggr=[[count(alias1)]] [group_alias_0:Int32, count(alias1):Int64;N]\
                            // --Aggregate: groupBy=[[test.a + Int32(1) AS group_alias_0, test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            // ----TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                            //
                            // First aggregate(from bottom) refers to `test.a` column.
                            // Second aggregate refers to the `group_alias_0` column, Which is a valid field in the first aggregate.
                            // If we were to write plan above as below without alias
                            //
                            // Aggregate: groupBy=[[test.a + Int32(1)]], aggr=[[count(alias1)]] [group_alias_0:Int32, count(alias1):Int64;N]\
                            // --Aggregate: groupBy=[[test.a + Int32(1), test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            // ----TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                            //
                            // Second aggregate refers to the `test.a + Int32(1)` expression However, its input do not have `test.a` expression in it.
                            let alias_str = format!("group_alias_{i}");
                            let (qualifier, field) = schema.qualified_field(i);
                            (
                                group_expr.alias(alias_str.clone()),
                                (
                                    col(alias_str),
                                    Some(qualified_name(qualifier, field.name())),
                                ),
                            )
                        }
                    })
                    .unzip();

                // replace the distinct arg with alias
                let mut index = 1;
                let mut group_fields_set = HashSet::new();
                let mut inner_aggr_exprs = vec![];
                let outer_aggr_exprs = aggr_expr
                    .into_iter()
                    .map(|aggr_expr| match aggr_expr {
                        Expr::AggregateFunction(AggregateFunction {
                            func_def: AggregateFunctionDefinition::BuiltIn(fun),
                            mut args,
                            distinct,
                            ..
                        }) => {
                            if distinct {
                                if args.len() != 1 {
                                    return internal_err!("DISTINCT aggregate should have exactly one argument");
                                }
                                let arg = args.swap_remove(0);

                                if group_fields_set.insert(arg.display_name()?) {
                                    inner_group_exprs
                                        .push(arg.alias(SINGLE_DISTINCT_ALIAS));
                                }
                                Ok(Expr::AggregateFunction(AggregateFunction::new(
                                    fun,
                                    vec![col(SINGLE_DISTINCT_ALIAS)],
                                    false, // intentional to remove distinct here
                                    None,
                                    None,
                                    None,
                                )))
                                // if the aggregate function is not distinct, we need to rewrite it like two phase aggregation
                            } else {
                                index += 1;
                                let alias_str = format!("alias{}", index);
                                inner_aggr_exprs.push(
                                    Expr::AggregateFunction(AggregateFunction::new(
                                        fun.clone(),
                                        args,
                                        false,
                                        None,
                                        None,
                                        None,
                                    ))
                                    .alias(&alias_str),
                                );
                                Ok(Expr::AggregateFunction(AggregateFunction::new(
                                    fun,
                                    vec![col(&alias_str)],
                                    false,
                                    None,
                                    None,
                                    None,
                                )))
                            }
                        }
                        Expr::AggregateFunction(AggregateFunction {
                            func_def: AggregateFunctionDefinition::UDF(udf),
                            mut args,
                            distinct,
                            ..
                        }) => {
                            if distinct {
                                if args.len() != 1 {
                                    return internal_err!("DISTINCT aggregate should have exactly one argument");
                                }
                                let arg = args.swap_remove(0);

                                if group_fields_set.insert(arg.display_name()?) {
                                    inner_group_exprs
                                        .push(arg.alias(SINGLE_DISTINCT_ALIAS));
                                }
                                Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                                    udf,
                                    vec![col(SINGLE_DISTINCT_ALIAS)],
                                    false, // intentional to remove distinct here
                                    None,
                                    None,
                                    None,
                                )))
                                // if the aggregate function is not distinct, we need to rewrite it like two phase aggregation
                            } else {
                                index += 1;
                                let alias_str = format!("alias{}", index);
                                inner_aggr_exprs.push(
                                    Expr::AggregateFunction(AggregateFunction::new_udf(
                                        Arc::clone(&udf),
                                        args,
                                        false,
                                        None,
                                        None,
                                        None,
                                    ))
                                    .alias(&alias_str),
                                );
                                Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                                    udf,
                                    vec![col(&alias_str)],
                                    false,
                                    None,
                                    None,
                                    None,
                                )))
                            }
                        }
                        _ => Ok(aggr_expr),
                    })
                    .collect::<Result<Vec<_>>>()?;

                // construct the inner AggrPlan
                let inner_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                    input,
                    inner_group_exprs,
                    inner_aggr_exprs,
                )?);

                let outer_group_exprs = out_group_expr_with_alias
                    .iter()
                    .map(|(expr, _)| expr.clone())
                    .collect();

                // so the aggregates are displayed in the same way even after the rewrite
                // this optimizer has two kinds of alias:
                // - group_by aggr
                // - aggr expr
                let alias_expr: Vec<_> = out_group_expr_with_alias
                    .into_iter()
                    .map(|(group_expr, original_field)| {
                        if let Some(name) = original_field {
                            group_expr.alias(name)
                        } else {
                            group_expr
                        }
                    })
                    .chain(outer_aggr_exprs.iter().cloned().enumerate().map(
                        |(idx, expr)| {
                            let idx = idx + group_size;
                            let (qualifier, field) = schema.qualified_field(idx);
                            let name = qualified_name(qualifier, field.name());
                            expr.alias(name)
                        },
                    ))
                    .collect();

                let outer_aggr = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(inner_agg),
                    outer_group_exprs,
                    outer_aggr_exprs,
                )?);
                Ok(Transformed::yes(project(outer_aggr, alias_expr)?))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::expr::{self, GroupingSet};
    use datafusion_expr::AggregateExt;
    use datafusion_expr::{
        lit, logical_plan::builder::LogicalPlanBuilder, max, min, AggregateFunction,
    };
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::{count, count_distinct, sum};
    use datafusion_functions_aggregate::sum::sum_udaf;

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq_display_indent(
            Arc::new(SingleDistinctToGroupBy::new()),
            plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn not_exist_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        // Do nothing
        let expected =
            "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]] [MAX(test.b):UInt32;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn single_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Projection: count(alias1) AS count(DISTINCT test.b) [count(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[]], aggr=[[count(alias1)]] [count(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[test.b AS alias1]], aggr=[[]] [alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_grouping_set() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a")],
            vec![col("b")],
        ]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[GROUPING SETS ((test.a), (test.b))]], aggr=[[count(DISTINCT test.c)]] [a:UInt32;N, b:UInt32;N, count(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_cube() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set = Expr::GroupingSet(GroupingSet::Cube(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[CUBE (test.a, test.b)]], aggr=[[count(DISTINCT test.c)]] [a:UInt32;N, b:UInt32;N, count(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    // Currently this optimization is disabled for CUBE/ROLLUP/GROUPING SET
    #[test]
    fn single_distinct_and_rollup() -> Result<()> {
        let table_scan = test_table_scan()?;

        let grouping_set =
            Expr::GroupingSet(GroupingSet::Rollup(vec![col("a"), col("b")]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![grouping_set], vec![count_distinct(col("c"))])?
            .build()?;

        // Should not be optimized
        let expected = "Aggregate: groupBy=[[ROLLUP (test.a, test.b)]], aggr=[[count(DISTINCT test.c)]] [a:UInt32;N, b:UInt32;N, count(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn single_distinct_expr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(lit(2) * col("b"))])?
            .build()?;

        let expected = "Projection: count(alias1) AS count(DISTINCT Int32(2) * test.b) [count(DISTINCT Int32(2) * test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[]], aggr=[[count(alias1)]] [count(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[Int32(2) * test.b AS alias1]], aggr=[[]] [alias1:Int32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn single_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Projection: test.a, count(alias1) AS count(DISTINCT test.b) [a:UInt32, count(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[test.a]], aggr=[[count(alias1)]] [a:UInt32, count(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count_distinct(col("c"))],
            )?
            .build()?;

        // Do nothing
        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[count(DISTINCT test.b), count(DISTINCT test.c)]] [a:UInt32, count(DISTINCT test.b):Int64;N, count(DISTINCT test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn one_field_two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    Expr::AggregateFunction(expr::AggregateFunction::new(
                        AggregateFunction::Max,
                        vec![col("b")],
                        true,
                        None,
                        None,
                        None,
                    )),
                ],
            )?
            .build()?;
        // Should work
        let expected = "Projection: test.a, count(alias1) AS count(DISTINCT test.b), MAX(alias1) AS MAX(DISTINCT test.b) [a:UInt32, count(DISTINCT test.b):Int64;N, MAX(DISTINCT test.b):UInt32;N]\
                            \n  Aggregate: groupBy=[[test.a]], aggr=[[count(alias1), MAX(alias1)]] [a:UInt32, count(alias1):Int64;N, MAX(alias1):UInt32;N]\
                            \n    Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn distinct_and_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        // Do nothing
        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[count(DISTINCT test.b), count(test.c)]] [a:UInt32, count(DISTINCT test.b):Int64;N, count(test.c):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn group_by_with_expr() -> Result<()> {
        let table_scan = test_table_scan().unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a") + lit(1)], vec![count_distinct(col("c"))])?
            .build()?;

        // Should work
        let expected = "Projection: group_alias_0 AS test.a + Int32(1), count(alias1) AS count(DISTINCT test.c) [test.a + Int32(1):Int32, count(DISTINCT test.c):Int64;N]\
                            \n  Aggregate: groupBy=[[group_alias_0]], aggr=[[count(alias1)]] [group_alias_0:Int32, count(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[test.a + Int32(1) AS group_alias_0, test.c AS alias1]], aggr=[[]] [group_alias_0:Int32, alias1:UInt32]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn two_distinct_and_one_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    sum(col("c")),
                    count_distinct(col("b")),
                    Expr::AggregateFunction(expr::AggregateFunction::new(
                        AggregateFunction::Max,
                        vec![col("b")],
                        true,
                        None,
                        None,
                        None,
                    )),
                ],
            )?
            .build()?;
        // Should work
        let expected = "Projection: test.a, sum(alias2) AS sum(test.c), count(alias1) AS count(DISTINCT test.b), MAX(alias1) AS MAX(DISTINCT test.b) [a:UInt32, sum(test.c):UInt64;N, count(DISTINCT test.b):Int64;N, MAX(DISTINCT test.b):UInt32;N]\
                            \n  Aggregate: groupBy=[[test.a]], aggr=[[sum(alias2), count(alias1), MAX(alias1)]] [a:UInt32, sum(alias2):UInt64;N, count(alias1):Int64;N, MAX(alias1):UInt32;N]\
                            \n    Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[sum(test.c) AS alias2]] [a:UInt32, alias1:UInt32, alias2:UInt64;N]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn one_distinctand_and_two_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![sum(col("c")), max(col("c")), count_distinct(col("b"))],
            )?
            .build()?;
        // Should work
        let expected = "Projection: test.a, sum(alias2) AS sum(test.c), MAX(alias3) AS MAX(test.c), count(alias1) AS count(DISTINCT test.b) [a:UInt32, sum(test.c):UInt64;N, MAX(test.c):UInt32;N, count(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[test.a]], aggr=[[sum(alias2), MAX(alias3), count(alias1)]] [a:UInt32, sum(alias2):UInt64;N, MAX(alias3):UInt32;N, count(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[sum(test.c) AS alias2, MAX(test.c) AS alias3]] [a:UInt32, alias1:UInt32, alias2:UInt64;N, alias3:UInt32;N]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn one_distinct_and_one_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("c")],
                vec![min(col("a")), count_distinct(col("b"))],
            )?
            .build()?;
        // Should work
        let expected = "Projection: test.c, MIN(alias2) AS MIN(test.a), count(alias1) AS count(DISTINCT test.b) [c:UInt32, MIN(test.a):UInt32;N, count(DISTINCT test.b):Int64;N]\
                            \n  Aggregate: groupBy=[[test.c]], aggr=[[MIN(alias2), count(alias1)]] [c:UInt32, MIN(alias2):UInt32;N, count(alias1):Int64;N]\
                            \n    Aggregate: groupBy=[[test.c, test.b AS alias1]], aggr=[[MIN(test.a) AS alias2]] [c:UInt32, alias1:UInt32, alias2:UInt32;N]\
                            \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn common_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        // sum(a) FILTER (WHERE a > 5)
        let expr = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            sum_udaf(),
            vec![col("a")],
            false,
            Some(Box::new(col("a").gt(lit(5)))),
            None,
            None,
        ));
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![expr, count_distinct(col("b"))])?
            .build()?;
        // Do nothing
        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a) FILTER (WHERE test.a > Int32(5)), count(DISTINCT test.b)]] [c:UInt32, sum(test.a) FILTER (WHERE test.a > Int32(5)):UInt64;N, count(DISTINCT test.b):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn distinct_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(DISTINCT a) FILTER (WHERE a > 5)
        let expr = count_udaf()
            .call(vec![col("a")])
            .distinct()
            .filter(col("a").gt(lit(5)))
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![sum(col("a")), expr])?
            .build()?;
        // Do nothing
        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a), count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5))]] [c:UInt32, sum(test.a):UInt64;N, count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5)):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn common_with_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // SUM(a ORDER BY a)
        let expr = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            sum_udaf(),
            vec![col("a")],
            false,
            None,
            Some(vec![col("a")]),
            None,
        ));
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![expr, count_distinct(col("b"))])?
            .build()?;
        // Do nothing
        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a) ORDER BY [test.a], count(DISTINCT test.b)]] [c:UInt32, sum(test.a) ORDER BY [test.a]:UInt64;N, count(DISTINCT test.b):Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn distinct_with_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(DISTINCT a ORDER BY a)
        let expr = count_udaf()
            .call(vec![col("a")])
            .distinct()
            .order_by(vec![col("a").sort(true, false)])
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![sum(col("a")), expr])?
            .build()?;
        // Do nothing
        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a), count(DISTINCT test.a) ORDER BY [test.a ASC NULLS LAST]]] [c:UInt32, sum(test.a):UInt64;N, count(DISTINCT test.a) ORDER BY [test.a ASC NULLS LAST]:Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_with_filter_and_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // count(DISTINCT a ORDER BY a) FILTER (WHERE a > 5)
        let expr = count_udaf()
            .call(vec![col("a")])
            .distinct()
            .filter(col("a").gt(lit(5)))
            .order_by(vec![col("a").sort(true, false)])
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![sum(col("a")), expr])?
            .build()?;
        // Do nothing
        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[sum(test.a), count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5)) ORDER BY [test.a ASC NULLS LAST]]] [c:UInt32, sum(test.a):UInt64;N, count(DISTINCT test.a) FILTER (WHERE test.a > Int32(5)) ORDER BY [test.a ASC NULLS LAST]:Int64;N]\
                            \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(plan, expected)
    }
}
