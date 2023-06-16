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

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, OwnedTableReference, Result};
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};

/// Optimization rule that eliminate unnecessary [LogicalPlan::SubqueryAlias].
#[derive(Default)]
pub struct EliminateSubqueryAliases;

impl EliminateSubqueryAliases {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl OptimizerRule for EliminateSubqueryAliases {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, schema, .. }) => {
                let exprs = input
                    .expressions()
                    .iter()
                    .zip(schema.fields().iter())
                    .map(|(expr, field)| match expr {
                        Expr::Alias(_, name) => Expr::Column(Column::new(
                            Option::<OwnedTableReference>::None,
                            name.clone(),
                        ))
                        .alias(field.qualified_name()),
                        _ => expr.clone().alias(field.qualified_name()),
                    })
                    .collect::<Vec<Expr>>();

                let new_plan = LogicalPlan::Projection(
                    Projection::try_new(exprs, input.clone()).unwrap(),
                );

                Ok(Some(new_plan))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_subquery_aliases"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::analyzer::count_wildcard_rule::COUNT_STAR;
    use crate::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
    use crate::optimizer::Optimizer;
    use crate::test::{
        scan_tpch_table, test_subquery_with_name, test_table_scan,
        test_table_scan_with_name,
    };
    use crate::OptimizerContext;
    use arrow::datatypes::DataType;
    use datafusion_expr::Expr::Wildcard;
    use datafusion_expr::{
        col, count, in_subquery, lit, out_ref_col, LogicalPlanBuilder,
    };
    use log::debug;
    use std::sync::Arc;

    pub fn assert_multi_rules_optimized_plan_eq_display_indent(
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
        plan: &LogicalPlan,
        expected: &str,
    ) {
        assert_eq!(
            multi_rules_optimized_plan_eq_display_indent(rules, plan),
            expected
        );
    }

    pub fn multi_rules_optimized_plan_eq_display_indent(
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
        plan: &LogicalPlan,
    ) -> String {
        let optimizer = Optimizer::with_rules(rules);
        let mut optimized_plan = plan.clone();
        for rule in &optimizer.rules {
            optimized_plan = optimizer
                .optimize_recursively(rule, &optimized_plan, &OptimizerContext::new())
                .expect("failed to optimize plan")
                .unwrap_or_else(|| optimized_plan.clone());
        }
        let formatted_plan = optimized_plan.display_indent_schema().to_string();
        formatted_plan
    }

    #[test]
    fn eliminate_subquery_aliases() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .project(vec![col("a"), col("b"), col("c")])?
            .alias("t1")?
            .build()?;

        let plan = LogicalPlanBuilder::from(sq)
            .project(vec![col("a")])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent_schema());
        let expected = "Projection: t1.a [a:UInt32]\
          \n  Projection: t.a AS t1.a, t.b AS t1.b, t.c AS t1.c [t1.a:UInt32, t1.b:UInt32, t1.c:UInt32]\
          \n    Projection: t.a, t.b, t.c [a:UInt32, b:UInt32, c:UInt32]\
          \n      TableScan: t [a:UInt32, b:UInt32, c:UInt32]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(EliminateSubqueryAliases::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn eliminate_subquery_aliases_wildcard() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .project(vec![Wildcard])?
            .alias("t1")?
            .build()?;

        let plan = LogicalPlanBuilder::from(sq)
            .project(vec![Wildcard])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent_schema());
        let expected = "Projection: t1.a, t1.b, t1.c [a:UInt32, b:UInt32, c:UInt32]\
          \n  Projection: t.a AS t1.a, t.b AS t1.b, t.c AS t1.c [t1.a:UInt32, t1.b:UInt32, t1.c:UInt32]\
          \n    Projection: t.a, t.b, t.c [a:UInt32, b:UInt32, c:UInt32]\
          \n      TableScan: t [a:UInt32, b:UInt32, c:UInt32]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(EliminateSubqueryAliases::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn eliminate_multi_subquery_aliases() -> Result<()> {
        let sq_t1 = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .project(vec![Wildcard])?
            .alias("t1")?
            .build()?;

        let sq_t2 = LogicalPlanBuilder::from(sq_t1)
            .project(vec![Wildcard])?
            .alias("t2")?
            .build()?;

        let plan = LogicalPlanBuilder::from(sq_t2)
            .project(vec![Wildcard])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent_schema());
        let expected = "Projection: t2.a, t2.b, t2.c [a:UInt32, b:UInt32, c:UInt32]\
          \n  Projection: t1.a AS t2.a, t1.b AS t2.b, t1.c AS t2.c [t2.a:UInt32, t2.b:UInt32, t2.c:UInt32]\
          \n    Projection: t1.a, t1.b, t1.c [a:UInt32, b:UInt32, c:UInt32]\
          \n      Projection: t.a AS t1.a, t.b AS t1.b, t.c AS t1.c [t1.a:UInt32, t1.b:UInt32, t1.c:UInt32]\
          \n        Projection: t.a, t.b, t.c [a:UInt32, b:UInt32, c:UInt32]\
          \n          TableScan: t [a:UInt32, b:UInt32, c:UInt32]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(EliminateSubqueryAliases::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    //related: https://github.com/apache/arrow-datafusion/issues/6447
    #[test]
    fn eliminate_subquery_aliases_with_agg() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .aggregate(Vec::<Expr>::new(), vec![count(col("a"))])?
            .project(vec![count(col("a")).alias(COUNT_STAR)])?
            .alias("t1")?
            .build()?;

        let plan = LogicalPlanBuilder::from(sq)
            .project(vec![Wildcard])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent_schema());
        let expected = "Projection: t1.COUNT(*) [COUNT(*):Int64;N]\
          \n  Projection: COUNT(*) AS t1.COUNT(*) [t1.COUNT(*):Int64;N]\
          \n    Projection: COUNT(t.a) AS COUNT(*) [COUNT(*):Int64;N]\
          \n      Aggregate: groupBy=[[]], aggr=[[COUNT(t.a)]] [COUNT(t.a):Int64;N]\
          \n        TableScan: t [a:UInt32, b:UInt32, c:UInt32]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(EliminateSubqueryAliases::new())],
            &plan,
            expected,
        );
        Ok(())
    }
    #[test]
    fn eliminate_subquery_aliases_with_gby_agg() -> Result<()> {
        let sq = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .aggregate(vec![col("b")], vec![count(col("a"))])?
            .project(vec![col("b"), count(col("a")).alias(COUNT_STAR)])?
            .alias("t1")?
            .build()?;

        let plan = LogicalPlanBuilder::from(sq)
            .project(vec![Wildcard])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent_schema());
        let expected = "Projection: t1.b, t1.COUNT(*) [b:UInt32, COUNT(*):Int64;N]\
          \n  Projection: t.b AS t1.b, COUNT(*) AS t1.COUNT(*) [t1.b:UInt32, t1.COUNT(*):Int64;N]\
          \n    Projection: t.b, COUNT(t.a) AS COUNT(*) [b:UInt32, COUNT(*):Int64;N]\
          \n      Aggregate: groupBy=[[t.b]], aggr=[[COUNT(t.a)]] [b:UInt32, COUNT(t.a):Int64;N]\
          \n        TableScan: t [a:UInt32, b:UInt32, c:UInt32]";
        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(EliminateSubqueryAliases::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn in_subquery_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        debug!(
            "plan to optimize:\n{}",
            multi_rules_optimized_plan_eq_display_indent(
                vec![Arc::new(DecorrelatePredicateSubquery::new())],
                &plan
            )
        );
        let expected = "Projection: test.b [b:UInt32]\
            \n  LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
            \n    Projection: sq.c AS __correlated_sq_1.c [__correlated_sq_1.c:UInt32]\
            \n      Projection: sq.c [c:UInt32]\
            \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![
                Arc::new(DecorrelatePredicateSubquery::new()),
                Arc::new(EliminateSubqueryAliases::new()),
            ],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test multiple correlated subqueries
    /// See subqueries.rs where_in_multiple()
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), orders.clone())
                    .and(in_subquery(col("customer.c_custkey"), orders)),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        debug!(
            "plan to optimize:\n{}",
            multi_rules_optimized_plan_eq_display_indent(
                vec![Arc::new(DecorrelatePredicateSubquery::new())],
                &plan
            )
        );
        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      Projection: orders.o_custkey AS __correlated_sq_1.o_custkey [__correlated_sq_1.o_custkey:Int64]\
        \n        Projection: orders.o_custkey [o_custkey:Int64]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n    Projection: orders.o_custkey AS __correlated_sq_2.o_custkey [__correlated_sq_2.o_custkey:Int64]\
        \n      Projection: orders.o_custkey [o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![
                Arc::new(DecorrelatePredicateSubquery::new()),
                Arc::new(EliminateSubqueryAliases::new()),
            ],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test recursive correlated subqueries
    /// See subqueries.rs where_in_recursive()
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(
                    col("lineitem.l_orderkey")
                        .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
                )?
                .project(vec![col("lineitem.l_orderkey")])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    in_subquery(col("orders.o_orderkey"), lineitem).and(
                        col("orders.o_custkey")
                            .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                    ),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), orders))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        debug!(
            "plan to optimize:\n{}",
            multi_rules_optimized_plan_eq_display_indent(
                vec![Arc::new(DecorrelatePredicateSubquery::new())],
                &plan
            )
        );
        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    Projection: orders.o_custkey AS __correlated_sq_1.o_custkey [__correlated_sq_1.o_custkey:Int64]\
        \n      Projection: orders.o_custkey [o_custkey:Int64]\
        \n        LeftSemi Join:  Filter: orders.o_orderkey = __correlated_sq_2.l_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n          Projection: lineitem.l_orderkey AS __correlated_sq_2.l_orderkey [__correlated_sq_2.l_orderkey:Int64]\
        \n            Projection: lineitem.l_orderkey [l_orderkey:Int64]\
        \n              TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![
                Arc::new(DecorrelatePredicateSubquery::new()),
                Arc::new(EliminateSubqueryAliases::new()),
            ],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn union_all_on_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b")])?
            .alias("test2")?;

        let plan = table
            .clone()
            .union(table.build()?)?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        debug!("plan to optimize:\n{}", plan.display_indent_schema());

        // filter appears below Union
        let expected = "Filter: test2.b = Int64(1) [b:UInt32]\
          \n  Union [b:UInt32]\
          \n    Projection: b AS test2.b [test2.b:UInt32]\
          \n      Projection: test.a AS b [b:UInt32]\
          \n        TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
          \n    Projection: b AS test2.b [test2.b:UInt32]\
          \n      Projection: test.a AS b [b:UInt32]\
          \n        TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(EliminateSubqueryAliases::new())],
            &plan,
            expected,
        );
        Ok(())
    }
}
