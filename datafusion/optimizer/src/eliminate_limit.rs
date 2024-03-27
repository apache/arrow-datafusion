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

//! Optimizer rule to replace `LIMIT 0` or
//! `LIMIT whose ancestor LIMIT's skip is greater than or equal to current's fetch`
//! on a plan with an empty relation.
//! This rule also removes OFFSET 0 from the [LogicalPlan]
//! This saves time in planning and executing the query.
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan};

/// Optimization rule that eliminate LIMIT 0 or useless LIMIT(skip:0, fetch:None).
/// It can cooperate with `propagate_empty_relation` and `limit_push_down`.
#[derive(Default)]
pub struct EliminateLimit;

impl EliminateLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateLimit {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Limit(limit) = plan {
            match limit.fetch {
                Some(fetch) => {
                    if fetch == 0 {
                        return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: limit.input.schema().clone(),
                        })));
                    }
                }
                None => {
                    if limit.skip == 0 {
                        let input = limit.input.as_ref();
                        // input also can be Limit, so we should apply again.
                        return Ok(Some(
                            self.try_optimize(input, _config)?
                                .unwrap_or_else(|| input.clone()),
                        ));
                    }
                }
            }
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        "eliminate_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::Optimizer;
    use crate::test::*;
    use crate::OptimizerContext;
    use datafusion_common::Column;
    use datafusion_expr::{
        col,
        logical_plan::{builder::LogicalPlanBuilder, JoinType},
        sum,
    };
    use std::sync::Arc;

    use crate::push_down_limit::PushDownLimit;

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        let plan_schema = plan.schema().clone();
        let optimizer = Optimizer::with_rules(vec![Arc::new(EliminateLimit::new())]);
        let optimized_plan = optimizer
            .optimize_recursively(
                optimizer.rules.first().unwrap(),
                plan,
                &OptimizerContext::new(),
            )?
            .data;

        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan_schema.as_ref(), optimized_plan.schema().as_ref());
        Ok(())
    }

    fn assert_optimized_plan_eq_with_pushdown(
        plan: LogicalPlan,
        expected: &str,
    ) -> Result<()> {
        let plan_schema = plan.schema().clone();
        fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}
        let config = OptimizerContext::new().with_max_passes(1);
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(PushDownLimit::new()),
            Arc::new(EliminateLimit::new()),
        ]);
        let optimized_plan = optimizer
            .optimize(plan, &config, observe)
            .expect("failed to optimize plan")
            .data;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);
        assert_eq!(&plan_schema, optimized_plan.schema());
        Ok(())
    }

    #[test]
    fn limit_0_root() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(0))?
            .build()?;
        // No aggregate / scan / limit
        let expected = "EmptyRelation";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn limit_0_nested() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(0))?
            .union(plan1)?
            .build()?;

        // Left side is removed
        let expected = "Union\
            \n  EmptyRelation\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
            \n    TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn limit_fetch_with_ancestor_limit_skip() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(2))?
            .limit(2, None)?
            .build()?;

        // No aggregate / scan / limit
        let expected = "EmptyRelation";
        assert_optimized_plan_eq_with_pushdown(plan, expected)
    }

    #[test]
    fn multi_limit_offset_sort_eliminate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(2))?
            .sort(vec![col("a")])?
            .limit(2, Some(1))?
            .build()?;

        // After remove global-state, we don't record the parent <skip, fetch>
        // So, bottom don't know parent info, so can't eliminate.
        let expected = "Limit: skip=2, fetch=1\
        \n  Sort: test.a, fetch=3\
        \n    Limit: skip=0, fetch=2\
        \n      Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
        \n        TableScan: test";
        assert_optimized_plan_eq_with_pushdown(plan, expected)
    }

    #[test]
    fn limit_fetch_with_ancestor_limit_fetch() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(2))?
            .sort(vec![col("a")])?
            .limit(0, Some(1))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1\
            \n  Sort: test.a\
            \n    Limit: skip=0, fetch=2\
            \n      Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
            \n        TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn limit_with_ancestor_limit() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(2, Some(1))?
            .sort(vec![col("a")])?
            .limit(3, Some(1))?
            .build()?;

        let expected = "Limit: skip=3, fetch=1\
        \n  Sort: test.a\
        \n    Limit: skip=2, fetch=1\
        \n      Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
        \n        TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn limit_join_with_ancestor_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table_scan_inner = test_table_scan_with_name("test1")?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(2, Some(1))?
            .join_using(
                table_scan_inner,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .limit(3, Some(1))?
            .build()?;

        let expected = "Limit: skip=3, fetch=1\
            \n  Inner Join: Using test.a = test1.a\
            \n    Limit: skip=2, fetch=1\
            \n      TableScan: test\
            \n    TableScan: test1";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn remove_zero_offset() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, None)?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
            \n  TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }
}
