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

//! Optimizer rule to push down LIMIT in the query plan
//! It will push down through projection, limits (taking the smaller limit)

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_expr::logical_plan::{
    Join, JoinType, Limit, LogicalPlan, Sort, TableScan, Union,
};
use datafusion_expr::CrossJoin;

/// Optimization rule that tries to push down LIMIT.
#[derive(Default)]
pub struct PushDownLimit {}

impl PushDownLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Push down Limit.
impl OptimizerRule for PushDownLimit {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        use std::cmp::min;

        let LogicalPlan::Limit(limit) = plan else {
            return Ok(None);
        };

        if let LogicalPlan::Limit(child) = &*limit.input {
            // Merge the Parent Limit and the Child Limit.

            //  Case 0: Parent and Child are disjoint. (child_fetch <= skip)
            //   Before merging:
            //                     |........skip........|---fetch-->|              Parent Limit
            //    |...child_skip...|---child_fetch-->|                             Child Limit
            //   After merging:
            //    |.........(child_skip + skip).........|
            //   Before merging:
            //                     |...skip...|------------fetch------------>|     Parent Limit
            //    |...child_skip...|-------------child_fetch------------>|         Child Limit
            //   After merging:
            //    |....(child_skip + skip)....|---(child_fetch - skip)-->|

            //  Case 1: Parent is beyond the range of Child. (skip < child_fetch <= skip + fetch)
            //   Before merging:
            //                     |...skip...|------------fetch------------>|     Parent Limit
            //    |...child_skip...|-------------child_fetch------------>|         Child Limit
            //   After merging:
            //    |....(child_skip + skip)....|---(child_fetch - skip)-->|

            //  Case 2: Parent is in the range of Child. (skip + fetch < child_fetch)
            //   Before merging:
            //                     |...skip...|---fetch-->|                        Parent Limit
            //    |...child_skip...|-------------child_fetch------------>|         Child Limit
            //   After merging:
            //    |....(child_skip + skip)....|---fetch-->|
            let parent_skip = limit.skip;
            let new_fetch = match (limit.fetch, child.fetch) {
                (Some(fetch), Some(child_fetch)) => {
                    Some(min(fetch, child_fetch.saturating_sub(parent_skip)))
                }
                (Some(fetch), None) => Some(fetch),
                (None, Some(child_fetch)) => {
                    Some(child_fetch.saturating_sub(parent_skip))
                }
                (None, None) => None,
            };

            let plan = LogicalPlan::Limit(Limit {
                skip: child.skip + parent_skip,
                fetch: new_fetch,
                input: Box::new((*child.input).clone()),
            });
            return self
                .try_optimize(&plan, _config)
                .map(|opt_plan| opt_plan.or_else(|| Some(plan)));
        }

        let Some(fetch) = limit.fetch else {
            return Ok(None);
        };
        let skip = limit.skip;

        match limit.input.as_ref() {
            LogicalPlan::TableScan(scan) => {
                let limit = if fetch != 0 { fetch + skip } else { 0 };
                let new_fetch = scan.fetch.map(|x| min(x, limit)).or(Some(limit));
                if new_fetch == scan.fetch {
                    Ok(None)
                } else {
                    let new_input = LogicalPlan::TableScan(TableScan {
                        table_name: scan.table_name.clone(),
                        source: scan.source.clone(),
                        projection: scan.projection.clone(),
                        filters: scan.filters.clone(),
                        fetch: scan.fetch.map(|x| min(x, limit)).or(Some(limit)),
                        projected_schema: scan.projected_schema.clone(),
                    });
                    plan.with_new_exprs(plan.expressions(), vec![new_input])
                        .map(Some)
                }
            }
            LogicalPlan::Union(union) => {
                let new_inputs = union
                    .inputs
                    .iter()
                    .map(|x| {
                        LogicalPlan::Limit(Limit {
                            skip: 0,
                            fetch: Some(fetch + skip),
                            input: Box::new(x.clone()),
                        })
                    })
                    .collect::<Vec<_>>();
                let union = LogicalPlan::Union(Union {
                    inputs: new_inputs,
                    schema: union.schema.clone(),
                });
                plan.with_new_exprs(plan.expressions(), vec![union])
                    .map(Some)
            }

            LogicalPlan::CrossJoin(cross_join) => {
                let new_left = LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(fetch + skip),
                    input: cross_join.left.clone(),
                });
                let new_right = LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(fetch + skip),
                    input: cross_join.right.clone(),
                });
                let new_cross_join = LogicalPlan::CrossJoin(CrossJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    schema: plan.schema().clone(),
                });
                plan.with_new_exprs(plan.expressions(), vec![new_cross_join])
                    .map(Some)
            }

            LogicalPlan::Join(join) => {
                if let Some(new_join) = push_down_join(join, fetch + skip) {
                    let inputs = vec![LogicalPlan::Join(new_join)];
                    plan.with_new_exprs(plan.expressions(), inputs).map(Some)
                } else {
                    Ok(None)
                }
            }

            LogicalPlan::Sort(sort) => {
                let new_fetch = {
                    let sort_fetch = skip + fetch;
                    Some(sort.fetch.map(|f| f.min(sort_fetch)).unwrap_or(sort_fetch))
                };
                if new_fetch == sort.fetch {
                    Ok(None)
                } else {
                    let new_sort = LogicalPlan::Sort(Sort {
                        expr: sort.expr.clone(),
                        input: sort.input.clone(),
                        fetch: new_fetch,
                    });
                    plan.with_new_exprs(plan.expressions(), vec![new_sort])
                        .map(Some)
                }
            }
            child_plan @ (LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)) => {
                // commute
                let new_limit = plan.with_new_exprs(
                    plan.expressions(),
                    vec![child_plan.inputs()[0].clone()],
                )?;
                child_plan
                    .with_new_exprs(child_plan.expressions(), vec![new_limit])
                    .map(Some)
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "push_down_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn push_down_join(join: &Join, limit: usize) -> Option<Join> {
    use JoinType::*;

    fn is_no_join_condition(join: &Join) -> bool {
        join.on.is_empty() && join.filter.is_none()
    }

    let (left_limit, right_limit) = if is_no_join_condition(join) {
        match join.join_type {
            Left | Right | Full => (Some(limit), Some(limit)),
            LeftAnti | LeftSemi => (Some(limit), None),
            RightAnti | RightSemi => (None, Some(limit)),
            Inner => (None, None),
        }
    } else {
        match join.join_type {
            Left => (Some(limit), None),
            Right => (None, Some(limit)),
            _ => (None, None),
        }
    };

    match (left_limit, right_limit) {
        (None, None) => None,
        _ => {
            let left = match left_limit {
                Some(limit) => Box::new(LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(limit),
                    input: join.left.clone(),
                })),
                None => join.left.clone(),
            };
            let right = match right_limit {
                Some(limit) => Box::new(LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(limit),
                    input: join.right.clone(),
                })),
                None => join.right.clone(),
            };
            Some(Join {
                left,
                right,
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                join_constraint: join.join_constraint,
                schema: join.schema.clone(),
                null_equals_null: join.null_equals_null,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, vec};

    use super::*;
    use crate::test::*;

    use datafusion_expr::{
        col, exists,
        logical_plan::{builder::LogicalPlanBuilder, JoinType, LogicalPlan},
        max,
    };

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(PushDownLimit::new()), plan, expected)
    }

    #[test]
    fn limit_pushdown_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(0, Some(1000))?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Projection: test.a\
        \n  Limit: skip=0, fetch=1000\
        \n    TableScan: test, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_push_down_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1000))?
            .limit(0, Some(10))?
            .build()?;

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: skip=0, fetch=10\
        \n  TableScan: test, fetch=10";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_doesnt_push_down_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(0, Some(1000))?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: skip=0, fetch=1000\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[MAX(test.b)]]\
        \n    TableScan: test";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(0, Some(1000))?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: skip=0, fetch=1000\
        \n  Union\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_push_down_sort() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a")])?
            .limit(0, Some(10))?
            .build()?;

        // Should push down limit to sort
        let expected = "Limit: skip=0, fetch=10\
        \n  Sort: test.a, fetch=10\
        \n    TableScan: test";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_push_down_sort_skip() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a")])?
            .limit(5, Some(10))?
            .build()?;

        // Should push down limit to sort
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a, fetch=15\
        \n    TableScan: test";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn multi_stage_limit_recursive_to_deeper_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1000))?
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(0, Some(10))?
            .build()?;

        // Limit should use deeper LIMIT 1000, but Limit 10 shouldn't push down aggregation
        let expected = "Limit: skip=0, fetch=10\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[MAX(test.b)]]\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_pushdown_should_not_pushdown_limit_with_offset_only() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(10, None)?
            .build()?;

        // Should not push any limit down to table provider
        // When it has a select
        let expected = "Limit: skip=10, fetch=None\
        \n  TableScan: test";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_pushdown_with_offset_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(10, Some(1000))?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_pushdown_with_offset_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(0, Some(1000))?
            .limit(10, None)?
            .build()?;

        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=990\
        \n    TableScan: test, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_pushdown_with_limit_after_offset() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(10, None)?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_push_down_with_offset_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(10, None)?
            .limit(0, Some(1000))?
            .limit(0, Some(10))?
            .build()?;

        let expected = "Limit: skip=10, fetch=10\
        \n  TableScan: test, fetch=20";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_doesnt_push_down_with_offset_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(10, Some(1000))?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: skip=10, fetch=1000\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[MAX(test.b)]]\
        \n    TableScan: test";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_with_offset_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(10, Some(1000))?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: skip=10, fetch=1000\
        \n  Union\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_offset_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Inner Join: test.a = test2.a\
        \n    TableScan: test\
        \n    TableScan: test2";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn offset_limit_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Inner Join: test.a = test2.a\
        \n    TableScan: test\
        \n    TableScan: test2";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_offset_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Box::new(subquery)))?
            .limit(10, Some(100))?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Limit: skip=10, fetch=100\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Filter: test1.a = test1.a\
        \n        Projection: test1.a\
        \n          TableScan: test1\
        \n    Projection: test2.a\
        \n      TableScan: test2";

        assert_optimized_plan_equal(&outer_query, expected)
    }

    #[test]
    fn offset_limit_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Box::new(subquery)))?
            .limit(10, Some(100))?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Limit: skip=10, fetch=100\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Filter: test1.a = test1.a\
        \n        Projection: test1.a\
        \n          TableScan: test1\
        \n    Projection: test2.a\
        \n      TableScan: test2";

        assert_optimized_plan_equal(&outer_query, expected)
    }

    #[test]
    fn limit_should_push_down_join_without_condition() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;
        let left_keys: Vec<&str> = Vec::new();
        let right_keys: Vec<&str> = Vec::new();
        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Left,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Left Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Right,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Right Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Full,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Full Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::LeftSemi,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  LeftSemi Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_equal(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::LeftAnti,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  LeftAnti Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_equal(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::RightSemi,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  RightSemi Join: \
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::RightAnti,
                (left_keys, right_keys),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  RightAnti Join: \
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_left_outer_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=0, fetch=1000\
        \n  Left Join: test.a = test2.a\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_left_outer_join_with_offset() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Left Join: test.a = test2.a\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010\
        \n    TableScan: test2";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_right_outer_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=0, fetch=1000\
        \n  Right Join: test.a = test2.a\
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_right_outer_join_with_offset() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown with offset supported in right outer join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Right Join: test.a = test2.a\
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test2, fetch=1010";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn limit_push_down_cross_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .cross_join(LogicalPlanBuilder::from(table_scan_2).build()?)?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  CrossJoin:\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn skip_limit_push_down_cross_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .cross_join(LogicalPlanBuilder::from(table_scan_2).build()?)?
            .limit(1000, Some(1000))?
            .build()?;

        let expected = "Limit: skip=1000, fetch=1000\
        \n  CrossJoin:\
        \n    Limit: skip=0, fetch=2000\
        \n      TableScan: test, fetch=2000\
        \n    Limit: skip=0, fetch=2000\
        \n      TableScan: test2, fetch=2000";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn merge_limit_result_empty() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(1000))?
            .limit(1000, None)?
            .build()?;

        let expected = "Limit: skip=1000, fetch=0\
        \n  TableScan: test, fetch=0";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn skip_great_than_fetch() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(1))?
            .limit(1000, None)?
            .build()?;

        let expected = "Limit: skip=1000, fetch=0\
        \n  TableScan: test, fetch=0";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn push_down_subquery_alias() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .alias("a")?
            .limit(0, Some(1))?
            .limit(1000, None)?
            .build()?;

        let expected = "SubqueryAlias: a\
        \n  Limit: skip=1000, fetch=0\
        \n    TableScan: test, fetch=0";

        assert_optimized_plan_equal(&plan, expected)
    }
}
