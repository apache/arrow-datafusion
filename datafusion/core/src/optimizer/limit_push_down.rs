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
use datafusion_expr::logical_plan::SubqueryAlias;

use super::utils;
use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::plan::Projection;
use crate::logical_plan::{Limit, TableScan};
use crate::logical_plan::{LogicalPlan, Union};
use crate::optimizer::optimizer::OptimizerRule;
use std::sync::Arc;

/// Optimization rule that tries pushes down LIMIT n
/// where applicable to reduce the amount of scanned / processed data
#[derive(Default)]
pub struct LimitPushDown {}

impl LimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn limit_push_down(
    _optimizer: &LimitPushDown,
    upper_limit: Option<usize>,
    plan: &LogicalPlan,
    _execution_props: &ExecutionProps,
) -> Result<LogicalPlan> {
    match (plan, upper_limit) {
        (LogicalPlan::Limit(Limit { n, input }), upper_limit) => {
            let smallest = upper_limit.map(|x| std::cmp::min(x, *n)).unwrap_or(*n);
            Ok(LogicalPlan::Limit(Limit {
                n: smallest,
                // push down limit to plan (minimum of upper limit and current limit)
                input: Arc::new(limit_push_down(
                    _optimizer,
                    Some(smallest),
                    input.as_ref(),
                    _execution_props,
                )?),
            }))
        }
        (
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                filters,
                limit,
                projected_schema,
            }),
            Some(upper_limit),
        ) => Ok(LogicalPlan::TableScan(TableScan {
            table_name: table_name.clone(),
            source: source.clone(),
            projection: projection.clone(),
            filters: filters.clone(),
            limit: limit
                .map(|x| std::cmp::min(x, upper_limit))
                .or(Some(upper_limit)),
            projected_schema: projected_schema.clone(),
        })),
        (
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                alias,
            }),
            upper_limit,
        ) => {
            // Push down limit directly (projection doesn't change number of rows)
            Ok(LogicalPlan::Projection(Projection {
                expr: expr.clone(),
                input: Arc::new(limit_push_down(
                    _optimizer,
                    upper_limit,
                    input.as_ref(),
                    _execution_props,
                )?),
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
        (LogicalPlan::Union(Union { inputs, schema }), Some(upper_limit)) => {
            // Push down limit through UNION
            let new_inputs = inputs
                .iter()
                .map(|x| {
                    Ok(LogicalPlan::Limit(Limit {
                        n: upper_limit,
                        input: Arc::new(limit_push_down(
                            _optimizer,
                            Some(upper_limit),
                            x,
                            _execution_props,
                        )?),
                    }))
                })
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                schema: schema.clone(),
            }))
        }
        (
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
            }),
            upper_limit,
        ) => {
            // Push down limit directly
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: Arc::new(limit_push_down(
                    _optimizer,
                    upper_limit,
                    input.as_ref(),
                    _execution_props,
                )?),
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
        // For other nodes we can't push down the limit
        // But try to recurse and find other limit nodes to push down
        _ => {
            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| limit_push_down(_optimizer, None, plan, _execution_props))
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)
        }
    }
}

impl OptimizerRule for LimitPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        limit_push_down(self, None, plan, execution_props)
    }

    fn name(&self) -> &str {
        "limit_push_down"
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        logical_plan::{col, max, LogicalPlan, LogicalPlanBuilder},
        test::*,
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = LimitPushDown::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn limit_pushdown_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(1000)?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Limit: 1000\
        \n  Projection: #test.a\
        \n    TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
    #[test]
    fn limit_push_down_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(1000)?
            .limit(10)?
            .build()?;

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: 10\
        \n  Limit: 10\
        \n    TableScan: test projection=None, limit=10";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_doesnt_push_down_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(1000)?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: 1000\
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_should_push_down_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(1000)?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: 1000\
        \n  Union\
        \n    Limit: 1000\
        \n      TableScan: test projection=None, limit=1000\
        \n    Limit: 1000\
        \n      TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn multi_stage_limit_recurses_to_deeper_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(1000)?
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(10)?
            .build()?;

        // Limit should use deeper LIMIT 1000, but Limit 10 shouldn't push down aggregation
        let expected = "Limit: 10\
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
        \n    Limit: 1000\
        \n      TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
