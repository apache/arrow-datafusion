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

use crate::optimizer::{ApplyOrder, ApplyOrder::BottomUp};
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{Column, Result};
use datafusion_expr::utils::expand_wildcard;
use datafusion_expr::{
    aggregate_function::AggregateFunction as AggregateFunctionFunc, col,
    expr::AggregateFunction, LogicalPlanBuilder,
};
use datafusion_expr::{Aggregate, Distinct, DistinctOn, Expr, LogicalPlan};

/// Optimizer that replaces logical [[Distinct]] with a logical [[Aggregate]]
///
/// ```text
/// SELECT DISTINCT a, b FROM tab
/// ```
///
/// Into
/// ```text
/// SELECT a, b FROM tab GROUP BY a, b
/// ```
///
/// On the other hand, for a `DISTINCT ON` query the replacement is
/// a bit more involved and effectively converts
/// ```text
/// SELECT DISTINCT ON (a) b FROM tab ORDER BY a DESC, c
/// ```
///
/// into
/// ```text
/// SELECT b FROM (
///     SELECT a, FIRST_VALUE(b ORDER BY a DESC, c) AS b
///     FROM tab
///     GROUP BY a
/// )
/// ORDER BY a DESC
/// ```

/// Optimizer that replaces logical [[Distinct]] with a logical [[Aggregate]]
#[derive(Default)]
pub struct ReplaceDistinctWithAggregate {}

impl ReplaceDistinctWithAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ReplaceDistinctWithAggregate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let group_expr = expand_wildcard(input.schema(), input, None)?;
                let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
                    input.clone(),
                    group_expr,
                    vec![],
                )?);
                Ok(Some(aggregate))
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                select_expr,
                on_expr,
                sort_expr,
                input,
                schema,
            })) => {
                // Construct the aggregation expression to be used to fetch the selected expressions.
                let aggr_expr = select_expr
                    .iter()
                    .map(|e| {
                        Expr::AggregateFunction(AggregateFunction::new(
                            AggregateFunctionFunc::FirstValue,
                            vec![e.clone()],
                            false,
                            None,
                            sort_expr.clone(),
                            None,
                        ))
                    })
                    .collect::<Vec<Expr>>();

                // Build the aggregation plan
                let plan = LogicalPlanBuilder::from(input.as_ref().clone())
                    .aggregate(on_expr.clone(), aggr_expr.to_vec())?
                    .build()?;

                let plan = if let Some(sort_expr) = sort_expr {
                    // While sort expressions were used in the `FIRST_VALUE` aggregation itself above,
                    // this on it's own isn't enough to guarantee the proper output order of the grouping
                    // (`ON`) expression, so we need to sort those as well.
                    LogicalPlanBuilder::from(plan)
                        .sort(sort_expr[..on_expr.len()].to_vec())?
                        .build()?
                } else {
                    plan
                };

                // Whereas the aggregation plan by default outputs both the grouping and the aggregation
                // expressions, for `DISTINCT ON` we only need to emit the original selection expressions.
                let project_exprs = plan
                    .schema()
                    .iter()
                    .skip(on_expr.len())
                    .zip(schema.iter())
                    .map(|((new_qualifier, new_field), (old_qualifier, old_field))| {
                        Ok(col(Column::from((new_qualifier, new_field.as_ref())))
                            .alias_qualified(old_qualifier.cloned(), old_field.name()))
                    })
                    .collect::<Result<Vec<Expr>>>()?;

                let plan = LogicalPlanBuilder::from(plan)
                    .project(project_exprs)?
                    .build()?;

                Ok(Some(plan))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "replace_distinct_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use crate::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
    use crate::test::{assert_optimized_plan_eq, test_table_scan};
    use datafusion_expr::{col, LogicalPlanBuilder};
    use std::sync::Arc;

    #[test]
    fn replace_distinct() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]\
                            \n  Projection: test.a, test.b\
                            \n    TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }

    #[test]
    fn replace_distinct_on() -> datafusion_common::Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .distinct_on(
                vec![col("a")],
                vec![col("b")],
                Some(vec![col("a").sort(false, true), col("c").sort(true, false)]),
            )?
            .build()?;

        let expected = "Projection: FIRST_VALUE(test.b) ORDER BY [test.a DESC NULLS FIRST, test.c ASC NULLS LAST] AS b\
        \n  Sort: test.a DESC NULLS FIRST\
        \n    Aggregate: groupBy=[[test.a]], aggr=[[FIRST_VALUE(test.b) ORDER BY [test.a DESC NULLS FIRST, test.c ASC NULLS LAST]]]\
        \n      TableScan: test";

        assert_optimized_plan_eq(
            Arc::new(ReplaceDistinctWithAggregate::new()),
            &plan,
            expected,
        )
    }
}
