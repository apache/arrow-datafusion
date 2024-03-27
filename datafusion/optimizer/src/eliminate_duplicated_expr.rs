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
use datafusion_common::Result;
use datafusion_expr::expr::Sort as ExprSort;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Aggregate, Expr, Sort};
use hashbrown::HashSet;

/// Optimization rule that eliminate duplicated expr.
#[derive(Default)]
pub struct EliminateDuplicatedExpr;

impl EliminateDuplicatedExpr {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateDuplicatedExpr {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Sort(sort) => {
                let normalized_sort_keys = sort
                    .expr
                    .iter()
                    .map(|e| match e {
                        Expr::Sort(ExprSort { expr, .. }) => {
                            Expr::Sort(ExprSort::new(expr.clone(), true, false))
                        }
                        _ => e.clone(),
                    })
                    .collect::<Vec<_>>();

                // dedup sort.expr and keep order
                let mut dedup_expr = Vec::new();
                let mut dedup_set = HashSet::new();
                sort.expr.iter().zip(normalized_sort_keys.iter()).for_each(
                    |(expr, normalized_expr)| {
                        if !dedup_set.contains(normalized_expr) {
                            dedup_expr.push(expr);
                            dedup_set.insert(normalized_expr);
                        }
                    },
                );
                if dedup_expr.len() == sort.expr.len() {
                    Ok(None)
                } else {
                    Ok(Some(LogicalPlan::Sort(Sort {
                        expr: dedup_expr.into_iter().cloned().collect::<Vec<_>>(),
                        input: sort.input.clone(),
                        fetch: sort.fetch,
                    })))
                }
            }
            LogicalPlan::Aggregate(agg) => {
                // dedup agg.groupby and keep order
                let mut dedup_expr = Vec::new();
                let mut dedup_set = HashSet::new();
                agg.group_expr.iter().for_each(|expr| {
                    if !dedup_set.contains(expr) {
                        dedup_expr.push(expr.clone());
                        dedup_set.insert(expr);
                    }
                });
                if dedup_expr.len() == agg.group_expr.len() {
                    Ok(None)
                } else {
                    Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                        agg.input.clone(),
                        dedup_expr,
                        agg.aggr_expr.clone(),
                    )?)))
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_duplicated_expr"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder};
    use std::sync::Arc;

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        crate::test::assert_optimized_plan_eq(
            Arc::new(EliminateDuplicatedExpr::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn eliminate_sort_expr() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a"), col("a"), col("b"), col("c")])?
            .limit(5, Some(10))?
            .build()?;
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a, test.b, test.c\
        \n    TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn eliminate_sort_exprs_with_options() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let sort_exprs = vec![
            col("a").sort(true, true),
            col("b").sort(true, false),
            col("a").sort(false, false),
            col("b").sort(false, true),
        ];
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(sort_exprs)?
            .limit(5, Some(10))?
            .build()?;
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a ASC NULLS FIRST, test.b ASC NULLS LAST\
        \n    TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }
}
