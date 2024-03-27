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
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::JoinType::Inner;
use datafusion_expr::{
    logical_plan::{EmptyRelation, LogicalPlan},
    CrossJoin, Expr,
};

/// Eliminates joins when inner join condition is false.
/// Replaces joins when inner join condition is true with a cross join.
#[derive(Default)]
pub struct EliminateJoin;

impl EliminateJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateJoin {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(join) if join.join_type == Inner && join.on.is_empty() => {
                match join.filter {
                    Some(Expr::Literal(ScalarValue::Boolean(Some(true)))) => {
                        Ok(Some(LogicalPlan::CrossJoin(CrossJoin {
                            left: join.left.clone(),
                            right: join.right.clone(),
                            schema: join.schema.clone(),
                        })))
                    }
                    Some(Expr::Literal(ScalarValue::Boolean(Some(false)))) => {
                        Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        })))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use crate::eliminate_join::EliminateJoin;
    use crate::test::*;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::JoinType::Inner;
    use datafusion_expr::{logical_plan::builder::LogicalPlanBuilder, Expr, LogicalPlan};
    use std::sync::Arc;

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateJoin::new()), plan, expected)
    }

    #[test]
    fn join_on_false() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .join_on(
                LogicalPlanBuilder::empty(false).build()?,
                Inner,
                Some(Expr::Literal(ScalarValue::Boolean(Some(false)))),
            )?
            .build()?;

        let expected = "EmptyRelation";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn join_on_true() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .join_on(
                LogicalPlanBuilder::empty(false).build()?,
                Inner,
                Some(Expr::Literal(ScalarValue::Boolean(Some(true)))),
            )?
            .build()?;

        let expected = "\
        CrossJoin:\
        \n  EmptyRelation\
        \n  EmptyRelation";
        assert_optimized_plan_equal(plan, expected)
    }
}
