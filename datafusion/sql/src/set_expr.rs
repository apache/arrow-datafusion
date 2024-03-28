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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{not_impl_err, Result};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_plan(*s, planner_context),
            SetExpr::Values(v) => self.sql_values_to_plan(v, planner_context),
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                let left_plan = self.set_expr_to_plan(*left, planner_context)?;
                let right_plan = self.set_expr_to_plan(*right, planner_context)?;
                self.set_operation_to_plan(op, left_plan, right_plan, set_quantifier)
            }
            SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
            _ => not_impl_err!("Query {set_expr} not implemented yet"),
        }
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All => Ok(true),
            SetQuantifier::Distinct | SetQuantifier::None => Ok(false),
            SetQuantifier::ByName => {
                not_impl_err!("UNION BY NAME not implemented")
            }
            SetQuantifier::AllByName => {
                not_impl_err!("UNION ALL BY NAME not implemented")
            }
            SetQuantifier::DistinctByName => {
                not_impl_err!("UNION DISTINCT BY NAME not implemented")
            }
        }
    }

    pub(super) fn set_operation_to_plan(
        &self,
        op: SetOperator,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        set_quantifier: SetQuantifier,
    ) -> Result<LogicalPlan> {
        let all = Self::is_union_all(set_quantifier)?;
        match (op, all) {
            (SetOperator::Union, true) => LogicalPlanBuilder::from(left_plan)
                .union(right_plan)?
                .build(),
            (SetOperator::Union, false) => LogicalPlanBuilder::from(left_plan)
                .union_distinct(right_plan)?
                .build(),
            (SetOperator::Intersect, true) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, true)
            }
            (SetOperator::Intersect, false) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, false)
            }
            (SetOperator::Except, true) => {
                LogicalPlanBuilder::except(left_plan, right_plan, true)
            }
            (SetOperator::Except, false) => {
                LogicalPlanBuilder::except(left_plan, right_plan, false)
            }
        }
    }
}
