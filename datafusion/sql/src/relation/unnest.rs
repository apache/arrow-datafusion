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

use datafusion_common::{DFSchema, Result, UnnestOptions};
use datafusion_expr::expr::Expr;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::Expr as SQLExpr;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn plan_unnest(
        &self,
        array_exprs: Vec<SQLExpr>,
        planner_context: &mut PlannerContext,
        array_options: Vec<UnnestOptions>,
    ) -> Result<LogicalPlan> {
        // No pre-defiend schema for Unnest
        let schema = DFSchema::empty();

        let exprs: Vec<Expr> = array_exprs
            .into_iter()
            .map(|sql| self.sql_expr_to_logical_expr(sql, &schema, planner_context))
            .collect::<Result<Vec<Expr>>>()?;

        let plan = LogicalPlanBuilder::empty(true).build()?;

        LogicalPlanBuilder::from(plan)
            .unnest_arrays(exprs, array_options)?
            .build()
    }
}
