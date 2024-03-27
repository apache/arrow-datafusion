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
use datafusion_common::{not_impl_err, plan_err, DFSchema, Result, TableReference};
use datafusion_expr::{expr::Unnest, Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{FunctionArg, FunctionArgExpr, TableFactor};

mod join;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a `LogicalPlan` that scans the named relation
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if let Some(func_args) = args {
                    let tbl_func_name = name.0.first().unwrap().value.to_string();
                    let args = func_args
                        .into_iter()
                        .flat_map(|arg| {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            {
                                self.sql_expr_to_logical_expr(
                                    expr,
                                    &DFSchema::empty(),
                                    planner_context,
                                )
                            } else {
                                plan_err!("Unsupported function argument type: {:?}", arg)
                            }
                        })
                        .collect::<Vec<_>>();
                    let provider = self
                        .context_provider
                        .get_table_function_source(&tbl_func_name, args)?;
                    let plan = LogicalPlanBuilder::scan(
                        TableReference::Bare {
                            table: String::from("tmp_table").into(),
                        },
                        provider,
                        None,
                    )?
                    .build()?;
                    (plan, alias)
                } else {
                    // normalize name and alias
                    let table_ref = self.object_name_to_table_reference(name)?;
                    let table_name = table_ref.to_string();
                    let cte = planner_context.get_cte(&table_name);
                    (
                        match (
                            cte,
                            self.context_provider.get_table_source(table_ref.clone()),
                        ) {
                            (Some(cte_plan), _) => Ok(cte_plan.clone()),
                            (_, Ok(provider)) => {
                                LogicalPlanBuilder::scan(table_ref, provider, None)?
                                    .build()
                            }
                            (None, Err(e)) => Err(e),
                        }?,
                        alias,
                    )
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self.query_to_plan(*subquery, planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(*table_with_joins, planner_context)?,
                alias,
            ),
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
            } => {
                // Unnest table factor has empty input
                let schema = DFSchema::empty();
                let input = LogicalPlanBuilder::empty(true).build()?;
                let exprs = array_exprs
                    .into_iter()
                    .map(|expr| {
                        self.sql_expr_to_logical_expr(expr, &schema, planner_context)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::check_unnest_args(&exprs, &schema)?;
                let unnest_expr = Expr::Unnest(Unnest { exprs });
                let logical_plan = self.try_process_unnest(input, vec![unnest_expr])?;
                (logical_plan, alias)
            }
            TableFactor::UNNEST { .. } => {
                return not_impl_err!(
                    "UNNEST table factor with offset is not supported yet"
                );
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }
}
