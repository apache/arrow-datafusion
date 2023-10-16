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
use datafusion_common::{not_impl_err, DataFusionError, Result, UnnestOptions};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::TableFactor;
mod join;
mod unnest;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a `LogicalPlan` that scans the named relation
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. } => {
                // normalize name and alias
                let table_ref = self.object_name_to_table_reference(name)?;
                let table_name = table_ref.to_string();
                let cte = planner_context.get_cte(&table_name);
                (
                    match (
                        cte,
                        self.schema_provider.get_table_provider(table_ref.clone()),
                    ) {
                        (Some(cte_plan), _) => Ok(cte_plan.clone()),
                        (_, Ok(provider)) => {
                            LogicalPlanBuilder::scan(table_ref, provider, None)?.build()
                        }
                        (None, Err(e)) => Err(e),
                    }?,
                    alias,
                )
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

            // TODO: Support UnnestOptions
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: _,
                with_offset_alias: _,
            } => {
                // TODO: Support UnnestOptions
                let array_options: Vec<UnnestOptions> =
                    vec![UnnestOptions::default(); array_exprs.len()];

                // If column aliases are not supplied, then for a function returning a base data type,
                // the column name is also the same as the function name.
                if let Some(mut alias) = alias {
                    if alias.columns.is_empty() {
                        alias.columns = vec![alias.name.clone()];
                    }
                    (
                        self.plan_unnest(array_exprs, planner_context, array_options)?,
                        Some(alias),
                    )
                } else {
                    (
                        self.plan_unnest(array_exprs, planner_context, array_options)?,
                        None,
                    )
                }
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
