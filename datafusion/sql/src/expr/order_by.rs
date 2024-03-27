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
use datafusion_common::{plan_datafusion_err, plan_err, Column, DFSchema, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::Expr;
use sqlparser::ast::{Expr as SQLExpr, OrderByExpr, Value};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Convert sql [OrderByExpr] to `Vec<Expr>`.
    ///
    /// If `literal_to_column` is true, treat any numeric literals (e.g. `2`) as a 1 based index
    /// into the SELECT list (e.g. `SELECT a, b FROM table ORDER BY 2`).
    /// If false, interpret numeric literals as constant values.
    pub(crate) fn order_by_to_sort_expr(
        &self,
        exprs: &[OrderByExpr],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        literal_to_column: bool,
    ) -> Result<Vec<Expr>> {
        let mut expr_vec = vec![];
        for e in exprs {
            let OrderByExpr {
                asc,
                expr,
                nulls_first,
            } = e;

            let expr = match expr {
                SQLExpr::Value(Value::Number(v, _)) if literal_to_column => {
                    let field_index = v
                        .parse::<usize>()
                        .map_err(|err| plan_datafusion_err!("{}", err))?;

                    if field_index == 0 {
                        return plan_err!(
                            "Order by index starts at 1 for column indexes"
                        );
                    } else if schema.fields().len() < field_index {
                        return plan_err!(
                            "Order by column out of bounds, specified: {}, max: {}",
                            field_index,
                            schema.fields().len()
                        );
                    }

                    Expr::Column(Column::from(schema.qualified_field(field_index - 1)))
                }
                e => self.sql_expr_to_logical_expr(e.clone(), schema, planner_context)?,
            };
            let asc = asc.unwrap_or(true);
            expr_vec.push(Expr::Sort(Sort::new(
                Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first.unwrap_or(!asc),
            )))
        }
        Ok(expr_vec)
    }
}
