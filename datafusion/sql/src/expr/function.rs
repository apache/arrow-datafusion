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
use datafusion_common::{DFSchema, DataFusionError, Result};
use datafusion_expr::window_frame::regularize;
use datafusion_expr::{
    expr, window_function, AggregateFunction, BuiltinScalarFunction, Expr, WindowFrame,
    WindowFunction,
};
use sqlparser::ast::{
    Expr as SQLExpr, Function as SQLFunction, FunctionArg, FunctionArgExpr,
};
use std::str::FromStr;

use super::arrow_cast::ARROW_CAST_NAME;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn sql_function_to_expr(
        &self,
        mut function: SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let name = if function.name.0.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            function.name.to_string()
        } else {
            self.normalizer.normalize(function.name.0[0].clone())
        };

        // next, scalar built-in
        if let Ok(fun) = BuiltinScalarFunction::from_str(&name) {
            let args =
                self.function_args_to_expr(function.args, schema, planner_context)?;
            return Ok(Expr::ScalarFunction { fun, args });
        };

        // then, window function
        if let Some(window) = function.over.take() {
            let partition_by = window
                .partition_by
                .into_iter()
                .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                .collect::<Result<Vec<_>>>()?;
            let order_by = window
                .order_by
                .into_iter()
                .map(|e| self.order_by_to_sort_expr(e, schema, planner_context))
                .collect::<Result<Vec<_>>>()?;
            let window_frame = window
                .window_frame
                .as_ref()
                .map(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    regularize(window_frame, order_by.len())
                })
                .transpose()?;
            let window_frame = if let Some(window_frame) = window_frame {
                window_frame
            } else {
                WindowFrame::new(!order_by.is_empty())
            };
            let fun = self.find_window_func(&name)?;
            let expr = match fun {
                WindowFunction::AggregateFunction(aggregate_fun) => {
                    let (aggregate_fun, args) = self.aggregate_fn_to_expr(
                        aggregate_fun,
                        function.args,
                        schema,
                        planner_context,
                    )?;

                    Expr::WindowFunction(expr::WindowFunction::new(
                        WindowFunction::AggregateFunction(aggregate_fun),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                    ))
                }
                _ => Expr::WindowFunction(expr::WindowFunction::new(
                    fun,
                    self.function_args_to_expr(function.args, schema, planner_context)?,
                    partition_by,
                    order_by,
                    window_frame,
                )),
            };
            return Ok(expr);
        }

        // next, aggregate built-ins
        if let Ok(fun) = AggregateFunction::from_str(&name) {
            let distinct = function.distinct;
            let (fun, args) =
                self.aggregate_fn_to_expr(fun, function.args, schema, planner_context)?;
            return Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                fun, args, distinct, None,
            )));
        };

        // finally, user-defined functions (UDF) and UDAF
        if let Some(fm) = self.schema_provider.get_function_meta(&name) {
            let args =
                self.function_args_to_expr(function.args, schema, planner_context)?;
            return Ok(Expr::ScalarUDF { fun: fm, args });
        }

        // User defined aggregate functions
        if let Some(fm) = self.schema_provider.get_aggregate_meta(&name) {
            let args =
                self.function_args_to_expr(function.args, schema, planner_context)?;
            return Ok(Expr::AggregateUDF {
                fun: fm,
                args,
                filter: None,
            });
        }

        // Special case arrow_cast (as its type is dependent on its argument value)
        if name == ARROW_CAST_NAME {
            let args =
                self.function_args_to_expr(function.args, schema, planner_context)?;
            return super::arrow_cast::create_arrow_cast(args, schema);
        }

        // Could not find the relevant function, so return an error
        Err(DataFusionError::Plan(format!("Invalid function '{name}'")))
    }

    pub(super) fn sql_named_function_to_expr(
        &self,
        expr: SQLExpr,
        fun: BuiltinScalarFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args = vec![self.sql_expr_to_logical_expr(expr, schema, planner_context)?];
        Ok(Expr::ScalarFunction { fun, args })
    }

    pub(super) fn find_window_func(&self, name: &str) -> Result<WindowFunction> {
        window_function::find_df_window_func(name)
            .or_else(|| {
                self.schema_provider
                    .get_aggregate_meta(name)
                    .map(WindowFunction::AggregateUDF)
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("There is no window function named {name}"))
            })
    }

    fn sql_fn_arg_to_logical_expr(
        &self,
        sql: FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.sql_expr_to_logical_expr(arg, schema, planner_context),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Ok(Expr::Wildcard),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.sql_expr_to_logical_expr(arg, schema, planner_context)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported qualified wildcard argument: {sql:?}"
            ))),
        }
    }

    pub(super) fn function_args_to_expr(
        &self,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        args.into_iter()
            .map(|a| self.sql_fn_arg_to_logical_expr(a, schema, planner_context))
            .collect::<Result<Vec<Expr>>>()
    }

    pub(super) fn aggregate_fn_to_expr(
        &self,
        fun: AggregateFunction,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(AggregateFunction, Vec<Expr>)> {
        let args = match fun {
            // Special case rewrite COUNT(*) to COUNT(constant)
            AggregateFunction::Count => args
                .into_iter()
                .map(|a| self.sql_fn_arg_to_logical_expr(a, schema, planner_context))
                .collect::<Result<Vec<Expr>>>()?,
            _ => self.function_args_to_expr(args, schema, planner_context)?,
        };

        Ok((fun, args))
    }
}
