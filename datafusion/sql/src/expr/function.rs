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
use arrow_schema::DataType;
use datafusion_common::{
    not_impl_err, plan_datafusion_err, plan_err, DFSchema, Dependency, Result,
};
use datafusion_expr::window_frame::{check_window_frame, regularize_window_order_by};
use datafusion_expr::{
    expr, AggregateFunction, Expr, ExprSchemable, WindowFrame, WindowFunctionDefinition,
};
use datafusion_expr::{
    expr::{ScalarFunction, Unnest},
    BuiltInWindowFunction, BuiltinScalarFunction,
};
use sqlparser::ast::{
    Expr as SQLExpr, Function as SQLFunction, FunctionArg, FunctionArgExpr, WindowType,
};
use std::str::FromStr;
use strum::IntoEnumIterator;

/// Suggest a valid function based on an invalid input function name
pub fn suggest_valid_function(
    input_function_name: &str,
    is_window_func: bool,
    ctx: &dyn ContextProvider,
) -> String {
    let valid_funcs = if is_window_func {
        // All aggregate functions and builtin window functions
        let mut funcs = Vec::new();

        funcs.extend(AggregateFunction::iter().map(|func| func.to_string()));
        funcs.extend(ctx.udafs_names());
        funcs.extend(BuiltInWindowFunction::iter().map(|func| func.to_string()));
        funcs.extend(ctx.udwfs_names());

        funcs
    } else {
        // All scalar functions and aggregate functions
        let mut funcs = Vec::new();

        funcs.extend(BuiltinScalarFunction::iter().map(|func| func.to_string()));
        funcs.extend(ctx.udfs_names());
        funcs.extend(AggregateFunction::iter().map(|func| func.to_string()));
        funcs.extend(ctx.udafs_names());

        funcs
    };
    find_closest_match(valid_funcs, input_function_name)
}

/// Find the closest matching string to the target string in the candidates list, using edit distance(case insensitve)
/// Input `candidates` must not be empty otherwise it will panic
fn find_closest_match(candidates: Vec<String>, target: &str) -> String {
    let target = target.to_lowercase();
    candidates
        .into_iter()
        .min_by_key(|candidate| {
            datafusion_common::utils::datafusion_strsim::levenshtein(
                &candidate.to_lowercase(),
                &target,
            )
        })
        .expect("No candidates provided.") // Panic if `candidates` argument is empty
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn sql_function_to_expr(
        &self,
        function: SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let SQLFunction {
            name,
            args,
            over,
            distinct,
            filter,
            null_treatment,
            special: _, // true if not called with trailing parens
            order_by,
        } = function;

        // If function is a window function (it has an OVER clause),
        // it shouldn't have ordering requirement as function argument
        // required ordering should be defined in OVER clause.
        let is_function_window = over.is_some();

        let name = if name.0.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            name.to_string()
        } else {
            crate::utils::normalize_ident(name.0[0].clone())
        };

        // user-defined function (UDF) should have precedence in case it has the same name as a scalar built-in function
        if let Some(fm) = self.context_provider.get_function_meta(&name) {
            let args = self.function_args_to_expr(args, schema, planner_context)?;
            return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fm, args)));
        }

        // Build Unnest expression
        if name.eq("unnest") {
            let exprs =
                self.function_args_to_expr(args.clone(), schema, planner_context)?;
            Self::check_unnest_args(&exprs, schema)?;
            return Ok(Expr::Unnest(Unnest { exprs }));
        }

        // next, scalar built-in
        if let Ok(fun) = BuiltinScalarFunction::from_str(&name) {
            let args = self.function_args_to_expr(args, schema, planner_context)?;
            return Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)));
        };

        if !order_by.is_empty() && is_function_window {
            return plan_err!(
                "Aggregate ORDER BY is not implemented for window functions"
            );
        }

        // then, window function
        if let Some(WindowType::WindowSpec(window)) = over {
            let partition_by = window
                .partition_by
                .into_iter()
                // ignore window spec PARTITION BY for scalar values
                // as they do not change and thus do not generate new partitions
                .filter(|e| !matches!(e, sqlparser::ast::Expr::Value { .. },))
                .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                .collect::<Result<Vec<_>>>()?;
            let mut order_by = self.order_by_to_sort_expr(
                &window.order_by,
                schema,
                planner_context,
                // Numeric literals in window function ORDER BY are treated as constants
                false,
            )?;

            let func_deps = schema.functional_dependencies();
            // Find whether ties are possible in the given ordering
            let is_ordering_strict = order_by.iter().find_map(|orderby_expr| {
                if let Expr::Sort(sort_expr) = orderby_expr {
                    if let Expr::Column(col) = sort_expr.expr.as_ref() {
                        let idx = schema.index_of_column(col).ok()?;
                        return if func_deps.iter().any(|dep| {
                            dep.source_indices == vec![idx]
                                && dep.mode == Dependency::Single
                        }) {
                            Some(true)
                        } else {
                            Some(false)
                        };
                    }
                }
                Some(false)
            });

            let window_frame = window
                .window_frame
                .as_ref()
                .map(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    check_window_frame(&window_frame, order_by.len())
                        .map(|_| window_frame)
                })
                .transpose()?;

            let window_frame = if let Some(window_frame) = window_frame {
                regularize_window_order_by(&window_frame, &mut order_by)?;
                window_frame
            } else if let Some(is_ordering_strict) = is_ordering_strict {
                WindowFrame::new(Some(is_ordering_strict))
            } else {
                WindowFrame::new((!order_by.is_empty()).then_some(false))
            };

            if let Ok(fun) = self.find_window_func(&name) {
                let expr = match fun {
                    WindowFunctionDefinition::AggregateFunction(aggregate_fun) => {
                        let args =
                            self.function_args_to_expr(args, schema, planner_context)?;

                        Expr::WindowFunction(expr::WindowFunction::new(
                            WindowFunctionDefinition::AggregateFunction(aggregate_fun),
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            null_treatment,
                        ))
                    }
                    _ => Expr::WindowFunction(expr::WindowFunction::new(
                        fun,
                        self.function_args_to_expr(args, schema, planner_context)?,
                        partition_by,
                        order_by,
                        window_frame,
                        null_treatment,
                    )),
                };
                return Ok(expr);
            }
        } else {
            // User defined aggregate functions (UDAF) have precedence in case it has the same name as a scalar built-in function
            if let Some(fm) = self.context_provider.get_aggregate_meta(&name) {
                let order_by =
                    self.order_by_to_sort_expr(&order_by, schema, planner_context, true)?;
                let order_by = (!order_by.is_empty()).then_some(order_by);
                let args = self.function_args_to_expr(args, schema, planner_context)?;
                return Ok(Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                    fm, args, false, None, order_by,
                )));
            }

            // next, aggregate built-ins
            if let Ok(fun) = AggregateFunction::from_str(&name) {
                let order_by =
                    self.order_by_to_sort_expr(&order_by, schema, planner_context, true)?;
                let order_by = (!order_by.is_empty()).then_some(order_by);
                let args = self.function_args_to_expr(args, schema, planner_context)?;
                let filter: Option<Box<Expr>> = filter
                    .map(|e| self.sql_expr_to_logical_expr(*e, schema, planner_context))
                    .transpose()?
                    .map(Box::new);

                return Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                    fun,
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                )));
            };
        }

        // Could not find the relevant function, so return an error
        let suggested_func_name =
            suggest_valid_function(&name, is_function_window, self.context_provider);
        plan_err!("Invalid function '{name}'.\nDid you mean '{suggested_func_name}'?")
    }

    pub(super) fn sql_named_function_to_expr(
        &self,
        expr: SQLExpr,
        fun: BuiltinScalarFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args = vec![self.sql_expr_to_logical_expr(expr, schema, planner_context)?];
        Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)))
    }

    pub(super) fn find_window_func(
        &self,
        name: &str,
    ) -> Result<WindowFunctionDefinition> {
        expr::find_df_window_func(name)
            // next check user defined aggregates
            .or_else(|| {
                self.context_provider
                    .get_aggregate_meta(name)
                    .map(WindowFunctionDefinition::AggregateUDF)
            })
            // next check user defined window functions
            .or_else(|| {
                self.context_provider
                    .get_window_meta(name)
                    .map(WindowFunctionDefinition::WindowUDF)
            })
            .ok_or_else(|| {
                plan_datafusion_err!("There is no window function named {name}")
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
                operator: _,
            } => self.sql_expr_to_logical_expr(arg, schema, planner_context),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
                operator: _,
            } => Ok(Expr::Wildcard { qualifier: None }),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.sql_expr_to_logical_expr(arg, schema, planner_context)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Ok(Expr::Wildcard { qualifier: None })
            }
            _ => not_impl_err!("Unsupported qualified wildcard argument: {sql:?}"),
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

    pub(crate) fn check_unnest_args(args: &[Expr], schema: &DFSchema) -> Result<()> {
        // Currently only one argument is supported
        let arg = match args.len() {
            0 => {
                return plan_err!("unnest() requires at least one argument");
            }
            1 => &args[0],
            _ => {
                return not_impl_err!("unnest() does not support multiple arguments yet");
            }
        };
        // Check argument type, array types are supported
        match arg.get_type(schema)? {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _) => Ok(()),
            DataType::Struct(_) => {
                not_impl_err!("unnest() does not support struct yet")
            }
            DataType::Null => {
                not_impl_err!("unnest() does not support null yet")
            }
            _ => {
                plan_err!("unnest() can only be applied to array, struct and null")
            }
        }
    }
}
