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

use crate::analyzer::AnalyzerRule;
use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::utils::{
    add_hidden_grouping_set_expr, contains_grouping_set_without_hidden_expr,
    distinct_group_exprs, enumerate_grouping_sets, generate_grouping_ids,
};
use datafusion_expr::{
    aggregate_function, bitwise_and, bitwise_shift_right, cast, lit, Projection,
};
use datafusion_expr::{Aggregate, Expr, LogicalPlan};
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};

pub struct ResolveGroupingAnalytics;

impl ResolveGroupingAnalytics {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

// Internal column used to represent the grouping_id, used by the grouping functions.
// It is "spark_grouping_id" in Spark
const INTERNAL_GROUPING_ID: &str = "grouping_id";
// Internal column used to represent different grouping sets when there are duplicated grouping sets
const INTERNAL_GROUPING_SET_ID: &str = "grouping_set_id";

impl AnalyzerRule for ResolveGroupingAnalytics {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        plan.transform_down(&|plan| match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                group_expr,
                ..
            }) if contains_grouping_set_without_hidden_expr(&group_expr) => {
                let mut expanded_grouping = enumerate_grouping_sets(&group_expr)?;
                let mut new_project_exec = vec![];
                if let [Expr::GroupingSet(ref mut grouping_set)] = expanded_grouping.as_mut_slice() {
                    let new_agg_expr = if contains_grouping_funcs_as_agg_expr(&aggr_expr) {
                        let gid_column = Expr::HiddenColumn(
                            DataType::UInt32,
                            INTERNAL_GROUPING_ID.to_string(),
                        );
                        let hidden_name = gid_column.display_name()?;
                        let grouping_ids = generate_grouping_ids(grouping_set)?;
                        let hidden_grouping_expr = |group_set_idx: usize| Expr::HiddenExpr(Box::new(lit(grouping_ids[group_set_idx])
                            .alias(hidden_name.clone())), Box::new(gid_column.clone()));
                        add_hidden_grouping_set_expr(grouping_set, hidden_grouping_expr)?;

                        let distinct_group_by = distinct_group_exprs(&group_expr, false);
                        let mut new_agg_expr = vec![];
                        aggr_expr.into_iter().try_for_each(|expr| {
                            let new_expr = replace_grouping_func(
                                expr.clone(),
                                &distinct_group_by,
                                gid_column.clone(),
                            )?;
                            // The grouping func is rewrited to a normal expr, not the AggregateFunction anymore, remove it from the aggr_expr
                            if new_expr.ne(&expr) {
                                new_project_exec.push(new_expr);
                            } else {
                                new_agg_expr.push(new_expr);
                            }
                            Ok::<(), DataFusionError>(())
                        })?;
                        new_agg_expr
                    } else {
                        aggr_expr
                    };
                    if grouping_set.contains_duplicate_grouping() {
                        let grouping_set_id_column = Expr::HiddenColumn(
                            DataType::UInt32,
                            INTERNAL_GROUPING_SET_ID.to_string(),
                        );
                        let hidden_name = grouping_set_id_column.display_name()?;
                        let hidden_grouping_expr = |group_set_idx: usize| Expr::HiddenExpr(Box::new(lit((group_set_idx + 1) as u32)
                            .alias(hidden_name.clone())), Box::new(grouping_set_id_column.clone()));
                        add_hidden_grouping_set_expr(grouping_set, hidden_grouping_expr)?;
                    }

                    let aggregate = Aggregate::try_new(
                        input,
                        vec![Expr::GroupingSet(grouping_set.clone())],
                        new_agg_expr,
                    )?;
                    let agg_schema = aggregate.schema.clone();
                    let new_agg = LogicalPlan::Aggregate(aggregate);
                    if !new_project_exec.is_empty() {
                        let mut expr: Vec<Expr> = agg_schema
                            .fields()
                            .iter()
                            .map(|field| field.qualified_column())
                            .map(Expr::Column)
                            .collect();
                        expr.append(&mut new_project_exec);
                        Ok(Transformed::Yes(LogicalPlan::Projection(Projection::try_new(expr, Arc::new(new_agg))?)))
                    } else {
                        Ok(Transformed::Yes(new_agg))
                    }
                } else {
                    Err(DataFusionError::Plan(
                        "Invalid group by expressions, GroupingSet must be the only expression"
                            .to_string(),
                    ))
                }
            }
            _ => Ok(Transformed::No(plan)),
        })
    }
    fn name(&self) -> &str {
        "resolve_grouping_analytics"
    }
}

fn contains_grouping_funcs_as_agg_expr(aggr_expr: &[Expr]) -> bool {
    aggr_expr.iter().any(|expr| {
        matches!(
            expr,
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::Grouping,
                ..
            }) | Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::GroupingId,
                ..
            })
        )
    })
}

fn replace_grouping_func(
    expr: Expr,
    group_by_exprs: &[Expr],
    gid_column: Expr,
) -> Result<Expr> {
    expr.transform(&|expr| {
        let display_name = expr.display_name()?;
        match expr {
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::Grouping,
                args,
                ..
            }) => {
                let grouping_col = &args[0];
                match group_by_exprs.iter().position(|e| e == grouping_col) {
                    Some(idx) => Ok(Transformed::Yes(cast(
                        bitwise_and(
                            bitwise_shift_right(
                                gid_column.clone(),
                                lit((group_by_exprs.len() - 1 - idx) as u32),
                            ),
                            lit(1u32),
                        ),
                        DataType::UInt8,
                    ).alias(display_name))),
                    None => Err(DataFusionError::Plan(format!(
                        "Column of GROUPING({:?}) can't be found in GROUP BY columns {:?}",
                        grouping_col, group_by_exprs
                    ))),
                }
            }
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::GroupingId,
                args,
                ..
            }) => {
                if group_by_exprs.is_empty()
                    || (group_by_exprs.len() == args.len()
                        && group_by_exprs.iter().zip(args.iter()).all(|(g, a)| g == a))
                {
                    Ok(Transformed::Yes(gid_column.clone().alias(display_name)))
                } else {
                    Err(DataFusionError::Plan(format!(
                        "Columns of GROUPING_ID({:?}) does not match GROUP BY columns {:?}",
                        args, group_by_exprs
                    )))
                }
            }
            _ => Ok(Transformed::No(expr)),
        }
    })
}
