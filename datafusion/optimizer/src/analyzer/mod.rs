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

mod count_wildcard_rule;
mod inline_table_scan;
mod resolve_grouping_analytics;

use crate::analyzer::count_wildcard_rule::CountWildcardRule;
use crate::analyzer::inline_table_scan::InlineTableScan;
use crate::analyzer::resolve_grouping_analytics::ResolveGroupingAnalytics;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, VisitRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::utils::inspect_expr_pre;
use datafusion_expr::{Expr, LogicalPlan};
use log::{debug, trace};
use std::sync::Arc;
use std::time::Instant;

/// [`AnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
///
/// For example, it may resolve [`Expr]s into more specific forms such
/// as a subquery reference, to do type coercion to ensure the types
/// of operands are correct.
///
/// This is different than an [`OptimizerRule`](crate::OptimizerRule)
/// which should preserve the semantics of the LogicalPlan but compute
/// it the same result in some more optimal way.
pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}
/// A rule-based Analyzer.
#[derive(Clone)]
pub struct Analyzer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
}

impl Default for Analyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer {
    /// Create a new analyzer using the recommended list of rules
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
            Arc::new(ResolveGroupingAnalytics::new()),
            Arc::new(CountWildcardRule::new()),
            Arc::new(InlineTableScan::new()),
        ];
        Self::with_rules(rules)
    }

    /// Create a new analyzer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>) -> Self {
        Self { rules }
    }

    /// Analyze the logical plan by applying analyzer rules, and
    /// do necessary check and fail the invalid plans
    pub fn execute_and_check(
        &self,
        plan: &LogicalPlan,
        config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        let start_time = Instant::now();
        let mut new_plan = plan.clone();

        // TODO add common rule executor for Analyzer and Optimizer
        for rule in &self.rules {
            new_plan = rule.analyze(new_plan, config)?;
        }
        check_plan(&new_plan)?;
        log_plan("Final analyzed plan", &new_plan);
        debug!("Analyzer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }
}

/// Log the plan in debug/tracing mode after some part of the optimizer runs
fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
}

/// Do necessary check and fail the invalid plan
fn check_plan(plan: &LogicalPlan) -> Result<()> {
    plan.apply(&mut |plan: &LogicalPlan| {
        for expr in plan.expressions().iter() {
            // recursively look for subqueries
            inspect_expr_pre(expr, |expr| match expr {
                Expr::Exists { subquery, .. }
                | Expr::InSubquery { subquery, .. }
                | Expr::ScalarSubquery(subquery) => {
                    check_subquery_expr(plan, &subquery.subquery, expr)
                }
                _ => Ok(()),
            })?;
        }

        Ok(VisitRecursion::Continue)
    })?;

    Ok(())
}

/// Do necessary check on subquery expressions and fail the invalid plan
/// 1) Check whether the outer plan is in the allowed outer plans list to use subquery expressions,
///    the allowed while list: [Projection, Filter, Window, Aggregate, Sort, Join].
/// 2) Check whether the inner plan is in the allowed inner plans list to use correlated(outer) expressions.
/// 3) Check and validate unsupported cases to use the correlated(outer) expressions inside the subquery(inner) plans/inner expressions.
/// For example, we do not want to support to use correlated expressions as the Join conditions in the subquery plan when the Join
/// is a Full Out Join
fn check_subquery_expr(
    outer_plan: &LogicalPlan,
    inner_plan: &LogicalPlan,
    expr: &Expr,
) -> Result<()> {
    check_plan(inner_plan)?;

    // Scalar subquery should only return one column
    if matches!(expr, Expr::ScalarSubquery(subquery) if subquery.subquery.schema().fields().len() > 1)
    {
        return Err(DataFusionError::Plan(
            "Scalar subquery should only return one column".to_string(),
        ));
    }

    match outer_plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Join(_) => Ok(()),
        LogicalPlan::Sort(_) => match expr {
            Expr::InSubquery { .. } | Expr::Exists { .. } => Err(DataFusionError::Plan(
                "In/Exist subquery can not be used in Sort plan nodes".to_string(),
            )),
            Expr::ScalarSubquery(_) => Ok(()),
            _ => Ok(()),
        },
        _ => Err(DataFusionError::Plan(
            "Subquery can only be used in Projection, Filter, \
            Window functions, Aggregate, Sort and Join plan nodes"
                .to_string(),
        )),
    }?;
    check_correlations_in_subquery(outer_plan, inner_plan, expr, true)
}

// Recursively check the unsupported outer references in the sub query plan.
fn check_correlations_in_subquery(
    outer_plan: &LogicalPlan,
    inner_plan: &LogicalPlan,
    expr: &Expr,
    can_contain_outer_ref: bool,
) -> Result<()> {
    // We want to support as many operators as possible inside the correlated subquery
    if !can_contain_outer_ref && contains_outer_reference(outer_plan, inner_plan, expr) {
        return Err(DataFusionError::Plan(
            "Accessing outer reference column is not allowed in the plan".to_string(),
        ));
    }
    match inner_plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_) => {
            inner_plan.apply_children(&mut |plan| {
                check_correlations_in_subquery(
                    outer_plan,
                    plan,
                    expr,
                    can_contain_outer_ref,
                )?;
                Ok(VisitRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Join(_) => {
            // TODO support correlation columns in the subquery join
            inner_plan.apply_children(&mut |plan| {
                check_correlations_in_subquery(
                    outer_plan,
                    plan,
                    expr,
                    can_contain_outer_ref,
                )?;
                Ok(VisitRecursion::Continue)
            })?;
            Ok(())
        }
        _ => Err(DataFusionError::Plan(
            "Unsupported operator in the subquery plan.".to_string(),
        )),
    }
}

fn contains_outer_reference(
    _outer_plan: &LogicalPlan,
    _inner_plan: &LogicalPlan,
    _expr: &Expr,
) -> bool {
    // TODO check outer references
    false
}
