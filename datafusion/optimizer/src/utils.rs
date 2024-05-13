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

//! Utility functions leveraged by the query optimizer rules

use std::collections::{BTreeSet, HashMap};

use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{Column, DFSchema, DFSchemaRef, Result};
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::utils as expr_utils;
use datafusion_expr::{logical_plan::LogicalPlan, Expr, Operator};

use log::{debug, trace};

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
///
/// Returning `Ok(None)` indicates that the plan can't be optimized by the `optimizer`.
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        let new_input = optimizer.try_optimize(input, config)?;
        plan_is_changed = plan_is_changed || new_input.is_some();
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
    if plan_is_changed {
        let exprs = plan.expressions();
        plan.with_new_exprs(exprs, new_inputs).map(Some)
    } else {
        Ok(None)
    }
}

pub(crate) fn collect_subquery_cols(
    exprs: &[Expr],
    subquery_schema: DFSchemaRef,
) -> Result<BTreeSet<Column>> {
    exprs.iter().try_fold(BTreeSet::new(), |mut cols, expr| {
        let mut using_cols: Vec<Column> = vec![];
        for col in expr.to_columns()?.into_iter() {
            if subquery_schema.has_column(&col) {
                using_cols.push(col);
            }
        }

        cols.extend(using_cols);
        Result::<_>::Ok(cols)
    })
}

pub(crate) fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| {
            Column::from_qualified_name(format!("{}.{}", subquery_alias, col.name))
        })
        .collect();
    let replace_map: HashMap<&Column, &Column> =
        cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
}

/// Log the plan in debug/tracing mode after some part of the optimizer runs
pub fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
}

/// Splits a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// See [`split_conjunction_owned`] for more details and an example.
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::split_conjunction` instead"
)]
pub fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    expr_utils::split_conjunction(expr)
}

/// Splits an owned conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// This is often used to "split" filter expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::split_conjunction_owned;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_conjunction_owned to split them
/// assert_eq!(split_conjunction_owned(expr), split);
/// ```
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::split_conjunction_owned` instead"
)]
pub fn split_conjunction_owned(expr: Expr) -> Vec<Expr> {
    expr_utils::split_conjunction_owned(expr)
}

/// Splits an owned binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// This is often used to "split" expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit, Operator};
/// # use datafusion_optimizer::utils::split_binary_owned;
/// # use std::ops::Add;
/// // a=1 + b=2
/// let expr = col("a").eq(lit(1)).add(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_binary_owned to split them
/// assert_eq!(split_binary_owned(expr, Operator::Plus), split);
/// ```
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::split_binary_owned` instead"
)]
pub fn split_binary_owned(expr: Expr, op: Operator) -> Vec<Expr> {
    expr_utils::split_binary_owned(expr, op)
}

/// Splits an binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// See [`split_binary_owned`] for more details and an example.
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::split_binary` instead"
)]
pub fn split_binary(expr: &Expr, op: Operator) -> Vec<&Expr> {
    expr_utils::split_binary(expr, op)
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical AND.
///
/// Returns None if the filters array is empty.
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::conjunction;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use conjunction to join them together with `AND`
/// assert_eq!(conjunction(split), Some(expr));
/// ```
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::conjunction` instead"
)]
pub fn conjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    expr_utils::conjunction(filters)
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical OR.
///
/// Returns None if the filters array is empty.
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::disjunction` instead"
)]
pub fn disjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    expr_utils::disjunction(filters)
}

/// returns a new [LogicalPlan] that wraps `plan` in a [LogicalPlan::Filter] with
/// its predicate be all `predicates` ANDed.
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::add_filter` instead"
)]
pub fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> Result<LogicalPlan> {
    expr_utils::add_filter(plan, predicates)
}

/// Looks for correlating expressions: for example, a binary expression with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::find_join_exprs` instead"
)]
pub fn find_join_exprs(exprs: Vec<&Expr>) -> Result<(Vec<Expr>, Vec<Expr>)> {
    expr_utils::find_join_exprs(exprs)
}

/// Returns the first (and only) element in a slice, or an error
///
/// # Arguments
///
/// * `slice` - The slice to extract from
///
/// # Return value
///
/// The first element, or an error
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::only_or_err` instead"
)]
pub fn only_or_err<T>(slice: &[T]) -> Result<&T> {
    expr_utils::only_or_err(slice)
}

/// merge inputs schema into a single schema.
#[deprecated(
    since = "34.0.0",
    note = "use `datafusion_expr::utils::merge_schema` instead"
)]
pub fn merge_schema(inputs: Vec<&LogicalPlan>) -> DFSchema {
    expr_utils::merge_schema(inputs)
}

/// Handles ensuring the name of rewritten expressions is not changed.
///
/// For example, if an expression `1 + 2` is rewritten to `3`, the name of the
/// expression should be preserved: `3 as "1 + 2"`
///
/// See <https://github.com/apache/datafusion/issues/3555> for details
pub struct NamePreserver {
    use_alias: bool,
}

/// If the name of an expression is remembered, it will be preserved when
/// rewriting the expression
pub struct SavedName(Option<String>);

impl NamePreserver {
    /// Create a new NamePreserver for rewriting the `expr` that is part of the specified plan
    pub fn new(plan: &LogicalPlan) -> Self {
        Self {
            use_alias: !matches!(plan, LogicalPlan::Filter(_) | LogicalPlan::Join(_)),
        }
    }

    /// Create a new NamePreserver for rewriting the `expr`s in `Projection`
    ///
    /// This will use aliases
    pub fn new_for_projection() -> Self {
        Self { use_alias: true }
    }

    pub fn save(&self, expr: &Expr) -> Result<SavedName> {
        let original_name = if self.use_alias {
            Some(expr.name_for_alias()?.into_owned())
        } else {
            None
        };

        Ok(SavedName(original_name))
    }
}

impl SavedName {
    /// Ensures the name of the rewritten expression is preserved
    pub fn restore(self, expr: Expr) -> Result<Expr> {
        let Self(original_name) = self;
        match original_name {
            Some(name) => expr.alias_if_changed(name),
            None => Ok(expr),
        }
    }
}
