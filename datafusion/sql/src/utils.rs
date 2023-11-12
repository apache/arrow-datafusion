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

//! SQL Utility Functions

use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion_common::logical_type::LogicalType;
use datafusion_common::tree_node::{Transformed, TreeNode};
use sqlparser::ast::Ident;

use datafusion_common::{exec_err, internal_err, plan_err};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::{Alias, GroupingSet, WindowFunction};
use datafusion_expr::expr_vec_fmt;
use datafusion_expr::utils::{expr_as_column_expr, find_column_exprs};
use datafusion_expr::{Expr, LogicalPlan};
use std::collections::HashMap;

/// Make a best-effort attempt at resolving all columns in the expression tree
pub(crate) fn resolve_columns(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.clone().transform_up(&|nested_expr| {
        match nested_expr {
            Expr::Column(col) => {
                let field = plan.schema().field_from_column(&col)?;
                Ok(Transformed::Yes(Expr::Column(field.qualified_column())))
            }
            _ => {
                // keep recursing
                Ok(Transformed::No(nested_expr))
            }
        }
    })
}

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
pub(crate) fn rebase_expr(
    expr: &Expr,
    base_exprs: &[Expr],
    plan: &LogicalPlan,
) -> Result<Expr> {
    expr.clone().transform_up(&|nested_expr| {
        if base_exprs.contains(&nested_expr) {
            Ok(Transformed::Yes(expr_as_column_expr(&nested_expr, plan)?))
        } else {
            Ok(Transformed::No(nested_expr))
        }
    })
}

/// Determines if the set of `Expr`'s are a valid projection on the input
/// `Expr::Column`'s.
pub(crate) fn check_columns_satisfy_exprs(
    columns: &[Expr],
    exprs: &[Expr],
    message_prefix: &str,
) -> Result<()> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_) => Ok(()),
        _ => internal_err!("Expr::Column are required"),
    })?;
    let column_exprs = find_column_exprs(exprs);
    for e in &column_exprs {
        match e {
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, message_prefix)?;
                }
            }
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, message_prefix)?;
                }
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                for exprs in lists_of_exprs {
                    for e in exprs {
                        check_column_satisfies_expr(columns, e, message_prefix)?;
                    }
                }
            }
            _ => check_column_satisfies_expr(columns, e, message_prefix)?,
        }
    }
    Ok(())
}

fn check_column_satisfies_expr(
    columns: &[Expr],
    expr: &Expr,
    message_prefix: &str,
) -> Result<()> {
    if !columns.contains(expr) {
        return plan_err!(
            "{}: Expression {} could not be resolved from available columns: {}",
            message_prefix,
            expr,
            expr_vec_fmt!(columns)
        );
    }
    Ok(())
}

/// Returns mapping of each alias (`String`) to the expression (`Expr`) it is
/// aliasing.
pub(crate) fn extract_aliases(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::Alias(Alias { expr, name, .. }) => Some((name.clone(), *expr.clone())),
            _ => None,
        })
        .collect::<HashMap<String, Expr>>()
}

/// Given an expression that's literal int encoding position, lookup the corresponding expression
/// in the select_exprs list, if the index is within the bounds and it is indeed a position literal;
/// Otherwise, return None
pub(crate) fn resolve_positions_to_exprs(
    expr: &Expr,
    select_exprs: &[Expr],
) -> Option<Expr> {
    match expr {
        // sql_expr_to_logical_expr maps number to i64
        // https://github.com/apache/arrow-datafusion/blob/8d175c759e17190980f270b5894348dc4cff9bbf/datafusion/src/sql/planner.rs#L882-L887
        Expr::Literal(ScalarValue::Int64(Some(position)))
            if position > &0_i64 && position <= &(select_exprs.len() as i64) =>
        {
            let index = (position - 1) as usize;
            let select_expr = &select_exprs[index];
            Some(match select_expr {
                Expr::Alias(Alias { expr, .. }) => *expr.clone(),
                _ => select_expr.clone(),
            })
        }
        _ => None,
    }
}

/// Rebuilds an `Expr` with columns that refer to aliases replaced by the
/// alias' underlying `Expr`.
pub(crate) fn resolve_aliases_to_exprs(
    expr: &Expr,
    aliases: &HashMap<String, Expr>,
) -> Result<Expr> {
    expr.clone().transform_up(&|nested_expr| match nested_expr {
        Expr::Column(c) if c.relation.is_none() => {
            if let Some(aliased_expr) = aliases.get(&c.name) {
                Ok(Transformed::Yes(aliased_expr.clone()))
            } else {
                Ok(Transformed::No(Expr::Column(c)))
            }
        }
        _ => Ok(Transformed::No(nested_expr)),
    })
}

/// given a slice of window expressions sharing the same sort key, find their common partition
/// keys.
pub fn window_expr_common_partition_keys(window_exprs: &[Expr]) -> Result<&[Expr]> {
    let all_partition_keys = window_exprs
        .iter()
        .map(|expr| match expr {
            Expr::WindowFunction(WindowFunction { partition_by, .. }) => Ok(partition_by),
            Expr::Alias(Alias { expr, .. }) => match expr.as_ref() {
                Expr::WindowFunction(WindowFunction { partition_by, .. }) => {
                    Ok(partition_by)
                }
                expr => exec_err!("Impossibly got non-window expr {expr:?}"),
            },
            expr => exec_err!("Impossibly got non-window expr {expr:?}"),
        })
        .collect::<Result<Vec<_>>>()?;
    let result = all_partition_keys
        .iter()
        .min_by_key(|s| s.len())
        .ok_or_else(|| {
            DataFusionError::Execution("No window expressions found".to_owned())
        })?;
    Ok(result)
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(
    precision: Option<u64>,
    scale: Option<u64>,
) -> Result<LogicalType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            return plan_err!("Cannot specify only scale for decimal data type")
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    if precision == 0
        || precision > DECIMAL256_MAX_PRECISION
        || scale.unsigned_abs() > precision
    {
        plan_err!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 76`, and `scale <= precision`."
        )
    } else if precision > DECIMAL128_MAX_PRECISION
        && precision <= DECIMAL256_MAX_PRECISION
    {
        Ok(LogicalType::Decimal256(precision, scale))
    } else {
        Ok(LogicalType::Decimal128(precision, scale))
    }
}

// Normalize an owned identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}
