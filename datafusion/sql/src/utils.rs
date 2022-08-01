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

use arrow::datatypes::{DataType, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use sqlparser::ast::Ident;

use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::utils::{expr_as_column_expr, find_column_exprs};
use datafusion_expr::{Expr, LogicalPlan};
use std::collections::HashMap;

/// Make a best-effort attempt at resolving all columns in the expression tree
pub(crate) fn resolve_columns(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    clone_with_replacement(expr, &|nested_expr| {
        match nested_expr {
            Expr::Column(col) => {
                let field = plan.schema().field_from_column(col)?;
                Ok(Some(Expr::Column(field.qualified_column())))
            }
            _ => {
                // keep recursing
                Ok(None)
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
    clone_with_replacement(expr, &|nested_expr| {
        if base_exprs.contains(nested_expr) {
            Ok(Some(expr_as_column_expr(nested_expr, plan)?))
        } else {
            Ok(None)
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
        _ => Err(DataFusionError::Internal(
            "Expr::Column are required".to_string(),
        )),
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
        return Err(DataFusionError::Plan(format!(
            "{}: Expression {:?} could not be resolved from available columns: {}",
            message_prefix,
            expr,
            columns
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )));
    }
    Ok(())
}

/// Returns a cloned `Expr`, but any of the `Expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `Expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `Expr`.
///
/// The function's return type is `Result<Option<Expr>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `Expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `Expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `Expr` is not provided. The `Expr` is
///       recreated, with all of its nested `Expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
fn clone_with_replacement<F>(expr: &Expr, replacement_fn: &F) -> Result<Expr>
where
    F: Fn(&Expr) -> Result<Option<Expr>>,
{
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested expressions.
        None => match expr {
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => Ok(Expr::AggregateFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
                distinct: *distinct,
            }),
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => Ok(Expr::WindowFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<_>>>()?,
                partition_by: partition_by
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<_>>>()?,
                order_by: order_by
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<_>>>()?,
                window_frame: *window_frame,
            }),
            Expr::AggregateUDF { fun, args } => Ok(Expr::AggregateUDF {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::Alias(nested_expr, alias_name) => Ok(Expr::Alias(
                Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                alias_name.clone(),
            )),
            Expr::Between {
                expr: nested_expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                negated: *negated,
                low: Box::new(clone_with_replacement(low, replacement_fn)?),
                high: Box::new(clone_with_replacement(high, replacement_fn)?),
            }),
            Expr::InList {
                expr: nested_expr,
                list,
                negated,
            } => Ok(Expr::InList {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                list: list
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
                negated: *negated,
            }),
            Expr::BinaryExpr { left, right, op } => Ok(Expr::BinaryExpr {
                left: Box::new(clone_with_replacement(left, replacement_fn)?),
                op: *op,
                right: Box::new(clone_with_replacement(right, replacement_fn)?),
            }),
            Expr::Case {
                expr: case_expr_opt,
                when_then_expr,
                else_expr: else_expr_opt,
            } => Ok(Expr::Case {
                expr: match case_expr_opt {
                    Some(case_expr) => {
                        Some(Box::new(clone_with_replacement(case_expr, replacement_fn)?))
                    }
                    None => None,
                },
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(a, b)| {
                        Ok((
                            Box::new(clone_with_replacement(a, replacement_fn)?),
                            Box::new(clone_with_replacement(b, replacement_fn)?),
                        ))
                    })
                    .collect::<Result<Vec<(_, _)>>>()?,
                else_expr: match else_expr_opt {
                    Some(else_expr) => {
                        Some(Box::new(clone_with_replacement(else_expr, replacement_fn)?))
                    }
                    None => None,
                },
            }),
            Expr::ScalarFunction { fun, args } => Ok(Expr::ScalarFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::ScalarUDF { fun, args } => Ok(Expr::ScalarUDF {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|arg| clone_with_replacement(arg, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::Negative(nested_expr) => Ok(Expr::Negative(Box::new(
                clone_with_replacement(nested_expr, replacement_fn)?,
            ))),
            Expr::Not(nested_expr) => Ok(Expr::Not(Box::new(clone_with_replacement(
                nested_expr,
                replacement_fn,
            )?))),
            Expr::IsNotNull(nested_expr) => Ok(Expr::IsNotNull(Box::new(
                clone_with_replacement(nested_expr, replacement_fn)?,
            ))),
            Expr::IsNull(nested_expr) => Ok(Expr::IsNull(Box::new(
                clone_with_replacement(nested_expr, replacement_fn)?,
            ))),
            Expr::Cast {
                expr: nested_expr,
                data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
            }),
            Expr::TryCast {
                expr: nested_expr,
                data_type,
            } => Ok(Expr::TryCast {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
            }),
            Expr::Sort {
                expr: nested_expr,
                asc,
                nulls_first,
            } => Ok(Expr::Sort {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                asc: *asc,
                nulls_first: *nulls_first,
            }),
            Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_) => Ok(expr.clone()),
            Expr::InSubquery {
                expr: nested_expr,
                subquery,
                negated,
            } => Ok(Expr::InSubquery {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                subquery: subquery.clone(),
                negated: *negated,
            }),
            Expr::Wildcard => Ok(Expr::Wildcard),
            Expr::QualifiedWildcard { .. } => Ok(expr.clone()),
            Expr::GetIndexedField { expr, key } => Ok(Expr::GetIndexedField {
                expr: Box::new(clone_with_replacement(expr.as_ref(), replacement_fn)?),
                key: key.clone(),
            }),
            Expr::GroupingSet(set) => match set {
                GroupingSet::Rollup(exprs) => Ok(Expr::GroupingSet(GroupingSet::Rollup(
                    exprs
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                ))),
                GroupingSet::Cube(exprs) => Ok(Expr::GroupingSet(GroupingSet::Cube(
                    exprs
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                ))),
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    let mut new_lists_of_exprs = vec![];
                    for exprs in lists_of_exprs {
                        new_lists_of_exprs.push(
                            exprs
                                .iter()
                                .map(|e| clone_with_replacement(e, replacement_fn))
                                .collect::<Result<Vec<Expr>>>()?,
                        );
                    }
                    Ok(Expr::GroupingSet(GroupingSet::GroupingSets(
                        new_lists_of_exprs,
                    )))
                }
            },
        },
    }
}

/// Returns mapping of each alias (`String`) to the expression (`Expr`) it is
/// aliasing.
pub(crate) fn extract_aliases(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::Alias(nested_expr, alias_name) => {
                Some((alias_name.clone(), *nested_expr.clone()))
            }
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
                Expr::Alias(nested_expr, _alias_name) => *nested_expr.clone(),
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
    clone_with_replacement(expr, &|nested_expr| match nested_expr {
        Expr::Column(c) if c.relation.is_none() => {
            if let Some(aliased_expr) = aliases.get(&c.name) {
                Ok(Some(aliased_expr.clone()))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    })
}

/// given a slice of window expressions sharing the same sort key, find their common partition
/// keys.
pub fn window_expr_common_partition_keys(window_exprs: &[Expr]) -> Result<&[Expr]> {
    let all_partition_keys = window_exprs
        .iter()
        .map(|expr| match expr {
            Expr::WindowFunction { partition_by, .. } => Ok(partition_by),
            expr => Err(DataFusionError::Execution(format!(
                "Impossibly got non-window expr {:?}",
                expr
            ))),
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
) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as usize, s as usize),
        (Some(p), None) => (p as usize, 0),
        (None, Some(_)) => {
            return Err(DataFusionError::Internal(
                "Cannot specify only scale for decimal data type".to_string(),
            ))
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision > DECIMAL128_MAX_PRECISION || scale > precision {
        Err(DataFusionError::Internal(format!(
            "For decimal(precision, scale) precision must be less than or equal to 38 and scale can't be greater than precision. Got ({}, {})",
            precision, scale
        )))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}
