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

use arrow::datatypes::{DataType, DECIMAL_MAX_PRECISION};
use sqlparser::ast::Ident;

use crate::logical_plan::ExprVisitable;
use crate::logical_plan::{Expr, LogicalPlan};
use crate::scalar::ScalarValue;
use crate::{
    error::{DataFusionError, Result},
    logical_plan::{Column, ExpressionVisitor, Recursion},
};
use std::collections::HashMap;

/// Collect all deeply nested `Expr::AggregateFunction` and
/// `Expr::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub(crate) fn find_aggregate_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::AggregateFunction { .. } | Expr::AggregateUDF { .. }
        )
    })
}

/// Collect all deeply nested `Expr::Sort`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub(crate) fn find_sort_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::Sort { .. })
    })
}

/// Collect all deeply nested `Expr::WindowFunction`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub(crate) fn find_window_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::WindowFunction { .. })
    })
}

/// Collect all deeply nested `Expr::Column`'s. They are returned in order of
/// appearance (depth first), with duplicates omitted.
pub(crate) fn find_column_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| matches!(nested_expr, Expr::Column(_)))
}

/// Collect all deeply nested `Expr::TableUDF`'s. They are returned in order of
/// appearance (depth first), with duplicates omitted.
pub(crate) fn find_udtf_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::TableUDF { .. })
    })
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

// Visitor that find expressions that match a particular predicate
struct Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    test_fn: &'a F,
    exprs: Vec<Expr>,
}

impl<'a, F> Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        if (self.test_fn)(expr) {
            if !(self.exprs.contains(expr)) {
                self.exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");
    exprs
}

/// Convert any `Expr` to an `Expr::Column`.
pub(crate) fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    match expr {
        Expr::Column(_) => Ok(expr.clone()),
        _ => Ok(Expr::Column(Column::from_name(expr.name(plan.schema())?))),
    }
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
pub(crate) fn can_columns_satisfy_exprs(
    columns: &[Expr],
    exprs: &[Expr],
) -> Result<bool> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_) => Ok(()),
        _ => Err(DataFusionError::Internal(
            "Expr::Column are required".to_string(),
        )),
    })?;

    Ok(find_column_exprs(exprs).iter().all(|c| columns.contains(c)))
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
                Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                alias_name.clone(),
            )),
            Expr::Between {
                expr: nested_expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                negated: *negated,
                low: Box::new(clone_with_replacement(&**low, replacement_fn)?),
                high: Box::new(clone_with_replacement(&**high, replacement_fn)?),
            }),
            Expr::InList {
                expr: nested_expr,
                list,
                negated,
            } => Ok(Expr::InList {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                list: list
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
                negated: *negated,
            }),
            Expr::BinaryExpr { left, right, op } => Ok(Expr::BinaryExpr {
                left: Box::new(clone_with_replacement(&**left, replacement_fn)?),
                op: *op,
                right: Box::new(clone_with_replacement(&**right, replacement_fn)?),
            }),
            Expr::Case {
                expr: case_expr_opt,
                when_then_expr,
                else_expr: else_expr_opt,
            } => Ok(Expr::Case {
                expr: match case_expr_opt {
                    Some(case_expr) => Some(Box::new(clone_with_replacement(
                        &**case_expr,
                        replacement_fn,
                    )?)),
                    None => None,
                },
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(a, b)| {
                        Ok((
                            Box::new(clone_with_replacement(&**a, replacement_fn)?),
                            Box::new(clone_with_replacement(&**b, replacement_fn)?),
                        ))
                    })
                    .collect::<Result<Vec<(_, _)>>>()?,
                else_expr: match else_expr_opt {
                    Some(else_expr) => Some(Box::new(clone_with_replacement(
                        &**else_expr,
                        replacement_fn,
                    )?)),
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
            Expr::TableUDF { fun, args } => Ok(Expr::TableUDF {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|arg| clone_with_replacement(arg, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
            }),
            Expr::Negative(nested_expr) => Ok(Expr::Negative(Box::new(
                clone_with_replacement(&**nested_expr, replacement_fn)?,
            ))),
            Expr::Not(nested_expr) => Ok(Expr::Not(Box::new(clone_with_replacement(
                &**nested_expr,
                replacement_fn,
            )?))),
            Expr::IsNotNull(nested_expr) => Ok(Expr::IsNotNull(Box::new(
                clone_with_replacement(&**nested_expr, replacement_fn)?,
            ))),
            Expr::IsNull(nested_expr) => Ok(Expr::IsNull(Box::new(
                clone_with_replacement(&**nested_expr, replacement_fn)?,
            ))),
            Expr::Cast {
                expr: nested_expr,
                data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
            }),
            Expr::TryCast {
                expr: nested_expr,
                data_type,
            } => Ok(Expr::TryCast {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
            }),
            Expr::Sort {
                expr: nested_expr,
                asc,
                nulls_first,
            } => Ok(Expr::Sort {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
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
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                subquery: subquery.clone(),
                negated: *negated,
            }),
            Expr::Wildcard => Ok(Expr::Wildcard),
            Expr::QualifiedWildcard { .. } => Ok(expr.clone()),
            Expr::GetIndexedField { expr, key } => Ok(Expr::GetIndexedField {
                expr: Box::new(clone_with_replacement(expr.as_ref(), replacement_fn)?),
                key: key.clone(),
            }),
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

type WindowSortKey = Vec<Expr>;

/// Generate a sort key for a given window expr's partition_by and order_bu expr
pub(crate) fn generate_sort_key(
    partition_by: &[Expr],
    order_by: &[Expr],
) -> WindowSortKey {
    let mut sort_key = vec![];
    partition_by.iter().for_each(|e| {
        let e = e.clone().sort(true, true);
        if !sort_key.contains(&e) {
            sort_key.push(e);
        }
    });
    order_by.iter().for_each(|e| {
        if !sort_key.contains(e) {
            sort_key.push(e.clone());
        }
    });
    sort_key
}

/// given a slice of window expressions sharing the same sort key, find their common partition
/// keys.
pub(crate) fn window_expr_common_partition_keys(
    window_exprs: &[Expr],
) -> Result<&[Expr]> {
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

/// group a slice of window expression expr by their order by expressions
pub(crate) fn group_window_expr_by_sort_keys(
    window_expr: &[Expr],
) -> Result<Vec<(WindowSortKey, Vec<&Expr>)>> {
    let mut result = vec![];
    window_expr.iter().try_for_each(|expr| match expr {
        Expr::WindowFunction { partition_by, order_by, .. } => {
            let sort_key = generate_sort_key(partition_by, order_by);
            if let Some((_, values)) = result.iter_mut().find(
                |group: &&mut (WindowSortKey, Vec<&Expr>)| matches!(group, (key, _) if *key == sort_key),
            ) {
                values.push(expr);
            } else {
                result.push((sort_key, vec![expr]))
            }
            Ok(())
        }
        other => Err(DataFusionError::Internal(format!(
            "Impossibly got non-window expr {:?}",
            other,
        ))),
    })?;
    Ok(result)
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(
    precision: Option<u64>,
    scale: Option<u64>,
) -> Result<DataType> {
    match (precision, scale) {
        (None, _) | (_, None) => {
            return Err(DataFusionError::Internal(format!(
                "Decimal(precision, scale) must both be specified, got ({:?}, {:?})",
                precision, scale
            )));
        }
        (Some(p), Some(s)) => {
            // Arrow decimal is i128 meaning 38 maximum decimal digits
            if (p as usize) > DECIMAL_MAX_PRECISION || s > p {
                return Err(DataFusionError::Internal(format!(
                    "For decimal(precision, scale) precision must be less than or equal to 38 and scale can't be greater than precision. Got ({}, {})",
                    p, s
                )));
            } else {
                Ok(DataType::Decimal(p as usize, s as usize))
            }
        }
    }
}

// Normalize an identifer to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::col;
    use crate::physical_plan::aggregates::AggregateFunction;
    use datafusion_expr::window_function::WindowFunction;

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_case() -> Result<()> {
        let result = group_window_expr_by_sort_keys(&[])?;
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_window() -> Result<()> {
        let max1 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let max2 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let min3 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Min),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let sum4 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
            args: vec![col("age")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;
        let key = vec![];
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> =
            vec![(key, vec![&max1, &max2, &min3, &sum4])];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys() -> Result<()> {
        let age_asc = Expr::Sort {
            expr: Box::new(col("age")),
            asc: true,
            nulls_first: true,
        };
        let name_desc = Expr::Sort {
            expr: Box::new(col("name")),
            asc: false,
            nulls_first: true,
        };
        let created_at_desc = Expr::Sort {
            expr: Box::new(col("created_at")),
            asc: false,
            nulls_first: true,
        };
        let max1 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![age_asc.clone(), name_desc.clone()],
            window_frame: None,
        };
        let max2 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let min3 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Min),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![age_asc.clone(), name_desc.clone()],
            window_frame: None,
        };
        let sum4 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
            args: vec![col("age")],
            partition_by: vec![],
            order_by: vec![name_desc.clone(), age_asc.clone(), created_at_desc.clone()],
            window_frame: None,
        };
        // FIXME use as_ref
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;

        let key1 = vec![age_asc.clone(), name_desc.clone()];
        let key2 = vec![];
        let key3 = vec![name_desc, age_asc, created_at_desc];

        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![
            (key1, vec![&max1, &min3]),
            (key2, vec![&max2]),
            (key3, vec![&sum4]),
        ];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_find_sort_exprs() -> Result<()> {
        let exprs = &[
            Expr::WindowFunction {
                fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
                args: vec![col("name")],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort {
                        expr: Box::new(col("age")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("name")),
                        asc: false,
                        nulls_first: true,
                    },
                ],
                window_frame: None,
            },
            Expr::WindowFunction {
                fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
                args: vec![col("age")],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort {
                        expr: Box::new(col("name")),
                        asc: false,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("age")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("created_at")),
                        asc: false,
                        nulls_first: true,
                    },
                ],
                window_frame: None,
            },
        ];
        let expected = vec![
            Expr::Sort {
                expr: Box::new(col("age")),
                asc: true,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("name")),
                asc: false,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("created_at")),
                asc: false,
                nulls_first: true,
            },
        ];
        let result = find_sort_exprs(exprs);
        assert_eq!(expected, result);
        Ok(())
    }
}
