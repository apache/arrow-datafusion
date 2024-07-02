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

use std::collections::HashMap;

use arrow_schema::{
    DataType, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE
};
use datafusion_common::logical_type::signature::LogicalType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    exec_err, internal_err, plan_err, Column, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::builder::get_unnested_columns;
use datafusion_expr::expr::{Alias, GroupingSet, Unnest, WindowFunction};
use datafusion_expr::utils::{expr_as_column_expr, find_column_exprs};
use datafusion_expr::{expr_vec_fmt, Expr, ExprSchemable, LogicalPlan};
use sqlparser::ast::Ident;
use datafusion_common::logical_type::{TypeRelation, ExtensionType};

/// Make a best-effort attempt at resolving all columns in the expression tree
pub(crate) fn resolve_columns(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.clone()
        .transform_up(|nested_expr| {
            match nested_expr {
                Expr::Column(col) => {
                    let (qualifier, field) =
                        plan.schema().qualified_field_from_column(&col)?;
                    Ok(Transformed::yes(Expr::Column(Column::from((
                        qualifier, field,
                    )))))
                }
                _ => {
                    // keep recursing
                    Ok(Transformed::no(nested_expr))
                }
            }
        })
        .data()
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
    expr.clone()
        .transform_down(|nested_expr| {
            if base_exprs.contains(&nested_expr) {
                Ok(Transformed::yes(expr_as_column_expr(&nested_expr, plan)?))
            } else {
                Ok(Transformed::no(nested_expr))
            }
        })
        .data()
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
/// in the select_exprs list, if the index is within the bounds and it is indeed a position literal,
/// otherwise, returns planning error.
/// If input expression is not an int literal, returns expression as-is.
pub(crate) fn resolve_positions_to_exprs(
    expr: Expr,
    select_exprs: &[Expr],
) -> Result<Expr> {
    match expr {
        // sql_expr_to_logical_expr maps number to i64
        // https://github.com/apache/datafusion/blob/8d175c759e17190980f270b5894348dc4cff9bbf/datafusion/src/sql/planner.rs#L882-L887
        Expr::Literal(ScalarValue::Int64(Some(position)))
            if position > 0_i64 && position <= select_exprs.len() as i64 =>
        {
            let index = (position - 1) as usize;
            let select_expr = &select_exprs[index];
            Ok(match select_expr {
                Expr::Alias(Alias { expr, .. }) => *expr.clone(),
                _ => select_expr.clone(),
            })
        }
        Expr::Literal(ScalarValue::Int64(Some(position))) => plan_err!(
            "Cannot find column with position {} in SELECT clause. Valid columns: 1 to {}",
            position, select_exprs.len()
        ),
        _ => Ok(expr),
    }
}

/// Rebuilds an `Expr` with columns that refer to aliases replaced by the
/// alias' underlying `Expr`.
pub(crate) fn resolve_aliases_to_exprs(
    expr: Expr,
    aliases: &HashMap<String, Expr>,
) -> Result<Expr> {
    expr.transform_up(|nested_expr| match nested_expr {
        Expr::Column(c) if c.relation.is_none() => {
            if let Some(aliased_expr) = aliases.get(&c.name) {
                Ok(Transformed::yes(aliased_expr.clone()))
            } else {
                Ok(Transformed::no(Expr::Column(c)))
            }
        }
        _ => Ok(Transformed::no(nested_expr)),
    })
    .data()
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
) -> Result<TypeRelation> {
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
        Ok(DataType::Decimal256(precision, scale).into())
    } else {
        Ok(DataType::Decimal128(precision, scale).into())
    }
}

// Normalize an owned identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}

/// The context is we want to rewrite unnest() into InnerProjection->Unnest->OuterProjection
/// Given an expression which contains unnest expr as one of its children,
/// Try transform depends on unnest type
/// - For list column: unnest(col) with type list -> unnest(col) with type list::item
/// - For struct column: unnest(struct(field1, field2)) -> unnest(struct).field1, unnest(struct).field2
/// The transformed exprs will be used in the outer projection
pub(crate) fn recursive_transform_unnest(
    input: &LogicalPlan,
    unnest_placeholder_columns: &mut Vec<String>,
    inner_projection_exprs: &mut Vec<Expr>,
    original_expr: Expr,
) -> Result<Vec<Expr>> {
    let mut transform =
        |unnest_expr: &Expr, expr_in_unnest: &Expr| -> Result<Vec<Expr>> {
            // Full context, we are trying to plan the execution as InnerProjection->Unnest->OuterProjection
            // inside unnest execution, each column inside the inner projection
            // will be transformed into new columns. Thus we need to keep track of these placeholding column names
            let placeholder_name = unnest_expr.display_name()?;

            unnest_placeholder_columns.push(placeholder_name.clone());
            // Add alias for the argument expression, to avoid naming conflicts
            // with other expressions in the select list. For example: `select unnest(col1), col1 from t`.
            // this extra projection is used to unnest transforming
            inner_projection_exprs
                .push(expr_in_unnest.clone().alias(placeholder_name.clone()));
            let schema = input.schema();

            let (data_type, _) = expr_in_unnest.data_type_and_nullable(schema)?;

            let outer_projection_columns =
                get_unnested_columns(&placeholder_name, &data_type)?;
            let expr = outer_projection_columns
                .iter()
                .map(|col| Expr::Column(col.0.clone()))
                .collect::<Vec<_>>();
            Ok(expr)
        };
    // expr transformed maybe either the same, or different from the originals exprs
    // for example:
    // - unnest(struct_col) will be transformed into unnest(struct_col).field1, unnest(struct_col).field2
    // - unnest(array_col) will be transformed into unnest(array_col)
    // - unnest(array_col) + 1 will be transformed into unnest(array_col) + 1

    // Specifically handle root level unnest expr, this is the only place
    // unnest on struct can be handled
    if let Expr::Unnest(Unnest { expr: ref arg }) = original_expr {
        return transform(&original_expr, arg);
    }
    let Transformed {
            data: transformed_expr,
            transformed,
            tnr: _,
        } = original_expr.transform_up(|expr: Expr| {
            if let Expr::Unnest(Unnest { expr: ref arg }) = expr {
                let (data_type, _) = arg.data_type_and_nullable(input.schema())?;
                if let LogicalType::Struct(_) = data_type.logical() {
                    return internal_err!("unnest on struct can ony be applied at the root level of select expression");
                }
                let transformed_exprs = transform(&expr, arg)?;
                Ok(Transformed::yes(transformed_exprs[0].clone()))
            } else {
                Ok(Transformed::no(expr))
            }
        })?;

    if !transformed {
        if matches!(&transformed_expr, Expr::Column(_)) {
            inner_projection_exprs.push(transformed_expr.clone());
            Ok(vec![transformed_expr])
        } else {
            // We need to evaluate the expr in the inner projection,
            // outer projection just select its name
            let column_name = transformed_expr.display_name()?;
            inner_projection_exprs.push(transformed_expr);
            Ok(vec![Expr::Column(Column::from_name(column_name))])
        }
    } else {
        Ok(vec![transformed_expr])
    }
}

// write test for recursive_transform_unnest
#[cfg(test)]
mod tests {
    use std::{ops::Add, sync::Arc};

    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow_schema::Fields;
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{col, lit, unnest, EmptyRelation, LogicalPlan};
    use datafusion_functions_aggregate::expr_fn::count;

    use crate::utils::{recursive_transform_unnest, resolve_positions_to_exprs};

    #[test]
    fn test_recursive_transform_unnest() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new(
                "struct_col",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("field1", ArrowDataType::Int32, false),
                    Field::new("field2", ArrowDataType::Int32, false),
                ])),
                false,
            ),
            Field::new(
                "array_col",
                ArrowDataType::List(Arc::new(Field::new(
                    "item",
                    ArrowDataType::Int64,
                    true,
                ))),
                true,
            ),
            Field::new("int_col", ArrowDataType::Int32, false),
        ]);

        let dfschema = DFSchema::try_from(schema)?;

        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(dfschema),
        });

        let mut unnest_placeholder_columns = vec![];
        let mut inner_projection_exprs = vec![];

        // unnest(struct_col)
        let original_expr = unnest(col("struct_col"));
        let transformed_exprs = recursive_transform_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            original_expr,
        )?;
        assert_eq!(
            transformed_exprs,
            vec![
                col("unnest(struct_col).field1"),
                col("unnest(struct_col).field2"),
            ]
        );
        assert_eq!(unnest_placeholder_columns, vec!["unnest(struct_col)"]);
        // still reference struct_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![col("struct_col").alias("unnest(struct_col)"),]
        );

        // unnest(array_col) + 1
        let original_expr = unnest(col("array_col")).add(lit(1i64));
        let transformed_exprs = recursive_transform_unnest(
            &input,
            &mut unnest_placeholder_columns,
            &mut inner_projection_exprs,
            original_expr,
        )?;
        assert_eq!(
            unnest_placeholder_columns,
            vec!["unnest(struct_col)", "unnest(array_col)"]
        );
        // only transform the unnest children
        assert_eq!(
            transformed_exprs,
            vec![col("unnest(array_col)").add(lit(1i64))]
        );

        // keep appending to the current vector
        // still reference array_col in original schema but with alias,
        // to avoid colliding with the projection on the column itself if any
        assert_eq!(
            inner_projection_exprs,
            vec![
                col("struct_col").alias("unnest(struct_col)"),
                col("array_col").alias("unnest(array_col)")
            ]
        );

        Ok(())
    }

    #[test]
    fn test_resolve_positions_to_exprs() -> Result<()> {
        let select_exprs = vec![col("c1"), col("c2"), count(lit(1))];

        // Assert 1 resolved as first column in select list
        let resolved = resolve_positions_to_exprs(lit(1i64), &select_exprs)?;
        assert_eq!(resolved, col("c1"));

        // Assert error if index out of select clause bounds
        let resolved = resolve_positions_to_exprs(lit(-1i64), &select_exprs);
        assert!(resolved.is_err_and(|e| e.message().contains(
            "Cannot find column with position -1 in SELECT clause. Valid columns: 1 to 3"
        )));

        let resolved = resolve_positions_to_exprs(lit(5i64), &select_exprs);
        assert!(resolved.is_err_and(|e| e.message().contains(
            "Cannot find column with position 5 in SELECT clause. Valid columns: 1 to 3"
        )));

        // Assert expression returned as-is
        let resolved = resolve_positions_to_exprs(lit("text"), &select_exprs)?;
        assert_eq!(resolved, lit("text"));

        let resolved = resolve_positions_to_exprs(col("fake"), &select_exprs)?;
        assert_eq!(resolved, col("fake"));

        Ok(())
    }
}
