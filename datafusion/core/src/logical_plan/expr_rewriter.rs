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

//! Expression rewriter

use super::Expr;
use crate::logical_plan::plan::Aggregate;
use crate::logical_plan::DFSchema;
use crate::logical_plan::ExprSchemable;
use crate::logical_plan::LogicalPlan;
use datafusion_common::Column;
use datafusion_common::Result;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Controls how the [ExprRewriter] recursion should proceed.
pub enum RewriteRecursion {
    /// Continue rewrite / visit this expression.
    Continue,
    /// Call [ExprRewriter::mutate()] immediately and return.
    Mutate,
    /// Do not rewrite / visit the children of this expression.
    Stop,
    /// Keep recursive but skip mutate on this expression
    Skip,
}

/// Trait for potentially recursively rewriting an [`Expr`] expression
/// tree. When passed to `Expr::rewrite`, `ExpressionVisitor::mutate` is
/// invoked recursively on all nodes of an expression tree. See the
/// comments on `Expr::rewrite` for details on its use
pub trait ExprRewriter<E: ExprRewritable = Expr>: Sized {
    /// Invoked before any children of `expr` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(&mut self, _expr: &E) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    /// Invoked after all children of `expr` have been mutated and
    /// returns a potentially modified expr.
    fn mutate(&mut self, expr: E) -> Result<E>;
}

/// a trait for marking types that are rewritable by [ExprRewriter]
pub trait ExprRewritable: Sized {
    /// rewrite the expression tree using the given [ExprRewriter]
    fn rewrite<R: ExprRewriter<Self>>(self, rewriter: &mut R) -> Result<Self>;
}

impl ExprRewritable for Expr {
    /// Performs a depth first walk of an expression and its children
    /// to rewrite an expression, consuming `self` producing a new
    /// [`Expr`].
    ///
    /// Implements a modified version of the [visitor
    /// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
    /// separate algorithms from the structure of the `Expr` tree and
    /// make it easier to write new, efficient expression
    /// transformation algorithms.
    ///
    /// For an expression tree such as
    /// ```text
    /// BinaryExpr (GT)
    ///    left: Column("foo")
    ///    right: Column("bar")
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(BinaryExpr(GT))
    /// pre_visit(Column("foo"))
    /// mutatate(Column("foo"))
    /// pre_visit(Column("bar"))
    /// mutate(Column("bar"))
    /// mutate(BinaryExpr(GT))
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`false`] is returned on a call to pre_visit, no
    /// children of that expression are visited, nor is mutate
    /// called on that expression
    ///
    fn rewrite<R>(self, rewriter: &mut R) -> Result<Self>
    where
        R: ExprRewriter<Self>,
    {
        let need_mutate = match rewriter.pre_visit(&self)? {
            RewriteRecursion::Mutate => return rewriter.mutate(self),
            RewriteRecursion::Stop => return Ok(self),
            RewriteRecursion::Continue => true,
            RewriteRecursion::Skip => false,
        };

        // recurse into all sub expressions(and cover all expression types)
        let expr = match self {
            Expr::Alias(expr, name) => Expr::Alias(rewrite_boxed(expr, rewriter)?, name),
            Expr::Column(_) => self.clone(),
            Expr::Exists { .. } => self.clone(),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: rewrite_boxed(expr, rewriter)?,
                subquery,
                negated,
            },
            Expr::ScalarSubquery(_) => self.clone(),
            Expr::ScalarVariable(ty, names) => Expr::ScalarVariable(ty, names),
            Expr::Literal(value) => Expr::Literal(value),
            Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: rewrite_boxed(left, rewriter)?,
                op,
                right: rewrite_boxed(right, rewriter)?,
            },
            Expr::Not(expr) => Expr::Not(rewrite_boxed(expr, rewriter)?),
            Expr::IsNotNull(expr) => Expr::IsNotNull(rewrite_boxed(expr, rewriter)?),
            Expr::IsNull(expr) => Expr::IsNull(rewrite_boxed(expr, rewriter)?),
            Expr::Negative(expr) => Expr::Negative(rewrite_boxed(expr, rewriter)?),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: rewrite_boxed(expr, rewriter)?,
                low: rewrite_boxed(low, rewriter)?,
                high: rewrite_boxed(high, rewriter)?,
                negated,
            },
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let expr = rewrite_option_box(expr, rewriter)?;
                let when_then_expr = when_then_expr
                    .into_iter()
                    .map(|(when, then)| {
                        Ok((
                            rewrite_boxed(when, rewriter)?,
                            rewrite_boxed(then, rewriter)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_expr = rewrite_option_box(else_expr, rewriter)?;

                Expr::Case {
                    expr,
                    when_then_expr,
                    else_expr,
                }
            }
            Expr::Cast { expr, data_type } => Expr::Cast {
                expr: rewrite_boxed(expr, rewriter)?,
                data_type,
            },
            Expr::TryCast { expr, data_type } => Expr::TryCast {
                expr: rewrite_boxed(expr, rewriter)?,
                data_type,
            },
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => Expr::Sort {
                expr: rewrite_boxed(expr, rewriter)?,
                asc,
                nulls_first,
            },
            Expr::ScalarFunction { args, fun } => Expr::ScalarFunction {
                args: rewrite_vec(args, rewriter)?,
                fun,
            },
            Expr::ScalarUDF { args, fun } => Expr::ScalarUDF {
                args: rewrite_vec(args, rewriter)?,
                fun,
            },
            Expr::TableUDF { args, fun } => Expr::TableUDF {
                args: rewrite_vec(args, rewriter)?,
                fun,
            },
            Expr::WindowFunction {
                args,
                fun,
                partition_by,
                order_by,
                window_frame,
            } => Expr::WindowFunction {
                args: rewrite_vec(args, rewriter)?,
                fun,
                partition_by: rewrite_vec(partition_by, rewriter)?,
                order_by: rewrite_vec(order_by, rewriter)?,
                window_frame,
            },
            Expr::AggregateFunction {
                args,
                fun,
                distinct,
            } => Expr::AggregateFunction {
                args: rewrite_vec(args, rewriter)?,
                fun,
                distinct,
            },
            Expr::AggregateUDF { args, fun } => Expr::AggregateUDF {
                args: rewrite_vec(args, rewriter)?,
                fun,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: rewrite_boxed(expr, rewriter)?,
                list: rewrite_vec(list, rewriter)?,
                negated,
            },
            Expr::Wildcard => Expr::Wildcard,
            Expr::QualifiedWildcard { qualifier } => {
                Expr::QualifiedWildcard { qualifier }
            }
            Expr::GetIndexedField { expr, key } => Expr::GetIndexedField {
                expr: rewrite_boxed(expr, rewriter)?,
                key,
            },
        };

        // now rewrite this expression itself
        if need_mutate {
            rewriter.mutate(expr)
        } else {
            Ok(expr)
        }
    }
}

#[allow(clippy::boxed_local)]
fn rewrite_boxed<R>(boxed_expr: Box<Expr>, rewriter: &mut R) -> Result<Box<Expr>>
where
    R: ExprRewriter,
{
    // TODO: It might be possible to avoid an allocation (the
    // Box::new) below by reusing the box.
    let expr: Expr = *boxed_expr;
    let rewritten_expr = expr.rewrite(rewriter)?;
    Ok(Box::new(rewritten_expr))
}

fn rewrite_option_box<R>(
    option_box: Option<Box<Expr>>,
    rewriter: &mut R,
) -> Result<Option<Box<Expr>>>
where
    R: ExprRewriter,
{
    option_box
        .map(|expr| rewrite_boxed(expr, rewriter))
        .transpose()
}

/// rewrite a `Vec` of `Expr`s with the rewriter
fn rewrite_vec<R>(v: Vec<Expr>, rewriter: &mut R) -> Result<Vec<Expr>>
where
    R: ExprRewriter,
{
    v.into_iter().map(|expr| expr.rewrite(rewriter)).collect()
}

/// Rewrite sort on aggregate expressions to sort on the column of aggregate output
/// For example, `max(x)` is written to `col("MAX(x)")`
pub fn rewrite_sort_cols_by_aggs(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| {
            let expr = e.into();
            match expr {
                Expr::Sort {
                    expr,
                    asc,
                    nulls_first,
                } => {
                    let sort = Expr::Sort {
                        expr: Box::new(rewrite_sort_col_by_aggs(*expr, plan)?),
                        asc,
                        nulls_first,
                    };
                    Ok(sort)
                }
                expr => Ok(expr),
            }
        })
        .collect()
}

fn rewrite_sort_col_by_aggs(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    match plan {
        LogicalPlan::Aggregate(Aggregate {
            input, aggr_expr, ..
        }) => {
            struct Rewriter<'a> {
                plan: &'a LogicalPlan,
                input: &'a LogicalPlan,
                aggr_expr: &'a Vec<Expr>,
            }

            impl<'a> ExprRewriter for Rewriter<'a> {
                fn mutate(&mut self, expr: Expr) -> Result<Expr> {
                    let normalized_expr = normalize_col(expr.clone(), self.plan);
                    if normalized_expr.is_err() {
                        // The expr is not based on Aggregate plan output. Skip it.
                        return Ok(expr);
                    }
                    let normalized_expr = normalized_expr.unwrap();
                    if let Some(found_agg) =
                        self.aggr_expr.iter().find(|a| (**a) == normalized_expr)
                    {
                        let agg = normalize_col(found_agg.clone(), self.plan)?;
                        let col = Expr::Column(
                            agg.to_field(self.input.schema())
                                .map(|f| f.qualified_column())?,
                        );
                        Ok(col)
                    } else {
                        Ok(expr)
                    }
                }
            }

            expr.rewrite(&mut Rewriter {
                plan,
                input,
                aggr_expr,
            })
        }
        LogicalPlan::Projection(_) => rewrite_sort_col_by_aggs(expr, plan.inputs()[0]),
        _ => Ok(expr),
    }
}

/// Recursively call [`Column::normalize_with_schemas`] on all Column expressions
/// in the `expr` expression tree.
pub fn normalize_col(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    normalize_col_with_schemas(expr, &plan.all_schemas(), &plan.using_columns()?)
}

/// Recursively call [`Column::normalize_with_schemas`] on all Column expressions
/// in the `expr` expression tree.
fn normalize_col_with_schemas(
    expr: Expr,
    schemas: &[&Arc<DFSchema>],
    using_columns: &[HashSet<Column>],
) -> Result<Expr> {
    struct ColumnNormalizer<'a> {
        schemas: &'a [&'a Arc<DFSchema>],
        using_columns: &'a [HashSet<Column>],
    }

    impl<'a> ExprRewriter for ColumnNormalizer<'a> {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            if let Expr::Column(c) = expr {
                Ok(Expr::Column(c.normalize_with_schemas(
                    self.schemas,
                    self.using_columns,
                )?))
            } else {
                Ok(expr)
            }
        }
    }

    expr.rewrite(&mut ColumnNormalizer {
        schemas,
        using_columns,
    })
}

/// Recursively normalize all Column expressions in a list of expression trees
pub fn normalize_cols(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| normalize_col(e.into(), plan))
        .collect()
}

/// Recursively replace all Column expressions in a given expression tree with Column expressions
/// provided by the hash map argument.
pub fn replace_col(e: Expr, replace_map: &HashMap<&Column, &Column>) -> Result<Expr> {
    struct ColumnReplacer<'a> {
        replace_map: &'a HashMap<&'a Column, &'a Column>,
    }

    impl<'a> ExprRewriter for ColumnReplacer<'a> {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            if let Expr::Column(c) = &expr {
                match self.replace_map.get(c) {
                    Some(new_c) => Ok(Expr::Column((*new_c).to_owned())),
                    None => Ok(expr),
                }
            } else {
                Ok(expr)
            }
        }
    }

    e.rewrite(&mut ColumnReplacer { replace_map })
}

/// Recursively 'unnormalize' (remove all qualifiers) from an
/// expression tree.
///
/// For example, if there were expressions like `foo.bar` this would
/// rewrite it to just `bar`.
pub fn unnormalize_col(expr: Expr) -> Expr {
    struct RemoveQualifier {}

    impl ExprRewriter for RemoveQualifier {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            if let Expr::Column(col) = expr {
                // let Column { relation: _, name } = col;
                Ok(Expr::Column(Column {
                    relation: None,
                    name: col.name,
                }))
            } else {
                Ok(expr)
            }
        }
    }

    expr.rewrite(&mut RemoveQualifier {})
        .expect("Unnormalize is infallable")
}

/// Recursively un-normalize all Column expressions in a list of expression trees
#[inline]
pub fn unnormalize_cols(exprs: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
    exprs.into_iter().map(unnormalize_col).collect()
}

/// Rewrite all table udfs to columns
/// For now it used by Projection Node (to get access to columns returning by TableUDFs Node)
pub fn rewrite_udtfs_to_columns(exprs: Vec<Expr>, schema: DFSchema) -> Vec<Expr> {
    struct ReplaceUdtfWithColumn<'a> {
        schema: &'a DFSchema,
    }
    impl<'a> ExprRewriter for ReplaceUdtfWithColumn<'a> {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            if let Expr::TableUDF { .. } = expr {
                Ok(Expr::Column(Column {
                    relation: None,
                    name: expr.name(self.schema).unwrap(),
                }))
            } else {
                Ok(expr)
            }
        }
    }

    exprs
        .into_iter()
        .map(|expr| {
            expr.rewrite(&mut ReplaceUdtfWithColumn { schema: &schema })
                .unwrap()
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::logical_plan::DFField;
    use crate::prelude::{col, lit};
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;

    #[derive(Default)]
    struct RecordingRewriter {
        v: Vec<String>,
    }
    impl ExprRewriter for RecordingRewriter {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            self.v.push(format!("Mutated {:?}", expr));
            Ok(expr)
        }

        fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
            self.v.push(format!("Previsited {:?}", expr));
            Ok(RewriteRecursion::Continue)
        }
    }

    #[test]
    fn rewriter_rewrite() {
        let mut rewriter = FooBarRewriter {};

        // rewrites "foo" --> "bar"
        let rewritten = col("state").eq(lit("foo")).rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, col("state").eq(lit("bar")));

        // doesn't wrewrite
        let rewritten = col("state").eq(lit("baz")).rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, col("state").eq(lit("baz")));
    }

    /// rewrites all "foo" string literals to "bar"
    struct FooBarRewriter {}
    impl ExprRewriter for FooBarRewriter {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            match expr {
                Expr::Literal(ScalarValue::Utf8(Some(utf8_val))) => {
                    let utf8_val = if utf8_val == "foo" {
                        "bar".to_string()
                    } else {
                        utf8_val
                    };
                    Ok(lit(utf8_val))
                }
                // otherwise, return the expression unchanged
                expr => Ok(expr),
            }
        }
    }

    #[test]
    fn normalize_cols() {
        let expr = col("a") + col("b") + col("c");

        // Schemas with some matching and some non matching cols
        let schema_a = make_schema_with_empty_metadata(vec![
            make_field("tableA", "a"),
            make_field("tableA", "aa"),
        ]);
        let schema_c = make_schema_with_empty_metadata(vec![
            make_field("tableC", "cc"),
            make_field("tableC", "c"),
        ]);
        let schema_b = make_schema_with_empty_metadata(vec![make_field("tableB", "b")]);
        // non matching
        let schema_f = make_schema_with_empty_metadata(vec![
            make_field("tableC", "f"),
            make_field("tableC", "ff"),
        ]);
        let schemas = vec![schema_c, schema_f, schema_b, schema_a]
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();
        let schemas = schemas.iter().collect::<Vec<_>>();

        let normalized_expr = normalize_col_with_schemas(expr, &schemas, &[]).unwrap();
        assert_eq!(
            normalized_expr,
            col("tableA.a") + col("tableB.b") + col("tableC.c")
        );
    }

    #[test]
    fn normalize_cols_priority() {
        let expr = col("a") + col("b");
        // Schemas with multiple matches for column a, first takes priority
        let schema_a = make_schema_with_empty_metadata(vec![make_field("tableA", "a")]);
        let schema_b = make_schema_with_empty_metadata(vec![make_field("tableB", "b")]);
        let schema_a2 = make_schema_with_empty_metadata(vec![make_field("tableA2", "a")]);
        let schemas = vec![schema_a2, schema_b, schema_a]
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();
        let schemas = schemas.iter().collect::<Vec<_>>();

        let normalized_expr = normalize_col_with_schemas(expr, &schemas, &[]).unwrap();
        assert_eq!(normalized_expr, col("tableA2.a") + col("tableB.b"));
    }

    #[test]
    fn normalize_cols_non_exist() {
        // test normalizing columns when the name doesn't exist
        let expr = col("a") + col("b");
        let schema_a = make_schema_with_empty_metadata(vec![make_field("tableA", "a")]);
        let schemas = vec![schema_a].into_iter().map(Arc::new).collect::<Vec<_>>();
        let schemas = schemas.iter().collect::<Vec<_>>();

        let error = normalize_col_with_schemas(expr, &schemas, &[])
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Column #b not found in provided schemas"
        );
    }

    #[test]
    fn unnormalize_cols() {
        let expr = col("tableA.a") + col("tableB.b");
        let unnormalized_expr = unnormalize_col(expr);
        assert_eq!(unnormalized_expr, col("a") + col("b"));
    }

    fn make_schema_with_empty_metadata(fields: Vec<DFField>) -> DFSchema {
        DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
    }

    fn make_field(relation: &str, column: &str) -> DFField {
        DFField::new(Some(relation), column, DataType::Int8, false)
    }

    #[test]
    fn rewriter_visit() {
        let mut rewriter = RecordingRewriter::default();
        col("state").eq(lit("CO")).rewrite(&mut rewriter).unwrap();

        assert_eq!(
            rewriter.v,
            vec![
                "Previsited #state = Utf8(\"CO\")",
                "Previsited #state",
                "Mutated #state",
                "Previsited Utf8(\"CO\")",
                "Mutated Utf8(\"CO\")",
                "Mutated #state = Utf8(\"CO\")"
            ]
        )
    }
}
