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

use crate::expr::Alias;
use crate::logical_plan::Projection;
use crate::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeTransformer};
use datafusion_common::Result;
use datafusion_common::{Column, DFSchema};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

mod order_by;
pub use order_by::rewrite_sort_cols_by_aggs;

/// Recursively call [`Column::normalize_with_schemas`] on all [`Column`] expressions
/// in the `expr` expression tree.
pub fn normalize_col(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.transform_up_old(&|expr| {
        Ok({
            if let Expr::Column(c) = expr {
                let col = LogicalPlanBuilder::normalize(plan, c)?;
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
}

/// Recursively call [`Column::normalize_with_schemas`] on all [`Column`] expressions
/// in the `expr` expression tree.
#[deprecated(
    since = "20.0.0",
    note = "use normalize_col_with_schemas_and_ambiguity_check instead"
)]
#[allow(deprecated)]
pub fn normalize_col_with_schemas(
    expr: Expr,
    schemas: &[&Arc<DFSchema>],
    using_columns: &[HashSet<Column>],
) -> Result<Expr> {
    expr.transform_up_old(&|expr| {
        Ok({
            if let Expr::Column(c) = expr {
                let col = c.normalize_with_schemas(schemas, using_columns)?;
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
}

/// See [`Column::normalize_with_schemas_and_ambiguity_check`] for usage
pub fn normalize_col_with_schemas_and_ambiguity_check(
    expr: Expr,
    schemas: &[&[&DFSchema]],
    using_columns: &[HashSet<Column>],
) -> Result<Expr> {
    expr.transform_up_old(&|expr| {
        Ok({
            if let Expr::Column(c) = expr {
                let col =
                    c.normalize_with_schemas_and_ambiguity_check(schemas, using_columns)?;
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
}

/// Recursively normalize all [`Column`] expressions in a list of expression trees
pub fn normalize_cols(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| normalize_col(e.into(), plan))
        .collect()
}

/// Recursively replace all [`Column`] expressions in a given expression tree with
/// `Column` expressions provided by the hash map argument.
pub fn replace_col(expr: Expr, replace_map: &HashMap<&Column, &Column>) -> Result<Expr> {
    expr.transform_up_old(&|expr| {
        Ok({
            if let Expr::Column(c) = &expr {
                match replace_map.get(c) {
                    Some(new_c) => Transformed::Yes(Expr::Column((*new_c).to_owned())),
                    None => Transformed::No(expr),
                }
            } else {
                Transformed::No(expr)
            }
        })
    })
}

/// Recursively 'unnormalize' (remove all qualifiers) from an
/// expression tree.
///
/// For example, if there were expressions like `foo.bar` this would
/// rewrite it to just `bar`.
pub fn unnormalize_col(expr: Expr) -> Expr {
    expr.transform_up_old(&|expr| {
        Ok({
            if let Expr::Column(c) = expr {
                let col = Column {
                    relation: None,
                    name: c.name,
                };
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
    .expect("Unnormalize is infallable")
}

/// Create a Column from the Scalar Expr
pub fn create_col_from_scalar_expr(
    scalar_expr: &Expr,
    subqry_alias: String,
) -> Result<Column> {
    match scalar_expr {
        Expr::Alias(Alias { name, .. }) => Ok(Column::new(Some(subqry_alias), name)),
        Expr::Column(Column { relation: _, name }) => {
            Ok(Column::new(Some(subqry_alias), name))
        }
        _ => {
            let scalar_column = scalar_expr.display_name()?;
            Ok(Column::new(Some(subqry_alias), scalar_column))
        }
    }
}

/// Recursively un-normalize all [`Column`] expressions in a list of expression trees
#[inline]
pub fn unnormalize_cols(exprs: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
    exprs.into_iter().map(unnormalize_col).collect()
}

/// Recursively remove all the ['OuterReferenceColumn'] and return the inside Column
/// in the expression tree.
pub fn strip_outer_reference(expr: Expr) -> Expr {
    expr.transform_up_old(&|expr| {
        Ok({
            if let Expr::OuterReferenceColumn(_, col) = expr {
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
    .expect("strip_outer_reference is infallable")
}

/// Returns plan with expressions coerced to types compatible with
/// schema types
pub fn coerce_plan_expr_for_schema(
    plan: &LogicalPlan,
    schema: &DFSchema,
) -> Result<LogicalPlan> {
    match plan {
        // special case Projection to avoid adding multiple projections
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let new_exprs =
                coerce_exprs_for_schema(expr.clone(), input.schema(), schema)?;
            let projection = Projection::try_new(new_exprs, input.clone())?;
            Ok(LogicalPlan::Projection(projection))
        }
        _ => {
            let exprs: Vec<Expr> = plan
                .schema()
                .fields()
                .iter()
                .map(|field| Expr::Column(field.qualified_column()))
                .collect();

            let new_exprs = coerce_exprs_for_schema(exprs, plan.schema(), schema)?;
            let add_project = new_exprs.iter().any(|expr| expr.try_into_col().is_err());
            if add_project {
                let projection = Projection::try_new(new_exprs, Arc::new(plan.clone()))?;
                Ok(LogicalPlan::Projection(projection))
            } else {
                Ok(plan.clone())
            }
        }
    }
}

fn coerce_exprs_for_schema(
    exprs: Vec<Expr>,
    src_schema: &DFSchema,
    dst_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, expr)| {
            let new_type = dst_schema.field(idx).data_type();
            if new_type != &expr.get_type(src_schema)? {
                match expr {
                    Expr::Alias(Alias { expr, name, .. }) => {
                        Ok(expr.cast_to(new_type, src_schema)?.alias(name))
                    }
                    _ => expr.cast_to(new_type, src_schema),
                }
            } else {
                Ok(expr.clone())
            }
        })
        .collect::<Result<_>>()
}

/// Recursively un-alias an expressions
#[inline]
pub fn unalias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(Alias { expr, .. }) => unalias(*expr),
        _ => expr,
    }
}

/// Rewrites `expr` using `rewriter`, ensuring that the output has the
/// same name as `expr` prior to rewrite, adding an alias if necessary.
///
/// This is important when optimizing plans to ensure the output
/// schema of plan nodes don't change after optimization
pub fn rewrite_preserving_name<R>(mut expr: Expr, transformer: &mut R) -> Result<Expr>
where
    R: TreeNodeTransformer<Node = Expr>,
{
    let original_name = expr.name_for_alias()?;
    expr.transform(transformer)?;
    expr.alias_if_changed(original_name)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expr::Sort;
    use crate::{col, lit, Cast};
    use arrow::datatypes::DataType;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_common::{DFField, DFSchema, ScalarValue};
    use std::ops::Add;

    #[derive(Default)]
    struct RecordingRewriter {
        v: Vec<String>,
    }

    impl TreeNodeTransformer for RecordingRewriter {
        type Node = Expr;

        fn pre_transform(&mut self, expr: &mut Expr) -> Result<TreeNodeRecursion> {
            self.v.push(format!("Previsited {expr}"));
            Ok(TreeNodeRecursion::Continue)
        }

        fn post_transform(&mut self, expr: &mut Expr) -> Result<TreeNodeRecursion> {
            self.v.push(format!("Mutated {expr}"));
            Ok(TreeNodeRecursion::Continue)
        }
    }

    #[test]
    fn rewriter_rewrite() {
        // rewrites all "foo" string literals to "bar"
        let transformer = |expr: Expr| -> Result<Transformed<Expr>> {
            match expr {
                Expr::Literal(ScalarValue::Utf8(Some(utf8_val))) => {
                    let utf8_val = if utf8_val == "foo" {
                        "bar".to_string()
                    } else {
                        utf8_val
                    };
                    Ok(Transformed::Yes(lit(utf8_val)))
                }
                // otherwise, return None
                _ => Ok(Transformed::No(expr)),
            }
        };

        // rewrites "foo" --> "bar"
        let rewritten = col("state")
            .eq(lit("foo"))
            .transform_up_old(&transformer)
            .unwrap();
        assert_eq!(rewritten, col("state").eq(lit("bar")));

        // doesn't rewrite
        let rewritten = col("state")
            .eq(lit("baz"))
            .transform_up_old(&transformer)
            .unwrap();
        assert_eq!(rewritten, col("state").eq(lit("baz")));
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
        let schemas = vec![schema_c, schema_f, schema_b, schema_a];
        let schemas = schemas.iter().collect::<Vec<_>>();

        let normalized_expr =
            normalize_col_with_schemas_and_ambiguity_check(expr, &[&schemas], &[])
                .unwrap();
        assert_eq!(
            normalized_expr,
            col("tableA.a") + col("tableB.b") + col("tableC.c")
        );
    }

    #[test]
    #[allow(deprecated)]
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
        let schema_a =
            make_schema_with_empty_metadata(vec![make_field("\"tableA\"", "a")]);
        let schemas = vec![schema_a];
        let schemas = schemas.iter().collect::<Vec<_>>();

        let error =
            normalize_col_with_schemas_and_ambiguity_check(expr, &[&schemas], &[])
                .unwrap_err()
                .strip_backtrace();
        assert_eq!(
            error,
            r#"Schema error: No field named b. Valid fields are "tableA".a."#
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
        DFField::new(Some(relation.to_string()), column, DataType::Int8, false)
    }

    #[test]
    fn rewriter_visit() {
        let mut rewriter = RecordingRewriter::default();
        let mut expr = col("state").eq(lit("CO"));
        expr.transform(&mut rewriter).unwrap();

        assert_eq!(
            rewriter.v,
            vec![
                "Previsited state = Utf8(\"CO\")",
                "Previsited state",
                "Mutated state",
                "Previsited Utf8(\"CO\")",
                "Mutated Utf8(\"CO\")",
                "Mutated state = Utf8(\"CO\")"
            ]
        )
    }

    #[test]
    fn test_rewrite_preserving_name() {
        test_rewrite(col("a"), col("a"));

        test_rewrite(col("a"), col("b"));

        // cast data types
        test_rewrite(
            col("a"),
            Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int32)),
        );

        // change literal type from i32 to i64
        test_rewrite(col("a").add(lit(1i32)), col("a").add(lit(1i64)));

        // SortExpr a+1 ==> b + 2
        test_rewrite(
            Expr::Sort(Sort::new(Box::new(col("a").add(lit(1i32))), true, false)),
            Expr::Sort(Sort::new(Box::new(col("b").add(lit(2i64))), true, false)),
        );
    }

    /// rewrites `expr_from` to `rewrite_to` using
    /// `rewrite_preserving_name` verifying the result is `expected_expr`
    fn test_rewrite(expr_from: Expr, rewrite_to: Expr) {
        struct TestTransformer {}

        impl TreeNodeTransformer for TestTransformer {
            type Node = Expr;

            fn pre_transform(
                &mut self,
                _node: &mut Self::Node,
            ) -> Result<TreeNodeRecursion> {
                Ok(TreeNodeRecursion::Continue)
            }

            fn post_transform(
                &mut self,
                _node: &mut Self::Node,
            ) -> Result<TreeNodeRecursion> {
                Ok(TreeNodeRecursion::Continue)
            }
        }

        let mut transformer = TestTransformer {};
        let expr = rewrite_preserving_name(expr_from.clone(), &mut transformer).unwrap();

        let original_name = match &expr_from {
            Expr::Sort(Sort { expr, .. }) => expr.display_name(),
            expr => expr.display_name(),
        }
        .unwrap();

        let new_name = match &expr {
            Expr::Sort(Sort { expr, .. }) => expr.display_name(),
            expr => expr.display_name(),
        }
        .unwrap();

        assert_eq!(
            original_name, new_name,
            "mismatch rewriting expr_from: {expr_from} to {rewrite_to}"
        )
    }
}
