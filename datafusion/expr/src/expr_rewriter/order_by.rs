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

//! Rewrite for order by expressions

use crate::expr::{Alias, Sort};
use crate::expr_rewriter::normalize_col;
use crate::{Cast, Expr, ExprSchemable, LogicalPlan, TryCast};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result};

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
                Expr::Sort(Sort {
                    expr,
                    asc,
                    nulls_first,
                }) => {
                    let sort = Expr::Sort(Sort::new(
                        Box::new(rewrite_sort_col_by_aggs(*expr, plan)?),
                        asc,
                        nulls_first,
                    ));
                    Ok(sort)
                }
                expr => Ok(expr),
            }
        })
        .collect()
}

fn rewrite_sort_col_by_aggs(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    let plan_inputs = plan.inputs();

    // Joins, and Unions are not yet handled (should have a projection
    // on top of them)
    if plan_inputs.len() == 1 {
        let proj_exprs = plan.expressions();
        rewrite_in_terms_of_projection(expr, proj_exprs, plan_inputs[0])
    } else {
        Ok(expr)
    }
}

/// Rewrites a sort expression in terms of the output of the previous [`LogicalPlan`]
///
/// Example:
///
/// Given an input expression such as `col(a) + col(b) + col(c)`
///
/// into `col(a) + col("b + c")`
///
/// Remember that:
/// 1. given a projection with exprs: [a, b + c]
/// 2. t produces an output schema with two columns "a", "b + c"
fn rewrite_in_terms_of_projection(
    expr: Expr,
    proj_exprs: Vec<Expr>,
    input: &LogicalPlan,
) -> Result<Expr> {
    // assumption is that each item in exprs, such as "b + c" is
    // available as an output column named "b + c"
    expr.transform(&|expr| {
        // search for unnormalized names first such as "c1" (such as aliases)
        if let Some(found) = proj_exprs.iter().find(|a| (**a) == expr) {
            let col = Expr::Column(
                found
                    .to_field(input.schema())
                    .map(|f| f.qualified_column())?,
            );
            return Ok(Transformed::Yes(col));
        }

        // if that doesn't work, try to match the expression as an
        // output column -- however first it must be "normalized"
        // (e.g. "c1" --> "t.c1") because that normalization is done
        // at the input of the aggregate.

        let normalized_expr = if let Ok(e) = normalize_col(expr.clone(), input) {
            e
        } else {
            // The expr is not based on Aggregate plan output. Skip it.
            return Ok(Transformed::No(expr));
        };

        // expr is an actual expr like min(t.c2), but we are looking
        // for a column with the same "MIN(C2)", so translate there
        let name = normalized_expr.display_name()?;

        let search_col = Expr::Column(Column {
            relation: None,
            name,
        });

        // look for the column named the same as this expr
        if let Some(found) = proj_exprs.iter().find(|a| expr_match(&search_col, a)) {
            let found = found.clone();
            return Ok(Transformed::Yes(match normalized_expr {
                Expr::Cast(Cast { expr: _, data_type }) => Expr::Cast(Cast {
                    expr: Box::new(found),
                    data_type,
                }),
                Expr::TryCast(TryCast { expr: _, data_type }) => Expr::TryCast(TryCast {
                    expr: Box::new(found),
                    data_type,
                }),
                _ => found,
            }));
        }

        Ok(Transformed::No(expr))
    })
}

/// Does the underlying expr match e?
/// so avg(c) as average will match avgc
fn expr_match(needle: &Expr, expr: &Expr) -> bool {
    // check inside aliases
    if let Expr::Alias(Alias { expr, .. }) = &expr {
        expr.as_ref() == needle
    } else {
        expr == needle
    }
}

#[cfg(test)]
mod test {
    use std::ops::Add;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::logical_type::LogicalType;

    use crate::{
        avg, cast, col, lit, logical_plan::builder::LogicalTableSource, min, try_cast,
        LogicalPlanBuilder,
    };

    use super::*;

    #[test]
    fn rewrite_sort_cols_by_agg() {
        //  gby c1, agg: min(c2)
        let agg = make_input()
            .aggregate(
                // gby: c1
                vec![col("c1")],
                // agg: min(c2)
                vec![min(col("c2"))],
            )
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "c1 --> c1",
                input: sort(col("c1")),
                expected: sort(col("c1")),
            },
            TestCase {
                desc: "c1 + c2 --> c1 + c2",
                input: sort(col("c1") + col("c1")),
                expected: sort(col("c1") + col("c1")),
            },
            TestCase {
                desc: r#"min(c2) --> "min(c2)"#,
                input: sort(min(col("c2"))),
                expected: sort(min(col("c2"))),
            },
            TestCase {
                desc: r#"c1 + min(c2) --> "c1 + min(c2)"#,
                input: sort(col("c1") + min(col("c2"))),
                expected: sort(col("c1") + min(col("c2"))),
            },
        ];

        for case in cases {
            case.run(&agg)
        }
    }

    #[test]
    fn rewrite_sort_cols_by_agg_alias() {
        let agg = make_input()
            .aggregate(
                // gby c1
                vec![col("c1")],
                // agg: min(c2), avg(c3)
                vec![min(col("c2")), avg(col("c3"))],
            )
            .unwrap()
            //  projects out an expression "c1" that is different than the column "c1"
            .project(vec![
                // c1 + 1 as c1,
                col("c1").add(lit(1)).alias("c1"),
                // min(c2)
                min(col("c2")),
                // avg("c3") as average
                avg(col("c3")).alias("average"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "c1 --> c1  -- column *named* c1 that came out of the projection, (not t.c1)",
                input: sort(col("c1")),
                // should be "c1" not t.c1
                expected: sort(col("c1")),
            },
            TestCase {
                desc: r#"min(c2) --> "MIN(c2)" -- (column *named* "min(t.c2)"!)"#,
                input: sort(min(col("c2"))),
                expected: sort(col("MIN(t.c2)")),
            },
            TestCase {
                desc: r#"c1 + min(c2) --> "c1 + MIN(c2)" -- (column *named* "min(t.c2)"!)"#,
                input: sort(col("c1") + min(col("c2"))),
                // should be "c1" not t.c1
                expected: sort(col("c1") + col("MIN(t.c2)")),
            },
            TestCase {
                desc: r#"avg(c3) --> "AVG(t.c3)" as average (column *named* "AVG(t.c3)", aliased)"#,
                input: sort(avg(col("c3"))),
                expected: sort(col("AVG(t.c3)").alias("average")),
            },
        ];

        for case in cases {
            case.run(&agg)
        }
    }

    #[test]
    fn preserve_cast() {
        let plan = make_input()
            .project(vec![col("c2").alias("c2")])
            .unwrap()
            .project(vec![col("c2").alias("c2")])
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "Cast is preserved by rewrite_sort_cols_by_aggs",
                input: sort(cast(col("c2"), LogicalType::Int64)),
                expected: sort(cast(col("c2").alias("c2"), LogicalType::Int64)),
            },
            TestCase {
                desc: "TryCast is preserved by rewrite_sort_cols_by_aggs",
                input: sort(try_cast(col("c2"), LogicalType::Int64)),
                expected: sort(try_cast(col("c2").alias("c2"), LogicalType::Int64)),
            },
        ];

        for case in cases {
            case.run(&plan)
        }
    }

    struct TestCase {
        desc: &'static str,
        input: Expr,
        expected: Expr,
    }

    impl TestCase {
        /// calls rewrite_sort_cols_by_aggs for expr and compares it to expected_expr
        fn run(self, input_plan: &LogicalPlan) {
            let Self {
                desc,
                input,
                expected,
            } = self;

            println!("running: '{desc}'");
            let mut exprs =
                rewrite_sort_cols_by_aggs(vec![input.clone()], input_plan).unwrap();

            assert_eq!(exprs.len(), 1);
            let rewritten = exprs.pop().unwrap();

            assert_eq!(
                rewritten, expected,
                "\n\ninput:{input:?}\nrewritten:{rewritten:?}\nexpected:{expected:?}\n"
            );
        }
    }

    /// Scan of a table: t(c1 int, c2 varchar, c3 float)
    fn make_input() -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Float64, true),
        ]));
        let projection = None;
        LogicalPlanBuilder::scan(
            "t",
            Arc::new(LogicalTableSource::new(schema)),
            projection,
        )
        .unwrap()
    }

    fn sort(expr: Expr) -> Expr {
        let asc = true;
        let nulls_first = true;
        expr.sort(asc, nulls_first)
    }
}
