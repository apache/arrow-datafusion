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

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Column, DFField, DFSchema, DFSchemaRef, Result};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::Expr::{Alias, Exists, InSubquery, ScalarSubquery};
use datafusion_expr::{
    aggregate_function, count, expr, lit, window_function, Aggregate, Expr, Filter,
    LogicalPlan, Projection, Sort, Subquery, Window,
};
use std::string::ToString;
use std::sync::Arc;

use crate::analyzer::AnalyzerRule;

pub const COUNT_STAR: &str = "COUNT(*)";

/// Rewrite `Count(Expr:Wildcard)` to `Count(Expr:Literal)`.
///
/// Resolves issue: <https://github.com/apache/arrow-datafusion/issues/5473>
#[derive(Default)]
pub struct CountWildcardRule {}

impl CountWildcardRule {
    pub fn new() -> Self {
        CountWildcardRule {}
    }
}

impl AnalyzerRule for CountWildcardRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down(&analyze_internal)
    }

    fn name(&self) -> &str {
        "count_wildcard_rule"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut rewriter = CountWildcardRewriter {};
    match plan {
        LogicalPlan::Window(window) => {
            let window_expr = window
                .window_expr
                .iter()
                .map(|expr| expr.clone().rewrite(&mut rewriter).unwrap())
                .collect::<Vec<Expr>>();

            Ok(Transformed::Yes(LogicalPlan::Window(Window {
                input: window.input.clone(),
                window_expr,
                schema: rewrite_schema(&window.schema),
            })))
        }
        LogicalPlan::Aggregate(agg) => {
            let aggr_expr = agg
                .aggr_expr
                .iter()
                .map(|expr| expr.clone().rewrite(&mut rewriter).unwrap())
                .collect();

            Ok(Transformed::Yes(LogicalPlan::Aggregate(
                Aggregate::try_new_with_schema(
                    agg.input.clone(),
                    agg.group_expr.clone(),
                    aggr_expr,
                    rewrite_schema(&agg.schema),
                )?,
            )))
        }
        LogicalPlan::Sort(Sort { expr, input, fetch }) => {
            let sort_expr = expr
                .iter()
                .map(|expr| expr.clone().rewrite(&mut rewriter).unwrap())
                .collect();
            Ok(Transformed::Yes(LogicalPlan::Sort(Sort {
                expr: sort_expr,
                input,
                fetch,
            })))
        }
        LogicalPlan::Projection(projection) => {
            let projection_expr = projection
                .expr
                .iter()
                .map(|expr| expr.clone().rewrite(&mut rewriter).unwrap())
                .collect();
            Ok(Transformed::Yes(LogicalPlan::Projection(
                Projection::try_new_with_schema(
                    projection_expr,
                    projection.input,
                    // rewrite_schema(projection.schema.clone()),
                    rewrite_schema(&projection.schema),
                )?,
            )))
        }
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            let predicate = predicate.rewrite(&mut rewriter).unwrap();
            Ok(Transformed::Yes(LogicalPlan::Filter(
                Filter::try_new(predicate, input).unwrap(),
            )))
        }

        _ => Ok(Transformed::No(plan)),
    }
}

struct CountWildcardRewriter {}

impl TreeNodeRewriter for CountWildcardRewriter {
    type N = Expr;

    fn mutate(&mut self, old_expr: Expr) -> Result<Expr> {
        let new_expr = match old_expr.clone() {
            Expr::Column(Column { name, relation }) if name.contains(COUNT_STAR) => {
                Expr::Column(Column {
                    name: name.replace(
                        COUNT_STAR,
                        count(lit(COUNT_STAR_EXPANSION)).to_string().as_str(),
                    ),
                    relation: relation.clone(),
                })
            }
            Expr::WindowFunction(expr::WindowFunction {
                fun:
                    window_function::WindowFunction::AggregateFunction(
                        aggregate_function::AggregateFunction::Count,
                    ),
                args,
                partition_by,
                order_by,
                window_frame,
            }) if args.len() == 1 => match args[0] {
                Expr::Wildcard => {
                    Expr::WindowFunction(datafusion_expr::expr::WindowFunction {
                        fun: window_function::WindowFunction::AggregateFunction(
                            aggregate_function::AggregateFunction::Count,
                        ),
                        args: vec![lit(COUNT_STAR_EXPANSION)],
                        partition_by,
                        order_by,
                        window_frame,
                    })
                }

                _ => old_expr,
            },
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::Count,
                args,
                distinct,
                filter,
            }) if args.len() == 1 => match args[0] {
                Expr::Wildcard => Expr::AggregateFunction(AggregateFunction {
                    fun: aggregate_function::AggregateFunction::Count,
                    args: vec![lit(COUNT_STAR_EXPANSION)],
                    distinct,
                    filter,
                }),
                _ => old_expr,
            },

            ScalarSubquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => {
                let new_plan = subquery
                    .as_ref()
                    .clone()
                    .transform_down(&analyze_internal)
                    .unwrap();
                ScalarSubquery(Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns,
                })
            }
            InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_plan = subquery
                    .subquery
                    .as_ref()
                    .clone()
                    .transform_down(&analyze_internal)
                    .unwrap();

                InSubquery {
                    expr,
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                    },
                    negated,
                }
            }
            Exists { subquery, negated } => {
                let new_plan = subquery
                    .subquery
                    .as_ref()
                    .clone()
                    .transform_down(&analyze_internal)
                    .unwrap();

                Exists {
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                    },
                    negated,
                }
            }
            Alias(expr, name) => Alias(expr, replace_count_star(name)),
            _ => old_expr,
        };
        Ok(new_expr)
    }
}
fn rewrite_schema(schema: &DFSchema) -> DFSchemaRef {
    let new_fields = schema
        .fields()
        .iter()
        .map(|field| {
            DFField::new(
                field.qualifier().cloned(),
                &replace_count_star(field.field().name().clone()),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect::<Vec<DFField>>();

    DFSchemaRef::new(
        DFSchema::new_with_metadata(new_fields, schema.metadata().clone()).unwrap(),
    )
}

fn replace_count_star(name: String) -> String {
    if name.contains(COUNT_STAR) {
        name.replace(
            COUNT_STAR,
            count(lit(COUNT_STAR_EXPANSION)).to_string().as_str(),
        )
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::Analyzer;
    use crate::test::*;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::expr::Sort;
    use datafusion_expr::{
        col, count, exists, expr, in_subquery, lit, logical_plan::LogicalPlanBuilder,
        max, scalar_subquery, AggregateFunction, Expr, WindowFrame, WindowFrameBound,
        WindowFrameUnits, WindowFunction,
    };

    fn assert_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_analyzed_plan_eq_display_indent(
            Arc::new(CountWildcardRule::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn test_count_wildcard_on_sort() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("b")], vec![count(Expr::Wildcard)])?
            .project(vec![count(Expr::Wildcard)])?
            .sort(vec![count(Expr::Wildcard).sort(true, false)])?
            .build()?;
        let expected = "Sort: COUNT(UInt8(1)) ASC NULLS LAST [COUNT(UInt8(1)):Int64;N]\
          \n  Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
          \n    Aggregate: groupBy=[[test.b]], aggr=[[COUNT(UInt8(1))]] [b:UInt32, COUNT(UInt8(1)):Int64;N]\
          \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_where_in() -> Result<()> {
        let table_scan_t1 = test_table_scan_with_name("t1")?;
        let table_scan_t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(table_scan_t1)
            .filter(in_subquery(
                col("a"),
                Arc::new(
                    LogicalPlanBuilder::from(table_scan_t2)
                        .aggregate(Vec::<Expr>::new(), vec![count(Expr::Wildcard)])?
                        .project(vec![count(Expr::Wildcard)])?
                        .build()?,
                ),
            ))?
            .build()?;

        let expected = "Filter: t1.a IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
              \n  Subquery: [COUNT(UInt8(1)):Int64;N]\
              \n    Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
              \n      Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]] [COUNT(UInt8(1)):Int64;N]\
              \n        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
              \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_where_exists() -> Result<()> {
        let table_scan_t1 = test_table_scan_with_name("t1")?;
        let table_scan_t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(table_scan_t1)
            .filter(exists(Arc::new(
                LogicalPlanBuilder::from(table_scan_t2)
                    .aggregate(Vec::<Expr>::new(), vec![count(Expr::Wildcard)])?
                    .project(vec![count(Expr::Wildcard)])?
                    .build()?,
            )))?
            .build()?;

        let expected = "Filter: EXISTS (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
          \n  Subquery: [COUNT(UInt8(1)):Int64;N]\
          \n    Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
          \n      Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]] [COUNT(UInt8(1)):Int64;N]\
          \n        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
          \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_where_scalar_subquery() -> Result<()> {
        let table_scan_t1 = test_table_scan_with_name("t1")?;
        let table_scan_t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(table_scan_t1)
            .filter(
                scalar_subquery(Arc::new(
                    LogicalPlanBuilder::from(table_scan_t2)
                        .filter(col("t1.a").eq(col("t2.a")))?
                        .aggregate(
                            Vec::<Expr>::new(),
                            vec![count(lit(COUNT_STAR_EXPANSION))],
                        )?
                        .project(vec![count(lit(COUNT_STAR_EXPANSION))])?
                        .build()?,
                ))
                .gt(lit(ScalarValue::UInt8(Some(0)))),
            )?
            .project(vec![col("t1.a"), col("t1.b")])?
            .build()?;

        let expected = "Projection: t1.a, t1.b [a:UInt32, b:UInt32]\
              \n  Filter: (<subquery>) > UInt8(0) [a:UInt32, b:UInt32, c:UInt32]\
              \n    Subquery: [COUNT(UInt8(1)):Int64;N]\
              \n      Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
              \n        Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]] [COUNT(UInt8(1)):Int64;N]\
              \n          Filter: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32]\
              \n            TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
              \n    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![Expr::WindowFunction(expr::WindowFunction::new(
                WindowFunction::AggregateFunction(AggregateFunction::Count),
                vec![Expr::Wildcard],
                vec![],
                vec![Expr::Sort(Sort::new(Box::new(col("a")), false, true))],
                WindowFrame {
                    units: WindowFrameUnits::Range,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::UInt32(Some(
                        6,
                    ))),
                    end_bound: WindowFrameBound::Following(ScalarValue::UInt32(Some(2))),
                },
            ))])?
            .project(vec![count(Expr::Wildcard)])?
            .build()?;

        let expected = "Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
              \n  WindowAggr: windowExpr=[[COUNT(UInt8(1)) ORDER BY [test.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING]] [a:UInt32, b:UInt32, c:UInt32, COUNT(UInt8(1)) ORDER BY [test.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING:Int64;N]\
              \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count(Expr::Wildcard)])?
            .project(vec![count(Expr::Wildcard)])?
            .build()?;

        let expected = "Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
              \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]] [COUNT(UInt8(1)):Int64;N]\
              \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn test_count_wildcard_on_nesting() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(count(Expr::Wildcard))])?
            .project(vec![count(Expr::Wildcard)])?
            .build()?;

        let expected = "Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]\
          \n  Aggregate: groupBy=[[]], aggr=[[MAX(COUNT(UInt8(1)))]] [MAX(COUNT(UInt8(1))):Int64;N]\
          \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(&plan, expected)
    }
}
