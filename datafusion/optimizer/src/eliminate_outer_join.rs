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

//! Optimizer rule to eliminate left/right/full join to inner join if possible.
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::logical_plan::{Join, JoinType, LogicalPlan};
use datafusion_expr::{Expr, Operator};

use crate::optimizer::ApplyOrder;
use datafusion_expr::expr::{BinaryExpr, Cast, TryCast};
use std::sync::Arc;

#[derive(Default)]
///
/// Attempt to replace outer joins with inner joins.
///
/// Outer joins are typically more expensive to compute at runtime
/// than inner joins and prevent various forms of predicate pushdown
/// and other optimizations, so removing them if possible is beneficial.
///
/// Inner joins filter out rows that do match. Outer joins pass rows
/// that do not match padded with nulls. If there is a filter in the
/// query that would filter any such null rows after the join the rows
/// introduced by the outer join are filtered.
///
/// For example, in the `select ... from a left join b on ... where b.xx = 100;`
///
/// For rows when `b.xx` is null (as it would be after an outer join),
/// the `b.xx = 100` predicate filters them out and there is no
/// need to produce null rows for output.
///
/// Generally, an outer join can be rewritten to inner join if the
/// filters from the WHERE clause return false while any inputs are
/// null and columns of those quals are come from nullable side of
/// outer join.
pub struct EliminateOuterJoin;

impl EliminateOuterJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Attempt to eliminate outer joins.
impl OptimizerRule for EliminateOuterJoin {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => match filter.input.as_ref() {
                LogicalPlan::Join(join) => {
                    let mut non_nullable_cols: Vec<Column> = vec![];

                    extract_non_nullable_columns(
                        &filter.predicate,
                        &mut non_nullable_cols,
                        join.left.schema(),
                        join.right.schema(),
                        true,
                    )?;

                    let new_join_type = if join.join_type.is_outer() {
                        let mut left_non_nullable = false;
                        let mut right_non_nullable = false;
                        for col in non_nullable_cols.iter() {
                            if join.left.schema().has_column(col) {
                                left_non_nullable = true;
                            }
                            if join.right.schema().has_column(col) {
                                right_non_nullable = true;
                            }
                        }
                        eliminate_outer(
                            join.join_type,
                            left_non_nullable,
                            right_non_nullable,
                        )
                    } else {
                        join.join_type
                    };
                    let new_join = LogicalPlan::Join(Join {
                        left: Arc::new((*join.left).clone()),
                        right: Arc::new((*join.right).clone()),
                        join_type: new_join_type,
                        join_constraint: join.join_constraint,
                        on: join.on.clone(),
                        filter: join.filter.clone(),
                        schema: join.schema.clone(),
                        null_equals_null: join.null_equals_null,
                    });
                    let new_plan = plan.with_new_inputs(&[new_join])?;
                    Ok(Some(new_plan))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_outer_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

pub fn eliminate_outer(
    join_type: JoinType,
    left_non_nullable: bool,
    right_non_nullable: bool,
) -> JoinType {
    let mut new_join_type = join_type;
    match join_type {
        JoinType::Left => {
            if right_non_nullable {
                new_join_type = JoinType::Inner;
            }
        }
        JoinType::Right => {
            if left_non_nullable {
                new_join_type = JoinType::Inner;
            }
        }
        JoinType::Full => {
            if left_non_nullable && right_non_nullable {
                new_join_type = JoinType::Inner;
            } else if left_non_nullable {
                new_join_type = JoinType::Left;
            } else if right_non_nullable {
                new_join_type = JoinType::Right;
            }
        }
        _ => {}
    }
    new_join_type
}

/// Recursively traverses expr, if expr returns false when
/// any inputs are null, treats columns of both sides as non_nullable columns.
///
/// For and/or expr, extracts from all sub exprs and merges the columns.
/// For or expr, if one of sub exprs returns true, discards all columns from or expr.
/// For IS NOT NULL/NOT expr, always returns false for NULL input.
///     extracts columns from these exprs.
/// For all other exprs, fall through
fn extract_non_nullable_columns(
    expr: &Expr,
    non_nullable_cols: &mut Vec<Column>,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
    top_level: bool,
) -> Result<()> {
    match expr {
        Expr::Column(col) => {
            non_nullable_cols.push(col.clone());
            Ok(())
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            // If one of the inputs are null for these operators, the results should be false.
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                extract_non_nullable_columns(
                    left,
                    non_nullable_cols,
                    left_schema,
                    right_schema,
                    false,
                )?;
                extract_non_nullable_columns(
                    right,
                    non_nullable_cols,
                    left_schema,
                    right_schema,
                    false,
                )
            }
            Operator::And | Operator::Or => {
                // treat And as Or if does not from top level, such as
                // not (c1 < 10 and c2 > 100)
                if top_level && *op == Operator::And {
                    extract_non_nullable_columns(
                        left,
                        non_nullable_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    )?;
                    extract_non_nullable_columns(
                        right,
                        non_nullable_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    )?;
                    return Ok(());
                }

                let mut left_non_nullable_cols: Vec<Column> = vec![];
                let mut right_non_nullable_cols: Vec<Column> = vec![];

                extract_non_nullable_columns(
                    left,
                    &mut left_non_nullable_cols,
                    left_schema,
                    right_schema,
                    top_level,
                )?;
                extract_non_nullable_columns(
                    right,
                    &mut right_non_nullable_cols,
                    left_schema,
                    right_schema,
                    top_level,
                )?;

                // for query: select *** from a left join b where b.c1 ... or b.c2 ...
                // this can be eliminated to inner join.
                // for query: select *** from a left join b where a.c1 ... or b.c2 ...
                // this can not be eliminated.
                // If columns of relation exist in both sub exprs, any columns of this relation
                // can be added to non nullable columns.
                if !left_non_nullable_cols.is_empty()
                    && !right_non_nullable_cols.is_empty()
                {
                    for left_col in &left_non_nullable_cols {
                        for right_col in &right_non_nullable_cols {
                            if (left_schema.has_column(left_col)
                                && left_schema.has_column(right_col))
                                || (right_schema.has_column(left_col)
                                    && right_schema.has_column(right_col))
                            {
                                non_nullable_cols.push(left_col.clone());
                                break;
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        },
        Expr::Not(arg) => extract_non_nullable_columns(
            arg,
            non_nullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        Expr::IsNotNull(arg) => {
            if !top_level {
                return Ok(());
            }
            extract_non_nullable_columns(
                arg,
                non_nullable_cols,
                left_schema,
                right_schema,
                false,
            )
        }
        Expr::Cast(Cast { expr, data_type: _ })
        | Expr::TryCast(TryCast { expr, data_type: _ }) => extract_non_nullable_columns(
            expr,
            non_nullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{
        binary_expr, cast, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        try_cast,
        Operator::{And, Or},
    };

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateOuterJoin::new()), plan, expected)
    }

    #[test]
    fn eliminate_left_with_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_null())?
            .build()?;
        let expected = "\
        Filter: t2.b IS NULL\
        \n  Left Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_left_with_not_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_not_null())?
            .build()?;
        let expected = "\
        Filter: t2.b IS NOT NULL\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_right_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                Or,
                col("t1.c").lt(lit(20u32)),
            ))?
            .build()?;
        let expected = "\
        Filter: t1.b > UInt32(10) OR t1.c < UInt32(20)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_full_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;
        let expected = "\
        Filter: t1.b > UInt32(10) AND t2.c < UInt32(20)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_full_with_type_cast() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                cast(col("t1.b"), LogicalType::Int64).gt(lit(10u32)),
                And,
                try_cast(col("t2.c"), LogicalType::Int64).lt(lit(20u32)),
            ))?
            .build()?;
        let expected = "\
        Filter: CAST(t1.b AS Int64) > UInt32(10) AND TRY_CAST(t2.c AS Int64) < UInt32(20)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(&plan, expected)
    }
}
