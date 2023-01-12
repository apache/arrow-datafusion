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

//! Optimizer rule to eliminate cross join to inner join if join predicates are available in filters.
use std::sync::Arc;

use crate::utils::{conjunction, split_conjunction_owned};
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DataFusionError, Result};
use datafusion_expr::expr::{BinaryExpr, Expr};
use datafusion_expr::logical_plan::{
    CrossJoin, Filter, JoinType, LogicalPlan, Projection,
};
use datafusion_expr::utils::{find_valid_equijoin_key_pair, is_valid_join_predicate};
use datafusion_expr::{build_join_schema, LogicalPlanBuilder, Operator};

#[derive(Default)]
pub struct EliminateCrossJoin;

impl EliminateCrossJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Attempt to reorder join tp eliminate cross joins to inner joins.
/// for queries:
/// 'select ... from a, b where a.x = b.y and b.xx = 100;'
/// 'select ... from a, b where (a.x = b.y and b.xx = 100) or (a.x = b.y and b.xx = 200);'
/// 'select ... from a, b, c where (a.x = b.y and b.xx = 100 and a.z = c.z)
/// or (a.x = b.y and b.xx = 200 and a.z=c.z);'
/// For above queries, the join predicate is available in filters and they are moved to
/// join nodes appropriately
/// This fix helps to improve the performance of TPCH Q19. issue#78
///
impl OptimizerRule for EliminateCrossJoin {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let input = filter.input.as_ref().clone();

                let mut all_inputs: Vec<LogicalPlan> = vec![];
                let mut possible_join_predicates: Vec<Expr> =
                    split_conjunction_owned(filter.predicate.clone());

                match &input {
                    LogicalPlan::Join(join) if (join.join_type == JoinType::Inner) => {
                        // The filter of inner join will lost, skip this rule.
                        // issue: https://github.com/apache/arrow-datafusion/issues/4844
                        if join.filter.is_some() {
                            return Ok(None);
                        }

                        flatten_join_inputs(
                            &input,
                            &mut possible_join_predicates,
                            &mut all_inputs,
                        )?;
                    }
                    LogicalPlan::CrossJoin(_) => {
                        flatten_join_inputs(
                            &input,
                            &mut possible_join_predicates,
                            &mut all_inputs,
                        )?;
                    }
                    _ => {
                        return Ok(Some(utils::optimize_children(self, plan, config)?));
                    }
                }

                let mut left = all_inputs.remove(0);
                while !all_inputs.is_empty() {
                    left = find_inner_join(
                        &left,
                        &mut all_inputs,
                        &mut possible_join_predicates,
                    )?;
                }

                left = utils::optimize_children(self, &left, config)?;

                if plan.schema() != left.schema() {
                    left = LogicalPlan::Projection(Projection::new_from_schema(
                        Arc::new(left.clone()),
                        plan.schema().clone(),
                    ));
                }

                // If the rest predicate is not empty.
                let other_predicate = conjunction(possible_join_predicates);
                if let Some(predicate) = other_predicate {
                    Ok(Some(LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(left),
                    )?)))
                } else {
                    Ok(Some(left))
                }
            }

            _ => Ok(Some(utils::optimize_children(self, plan, config)?)),
        }
    }

    fn name(&self) -> &str {
        "eliminate_cross_join"
    }
}

fn flatten_join_inputs(
    plan: &LogicalPlan,
    possible_join_predicates: &mut Vec<Expr>,
    all_inputs: &mut Vec<LogicalPlan>,
) -> Result<()> {
    let children = match plan {
        LogicalPlan::Join(join) => {
            let equijoin_predicates: Vec<Expr> = join
                .on
                .iter()
                .map(|(l, r)| Expr::eq(l.clone(), r.clone()))
                .collect();
            let non_equijoin_predicates = join
                .filter
                .as_ref()
                .map(|expr| split_conjunction_owned(expr.clone()))
                .unwrap_or_else(Vec::new);
            possible_join_predicates.extend(equijoin_predicates);
            possible_join_predicates.extend(non_equijoin_predicates);

            let left = &*(join.left);
            let right = &*(join.right);
            Ok::<Vec<&LogicalPlan>, DataFusionError>(vec![left, right])
        }
        LogicalPlan::CrossJoin(join) => {
            let left = &*(join.left);
            let right = &*(join.right);
            Ok::<Vec<&LogicalPlan>, DataFusionError>(vec![left, right])
        }
        _ => {
            return Err(DataFusionError::Plan(
                "flatten_join_inputs just can call join/cross_join".to_string(),
            ));
        }
    }?;

    for child in children.iter() {
        match *child {
            LogicalPlan::Join(left_join) => {
                if left_join.join_type == JoinType::Inner {
                    flatten_join_inputs(child, possible_join_predicates, all_inputs)?;
                } else {
                    all_inputs.push((*child).clone());
                }
            }
            LogicalPlan::CrossJoin(_) => {
                flatten_join_inputs(child, possible_join_predicates, all_inputs)?;
            }
            _ => all_inputs.push((*child).clone()),
        }
    }
    Ok(())
}

fn find_inner_join(
    left_input: &LogicalPlan,
    rights: &mut Vec<LogicalPlan>,
    possible_join_predicates: &mut Vec<Expr>,
) -> Result<LogicalPlan> {
    let mut candidate_right_input: Option<(usize, Vec<Expr>)> = None;
    for (i, right_input) in rights.iter().enumerate() {
        let predicate_schemas =
            [left_input.schema().clone(), right_input.schema().clone()];

        // equijoin_flag indicct if join_predicates contains equijoin expr.
        let (join_predicates, equijoin_flag): (Vec<Expr>, bool) =
            possible_join_predicates.iter().try_fold(
                (Vec::<Expr>::new(), false),
                |(mut join_filters, mut equijoin_flag), expr| {
                    let is_join_filter =
                        is_valid_join_predicate(expr, &predicate_schemas)?;
                    if is_join_filter {
                        join_filters.push(expr.clone());
                    }

                    let has_equijoin = match expr {
                        Expr::BinaryExpr(BinaryExpr {
                            left,
                            op: Operator::Eq,
                            right,
                        }) => find_valid_equijoin_key_pair(
                            left,
                            right,
                            left_input.schema().clone(),
                            right_input.schema().clone(),
                        )?
                        .is_some(),
                        _ => false,
                    };

                    if has_equijoin && !equijoin_flag {
                        equijoin_flag = !equijoin_flag;
                    }

                    Result::Ok((join_filters, equijoin_flag))
                },
            )?;

        // if there are equijoin expr, choose it first.
        if !join_predicates.is_empty() && equijoin_flag {
            possible_join_predicates
                .retain(|expr| join_predicates.iter().all(|filter| expr != filter));

            let join_filter = conjunction(join_predicates);
            let right_input = rights.remove(i);

            return LogicalPlanBuilder::from(left_input.clone())
                .join(
                    right_input,
                    JoinType::Inner,
                    (Vec::<Column>::new(), Vec::<Column>::new()),
                    join_filter,
                )?
                .build();
            // let join_schema = Arc::new(build_join_schema(
            //     left_input.schema(),
            //     right_input.schema(),
            //     &JoinType::Inner,
            // )?);

            // return Ok(LogicalPlan::Join(Join {
            //     left: Arc::new(left_input.clone()),
            //     right: Arc::new(right_input),
            //     join_type: JoinType::Inner,
            //     join_constraint: JoinConstraint::On,
            //     on: Vec::<(Expr, Expr)>::new(),
            //     filter: join_filter,
            //     schema: join_schema,
            //     null_equals_null: false,
            // }));
        }

        // If there is non equijoin predicate for the left and right,
        // choose the first plan whose join predicates is not empty.
        if !join_predicates.is_empty() && candidate_right_input.is_none() {
            candidate_right_input = Some((i, join_predicates));
        }
    }

    if let Some((right_idx, join_filters)) = candidate_right_input {
        let right_input = rights.remove(right_idx);
        possible_join_predicates
            .retain(|expr| join_filters.iter().all(|filter| expr != filter));
        let join_filter = conjunction(join_filters);

        LogicalPlanBuilder::from(left_input.clone())
            .join(
                right_input,
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                join_filter,
            )?
            .build()
        // let join_schema = Arc::new(build_join_schema(
        //     left_input.schema(),
        //     right_input.schema(),
        //     &JoinType::Inner,
        // )?);

        // Ok(LogicalPlan::Join(Join {
        //     left: Arc::new(left_input.clone()),
        //     right: Arc::new(right_input),
        //     join_type: JoinType::Inner,
        //     join_constraint: JoinConstraint::On,
        //     on: Vec::<(Expr, Expr)>::new(),
        //     filter: join_filter,
        //     schema: join_schema,
        //     null_equals_null: false,
        // }))
    } else {
        // if no join predicate exists, choose first with cross join
        let right = rights.remove(0);
        let join_schema = Arc::new(build_join_schema(
            left_input.schema(),
            right.schema(),
            &JoinType::Inner,
        )?);

        Ok(LogicalPlan::CrossJoin(CrossJoin {
            left: Arc::new(left_input.clone()),
            right: Arc::new(right),
            schema: join_schema,
        }))
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::Column;
    use datafusion_expr::{
        binary_expr, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        Operator::{And, Or},
    };

    use crate::optimizer::OptimizerContext;
    use crate::test::*;

    use super::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: Vec<&str>) {
        let rule = EliminateCrossJoin::new();
        let optimized_plan = rule
            .try_optimize(plan, &OptimizerContext::new())
            .unwrap()
            .expect("failed to optimize plan");
        let formatted = optimized_plan.display_indent_schema().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        assert_eq!(plan.schema(), optimized_plan.schema())
    }

    #[test]
    fn eliminate_cross_with_simple_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since filter has Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                col("t1.a").eq(col("t2.a")),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_simple_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join since filter OR expression and there is no common
        // Join predicates in left and right of OR expr.
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                col("t1.a").eq(col("t2.a")),
                Or,
                col("t2.b").eq(col("t1.a")),
            ))?
            .build()?;

        // let expected = vec![
        //     "Filter: t1.a = t2.a OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
        //     "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
        //     "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
        //     "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        // ];
        let expected = vec![
            "Inner Join:  Filter: t1.a = t2.a OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(20u32))),
                And,
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").eq(lit(10u32))),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(20) AND t1.a = t2.a AND t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since Or predicates have common Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_not_possible_simple() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.b").eq(col("t2.b")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.b = t2.b AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_not_possible() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    /// ```txt
    /// filter: a.id = b.id and a.id = c.id
    ///   cross_join a (bc)
    ///     cross_join b c
    /// ```
    /// Without reorder, it will be
    /// ```txt
    ///   inner_join a (bc) on a.id = b.id and a.id = c.id
    ///     cross_join b c
    /// ```
    /// Reorder it to be
    /// ```txt
    ///   inner_join (ab)c and a.id = c.id
    ///     inner_join a b on a.id = b.id
    /// ```
    fn reorder_join_to_eliminate_cross_join_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .cross_join(t3)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t3.c").lt(lit(15u32))),
                And,
                binary_expr(col("t3.a").eq(col("t2.a")), And, col("t3.b").lt(lit(15u32))),
            ))?
            .build()?;

        let expected = vec![
            "Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c, t3.a, t3.b, t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t3.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join:  Filter: t3.a = t1.a AND t3.c < UInt32(15) AND t3.b < UInt32(15) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t4.a")),
                    And,
                    col("t3.b").eq(col("t4.b")),
                ),
            ))?
            .build()?;

        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t1.a")),
                    And,
                    col("t4.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // the filter of (t1 join t2): t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688).
        // the filter of (t3 join t4): t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b
        // the outer filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688)
        // they are all `OR` expressions, so can not be split.
        let expected = vec![
            "Inner Join:  Filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_1() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t4.a")),
                    And,
                    col("t3.b").eq(col("t4.b")),
                ),
            ))?
            .build()?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(col("t3.a").eq(col("t1.a")), Or, col("t4.c").eq(lit(688u32))),
            ))?
            .build()?;

        // the filter of (t1 join t2): t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688).
        // the filter of (t3 join t4): t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b.
        // the outer filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a OR t4.c = UInt32(688)
        let expected = vec![
            "Inner Join:  Filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_2() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could not eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(col("t3.a").eq(col("t4.a")), Or, col("t3.b").eq(col("t4.b"))),
            ))?
            .build()?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t1.a")),
                    And,
                    col("t4.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // the filter of (t1 join t2): t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688).
        // the filter of (t3 join t4): t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b.
        // the outer filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a OR t4.c = UInt32(688)
        let expected = vec![
            "Inner Join:  Filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_3() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could not eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t4.a")),
                    And,
                    col("t3.b").eq(col("t4.b")),
                ),
            ))?
            .build()?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t1.a")),
                    And,
                    col("t4.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // the filter of (t1 join t2): t1.a = t2.a OR t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688).
        // the filter of (t3 join t4): t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b.
        // the outer filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688)
        let expected = vec![
            "Inner Join:  Filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t1.a = t2.a OR t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_4() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").lt(lit(15u32))),
                And,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3).cross_join(t4)?.build()?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(plan2)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t1.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t1.a")),
                        And,
                        col("t4.c").eq(lit(688u32)),
                    ),
                ),
                And,
                binary_expr(
                    binary_expr(
                        binary_expr(
                            col("t3.a").eq(col("t4.a")),
                            And,
                            col("t4.c").lt(lit(15u32)),
                        ),
                        Or,
                        binary_expr(
                            col("t3.a").eq(col("t4.a")),
                            And,
                            col("t3.c").eq(lit(688u32)),
                        ),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.b").eq(col("t4.b")),
                    ),
                ),
            ))?
            .build()?;

        // the filter of (t1 join t2): (t1.a = t2.a OR t2.c < UInt32(15)) AND t1.a = t2.a AND t2.c = UInt32(688).
        // the filter of (t3 join (t1 + t2)): empty.
        // the outer filter: (t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688)) AND (t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b)
        let expected = vec![
            "Inner Join:  Filter: (t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688)) AND (t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join:  Filter: (t1.a = t2.a OR t2.c < UInt32(15)) AND t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_5() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1).cross_join(t2)?.build()?;

        // could eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3).cross_join(t4)?.build()?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(plan2)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        binary_expr(
                            col("t3.a").eq(col("t1.a")),
                            And,
                            col("t4.c").lt(lit(15u32)),
                        ),
                        Or,
                        binary_expr(
                            col("t3.a").eq(col("t1.a")),
                            And,
                            col("t4.c").eq(lit(688u32)),
                        ),
                    ),
                    And,
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                col("t3.a").eq(col("t4.a")),
                                And,
                                col("t4.c").lt(lit(15u32)),
                            ),
                            Or,
                            binary_expr(
                                col("t3.a").eq(col("t4.a")),
                                And,
                                col("t3.c").eq(lit(688u32)),
                            ),
                        ),
                        Or,
                        binary_expr(
                            col("t3.a").eq(col("t4.a")),
                            And,
                            col("t3.b").eq(col("t4.b")),
                        ),
                    ),
                ),
                And,
                binary_expr(
                    binary_expr(
                        col("t1.a").eq(col("t2.a")),
                        Or,
                        col("t2.c").lt(lit(15u32)),
                    ),
                    And,
                    binary_expr(
                        col("t1.a").eq(col("t2.a")),
                        And,
                        col("t2.c").eq(lit(688u32)),
                    ),
                ),
            ))?
            .build()?;

        // the filter of (t1 join t2): (t1.a = t2.a OR t2.c < UInt32(15)) AND t1.a = t2.a AND t2.c = UInt32(688).
        // the filter of (t3 join (t1 + t2)): empty.
        // the outer filter: (t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688)) AND (t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b)
        let expected = vec![
            "Inner Join:  Filter: (t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a AND t4.c = UInt32(688)) AND (t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a AND t3.b = t4.b) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join:  Filter: (t1.a = t2.a OR t2.c < UInt32(15)) AND t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_with_expr_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since filter has Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32)),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a + UInt32(100) = t2.a * UInt32(2) AND t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_expr_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join since filter OR expression and there is no common
        // Join predicates in left and right of OR expr.
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32)),
                Or,
                col("t2.b").eq(col("t1.a")),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a + UInt32(100) = t2.a * UInt32(2) OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_common_expr_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join
        let common_join_key = (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32));
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(common_join_key.clone(), And, col("t2.c").lt(lit(20u32))),
                And,
                binary_expr(common_join_key, And, col("t2.c").eq(lit(10u32))),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a + UInt32(100) = t2.a * UInt32(2) AND t2.c < UInt32(20) AND t1.a + UInt32(100) = t2.a * UInt32(2) AND t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_common_expr_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since Or predicates have common Join predicates
        let common_join_key = (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32));
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .filter(binary_expr(
                binary_expr(common_join_key.clone(), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(common_join_key, And, col("t2.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.a + UInt32(100) = t2.a * UInt32(2) AND t2.c < UInt32(15) OR t1.a + UInt32(100) = t2.a * UInt32(2) AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reorder_join_with_expr_key_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(t2)?
            .cross_join(t3)?
            .filter(binary_expr(
                binary_expr(
                    (col("t3.a") + lit(100u32)).eq(col("t1.a") * lit(2u32)),
                    And,
                    col("t3.c").lt(lit(15u32)),
                ),
                And,
                binary_expr(
                    (col("t3.a") + lit(100u32)).eq(col("t2.a") * lit(2u32)),
                    And,
                    col("t3.b").lt(lit(15u32)),
                ),
            ))?
            .build()?;

        let expected = vec![
            "Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c, t3.a, t3.b, t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t3.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join:  Filter: t3.a + UInt32(100) = t1.a * UInt32(2) AND t3.c < UInt32(15) AND t3.b < UInt32(15) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn inner_join_with_join_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        let t1_join_t2 = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(col("t1.b").gt(col("t2.b"))),
            )?
            .build()?;

        let plan = LogicalPlanBuilder::from(t1_join_t2)
            .cross_join(t3)?
            .filter((col("t1.a").gt(col("t2.a"))).and(col("t1.c").lt(col("t3.a"))))?
            .build()?;

        let expected = vec![
            "Inner Join:  Filter: t1.c < t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join:  Filter: t1.a > t2.a AND t1.b > t2.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "  TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
