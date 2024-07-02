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

//! [`EliminateCrossJoin`] converts `CROSS JOIN` to `INNER JOIN` if join predicates are available.
use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

use crate::join_key_set::JoinKeySet;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{internal_err, Result};
use datafusion_expr::expr::{BinaryExpr, Expr};
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use datafusion_expr::logical_plan::{
    CrossJoin, Filter, Join, JoinConstraint, JoinType, LogicalPlan, Projection,
};
use datafusion_expr::utils::{can_hash, find_valid_equijoin_key_pair};
use datafusion_expr::{build_join_schema, ExprSchemable, Operator};

#[derive(Default)]
pub struct EliminateCrossJoin;

impl EliminateCrossJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Eliminate cross joins by rewriting them to inner joins when possible.
///
/// # Example
/// The initial plan for this query:
/// ```sql
/// select ... from a, b where a.x = b.y and b.xx = 100;
/// ```
///
/// Looks like this:
/// ```text
/// Filter(a.x = b.y AND b.xx = 100)
///  CrossJoin
///   TableScan a
///   TableScan b
/// ```
///
/// After the rule is applied, the plan will look like this:
/// ```text
/// Filter(b.xx = 100)
///   InnerJoin(a.x = b.y)
///     TableScan a
///     TableScan b
/// ```
///
/// # Other Examples
/// * 'select ... from a, b where a.x = b.y and b.xx = 100;'
/// * 'select ... from a, b where (a.x = b.y and b.xx = 100) or (a.x = b.y and b.xx = 200);'
/// * 'select ... from a, b, c where (a.x = b.y and b.xx = 100 and a.z = c.z)
/// * or (a.x = b.y and b.xx = 200 and a.z=c.z);'
/// * 'select ... from a, b where a.x > b.y'
///
/// For above queries, the join predicate is available in filters and they are moved to
/// join nodes appropriately
///
/// This fix helps to improve the performance of TPCH Q19. issue#78
impl OptimizerRule for EliminateCrossJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let plan_schema = Arc::clone(plan.schema());
        let mut possible_join_keys = JoinKeySet::new();
        let mut all_inputs: Vec<LogicalPlan> = vec![];

        let parent_predicate = if let LogicalPlan::Filter(filter) = plan {
            // if input isn't a join that can potentially be rewritten
            // avoid unwrapping the input
            let rewriteable = matches!(
                filter.input.as_ref(),
                LogicalPlan::Join(Join {
                    join_type: JoinType::Inner,
                    ..
                }) | LogicalPlan::CrossJoin(_)
            );

            if !rewriteable {
                // recursively try to rewrite children
                return rewrite_children(self, LogicalPlan::Filter(filter), config);
            }

            if !can_flatten_join_inputs(&filter.input) {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }

            let Filter {
                input, predicate, ..
            } = filter;
            flatten_join_inputs(
                unwrap_arc(input),
                &mut possible_join_keys,
                &mut all_inputs,
            )?;

            extract_possible_join_keys(&predicate, &mut possible_join_keys);
            Some(predicate)
        } else if matches!(
            plan,
            LogicalPlan::Join(Join {
                join_type: JoinType::Inner,
                ..
            })
        ) {
            if !can_flatten_join_inputs(&plan) {
                return Ok(Transformed::no(plan));
            }
            flatten_join_inputs(plan, &mut possible_join_keys, &mut all_inputs)?;
            None
        } else {
            // recursively try to rewrite children
            return rewrite_children(self, plan, config);
        };

        // Join keys are handled locally:
        let mut all_join_keys = JoinKeySet::new();
        let mut left = all_inputs.remove(0);
        while !all_inputs.is_empty() {
            left = find_inner_join(
                left,
                &mut all_inputs,
                &possible_join_keys,
                &mut all_join_keys,
            )?;
        }

        left = rewrite_children(self, left, config)?.data;

        if &plan_schema != left.schema() {
            left = LogicalPlan::Projection(Projection::new_from_schema(
                Arc::new(left),
                Arc::clone(&plan_schema),
            ));
        }

        let Some(predicate) = parent_predicate else {
            return Ok(Transformed::yes(left));
        };

        // If there are no join keys then do nothing:
        if all_join_keys.is_empty() {
            Filter::try_new(predicate, Arc::new(left))
                .map(|filter| Transformed::yes(LogicalPlan::Filter(filter)))
        } else {
            // Remove join expressions from filter:
            match remove_join_expressions(predicate, &all_join_keys) {
                Some(filter_expr) => Filter::try_new(filter_expr, Arc::new(left))
                    .map(|filter| Transformed::yes(LogicalPlan::Filter(filter))),
                _ => Ok(Transformed::yes(left)),
            }
        }
    }

    fn name(&self) -> &str {
        "eliminate_cross_join"
    }
}

fn rewrite_children(
    optimizer: &impl OptimizerRule,
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let transformed_plan = plan.map_children(|input| optimizer.rewrite(input, config))?;

    // recompute schema if the plan was transformed
    if transformed_plan.transformed {
        transformed_plan.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(transformed_plan)
    }
}

/// Recursively accumulate possible_join_keys and inputs from inner joins
/// (including cross joins).
///
/// Assumes can_flatten_join_inputs has returned true and thus the plan can be
/// flattened. Adds all leaf inputs to `all_inputs` and join_keys to
/// possible_join_keys
fn flatten_join_inputs(
    plan: LogicalPlan,
    possible_join_keys: &mut JoinKeySet,
    all_inputs: &mut Vec<LogicalPlan>,
) -> Result<()> {
    match plan {
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            // checked in can_flatten_join_inputs
            if join.filter.is_some() {
                return internal_err!(
                    "should not have filter in inner join in flatten_join_inputs"
                );
            }
            possible_join_keys.insert_all_owned(join.on);
            flatten_join_inputs(unwrap_arc(join.left), possible_join_keys, all_inputs)?;
            flatten_join_inputs(unwrap_arc(join.right), possible_join_keys, all_inputs)?;
        }
        LogicalPlan::CrossJoin(join) => {
            flatten_join_inputs(unwrap_arc(join.left), possible_join_keys, all_inputs)?;
            flatten_join_inputs(unwrap_arc(join.right), possible_join_keys, all_inputs)?;
        }
        _ => {
            all_inputs.push(plan);
        }
    };
    Ok(())
}

/// Returns true if the plan is a Join or Cross join could be flattened with
/// `flatten_join_inputs`
///
/// Must stay in sync with `flatten_join_inputs`
fn can_flatten_join_inputs(plan: &LogicalPlan) -> bool {
    // can only flatten inner / cross joins
    match plan {
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            // The filter of inner join will lost, skip this rule.
            // issue: https://github.com/apache/datafusion/issues/4844
            if join.filter.is_some() {
                return false;
            }
        }
        LogicalPlan::CrossJoin(_) => {}
        _ => return false,
    };

    for child in plan.inputs() {
        match child {
            LogicalPlan::Join(Join {
                join_type: JoinType::Inner,
                ..
            })
            | LogicalPlan::CrossJoin(_) => {
                if !can_flatten_join_inputs(child) {
                    return false;
                }
            }
            // the child is not a join/cross join
            _ => (),
        }
    }
    true
}

/// Finds the next to join with the left input plan,
///
/// Finds the next `right` from `rights` that can be joined with `left_input`
/// plan based on the join keys in `possible_join_keys`.
///
/// If such a matching `right` is found:
/// 1. Adds the matching join keys to `all_join_keys`.
/// 2. Returns `left_input JOIN right ON (all join keys)`.
///
/// If no matching `right` is found:
/// 1. Removes the first plan from `rights`
/// 2. Returns `left_input CROSS JOIN right`.
fn find_inner_join(
    left_input: LogicalPlan,
    rights: &mut Vec<LogicalPlan>,
    possible_join_keys: &JoinKeySet,
    all_join_keys: &mut JoinKeySet,
) -> Result<LogicalPlan> {
    for (i, right_input) in rights.iter().enumerate() {
        let mut join_keys = vec![];

        for (l, r) in possible_join_keys.iter() {
            let key_pair = find_valid_equijoin_key_pair(
                l,
                r,
                left_input.schema(),
                right_input.schema(),
            )?;

            // Save join keys
            if let Some((valid_l, valid_r)) = key_pair {
                if can_hash(&valid_l.get_type(left_input.schema())?) {
                    join_keys.push((valid_l, valid_r));
                }
            }
        }

        // Found one or more matching join keys
        if !join_keys.is_empty() {
            all_join_keys.insert_all(join_keys.iter());
            let right_input = rights.remove(i);
            let join_schema = Arc::new(build_join_schema(
                left_input.schema(),
                right_input.schema(),
                &JoinType::Inner,
            )?);

            return Ok(LogicalPlan::Join(Join {
                left: Arc::new(left_input),
                right: Arc::new(right_input),
                join_type: JoinType::Inner,
                join_constraint: JoinConstraint::On,
                on: join_keys,
                filter: None,
                schema: join_schema,
                null_equals_null: false,
            }));
        }
    }

    // no matching right plan had any join keys, cross join with the first right
    // plan
    let right = rights.remove(0);
    let join_schema = Arc::new(build_join_schema(
        left_input.schema(),
        right.schema(),
        &JoinType::Inner,
    )?);

    Ok(LogicalPlan::CrossJoin(CrossJoin {
        left: Arc::new(left_input),
        right: Arc::new(right),
        schema: join_schema,
    }))
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(expr: &Expr, join_keys: &mut JoinKeySet) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            Operator::Eq => {
                // insert handles ensuring  we don't add the same Join keys multiple times
                join_keys.insert(left, right);
            }
            Operator::And => {
                extract_possible_join_keys(left, join_keys);
                extract_possible_join_keys(right, join_keys)
            }
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let mut left_join_keys = JoinKeySet::new();
                let mut right_join_keys = JoinKeySet::new();

                extract_possible_join_keys(left, &mut left_join_keys);
                extract_possible_join_keys(right, &mut right_join_keys);

                join_keys.insert_intersection(left_join_keys, right_join_keys)
            }
            _ => (),
        };
    }
}

/// Remove join expressions from a filter expression
///
/// # Returns
/// * `Some()` when there are few remaining predicates in filter_expr
/// * `None` otherwise
fn remove_join_expressions(expr: Expr, join_keys: &JoinKeySet) -> Option<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) if join_keys.contains(&left, &right) => {
            // was a join key, so remove it
            None
        }
        // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(op, Operator::And | Operator::Or) =>
        {
            let l = remove_join_expressions(*left, join_keys);
            let r = remove_join_expressions(*right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(ll),
                    op,
                    Box::new(rr),
                ))),
                (Some(ll), _) => Some(ll),
                (_, Some(rr)) => Some(rr),
                _ => None,
            }
        }

        _ => Some(expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::OptimizerContext;
    use crate::test::*;

    use datafusion_expr::{
        binary_expr, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        Operator::{And, Or},
    };

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: Vec<&str>) {
        let starting_schema = plan.schema().clone();
        let rule = EliminateCrossJoin::new();
        let transformed_plan = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        assert!(transformed_plan.transformed, "failed to optimize plan");
        let optimized_plan = transformed_plan.data;
        let formatted = optimized_plan.display_indent_schema().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        assert_eq!(&starting_schema, optimized_plan.schema())
    }

    fn assert_optimization_rule_fails(plan: LogicalPlan) {
        let rule = EliminateCrossJoin::new();
        let transformed_plan = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        assert!(!transformed_plan.transformed)
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
            "Filter: t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: t1.a = t2.a OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t2.c < UInt32(20) AND t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.b = t2.b AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }

    #[test]
    /// See https://github.com/apache/datafusion/issues/7530
    fn eliminate_cross_not_possible_nested_inner_join_with_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        // could not eliminate to inner join with filter
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t3,
                JoinType::Inner,
                (vec!["t1.a"], vec!["t3.a"]),
                Some(col("t1.a").gt(lit(20u32))),
            )?
            .join(t2, JoinType::Inner, (vec!["t1.a"], vec!["t2.a"]), None)?
            .filter(col("t1.a").gt(lit(15u32)))?
            .build()?;

        assert_optimization_rule_fails(plan);

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
            "Filter: t3.c < UInt32(15) AND t3.b < UInt32(15) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c, t3.a, t3.b, t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t3.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: t4.c < UInt32(15) OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: t4.c < UInt32(15) OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: t4.c < UInt32(15) OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t1.a = t2.a OR t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: (t4.c < UInt32(15) OR t4.c = UInt32(688)) AND (t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Filter: t2.c < UInt32(15) AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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

        let expected = vec![
            "Filter: (t4.c < UInt32(15) OR t4.c = UInt32(688)) AND (t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b) AND t2.c < UInt32(15) AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]"];

        assert_optimized_plan_eq(plan, expected);

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
              "Filter: t1.a + UInt32(100) = t2.a * UInt32(2) OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
              "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
              "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
              "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t2.c < UInt32(20) AND t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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
        "Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
        "  Inner Join: t1.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
        "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
        "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

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
            "Filter: t3.c < UInt32(15) AND t3.b < UInt32(15) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c, t3.a, t3.b, t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t3.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a * UInt32(2) = t3.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }
}
