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

//! [`PropagateEmptyRelation`] eliminates nodes fed by `EmptyRelation`

use std::sync::Arc;

use datafusion_common::tree_node::Transformed;
use datafusion_common::JoinType;
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{EmptyRelation, Projection, Union};

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that bottom-up to eliminate plan by propagating empty_relation.
#[derive(Default)]
pub struct PropagateEmptyRelation;

impl PropagateEmptyRelation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PropagateEmptyRelation {
    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        internal_err!("Should have called PropagateEmptyRelation::rewrite")
    }

    fn name(&self) -> &str {
        "propagate_empty_relation"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::EmptyRelation(_) => Ok(Transformed::no(plan)),
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Limit(_) => {
                let empty = empty_child(&plan)?;
                if let Some(empty_plan) = empty {
                    return Ok(Transformed::yes(empty_plan));
                }
                Ok(Transformed::no(plan))
            }
            LogicalPlan::CrossJoin(ref join) => {
                let (left_empty, right_empty) = binary_plan_children_is_empty(&plan)?;
                if left_empty || right_empty {
                    return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: plan.schema().clone(),
                        },
                    )));
                }
                Ok(Transformed::no(LogicalPlan::CrossJoin(join.clone())))
            }

            LogicalPlan::Join(ref join) => {
                // TODO: For Join, more join type need to be careful:
                // For LeftAnti Join, if the right side is empty, the Join result is left side(should exclude null ??).
                // For RightAnti Join, if the left side is empty, the Join result is right side(should exclude null ??).
                // For Full Join, only both sides are empty, the Join result is empty.
                // For LeftOut/Full Join, if the right side is empty, the Join can be eliminated with a Projection with left side
                // columns + right side columns replaced with null values.
                // For RightOut/Full Join, if the left side is empty, the Join can be eliminated with a Projection with right side
                // columns + left side columns replaced with null values.
                let (left_empty, right_empty) = binary_plan_children_is_empty(&plan)?;

                match join.join_type {
                    JoinType::Inner if left_empty || right_empty => Ok(Transformed::yes(
                        LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        }),
                    )),
                    JoinType::Left if left_empty => Ok(Transformed::yes(
                        LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        }),
                    )),
                    JoinType::Right if right_empty => Ok(Transformed::yes(
                        LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        }),
                    )),
                    JoinType::LeftSemi if left_empty || right_empty => Ok(
                        Transformed::yes(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        })),
                    ),
                    JoinType::RightSemi if left_empty || right_empty => Ok(
                        Transformed::yes(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        })),
                    ),
                    JoinType::LeftAnti if left_empty => Ok(Transformed::yes(
                        LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        }),
                    )),
                    JoinType::RightAnti if right_empty => Ok(Transformed::yes(
                        LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema.clone(),
                        }),
                    )),
                    _ => Ok(Transformed::no(LogicalPlan::Join(join.clone()))),
                }
            }
            LogicalPlan::Aggregate(ref agg) => {
                if !agg.group_expr.is_empty() {
                    if let Some(empty_plan) = empty_child(&plan)? {
                        return Ok(Transformed::yes(empty_plan));
                    }
                }
                Ok(Transformed::no(LogicalPlan::Aggregate(agg.clone())))
            }
            LogicalPlan::Union(ref union) => {
                let new_inputs = union
                    .inputs
                    .iter()
                    .filter(|input| match &***input {
                        LogicalPlan::EmptyRelation(empty) => empty.produce_one_row,
                        _ => true,
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                if new_inputs.len() == union.inputs.len() {
                    Ok(Transformed::no(plan))
                } else if new_inputs.is_empty() {
                    Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: plan.schema().clone(),
                        },
                    )))
                } else if new_inputs.len() == 1 {
                    let child = unwrap_arc(new_inputs[0].clone());
                    if child.schema().eq(plan.schema()) {
                        Ok(Transformed::yes(child))
                    } else {
                        Ok(Transformed::yes(LogicalPlan::Projection(
                            Projection::new_from_schema(
                                Arc::new(child),
                                plan.schema().clone(),
                            ),
                        )))
                    }
                } else {
                    Ok(Transformed::yes(LogicalPlan::Union(Union {
                        inputs: new_inputs,
                        schema: union.schema.clone(),
                    })))
                }
            }

            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn binary_plan_children_is_empty(plan: &LogicalPlan) -> Result<(bool, bool)> {
    match plan.inputs()[..] {
        [left, right] => {
            let left_empty = match left {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            };
            let right_empty = match right {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            };
            Ok((left_empty, right_empty))
        }
        _ => plan_err!("plan just can have two child"),
    }
}

fn empty_child(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    match plan.inputs()[..] {
        [child] => match child {
            LogicalPlan::EmptyRelation(empty) => {
                if !empty.produce_one_row {
                    Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: plan.schema().clone(),
                    })))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        },
        _ => plan_err!("plan just can have one child"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use datafusion_common::{Column, DFSchema, JoinType, ScalarValue};
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        binary_expr, col, lit, logical_plan::builder::LogicalPlanBuilder, Expr, Operator,
    };

    use crate::eliminate_filter::EliminateFilter;
    use crate::eliminate_nested_union::EliminateNestedUnion;
    use crate::test::{
        assert_optimized_plan_eq, assert_optimized_plan_with_rules, test_table_scan,
        test_table_scan_fields, test_table_scan_with_name,
    };

    use super::*;

    fn assert_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(PropagateEmptyRelation::new()), plan, expected)
    }

    fn assert_together_optimized_plan(
        plan: LogicalPlan,
        expected: &str,
        eq: bool,
    ) -> Result<()> {
        assert_optimized_plan_with_rules(
            vec![
                Arc::new(EliminateFilter::new()),
                Arc::new(EliminateNestedUnion::new()),
                Arc::new(PropagateEmptyRelation::new()),
            ],
            plan,
            expected,
            eq,
        )
    }

    #[test]
    fn propagate_empty() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(true))))?
            .limit(10, None)?
            .project(vec![binary_expr(lit(1), Operator::Plus, lit(1))])?
            .build()?;

        let expected = "EmptyRelation";
        assert_eq(plan, expected)
    }

    #[test]
    fn cooperate_with_eliminate_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan(plan, expected, true)
    }

    #[test]
    fn propagate_union_empty() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan()?).build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("test2")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        let expected = "TableScan: test";
        assert_together_optimized_plan(plan, expected, true)
    }

    #[test]
    fn propagate_union_multi_empty() -> Result<()> {
        let one =
            LogicalPlanBuilder::from(test_table_scan_with_name("test1")?).build()?;
        let two = LogicalPlanBuilder::from(test_table_scan_with_name("test2")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let three = LogicalPlanBuilder::from(test_table_scan_with_name("test3")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let four =
            LogicalPlanBuilder::from(test_table_scan_with_name("test4")?).build()?;

        let plan = LogicalPlanBuilder::from(one)
            .union(two)?
            .union(three)?
            .union(four)?
            .build()?;

        let expected = "Union\
            \n  TableScan: test1\
            \n  TableScan: test4";
        assert_together_optimized_plan(plan, expected, true)
    }

    #[test]
    fn propagate_union_all_empty() -> Result<()> {
        let one = LogicalPlanBuilder::from(test_table_scan_with_name("test1")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let two = LogicalPlanBuilder::from(test_table_scan_with_name("test2")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let three = LogicalPlanBuilder::from(test_table_scan_with_name("test3")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let four = LogicalPlanBuilder::from(test_table_scan_with_name("test4")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(one)
            .union(two)?
            .union(three)?
            .union(four)?
            .build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan(plan, expected, true)
    }

    #[test]
    fn propagate_union_children_different_schema() -> Result<()> {
        let one_schema = Schema::new(vec![Field::new("t1a", DataType::UInt32, false)]);
        let t1_scan = table_scan(Some("test1"), &one_schema, None)?.build()?;
        let one = LogicalPlanBuilder::from(t1_scan)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let two_schema = Schema::new(vec![Field::new("t2a", DataType::UInt32, false)]);
        let t2_scan = table_scan(Some("test2"), &two_schema, None)?.build()?;
        let two = LogicalPlanBuilder::from(t2_scan).build()?;

        let three_schema = Schema::new(vec![Field::new("t3a", DataType::UInt32, false)]);
        let t3_scan = table_scan(Some("test3"), &three_schema, None)?.build()?;
        let three = LogicalPlanBuilder::from(t3_scan).build()?;

        let plan = LogicalPlanBuilder::from(one)
            .union(two)?
            .union(three)?
            .build()?;

        let expected = "Union\
            \n  TableScan: test2\
            \n  TableScan: test3";
        assert_together_optimized_plan(plan, expected, true)
    }

    #[test]
    fn propagate_union_alias() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan()?).build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("test2")?)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        let expected = "TableScan: test";
        assert_together_optimized_plan(plan, expected, true)
    }

    #[test]
    fn cross_join_empty() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right = LogicalPlanBuilder::empty(false).build()?;

        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan(plan, expected, true)
    }

    fn assert_empty_left_empty_right_lp(
        left_empty: bool,
        right_empty: bool,
        join_type: JoinType,
        eq: bool,
    ) -> Result<()> {
        let left_lp = if left_empty {
            let left_table_scan = test_table_scan()?;

            LogicalPlanBuilder::from(left_table_scan)
                .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
                .build()
        } else {
            let scan = test_table_scan_with_name("left").unwrap();
            LogicalPlanBuilder::from(scan).build()
        }?;

        let right_lp = if right_empty {
            let right_table_scan = test_table_scan_with_name("right")?;

            LogicalPlanBuilder::from(right_table_scan)
                .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
                .build()
        } else {
            let scan = test_table_scan_with_name("right").unwrap();
            LogicalPlanBuilder::from(scan).build()
        }?;

        let plan = LogicalPlanBuilder::from(left_lp)
            .join_using(
                right_lp,
                join_type,
                vec![Column::from_name("a".to_string())],
            )?
            .build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan(plan, expected, eq)
    }

    #[test]
    fn test_join_empty_propagation_rules() -> Result<()> {
        // test left join with empty left
        assert_empty_left_empty_right_lp(true, false, JoinType::Left, true)?;

        // test right join with empty right
        assert_empty_left_empty_right_lp(false, true, JoinType::Right, true)?;

        // test left semi join with empty left
        assert_empty_left_empty_right_lp(true, false, JoinType::LeftSemi, true)?;

        // test left semi join with empty right
        assert_empty_left_empty_right_lp(false, true, JoinType::LeftSemi, true)?;

        // test right semi join with empty left
        assert_empty_left_empty_right_lp(true, false, JoinType::RightSemi, true)?;

        // test right semi join with empty right
        assert_empty_left_empty_right_lp(false, true, JoinType::RightSemi, true)?;

        // test left anti join empty left
        assert_empty_left_empty_right_lp(true, false, JoinType::LeftAnti, true)?;

        // test right anti join empty right
        assert_empty_left_empty_right_lp(false, true, JoinType::RightAnti, true)
    }

    #[test]
    fn test_join_empty_propagation_rules_noop() -> Result<()> {
        // these cases should not result in an empty relation

        // test left join with empty right
        assert_empty_left_empty_right_lp(false, true, JoinType::Left, false)?;

        // test right join with empty left
        assert_empty_left_empty_right_lp(true, false, JoinType::Right, false)?;

        // test left semi with non-empty left and right
        assert_empty_left_empty_right_lp(false, false, JoinType::LeftSemi, false)?;

        // test right semi with non-empty left and right
        assert_empty_left_empty_right_lp(false, false, JoinType::RightSemi, false)?;

        // test left anti join with non-empty left and right
        assert_empty_left_empty_right_lp(false, false, JoinType::LeftAnti, false)?;

        // test left anti with non-empty left and empty right
        assert_empty_left_empty_right_lp(false, true, JoinType::LeftAnti, false)?;

        // test right anti join with non-empty left and right
        assert_empty_left_empty_right_lp(false, false, JoinType::RightAnti, false)?;

        // test right anti with empty left and non-empty right
        assert_empty_left_empty_right_lp(true, false, JoinType::RightAnti, false)
    }

    #[test]
    fn test_empty_with_non_empty() -> Result<()> {
        let table_scan = test_table_scan()?;

        let fields = test_table_scan_fields();

        let empty = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::from_unqualifed_fields(
                fields.into(),
                Default::default(),
            )?),
        });

        let one = LogicalPlanBuilder::from(empty.clone()).build()?;
        let two = LogicalPlanBuilder::from(table_scan).build()?;
        let three = LogicalPlanBuilder::from(empty).build()?;

        // Union
        //  EmptyRelation
        //  TableScan: test
        //  EmptyRelation
        let plan = LogicalPlanBuilder::from(one)
            .union(two)?
            .union(three)?
            .build()?;

        let expected = "Projection: a, b, c\
        \n  TableScan: test";

        assert_together_optimized_plan(plan, expected, true)
    }
}
