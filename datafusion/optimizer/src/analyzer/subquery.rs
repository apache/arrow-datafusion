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

use crate::analyzer::check_plan;
use crate::utils::collect_subquery_cols;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::expr_rewriter::strip_outer_reference;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{
    Aggregate, BinaryExpr, Cast, Expr, Filter, Join, JoinType, LogicalPlan, Operator,
    Window,
};
use std::ops::Deref;

/// Do necessary check on subquery expressions and fail the invalid plan
/// 1) Check whether the outer plan is in the allowed outer plans list to use subquery expressions,
///    the allowed while list: [Projection, Filter, Window, Aggregate, Join].
/// 2) Check whether the inner plan is in the allowed inner plans list to use correlated(outer) expressions.
/// 3) Check and validate unsupported cases to use the correlated(outer) expressions inside the subquery(inner) plans/inner expressions.
/// For example, we do not want to support to use correlated expressions as the Join conditions in the subquery plan when the Join
/// is a Full Out Join
pub fn check_subquery_expr(
    outer_plan: &LogicalPlan,
    inner_plan: &LogicalPlan,
    expr: &Expr,
) -> Result<()> {
    check_plan(inner_plan)?;
    if let Expr::ScalarSubquery(subquery) = expr {
        // Scalar subquery should only return one column
        if subquery.subquery.schema().fields().len() > 1 {
            return plan_err!(
                "Scalar subquery should only return one column, but found {}: {}",
                subquery.subquery.schema().fields().len(),
                subquery.subquery.schema().field_names().join(", ")
            );
        }
        // Correlated scalar subquery must be aggregated to return at most one row
        if !subquery.outer_ref_columns.is_empty() {
            match strip_inner_query(inner_plan) {
                LogicalPlan::Aggregate(agg) => {
                    check_aggregation_in_scalar_subquery(inner_plan, agg)
                }
                LogicalPlan::Filter(Filter { input, .. })
                    if matches!(input.as_ref(), LogicalPlan::Aggregate(_)) =>
                {
                    if let LogicalPlan::Aggregate(agg) = input.as_ref() {
                        check_aggregation_in_scalar_subquery(inner_plan, agg)
                    } else {
                        Ok(())
                    }
                }
                _ => {
                    if inner_plan
                        .max_rows()
                        .filter(|max_row| *max_row <= 1)
                        .is_some()
                    {
                        Ok(())
                    } else {
                        plan_err!(
                            "Correlated scalar subquery must be aggregated to return at most one row"
                        )
                    }
                }
            }?;
            match outer_plan {
                LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_) => Ok(()),
                LogicalPlan::Aggregate(Aggregate {group_expr, aggr_expr,..}) => {
                    if group_expr.contains(expr) && !aggr_expr.contains(expr) {
                        // TODO revisit this validation logic
                        plan_err!(
                            "Correlated scalar subquery in the GROUP BY clause must also be in the aggregate expressions"
                        )
                    } else {
                        Ok(())
                    }
                },
                _ => plan_err!(
                    "Correlated scalar subquery can only be used in Projection, Filter, Aggregate plan nodes"
                )
            }?;
        }
        check_correlations_in_subquery(inner_plan, true)
    } else {
        if let Expr::InSubquery(subquery) = expr {
            // InSubquery should only return one column
            if subquery.subquery.subquery.schema().fields().len() > 1 {
                return plan_err!(
                    "InSubquery should only return one column, but found {}: {}",
                    subquery.subquery.subquery.schema().fields().len(),
                    subquery.subquery.subquery.schema().field_names().join(", ")
                );
            }
        }
        match outer_plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Join(_) => Ok(()),
            _ => plan_err!(
                "In/Exist subquery can only be used in \
            Projection, Filter, Window functions, Aggregate and Join plan nodes"
            ),
        }?;
        check_correlations_in_subquery(inner_plan, false)
    }
}

// Recursively check the unsupported outer references in the sub query plan.
fn check_correlations_in_subquery(
    inner_plan: &LogicalPlan,
    is_scalar: bool,
) -> Result<()> {
    check_inner_plan(inner_plan, is_scalar, false, true)
}

// Recursively check the unsupported outer references in the sub query plan.
fn check_inner_plan(
    inner_plan: &LogicalPlan,
    is_scalar: bool,
    is_aggregate: bool,
    can_contain_outer_ref: bool,
) -> Result<()> {
    if !can_contain_outer_ref && contains_outer_reference(inner_plan) {
        return plan_err!("Accessing outer reference columns is not allowed in the plan");
    }
    // We want to support as many operators as possible inside the correlated subquery
    match inner_plan {
        LogicalPlan::Aggregate(_) => {
            inner_plan.visit_children(&mut |plan| {
                check_inner_plan(plan, is_scalar, true, can_contain_outer_ref)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            let (correlated, _): (Vec<_>, Vec<_>) = split_conjunction(predicate)
                .into_iter()
                .partition(|e| e.contains_outer());
            let maybe_unsupport = correlated
                .into_iter()
                .filter(|expr| !can_pullup_over_aggregation(expr))
                .collect::<Vec<_>>();
            if is_aggregate && is_scalar && !maybe_unsupport.is_empty() {
                return plan_err!(
                    "Correlated column is not allowed in predicate: {predicate}"
                );
            }
            check_inner_plan(input, is_scalar, is_aggregate, can_contain_outer_ref)
        }
        LogicalPlan::Window(window) => {
            check_mixed_out_refer_in_window(window)?;
            inner_plan.visit_children(&mut |plan| {
                check_inner_plan(plan, is_scalar, is_aggregate, can_contain_outer_ref)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Projection(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_) => {
            inner_plan.visit_children(&mut |plan| {
                check_inner_plan(plan, is_scalar, is_aggregate, can_contain_outer_ref)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(())
        }
        LogicalPlan::Join(Join {
            left,
            right,
            join_type,
            ..
        }) => match join_type {
            JoinType::Inner => {
                inner_plan.visit_children(&mut |plan| {
                    check_inner_plan(
                        plan,
                        is_scalar,
                        is_aggregate,
                        can_contain_outer_ref,
                    )?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(())
            }
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => {
                check_inner_plan(left, is_scalar, is_aggregate, can_contain_outer_ref)?;
                check_inner_plan(right, is_scalar, is_aggregate, false)
            }
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                check_inner_plan(left, is_scalar, is_aggregate, false)?;
                check_inner_plan(right, is_scalar, is_aggregate, can_contain_outer_ref)
            }
            JoinType::Full => {
                inner_plan.visit_children(&mut |plan| {
                    check_inner_plan(plan, is_scalar, is_aggregate, false)?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(())
            }
        },
        LogicalPlan::Extension(_) => Ok(()),
        _ => plan_err!("Unsupported operator in the subquery plan."),
    }
}

fn contains_outer_reference(inner_plan: &LogicalPlan) -> bool {
    inner_plan
        .expressions()
        .iter()
        .any(|expr| expr.contains_outer())
}

fn check_aggregation_in_scalar_subquery(
    inner_plan: &LogicalPlan,
    agg: &Aggregate,
) -> Result<()> {
    if agg.aggr_expr.is_empty() {
        return plan_err!(
            "Correlated scalar subquery must be aggregated to return at most one row"
        );
    }
    if !agg.group_expr.is_empty() {
        let correlated_exprs = get_correlated_expressions(inner_plan)?;
        let inner_subquery_cols =
            collect_subquery_cols(&correlated_exprs, agg.input.schema().clone())?;
        let mut group_columns = agg
            .group_expr
            .iter()
            .map(|group| Ok(group.to_columns()?.into_iter().collect::<Vec<_>>()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten();

        if !group_columns.all(|group| inner_subquery_cols.contains(&group)) {
            // Group BY columns must be a subset of columns in the correlated expressions
            return plan_err!(
                "A GROUP BY clause in a scalar correlated subquery cannot contain non-correlated columns"
            );
        }
    }
    Ok(())
}

fn strip_inner_query(inner_plan: &LogicalPlan) -> &LogicalPlan {
    match inner_plan {
        LogicalPlan::Projection(projection) => {
            strip_inner_query(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => strip_inner_query(alias.input.as_ref()),
        other => other,
    }
}

fn get_correlated_expressions(inner_plan: &LogicalPlan) -> Result<Vec<Expr>> {
    let mut exprs = vec![];
    inner_plan.visit_down(&mut |plan| {
        if let LogicalPlan::Filter(Filter { predicate, .. }) = plan {
            let (correlated, _): (Vec<_>, Vec<_>) = split_conjunction(predicate)
                .into_iter()
                .partition(|e| e.contains_outer());

            correlated
                .into_iter()
                .for_each(|expr| exprs.push(strip_outer_reference(expr.clone())));
            return Ok(TreeNodeRecursion::Continue);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(exprs)
}

/// Check whether the expression can pull up over the aggregation without change the result of the query
fn can_pullup_over_aggregation(expr: &Expr) -> bool {
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    {
        match (left.deref(), right.deref()) {
            (Expr::Column(_), right) if right.to_columns().unwrap().is_empty() => true,
            (left, Expr::Column(_)) if left.to_columns().unwrap().is_empty() => true,
            (Expr::Cast(Cast { expr, .. }), right)
                if matches!(expr.deref(), Expr::Column(_))
                    && right.to_columns().unwrap().is_empty() =>
            {
                true
            }
            (left, Expr::Cast(Cast { expr, .. }))
                if matches!(expr.deref(), Expr::Column(_))
                    && left.to_columns().unwrap().is_empty() =>
            {
                true
            }
            (_, _) => false,
        }
    } else {
        false
    }
}

/// Check whether the window expressions contain a mixture of out reference columns and inner columns
fn check_mixed_out_refer_in_window(window: &Window) -> Result<()> {
    let mixed = window.window_expr.iter().any(|win_expr| {
        win_expr.contains_outer() && !win_expr.to_columns().unwrap().is_empty()
    });
    if mixed {
        plan_err!(
            "Window expressions should not contain a mixed of outer references and inner columns"
        )
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion_common::{DFSchema, DFSchemaRef};
    use datafusion_expr::{Extension, UserDefinedLogicalNodeCore};

    use super::*;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockUserDefinedLogicalPlan {
        empty_schema: DFSchemaRef,
    }

    impl UserDefinedLogicalNodeCore for MockUserDefinedLogicalPlan {
        fn name(&self) -> &str {
            "MockUserDefinedLogicalPlan"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &datafusion_common::DFSchemaRef {
            &self.empty_schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockUserDefinedLogicalPlan")
        }

        fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
            Self {
                empty_schema: self.empty_schema.clone(),
            }
        }
    }

    #[test]
    fn wont_fail_extension_plan() {
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(MockUserDefinedLogicalPlan {
                empty_schema: DFSchemaRef::new(DFSchema::empty()),
            }),
        });

        check_inner_plan(&plan, false, false, true).unwrap();
    }
}
