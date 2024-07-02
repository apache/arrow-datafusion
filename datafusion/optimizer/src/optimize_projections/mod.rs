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

//! [`OptimizeProjections`] identifies and eliminates unused columns

mod required_indices;

use std::collections::HashSet;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{
    get_required_group_by_exprs_indices, internal_datafusion_err, internal_err, Column,
    JoinType, Result,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::Unnest;
use datafusion_expr::{
    logical_plan::LogicalPlan, projection_schema, Aggregate, Distinct, Expr, Projection,
    TableScan, Window,
};

use crate::optimize_projections::required_indices::RequiredIndicies;
use crate::utils::NamePreserver;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion,
};
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use hashbrown::HashMap;

/// Optimizer rule to prune unnecessary columns from intermediate schemas
/// inside the [`LogicalPlan`]. This rule:
/// - Removes unnecessary columns that do not appear at the output and/or are
///   not used during any computation step.
/// - Adds projections to decrease table column size before operators that
///   benefit from a smaller memory footprint at its input.
/// - Removes unnecessary [`LogicalPlan::Projection`]s from the [`LogicalPlan`].
///
/// `OptimizeProjections` is an optimizer rule that identifies and eliminates
/// columns from a logical plan that are not used by downstream operations.
/// This can improve query performance and reduce unnecessary data processing.
///
/// The rule analyzes the input logical plan, determines the necessary column
/// indices, and then removes any unnecessary columns. It also removes any
/// unnecessary projections from the plan tree.
#[derive(Default)]
pub struct OptimizeProjections {}

impl OptimizeProjections {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for OptimizeProjections {
    fn name(&self) -> &str {
        "optimize_projections"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // All output fields are necessary:
        let indices = RequiredIndicies::new_for_all_exprs(&plan);
        optimize_projections(plan, config, indices)
    }
}

/// Removes unnecessary columns (e.g. columns that do not appear in the output
/// schema and/or are not used during any computation step such as expression
/// evaluation) from the logical plan and its inputs.
///
/// # Parameters
///
/// - `plan`: A reference to the input `LogicalPlan` to optimize.
/// - `config`: A reference to the optimizer configuration.
/// - `indices`: A slice of column indices that represent the necessary column
///   indices for downstream (parent) plan nodes.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(LogicalPlan))`: An optimized `LogicalPlan` without unnecessary
///   columns.
/// - `Ok(None)`: Signal that the given logical plan did not require any change.
/// - `Err(error)`: An error occurred during the optimization process.
fn optimize_projections(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
    indices: RequiredIndicies,
) -> Result<Transformed<LogicalPlan>> {
    // Recursively rewrite any nodes that may be able to avoid computation given
    // their parents' required indices.
    match plan {
        LogicalPlan::Projection(proj) => {
            return merge_consecutive_projections(proj)?.transform_data(|proj| {
                rewrite_projection_given_requirements(proj, config, indices)
            })
        }
        LogicalPlan::Aggregate(aggregate) => {
            // Split parent requirements to GROUP BY and aggregate sections:
            let n_group_exprs = aggregate.group_expr_len()?;
            // Offset aggregate indices so that they point to valid indices at
            // `aggregate.aggr_expr`:
            let (group_by_reqs, aggregate_reqs) = indices.split_off(n_group_exprs);

            // Get absolutely necessary GROUP BY fields:
            let group_by_expr_existing = aggregate
                .group_expr
                .iter()
                .map(|group_by_expr| group_by_expr.display_name())
                .collect::<Result<Vec<_>>>()?;

            let new_group_bys = if let Some(simplest_groupby_indices) =
                get_required_group_by_exprs_indices(
                    aggregate.input.schema(),
                    &group_by_expr_existing,
                ) {
                // Some of the fields in the GROUP BY may be required by the
                // parent even if these fields are unnecessary in terms of
                // functional dependency.
                group_by_reqs
                    .append(&simplest_groupby_indices)
                    .get_at_indices(&aggregate.group_expr)
            } else {
                aggregate.group_expr
            };

            // Only use the absolutely necessary aggregate expressions required
            // by the parent:
            let mut new_aggr_expr = aggregate_reqs.get_at_indices(&aggregate.aggr_expr);

            // Aggregations always need at least one aggregate expression.
            // With a nested count, we don't require any column as input, but
            // still need to create a correct aggregate, which may be optimized
            // out later. As an example, consider the following query:
            //
            // SELECT count(*) FROM (SELECT count(*) FROM [...])
            //
            // which always returns 1.
            if new_aggr_expr.is_empty()
                && new_group_bys.is_empty()
                && !aggregate.aggr_expr.is_empty()
            {
                // take the old, first aggregate expression
                new_aggr_expr = aggregate.aggr_expr;
                new_aggr_expr.resize_with(1, || unreachable!());
            }

            let all_exprs_iter = new_group_bys.iter().chain(new_aggr_expr.iter());
            let schema = aggregate.input.schema();
            let necessary_indices =
                RequiredIndicies::new().with_exprs(schema, all_exprs_iter)?;
            let necessary_exprs = necessary_indices.get_required_exprs(schema);

            return optimize_projections(
                unwrap_arc(aggregate.input),
                config,
                necessary_indices,
            )?
            .transform_data(|aggregate_input| {
                // Simplify the input of the aggregation by adding a projection so
                // that its input only contains absolutely necessary columns for
                // the aggregate expressions. Note that necessary_indices refer to
                // fields in `aggregate.input.schema()`.
                add_projection_on_top_if_helpful(aggregate_input, necessary_exprs)
            })?
            .map_data(|aggregate_input| {
                // Create a new aggregate plan with the updated input and only the
                // absolutely necessary fields:
                Aggregate::try_new(
                    Arc::new(aggregate_input),
                    new_group_bys,
                    new_aggr_expr,
                )
                .map(LogicalPlan::Aggregate)
            });
        }
        LogicalPlan::Window(window) => {
            let input_schema = Arc::clone(window.input.schema());
            // Split parent requirements to child and window expression sections:
            let n_input_fields = input_schema.fields().len();
            // Offset window expression indices so that they point to valid
            // indices at `window.window_expr`:
            let (child_reqs, window_reqs) = indices.split_off(n_input_fields);

            // Only use window expressions that are absolutely necessary according
            // to parent requirements:
            let new_window_expr = window_reqs.get_at_indices(&window.window_expr);

            // Get all the required column indices at the input, either by the
            // parent or window expression requirements.
            let required_indices =
                child_reqs.with_exprs(&input_schema, &new_window_expr)?;

            return optimize_projections(
                unwrap_arc(window.input),
                config,
                required_indices.clone(),
            )?
            .transform_data(|window_child| {
                if new_window_expr.is_empty() {
                    // When no window expression is necessary, use the input directly:
                    Ok(Transformed::no(window_child))
                } else {
                    // Calculate required expressions at the input of the window.
                    // Please note that we use `input_schema`, because `required_indices`
                    // refers to that schema
                    let required_exprs =
                        required_indices.get_required_exprs(&input_schema);
                    let window_child =
                        add_projection_on_top_if_helpful(window_child, required_exprs)?
                            .data;
                    Window::try_new(new_window_expr, Arc::new(window_child))
                        .map(LogicalPlan::Window)
                        .map(Transformed::yes)
                }
            });
        }
        LogicalPlan::TableScan(table_scan) => {
            let TableScan {
                table_name,
                source,
                projection,
                filters,
                fetch,
                projected_schema: _,
            } = table_scan;

            // Get indices referred to in the original (schema with all fields)
            // given projected indices.
            let projection = match &projection {
                Some(projection) => indices.into_mapped_indices(|idx| projection[idx]),
                None => indices.into_inner(),
            };
            return TableScan::try_new(
                table_name,
                source,
                Some(projection),
                filters,
                fetch,
            )
            .map(LogicalPlan::TableScan)
            .map(Transformed::yes);
        }

        // Other node types are handled below
        _ => {}
    };

    // For other plan node types, calculate indices for columns they use and
    // try to rewrite their children
    let mut child_required_indices: Vec<RequiredIndicies> = match &plan {
        LogicalPlan::Sort(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Distinct(Distinct::On(_)) => {
            // Pass index requirements from the parent as well as column indices
            // that appear in this plan's expressions to its child. All these
            // operators benefit from "small" inputs, so the projection_beneficial
            // flag is `true`.
            plan.inputs()
                .into_iter()
                .map(|input| {
                    indices
                        .clone()
                        .with_projection_beneficial()
                        .with_plan_exprs(&plan, input.schema())
                })
                .collect::<Result<_>>()?
        }
        LogicalPlan::Limit(_) | LogicalPlan::Prepare(_) => {
            // Pass index requirements from the parent as well as column indices
            // that appear in this plan's expressions to its child. These operators
            // do not benefit from "small" inputs, so the projection_beneficial
            // flag is `false`.
            plan.inputs()
                .into_iter()
                .map(|input| indices.clone().with_plan_exprs(&plan, input.schema()))
                .collect::<Result<_>>()?
        }
        LogicalPlan::Copy(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => {
            // These plans require all their fields, and their children should
            // be treated as final plans -- otherwise, we may have schema a
            // mismatch.
            // TODO: For some subquery variants (e.g. a subquery arising from an
            //       EXISTS expression), we may not need to require all indices.
            plan.inputs()
                .into_iter()
                .map(RequiredIndicies::new_for_all_exprs)
                .collect()
        }
        LogicalPlan::Extension(extension) => {
            let Some(necessary_children_indices) =
                extension.node.necessary_children_exprs(indices.indices())
            else {
                // Requirements from parent cannot be routed down to user defined logical plan safely
                return Ok(Transformed::no(plan));
            };
            let children = extension.node.inputs();
            if children.len() != necessary_children_indices.len() {
                return internal_err!("Inconsistent length between children and necessary children indices. \
                Make sure `.necessary_children_exprs` implementation of the `UserDefinedLogicalNode` is \
                consistent with actual children length for the node.");
            }
            children
                .into_iter()
                .zip(necessary_children_indices)
                .map(|(child, necessary_indices)| {
                    RequiredIndicies::new_from_indices(necessary_indices)
                        .with_plan_exprs(&plan, child.schema())
                })
                .collect::<Result<Vec<_>>>()?
        }
        LogicalPlan::EmptyRelation(_)
        | LogicalPlan::RecursiveQuery(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::DescribeTable(_) => {
            // These operators have no inputs, so stop the optimization process.
            return Ok(Transformed::no(plan));
        }
        LogicalPlan::Join(join) => {
            let left_len = join.left.schema().fields().len();
            let (left_req_indices, right_req_indices) =
                split_join_requirements(left_len, indices, &join.join_type);
            let left_indices =
                left_req_indices.with_plan_exprs(&plan, join.left.schema())?;
            let right_indices =
                right_req_indices.with_plan_exprs(&plan, join.right.schema())?;
            // Joins benefit from "small" input tables (lower memory usage).
            // Therefore, each child benefits from projection:
            vec![
                left_indices.with_projection_beneficial(),
                right_indices.with_projection_beneficial(),
            ]
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_len = cross_join.left.schema().fields().len();
            let (left_indices, right_indices) =
                split_join_requirements(left_len, indices, &JoinType::Inner);
            // Joins benefit from "small" input tables (lower memory usage).
            // Therefore, each child benefits from projection:
            vec![
                left_indices.with_projection_beneficial(),
                right_indices.with_projection_beneficial(),
            ]
        }
        // these nodes are explicitly rewritten in the match statement above
        LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::TableScan(_) => {
            return internal_err!(
                "OptimizeProjection: should have handled in the match statement above"
            );
        }
        LogicalPlan::Unnest(Unnest {
            dependency_indices, ..
        }) => {
            vec![RequiredIndicies::new_from_indices(
                dependency_indices.clone(),
            )]
        }
    };

    // Required indices are currently ordered (child0, child1, ...)
    // but the loop pops off the last element, so we need to reverse the order
    child_required_indices.reverse();
    if child_required_indices.len() != plan.inputs().len() {
        return internal_err!(
            "OptimizeProjection: child_required_indices length mismatch with plan inputs"
        );
    }

    // Rewrite children of the plan
    let transformed_plan = plan.map_children(|child| {
        let required_indices = child_required_indices.pop().ok_or_else(|| {
            internal_datafusion_err!(
                "Unexpected number of required_indices in OptimizeProjections rule"
            )
        })?;

        let projection_beneficial = required_indices.projection_beneficial();
        let project_exprs = required_indices.get_required_exprs(child.schema());

        optimize_projections(child, config, required_indices)?.transform_data(
            |new_input| {
                if projection_beneficial {
                    add_projection_on_top_if_helpful(new_input, project_exprs)
                } else {
                    Ok(Transformed::no(new_input))
                }
            },
        )
    })?;

    // If any of the children are transformed, we need to potentially update the plan's schema
    if transformed_plan.transformed {
        transformed_plan.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(transformed_plan)
    }
}

/// Merges consecutive projections.
///
/// Given a projection `proj`, this function attempts to merge it with a previous
/// projection if it exists and if merging is beneficial. Merging is considered
/// beneficial when expressions in the current projection are non-trivial and
/// appear more than once in its input fields. This can act as a caching mechanism
/// for non-trivial computations.
///
/// # Parameters
///
/// * `proj` - A reference to the `Projection` to be merged.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(Projection))`: Merge was beneficial and successful. Contains the
///   merged projection.
/// - `Ok(None)`: Signals that merge is not beneficial (and has not taken place).
/// - `Err(error)`: An error occured during the function call.
fn merge_consecutive_projections(proj: Projection) -> Result<Transformed<Projection>> {
    let Projection {
        expr,
        input,
        schema,
        ..
    } = proj;
    let LogicalPlan::Projection(prev_projection) = input.as_ref() else {
        return Projection::try_new_with_schema(expr, input, schema).map(Transformed::no);
    };

    // Count usages (referrals) of each projection expression in its input fields:
    let mut column_referral_map = HashMap::<&Column, usize>::new();
    for columns in expr.iter().map(|expr| expr.column_refs()) {
        for col in columns.into_iter() {
            *column_referral_map.entry(col).or_default() += 1;
        }
    }

    // If an expression is non-trivial and appears more than once, do not merge
    // them as consecutive projections will benefit from a compute-once approach.
    // For details, see: https://github.com/apache/datafusion/issues/8296
    if column_referral_map.into_iter().any(|(col, usage)| {
        usage > 1
            && !is_expr_trivial(
                &prev_projection.expr
                    [prev_projection.schema.index_of_column(col).unwrap()],
            )
    }) {
        // no change
        return Projection::try_new_with_schema(expr, input, schema).map(Transformed::no);
    }

    let LogicalPlan::Projection(prev_projection) = unwrap_arc(input) else {
        // We know it is a `LogicalPlan::Projection` from check above
        unreachable!();
    };

    // Try to rewrite the expressions in the current projection using the
    // previous projection as input:
    let name_preserver = NamePreserver::new_for_projection();
    let mut original_names = vec![];
    let new_exprs = expr.into_iter().map_until_stop_and_collect(|expr| {
        original_names.push(name_preserver.save(&expr)?);

        // do not rewrite top level Aliases (rewriter will remove all aliases within exprs)
        match expr {
            Expr::Alias(Alias {
                expr,
                relation,
                name,
            }) => rewrite_expr(*expr, &prev_projection).map(|result| {
                result.update_data(|expr| Expr::Alias(Alias::new(expr, relation, name)))
            }),
            e => rewrite_expr(e, &prev_projection),
        }
    })?;

    // if the expressions could be rewritten, create a new projection with the
    // new expressions
    if new_exprs.transformed {
        // Add any needed aliases back to the expressions
        let new_exprs = new_exprs
            .data
            .into_iter()
            .zip(original_names.into_iter())
            .map(|(expr, original_name)| original_name.restore(expr))
            .collect::<Result<Vec<_>>>()?;
        Projection::try_new(new_exprs, prev_projection.input).map(Transformed::yes)
    } else {
        // not rewritten, so put the projection back together
        let input = Arc::new(LogicalPlan::Projection(prev_projection));
        Projection::try_new_with_schema(new_exprs.data, input, schema)
            .map(Transformed::no)
    }
}

// Check whether `expr` is trivial; i.e. it doesn't imply any computation.
fn is_expr_trivial(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_))
}

/// Rewrites a projection expression using the projection before it (i.e. its input)
/// This is a subroutine to the `merge_consecutive_projections` function.
///
/// # Parameters
///
/// * `expr` - A reference to the expression to rewrite.
/// * `input` - A reference to the input of the projection expression (itself
///   a projection).
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(Expr))`: Rewrite was successful. Contains the rewritten result.
/// - `Ok(None)`: Signals that `expr` can not be rewritten.
/// - `Err(error)`: An error occurred during the function call.
///
/// # Notes
/// This rewrite also removes any unnecessary layers of aliasing.
///
/// Without trimming, we can end up with unnecessary indirections inside expressions
/// during projection merges.
///
/// Consider:
///
/// ```text
/// Projection(a1 + b1 as sum1)
/// --Projection(a as a1, b as b1)
/// ----Source(a, b)
/// ```
///
/// After merge, we want to produce:
///
/// ```text
/// Projection(a + b as sum1)
/// --Source(a, b)
/// ```
///
/// Without trimming, we would end up with:
///
/// ```text
/// Projection((a as a1 + b as b1) as sum1)
/// --Source(a, b)
/// ```
fn rewrite_expr(expr: Expr, input: &Projection) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        match expr {
            //  remove any intermediate aliases
            Expr::Alias(alias) => Ok(Transformed::yes(*alias.expr)),
            Expr::Column(col) => {
                // Find index of column:
                let idx = input.schema.index_of_column(&col)?;
                // get the corresponding unaliased input expression
                //
                // For example:
                // * the input projection is [`a + b` as c, `d + e` as f]
                // * the current column is an expression "f"
                //
                // return the expression `d + e` (not `d + e` as f)
                let input_expr = input.expr[idx].clone().unalias_nested().data;
                Ok(Transformed::yes(input_expr))
            }
            // Unsupported type for consecutive projection merge analysis.
            _ => Ok(Transformed::no(expr)),
        }
    })
}

/// Accumulates outer-referenced columns by the
/// given expression, `expr`.
///
/// # Parameters
///
/// * `expr` - The expression to analyze for outer-referenced columns.
/// * `columns` - A mutable reference to a `HashSet<Column>` where detected
///   columns are collected.
fn outer_columns<'a>(expr: &'a Expr, columns: &mut HashSet<&'a Column>) {
    // inspect_expr_pre doesn't handle subquery references, so find them explicitly
    expr.apply(|expr| {
        match expr {
            Expr::OuterReferenceColumn(_, col) => {
                columns.insert(col);
            }
            Expr::ScalarSubquery(subquery) => {
                outer_columns_helper_multi(&subquery.outer_ref_columns, columns);
            }
            Expr::Exists(exists) => {
                outer_columns_helper_multi(&exists.subquery.outer_ref_columns, columns);
            }
            Expr::InSubquery(insubquery) => {
                outer_columns_helper_multi(
                    &insubquery.subquery.outer_ref_columns,
                    columns,
                );
            }
            _ => {}
        };
        Ok(TreeNodeRecursion::Continue)
    })
    // unwrap: closure above never returns Err, so can not be Err here
    .unwrap();
}

/// A recursive subroutine that accumulates outer-referenced columns by the
/// given expressions (`exprs`).
///
/// # Parameters
///
/// * `exprs` - The expressions to analyze for outer-referenced columns.
/// * `columns` - A mutable reference to a `HashSet<Column>` where detected
///   columns are collected.
fn outer_columns_helper_multi<'a, 'b>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    columns: &'b mut HashSet<&'a Column>,
) {
    exprs.into_iter().for_each(|e| outer_columns(e, columns));
}

/// Splits requirement indices for a join into left and right children based on
/// the join type.
///
/// This function takes the length of the left child, a slice of requirement
/// indices, and the type of join (e.g. `INNER`, `LEFT`, `RIGHT`) as arguments.
/// Depending on the join type, it divides the requirement indices into those
/// that apply to the left child and those that apply to the right child.
///
/// - For `INNER`, `LEFT`, `RIGHT` and `FULL` joins, the requirements are split
///   between left and right children. The right child indices are adjusted to
///   point to valid positions within the right child by subtracting the length
///   of the left child.
///
/// - For `LEFT ANTI`, `LEFT SEMI`, `RIGHT SEMI` and `RIGHT ANTI` joins, all
///   requirements are re-routed to either the left child or the right child
///   directly, depending on the join type.
///
/// # Parameters
///
/// * `left_len` - The length of the left child.
/// * `indices` - A slice of requirement indices.
/// * `join_type` - The type of join (e.g. `INNER`, `LEFT`, `RIGHT`).
///
/// # Returns
///
/// A tuple containing two vectors of `usize` indices: The first vector represents
/// the requirements for the left child, and the second vector represents the
/// requirements for the right child. The indices are appropriately split and
/// adjusted based on the join type.
fn split_join_requirements(
    left_len: usize,
    indices: RequiredIndicies,
    join_type: &JoinType,
) -> (RequiredIndicies, RequiredIndicies) {
    match join_type {
        // In these cases requirements are split between left/right children:
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            // Decrease right side indices by `left_len` so that they point to valid
            // positions within the right child:
            indices.split_off(left_len)
        }
        // All requirements can be re-routed to left child directly.
        JoinType::LeftAnti | JoinType::LeftSemi => (indices, RequiredIndicies::new()),
        // All requirements can be re-routed to right side directly.
        // No need to change index, join schema is right child schema.
        JoinType::RightSemi | JoinType::RightAnti => (RequiredIndicies::new(), indices),
    }
}

/// Adds a projection on top of a logical plan if doing so reduces the number
/// of columns for the parent operator.
///
/// This function takes a `LogicalPlan` and a list of projection expressions.
/// If the projection is beneficial (it reduces the number of columns in the
/// plan) a new `LogicalPlan` with the projection is created and returned, along
/// with a `true` flag. If the projection doesn't reduce the number of columns,
/// the original plan is returned with a `false` flag.
///
/// # Parameters
///
/// * `plan` - The input `LogicalPlan` to potentially add a projection to.
/// * `project_exprs` - A list of expressions for the projection.
///
/// # Returns
///
/// A `Transformed` indicating if a projection was added
fn add_projection_on_top_if_helpful(
    plan: LogicalPlan,
    project_exprs: Vec<Expr>,
) -> Result<Transformed<LogicalPlan>> {
    // Make sure projection decreases the number of columns, otherwise it is unnecessary.
    if project_exprs.len() >= plan.schema().fields().len() {
        Ok(Transformed::no(plan))
    } else {
        Projection::try_new(project_exprs, Arc::new(plan))
            .map(LogicalPlan::Projection)
            .map(Transformed::yes)
    }
}

/// Rewrite the given projection according to the fields required by its
/// ancestors.
///
/// # Parameters
///
/// * `proj` - A reference to the original projection to rewrite.
/// * `config` - A reference to the optimizer configuration.
/// * `indices` - A slice of indices representing the columns required by the
///   ancestors of the given projection.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(LogicalPlan))`: Contains the rewritten projection
/// - `Ok(None)`: No rewrite necessary.
/// - `Err(error)`: An error occured during the function call.
fn rewrite_projection_given_requirements(
    proj: Projection,
    config: &dyn OptimizerConfig,
    indices: RequiredIndicies,
) -> Result<Transformed<LogicalPlan>> {
    let Projection { expr, input, .. } = proj;

    let exprs_used = indices.get_at_indices(&expr);

    let required_indices =
        RequiredIndicies::new().with_exprs(input.schema(), exprs_used.iter())?;

    // rewrite the children projection, and if they are changed rewrite the
    // projection down
    optimize_projections(unwrap_arc(input), config, required_indices)?.transform_data(
        |input| {
            if is_projection_unnecessary(&input, &exprs_used)? {
                Ok(Transformed::yes(input))
            } else {
                Projection::try_new(exprs_used, Arc::new(input))
                    .map(LogicalPlan::Projection)
                    .map(Transformed::yes)
            }
        },
    )
}

/// Projection is unnecessary, when
/// - input schema of the projection, output schema of the projection are same, and
/// - all projection expressions are either Column or Literal
fn is_projection_unnecessary(input: &LogicalPlan, proj_exprs: &[Expr]) -> Result<bool> {
    Ok(&projection_schema(input, proj_exprs)? == input.schema()
        && proj_exprs.iter().all(is_expr_trivial))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Formatter;
    use std::ops::Add;
    use std::sync::Arc;
    use std::vec;

    use crate::optimize_projections::OptimizeProjections;
    use crate::optimizer::Optimizer;
    use crate::test::{
        assert_fields_eq, assert_optimized_plan_eq, scan_empty, test_table_scan,
        test_table_scan_fields, test_table_scan_with_name,
    };
    use crate::{OptimizerContext, OptimizerRule};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{
        Column, DFSchema, DFSchemaRef, JoinType, Result, TableReference,
    };
    use datafusion_expr::AggregateExt;
    use datafusion_expr::{
        binary_expr, build_join_schema,
        builder::table_scan_with_filters,
        col,
        expr::{self, Cast},
        lit,
        logical_plan::{builder::LogicalPlanBuilder, table_scan},
        max, min, not, try_cast, when, AggregateFunction, BinaryExpr, Expr, Extension,
        Like, LogicalPlan, Operator, Projection, UserDefinedLogicalNodeCore, WindowFrame,
        WindowFunctionDefinition,
    };

    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::expr_fn::count;

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(OptimizeProjections::new()), plan, expected)
    }

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct NoOpUserDefined {
        exprs: Vec<Expr>,
        schema: DFSchemaRef,
        input: Arc<LogicalPlan>,
    }

    impl NoOpUserDefined {
        fn new(schema: DFSchemaRef, input: Arc<LogicalPlan>) -> Self {
            Self {
                exprs: vec![],
                schema,
                input,
            }
        }

        fn with_exprs(mut self, exprs: Vec<Expr>) -> Self {
            self.exprs = exprs;
            self
        }
    }

    impl UserDefinedLogicalNodeCore for NoOpUserDefined {
        fn name(&self) -> &str {
            "NoOpUserDefined"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.exprs.clone()
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "NoOpUserDefined")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            mut inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                exprs,
                input: Arc::new(inputs.swap_remove(0)),
                schema: self.schema.clone(),
            })
        }

        fn necessary_children_exprs(
            &self,
            output_columns: &[usize],
        ) -> Option<Vec<Vec<usize>>> {
            // Since schema is same. Output columns requires their corresponding version in the input columns.
            Some(vec![output_columns.to_vec()])
        }
    }

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct UserDefinedCrossJoin {
        exprs: Vec<Expr>,
        schema: DFSchemaRef,
        left_child: Arc<LogicalPlan>,
        right_child: Arc<LogicalPlan>,
    }

    impl UserDefinedCrossJoin {
        fn new(left_child: Arc<LogicalPlan>, right_child: Arc<LogicalPlan>) -> Self {
            let left_schema = left_child.schema();
            let right_schema = right_child.schema();
            let schema = Arc::new(
                build_join_schema(left_schema, right_schema, &JoinType::Inner).unwrap(),
            );
            Self {
                exprs: vec![],
                schema,
                left_child,
                right_child,
            }
        }
    }

    impl UserDefinedLogicalNodeCore for UserDefinedCrossJoin {
        fn name(&self) -> &str {
            "UserDefinedCrossJoin"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.left_child, &self.right_child]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.exprs.clone()
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "UserDefinedCrossJoin")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            mut inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            assert_eq!(inputs.len(), 2);
            Ok(Self {
                exprs,
                left_child: Arc::new(inputs.remove(0)),
                right_child: Arc::new(inputs.remove(0)),
                schema: self.schema.clone(),
            })
        }

        fn necessary_children_exprs(
            &self,
            output_columns: &[usize],
        ) -> Option<Vec<Vec<usize>>> {
            let left_child_len = self.left_child.schema().fields().len();
            let mut left_reqs = vec![];
            let mut right_reqs = vec![];
            for &out_idx in output_columns {
                if out_idx < left_child_len {
                    left_reqs.push(out_idx);
                } else {
                    // Output indices further than the left_child_len
                    // comes from right children
                    right_reqs.push(out_idx - left_child_len)
                }
            }
            Some(vec![left_reqs, right_reqs])
        }
    }

    #[test]
    fn merge_two_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn merge_three_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .project(vec![col("a")])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn merge_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .project(vec![col("a").alias("alias")])?
            .build()?;

        let expected = "Projection: test.a AS alias\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn merge_nested_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("alias1").alias("alias2")])?
            .project(vec![col("alias2").alias("alias")])?
            .build()?;

        let expected = "Projection: test.a AS alias\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_nested_count() -> Result<()> {
        let schema = Schema::new(vec![Field::new("foo", DataType::Int32, false)]);

        let groups: Vec<Expr> = vec![];

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .aggregate(groups.clone(), vec![count(lit(1))])
            .unwrap()
            .aggregate(groups, vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();

        let expected = "Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]]\
        \n  Projection: \
        \n    Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]]\
        \n      TableScan: ?table? projection=[]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_neg_push_down() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![-col("a")])?
            .build()?;

        let expected = "Projection: (- test.a)\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_null() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_null()])?
            .build()?;

        let expected = "Projection: test.a IS NULL\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_not_null() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_null()])?
            .build()?;

        let expected = "Projection: test.a IS NOT NULL\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_true() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_true()])?
            .build()?;

        let expected = "Projection: test.a IS TRUE\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_not_true() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_true()])?
            .build()?;

        let expected = "Projection: test.a IS NOT TRUE\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_false() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_false()])?
            .build()?;

        let expected = "Projection: test.a IS FALSE\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_not_false() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_false()])?
            .build()?;

        let expected = "Projection: test.a IS NOT FALSE\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_unknown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_unknown()])?
            .build()?;

        let expected = "Projection: test.a IS UNKNOWN\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_is_not_unknown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").is_not_unknown()])?
            .build()?;

        let expected = "Projection: test.a IS NOT UNKNOWN\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_not() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![not(col("a"))])?
            .build()?;

        let expected = "Projection: NOT test.a\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_try_cast() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![try_cast(col("a"), DataType::Float64)])?
            .build()?;

        let expected = "Projection: TRY_CAST(test.a AS Float64)\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_similar_to() -> Result<()> {
        let table_scan = test_table_scan()?;
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit("[0-9]"));
        let similar_to_expr =
            Expr::SimilarTo(Like::new(false, expr, pattern, None, false));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![similar_to_expr])?
            .build()?;

        let expected = "Projection: test.a SIMILAR TO Utf8(\"[0-9]\")\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_between() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").between(lit(1), lit(3))])?
            .build()?;

        let expected = "Projection: test.a BETWEEN Int32(1) AND Int32(3)\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    // Test Case expression
    #[test]
    fn test_case_merged() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .project(vec![
                col("a"),
                when(col("a").eq(lit(1)), lit(10))
                    .otherwise(col("d"))?
                    .alias("d"),
            ])?
            .build()?;

        let expected = "Projection: test.a, CASE WHEN test.a = Int32(1) THEN Int32(10) ELSE Int32(0) END AS d\
        \n  TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    // Test outer projection isn't discarded despite the same schema as inner
    // https://github.com/apache/datafusion/issues/8942
    #[test]
    fn test_derived_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").add(lit(1)).alias("a"), lit(0).alias("d")])?
            .project(vec![
                col("a"),
                when(col("a").eq(lit(1)), lit(10))
                    .otherwise(col("d"))?
                    .alias("d"),
            ])?
            .build()?;

        let expected =
            "Projection: a, CASE WHEN a = Int32(1) THEN Int32(10) ELSE d END AS d\
        \n  Projection: test.a + Int32(1) AS a, Int32(0) AS d\
        \n    TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    // Since only column `a` is referred at the output. Scan should only contain projection=[a].
    // User defined node should be able to propagate necessary expressions by its parent to its child.
    #[test]
    fn test_user_defined_logical_plan_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpUserDefined::new(
                table_scan.schema().clone(),
                Arc::new(table_scan.clone()),
            )),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .build()?;

        let expected = "Projection: test.a, Int32(0) AS d\
        \n  NoOpUserDefined\
        \n    TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    // Only column `a` is referred at the output. However, User defined node itself uses column `b`
    // during its operation. Hence, scan should contain projection=[a, b].
    // User defined node should be able to propagate necessary expressions by its parent, as well as its own
    // required expressions.
    #[test]
    fn test_user_defined_logical_plan_node2() -> Result<()> {
        let table_scan = test_table_scan()?;
        let exprs = vec![Expr::Column(Column::from_qualified_name("b"))];
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                NoOpUserDefined::new(
                    table_scan.schema().clone(),
                    Arc::new(table_scan.clone()),
                )
                .with_exprs(exprs),
            ),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .build()?;

        let expected = "Projection: test.a, Int32(0) AS d\
        \n  NoOpUserDefined\
        \n    TableScan: test projection=[a, b]";
        assert_optimized_plan_equal(plan, expected)
    }

    // Only column `a` is referred at the output. However, User defined node itself uses expression `b+c`
    // during its operation. Hence, scan should contain projection=[a, b, c].
    // User defined node should be able to propagate necessary expressions by its parent, as well as its own
    // required expressions. Expressions doesn't have to be just column. Requirements from complex expressions
    // should be propagated also.
    #[test]
    fn test_user_defined_logical_plan_node3() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left_expr = Expr::Column(Column::from_qualified_name("b"));
        let right_expr = Expr::Column(Column::from_qualified_name("c"));
        let binary_expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left_expr),
            Operator::Plus,
            Box::new(right_expr),
        ));
        let exprs = vec![binary_expr];
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                NoOpUserDefined::new(
                    table_scan.schema().clone(),
                    Arc::new(table_scan.clone()),
                )
                .with_exprs(exprs),
            ),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("a"), lit(0).alias("d")])?
            .build()?;

        let expected = "Projection: test.a, Int32(0) AS d\
        \n  NoOpUserDefined\
        \n    TableScan: test projection=[a, b, c]";
        assert_optimized_plan_equal(plan, expected)
    }

    // Columns `l.a`, `l.c`, `r.a` is referred at the output.
    // User defined node should be able to propagate necessary expressions by its parent, to its children.
    // Even if it has multiple children.
    // left child should have `projection=[a, c]`, and right side should have `projection=[a]`.
    #[test]
    fn test_user_defined_logical_plan_node4() -> Result<()> {
        let left_table = test_table_scan_with_name("l")?;
        let right_table = test_table_scan_with_name("r")?;
        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(UserDefinedCrossJoin::new(
                Arc::new(left_table.clone()),
                Arc::new(right_table.clone()),
            )),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .project(vec![col("l.a"), col("l.c"), col("r.a"), lit(0).alias("d")])?
            .build()?;

        let expected = "Projection: l.a, l.c, r.a, Int32(0) AS d\
        \n  UserDefinedCrossJoin\
        \n    TableScan: l projection=[a, c]\
        \n    TableScan: r projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]]\
        \n  TableScan: test projection=[b]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[MAX(test.b)]]\
        \n  TableScan: test projection=[b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_group_by_with_table_alias() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[a.c]], aggr=[[MAX(a.b)]]\
        \n  SubqueryAlias: a\
        \n    TableScan: test projection=[b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_no_group_by_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("c").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]]\
        \n  Projection: test.b\
        \n    Filter: test.c > Int32(1)\
        \n      TableScan: test projection=[b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_with_periods() -> Result<()> {
        let schema = Schema::new(vec![Field::new("tag.one", DataType::Utf8, false)]);

        // Build a plan that looks as follows (note "tag.one" is a column named
        // "tag.one", not a column named "one" in a table named "tag"):
        //
        // Projection: tag.one
        //   Aggregate: groupBy=[], aggr=[MAX("tag.one") AS "tag.one"]
        //    TableScan
        let plan = table_scan(Some("m4"), &schema, None)?
            .aggregate(
                Vec::<Expr>::new(),
                vec![max(col(Column::new_unqualified("tag.one"))).alias("tag.one")],
            )?
            .project([col(Column::new_unqualified("tag.one"))])?
            .build()?;

        let expected = "\
        Aggregate: groupBy=[[]], aggr=[[MAX(m4.tag.one) AS tag.one]]\
        \n  TableScan: m4 projection=[tag.one]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn redundant_project() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.c, test.b\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn reorder_scan() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let plan = table_scan(Some("test"), &schema, Some(vec![1, 0, 2]))?.build()?;
        let expected = "TableScan: test projection=[b, a, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn reorder_scan_projection() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let plan = table_scan(Some("test"), &schema, Some(vec![1, 0, 2]))?
            .project(vec![col("a"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.b\
        \n  TableScan: test projection=[b, a]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn reorder_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .build()?;
        let expected = "Projection: test.c, test.b, test.a\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn noncontinuous_redundant_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("b").gt(lit(1)))?
            .filter(col("a").gt(lit(1)))?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.c, test.b\
        \n  Filter: test.a > Int32(1)\
        \n    Filter: test.b > Int32(1)\
        \n      Projection: test.c, test.a, test.b\
        \n        Filter: test.c > Int32(1)\
        \n          Projection: test.c, test.b, test.a\
        \n            TableScan: test projection=[a, b, c]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn join_schema_trim_full_join_column_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            .project(vec![col("a"), col("b"), col("c1")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Left Join: test.a = test2.c1\
        \n  TableScan: test projection=[a, b]\
        \n  TableScan: test2 projection=[c1]";

        let optimized_plan = optimize(plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan;
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    (
                        Some("test".into()),
                        Arc::new(Field::new("a", DataType::UInt32, false))
                    ),
                    (
                        Some("test".into()),
                        Arc::new(Field::new("b", DataType::UInt32, false))
                    ),
                    (
                        Some("test2".into()),
                        Arc::new(Field::new("c1", DataType::UInt32, true))
                    ),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_partial_join_column_projection() -> Result<()> {
        // test join column push down without explicit column projections

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            // projecting joined column `a` should push the right side column `c1` projection as
            // well into test2 table even though `c1` is not referenced in projection.
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Projection: test.a, test.b\
        \n  Left Join: test.a = test2.c1\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[c1]";

        let optimized_plan = optimize(plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    (
                        Some("test".into()),
                        Arc::new(Field::new("a", DataType::UInt32, false))
                    ),
                    (
                        Some("test".into()),
                        Arc::new(Field::new("b", DataType::UInt32, false))
                    ),
                    (
                        Some("test2".into()),
                        Arc::new(Field::new("c1", DataType::UInt32, true))
                    ),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_using_join() -> Result<()> {
        // shared join columns from using join should be pushed to both sides

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join_using(table2_scan, JoinType::Left, vec!["a"])?
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to table scan
        let expected = "Projection: test.a, test.b\
        \n  Left Join: Using test.a = test2.a\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[a]";

        let optimized_plan = optimize(plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    (
                        Some("test".into()),
                        Arc::new(Field::new("a", DataType::UInt32, false))
                    ),
                    (
                        Some("test".into()),
                        Arc::new(Field::new("b", DataType::UInt32, false))
                    ),
                    (
                        Some("test2".into()),
                        Arc::new(Field::new("a", DataType::UInt32, true))
                    ),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(table_scan)
            .project(vec![Expr::Cast(Cast::new(
                Box::new(col("c")),
                DataType::Float64,
            ))])?
            .build()?;

        let expected = "Projection: CAST(test.c AS Float64)\
        \n  TableScan: test projection=[c]";

        assert_optimized_plan_equal(projection, expected)
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);
        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "TableScan: test projection=[a, b]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn table_scan_projected_schema_non_qualified_relation() -> Result<()> {
        let table_scan = test_table_scan()?;
        let input_schema = table_scan.schema();
        assert_eq!(3, input_schema.fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // Build the LogicalPlan directly (don't use PlanBuilder), so
        // that the Column references are unqualified (e.g. their
        // relation is `None`). PlanBuilder resolves the expressions
        let expr = vec![col("test.a"), col("test.b")];
        let plan =
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(table_scan))?);

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "TableScan: test projection=[a, b]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a")])?
            .limit(0, Some(5))?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: skip=0, fetch=5\
        \n  Projection: test.c, test.a\
        \n    TableScan: test projection=[a, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;
        // should expand projection to all columns without projection
        let expected = "TableScan: test projection=[a, b, c]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1_i64), lit(2_i64)])?
            .build()?;
        let expected = "Projection: Int64(1), Int64(2)\
                      \n  TableScan: test projection=[]";
        assert_optimized_plan_equal(plan, expected)
    }

    /// tests that it removes unused columns in projections
    #[test]
    fn table_unused_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "b" in the first projection => remove it
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("c").gt(lit(1)))?
            .aggregate(vec![col("c")], vec![max(col("a"))])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "MAX(test.a)"]);

        let plan = optimize(plan).expect("failed to optimize plan");
        let expected = "\
        Aggregate: groupBy=[[test.c]], aggr=[[MAX(test.a)]]\
        \n  Filter: test.c > Int32(1)\
        \n    Projection: test.c, test.a\
        \n      TableScan: test projection=[a, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    /// tests that it removes un-needed projections
    #[test]
    fn table_unused_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        let expected = "\
        Projection: Int32(1) AS a\
        \n  TableScan: test projection=[]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn table_full_filter_pushdown() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let table_scan = table_scan_with_filters(
            Some("test"),
            &schema,
            None,
            vec![col("b").eq(lit(1))],
        )?
        .build()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        let expected = "\
        Projection: Int32(1) AS a\
        \n  TableScan: test projection=[], full_filters=[b = Int32(1)]";

        assert_optimized_plan_equal(plan, expected)
    }

    /// tests that optimizing twice yields same plan
    #[test]
    fn test_double_optimization() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        let optimized_plan1 = optimize(plan).expect("failed to optimize plan");
        let optimized_plan2 =
            optimize(optimized_plan1.clone()).expect("failed to optimize plan");

        let formatted_plan1 = format!("{optimized_plan1:?}");
        let formatted_plan2 = format!("{optimized_plan2:?}");
        assert_eq!(formatted_plan1, formatted_plan2);
        Ok(())
    }

    /// tests that it removes an aggregate is never used downstream
    #[test]
    fn table_unused_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "min(b)" => remove it
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("c")], vec![max(col("b")), min(col("b"))])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("MAX(test.b)")])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a", "MAX(test.b)"]);

        let expected = "Projection: test.c, test.a, MAX(test.b)\
        \n  Filter: test.c > Int32(1)\
        \n    Aggregate: groupBy=[[test.a, test.c]], aggr=[[MAX(test.b)]]\
        \n      TableScan: test projection=[a, b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn aggregate_filter_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let aggr_with_filter = count_udaf()
            .call(vec![col("b")])
            .filter(col("c").gt(lit(42)))
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count(col("b")), aggr_with_filter.alias("count2")],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[count(test.b), count(test.b) FILTER (WHERE test.c > Int32(42)) AS count2]]\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn pushdown_through_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .project(vec![col("a")])?
            .build()?;

        let expected = "Projection: test.a\
        \n  Distinct:\
        \n    TableScan: test projection=[a, b]";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let max1 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunctionDefinition::AggregateFunction(AggregateFunction::Max),
            vec![col("test.a")],
            vec![col("test.b")],
            vec![],
            WindowFrame::new(None),
            None,
        ));

        let max2 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunctionDefinition::AggregateFunction(AggregateFunction::Max),
            vec![col("test.b")],
            vec![],
            vec![],
            WindowFrame::new(None),
            None,
        ));
        let col1 = col(max1.display_name()?);
        let col2 = col(max2.display_name()?);

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![max1])?
            .window(vec![max2])?
            .project(vec![col1, col2])?
            .build()?;

        let expected = "Projection: MAX(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MAX(test.b) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(test.b) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    Projection: test.b, MAX(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n      WindowAggr: windowExpr=[[MAX(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n        TableScan: test projection=[a, b]";

        assert_optimized_plan_equal(plan, expected)
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        let optimizer = Optimizer::with_rules(vec![Arc::new(OptimizeProjections::new())]);
        let optimized_plan =
            optimizer.optimize(plan, &OptimizerContext::new(), observe)?;
        Ok(optimized_plan)
    }
}
