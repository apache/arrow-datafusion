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

//! Optimizer rule to prune unnecessary Columns from the intermediate schemas inside the [LogicalPlan].
//! This rule
//! - Removes unnecessary columns that are not showed at the output, and that are not used during computation.
//! - Adds projection to decrease table column size before operators that benefits from less memory at its input.
//! - Removes unnecessary [LogicalPlan::Projection] from the [LogicalPlan].
use crate::optimizer::ApplyOrder;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    get_required_group_by_exprs_indices, Column, DFSchema, JoinType, Result,
};
use datafusion_expr::{
    logical_plan::LogicalPlan, Aggregate, Expr, Projection, TableScan, Window,
};
use itertools::{izip, Itertools};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

/// A rule for optimizing logical plans by removing unused columns.
///
/// `RemoveUnusedColumns` is an optimizer rule that identifies and eliminates columns from a logical plan
/// that are not used in any downstream operations. This can improve query performance and reduce unnecessary
/// data processing.
///
/// The rule analyzes the input logical plan, determines the necessary column indices, and then removes any
/// unnecessary columns. Additionally, it eliminates any unnecessary projections in the plan.
#[derive(Default)]
pub struct RemoveUnusedColumns {}

impl RemoveUnusedColumns {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RemoveUnusedColumns {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // All of the fields at the output are necessary.
        let indices = require_all_indices(plan);
        let unnecessary_columns_removed =
            remove_unnecessary_columns(plan, config, &indices)?;
        let projections_eliminated = unnecessary_columns_removed
            .map(|plan| plan.transform_up(&eliminate_projection))
            .transpose()?;
        Ok(projections_eliminated)
    }

    fn name(&self) -> &str {
        "RemoveUnusedColumns"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

/// Helper function to accumulate outer-referenced columns referred by the `expr`.
///
/// # Arguments
///
/// * `expr` - The expression to be analyzed for outer-referenced columns.
/// * `columns` - A mutable reference to a `HashSet<Column>` where the detected columns are collected.
fn outer_columns_helper(expr: &Expr, columns: &mut HashSet<Column>) {
    match expr {
        Expr::OuterReferenceColumn(_, col) => {
            columns.insert(col.clone());
        }
        Expr::BinaryExpr(binary_expr) => {
            outer_columns_helper(&binary_expr.left, columns);
            outer_columns_helper(&binary_expr.right, columns);
        }
        Expr::ScalarSubquery(subquery) => {
            for expr in &subquery.outer_ref_columns {
                outer_columns_helper(expr, columns);
            }
        }
        Expr::Exists(exists) => {
            for expr in &exists.subquery.outer_ref_columns {
                outer_columns_helper(expr, columns);
            }
        }
        Expr::Alias(alias) => {
            outer_columns_helper(&alias.expr, columns);
        }
        _ => {}
    }
}

/// Retrieves a set of outer-referenced columns from an expression.
/// Please note that `expr.to_columns()` API doesn't return these columns.
///
/// # Arguments
///
/// * `expr` - The expression to be analyzed for outer-referenced columns.
///
/// # Returns
///
/// A `HashSet<Column>` containing columns that are referenced by the expression.
fn outer_columns(expr: &Expr) -> HashSet<Column> {
    let mut columns = HashSet::new();
    outer_columns_helper(expr, &mut columns);
    columns
}

/// Generates the required expressions(Column) that resides at `indices` of the `input_schema`.
///
/// # Arguments
///
/// * `input_schema` - A reference to the input schema.
/// * `indices` - A slice of `usize` indices specifying which columns are required.
///
/// # Returns
///
/// A vector of `Expr::Column` expressions, that sits at `indices` of the `input_schema`.
fn get_required_exprs(input_schema: &Arc<DFSchema>, indices: &[usize]) -> Vec<Expr> {
    let fields = input_schema.fields();
    indices
        .iter()
        .map(|&idx| Expr::Column(fields[idx].qualified_column()))
        .collect()
}

/// Get indices of the necessary fields referred by all of the `exprs` among input LogicalPlan.
///
/// # Arguments
///
/// * `input`: The input logical plan to analyze for index requirements.
/// * `exprs`: An iterator of expressions for which we want to find necessary field indices at the input.
///
/// # Returns
///
/// A [Result] object that contains the required field indices for the `input` operator, to be able to calculate
/// successfully all of the `exprs`.
fn indices_referred_by_exprs<'a, I: Iterator<Item = &'a Expr>>(
    input: &LogicalPlan,
    exprs: I,
) -> Result<Vec<usize>> {
    let new_indices = exprs
        .flat_map(|expr| {
            let mut cols = expr.to_columns()?;
            // Get outer referenced columns (expr.to_columns() doesn't return these columns).
            cols.extend(outer_columns(expr));
            cols.iter()
                .filter(|&col| input.schema().has_column(col))
                .map(|col| input.schema().index_of_column(col))
                .collect::<Result<Vec<_>>>()
        })
        .flatten()
        // Make sure no duplicate entries exists and indices are ordered.
        .sorted()
        .dedup()
        .collect::<Vec<_>>();
    Ok(new_indices)
}

/// Get all required indices for the input (indices required by parent + indices referred by `exprs`)
///
/// # Arguments
///
/// * `parent_required_indices` - A slice of indices required by the parent plan.
/// * `input` - The input logical plan to analyze for index requirements.
/// * `exprs` - An iterator of expressions used to determine required indices.
///
/// # Returns
///
/// A `Result` containing a vector of `usize` indices containing all required indices.
fn get_all_required_indices<'a, I: Iterator<Item = &'a Expr>>(
    parent_required_indices: &[usize],
    input: &LogicalPlan,
    exprs: I,
) -> Result<Vec<usize>> {
    let referred_indices = indices_referred_by_exprs(input, exprs)?;
    Ok(merge_vectors(parent_required_indices, &referred_indices))
}

/// Retrieves a list of expressions at specified indices from a slice of expressions.
///
/// This function takes a slice of expressions `exprs` and a slice of `usize` indices `indices`.
/// It returns a new vector containing the expressions from `exprs` that correspond to the provided indices (with bound check).
///
/// # Arguments
///
/// * `exprs` - A slice of expressions from which expressions are to be retrieved.
/// * `indices` - A slice of `usize` indices specifying the positions of the expressions to be retrieved.
///
/// # Returns
///
/// A vector of expressions that correspond to the specified indices. If any index is out of bounds,
/// the associated expression is skipped in the result.
fn get_at_indices(exprs: &[Expr], indices: &[usize]) -> Vec<Expr> {
    indices
        .iter()
        // Indices may point to further places than `exprs` len.
        .filter_map(|&idx| exprs.get(idx).cloned())
        .collect()
}

/// Merges two slices of `usize` values into a single vector with sorted (ascending) and deduplicated elements.
///
/// # Arguments
///
/// * `lhs` - The first slice of `usize` values to be merged.
/// * `rhs` - The second slice of `usize` values to be merged.
///
/// # Returns
///
/// A vector of `usize` values containing the merged, sorted, and deduplicated elements from `lhs` and `rhs`.
/// As an example merge of [3, 2, 4] and [3, 6, 1] will produce [1, 2, 3, 6]
fn merge_vectors(lhs: &[usize], rhs: &[usize]) -> Vec<usize> {
    let mut merged = lhs.to_vec();
    merged.extend(rhs);
    // Make sure to run sort before dedup.
    // Dedup removes consecutive same entries
    // If sort is run before it, all duplicates are removed.
    merged.sort();
    merged.dedup();
    merged
}

/// Splits requirement indices for a join into left and right children based on the join type.
///
/// This function takes the length of the left child, a slice of requirement indices, and the type
/// of join (e.g., INNER, LEFT, RIGHT, etc.) as arguments. Depending on the join type, it divides
/// the requirement indices into those that apply to the left child and those that apply to the right child.
///
/// - For INNER, LEFT, RIGHT, and FULL joins, the requirements are split between left and right children.
///   The right child indices are adjusted to point to valid positions in the right child by subtracting
///   the length of the left child.
///
/// - For LEFT ANTI, LEFT SEMI, RIGHT SEMI, and RIGHT ANTI joins, all requirements are re-routed to either
///   the left child or the right child directly, depending on the join type.
///
/// # Arguments
///
/// * `left_len` - The length of the left child.
/// * `indices` - A slice of requirement indices.
/// * `join_type` - The type of join (e.g., INNER, LEFT, RIGHT, etc.).
///
/// # Returns
///
/// A tuple containing two vectors of `usize` indices: the first vector represents the requirements for
/// the left child, and the second vector represents the requirements for the right child. The indices
/// are appropriately split and adjusted based on the join type.
fn split_join_requirements(
    left_len: usize,
    indices: &[usize],
    join_type: &JoinType,
) -> (Vec<usize>, Vec<usize>) {
    match join_type {
        // In these cases requirements split to left and right child.
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let (left_child_reqs, mut right_child_reqs): (Vec<usize>, Vec<usize>) =
                indices.iter().partition(|&&idx| idx < left_len);
            // Decrease right side index by `left_len` so that they point to valid positions in the right child.
            right_child_reqs.iter_mut().for_each(|idx| *idx -= left_len);
            (left_child_reqs, right_child_reqs)
        }
        // All requirements can be re-routed to left child directly.
        JoinType::LeftAnti | JoinType::LeftSemi => (indices.to_vec(), vec![]),
        // All requirements can be re-routed to right side directly. (No need to change index, join schema is right child schema.)
        JoinType::RightSemi | JoinType::RightAnti => (vec![], indices.to_vec()),
    }
}

/// Adds a projection on top of a logical plan if it is beneficial and reduces the number of columns for the parent operator.
///
/// This function takes a `LogicalPlan`, a list of projection expressions, and a flag indicating whether
/// the projection is beneficial. If the projection is beneficial and reduces the number of columns in
/// the plan, a new `LogicalPlan` with the projection is created and returned, along with a `true` flag.
/// If the projection is unnecessary or doesn't reduce the number of columns, the original plan is returned
/// with a `false` flag.
///
/// # Arguments
///
/// * `plan` - The input `LogicalPlan` to potentially add a projection to.
/// * `project_exprs` - A list of expressions for the projection.
/// * `projection_beneficial` - A flag indicating whether the projection is beneficial.
///
/// # Returns
///
/// A `Result` containing a tuple with two values: the resulting `LogicalPlan` (with or without
/// the added projection) and a `bool` flag indicating whether the projection was added (`true`) or not (`false`).
fn add_projection_on_top_if_helpful(
    plan: LogicalPlan,
    project_exprs: Vec<Expr>,
    projection_beneficial: bool,
) -> Result<(LogicalPlan, bool)> {
    // Make sure projection decreases table column size, otherwise it is unnecessary.
    if !projection_beneficial || project_exprs.len() >= plan.schema().fields().len() {
        Ok((plan, false))
    } else {
        let new_plan = Projection::try_new(project_exprs, Arc::new(plan))
            .map(LogicalPlan::Projection)?;
        Ok((new_plan, true))
    }
}

/// Collects and returns a vector of all indices of the fields in the schema of a logical plan.
///
/// # Arguments
///
/// * `plan` - A reference to the `LogicalPlan` for which indices are required.
///
/// # Returns
///
/// A vector of `usize` indices representing all fields in the schema of the provided logical plan.
fn require_all_indices(plan: &LogicalPlan) -> Vec<usize> {
    (0..plan.schema().fields().len()).collect()
}

/// Removes unnecessary columns (e.g Columns that are not referred at the output schema and
/// Columns that are not used during any computation, expression evaluation) from the logical plan and its inputs.
///
/// # Arguments
///
/// - `plan`: A reference to the input `LogicalPlan` to be optimized.
/// - `_config`: A reference to the optimizer configuration (not currently used).
/// - `indices`: A slice of column indices that represent the necessary column indices for downstream operations.
///
/// # Returns
///
/// - `Ok(Some(LogicalPlan))`: An optimized `LogicalPlan` with unnecessary columns removed.
/// - `Ok(None)`: If the optimization process results in a logical plan that doesn't require further propagation.
/// - `Err(error)`: If an error occurs during the optimization process.
fn remove_unnecessary_columns(
    plan: &LogicalPlan,
    _config: &dyn OptimizerConfig,
    indices: &[usize],
) -> Result<Option<LogicalPlan>> {
    // `child_required_indices` stores
    // - indices of the columns required for each child
    // - a flag indicating whether putting a projection above children is beneficial for the parent.
    // As an example LogicalPlan::Filter benefits from small tables. Hence for filter child this flag would be `true`.
    let child_required_indices: Option<Vec<(Vec<usize>, bool)>> = match plan {
        LogicalPlan::Sort(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Distinct(_) => {
            // Re-route required indices from the parent + column indices referred by expressions in the plan
            // to the child.
            // All of these operators benefits from small tables at their inputs. Hence projection_beneficial flag is `true`.
            let exprs = plan.expressions();
            let child_req_indices = plan
                .inputs()
                .into_iter()
                .map(|input| {
                    let required_indices =
                        get_all_required_indices(indices, input, exprs.iter())?;
                    Ok((required_indices, true))
                })
                .collect::<Result<Vec<_>>>()?;
            Some(child_req_indices)
        }
        LogicalPlan::Limit(_) | LogicalPlan::Prepare(_) => {
            // Re-route required indices from the parent + column indices referred by expressions in the plan
            // to the child.
            // Limit, Prepare doesn't benefit from small column numbers. Hence projection_beneficial flag is `false`.
            let exprs = plan.expressions();
            let child_req_indices = plan
                .inputs()
                .into_iter()
                .map(|input| {
                    let required_indices =
                        get_all_required_indices(indices, input, exprs.iter())?;
                    Ok((required_indices, false))
                })
                .collect::<Result<Vec<_>>>()?;
            Some(child_req_indices)
        }
        LogicalPlan::Copy(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Subquery(_) => {
            // Require all of the fields of the Dml, Ddl, Copy, Explain, Analyze, Subquery input(s).
            // Their child plan can be treated as final plan. Otherwise expected schema may not match.
            // TODO: For some subquery variants we may not need to require all indices for its input.
            //  such as Exists<SubQuery>.
            let child_requirements = plan
                .inputs()
                .iter()
                .map(|input| {
                    // Require all of the fields for each input.
                    // No projection since all of the fields at the child is required
                    (require_all_indices(input), false)
                })
                .collect::<Vec<_>>();
            Some(child_requirements)
        }
        LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::DescribeTable(_) => {
            // EmptyRelation, Values, DescribeTable, Statement has no inputs stop iteration

            // TODO: Add support for extension
            // It is not known how to direct requirements to children for LogicalPlan::Extension.
            // Safest behaviour is to stop propagation.
            None
        }
        LogicalPlan::Projection(proj) => {
            let exprs_used = get_at_indices(&proj.expr, indices);
            let required_indices =
                indices_referred_by_exprs(&proj.input, exprs_used.iter())?;
            if let Some(input) =
                remove_unnecessary_columns(&proj.input, _config, &required_indices)?
            {
                let new_proj = Projection::try_new(exprs_used, Arc::new(input))?;
                let new_proj = LogicalPlan::Projection(new_proj);
                return Ok(Some(new_proj));
            } else if exprs_used.len() < proj.expr.len() {
                // Projection expression used is different than the existing projection
                // In this case, even if child doesn't change we should update projection to use less columns.
                let new_proj = Projection::try_new(exprs_used, proj.input.clone())?;
                let new_proj = LogicalPlan::Projection(new_proj);
                return Ok(Some(new_proj));
            } else {
                // Projection doesn't change.
                return Ok(None);
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            // Split parent requirements to group by and aggregate sections
            let group_expr_len = aggregate.group_expr_len()?;
            let (group_by_reqs, mut aggregate_reqs): (Vec<usize>, Vec<usize>) =
                indices.iter().partition(|&&idx| idx < group_expr_len);
            // Offset aggregate indices so that they point to valid indices at the `aggregate.aggr_expr`
            aggregate_reqs
                .iter_mut()
                .for_each(|idx| *idx -= group_expr_len);

            // Get absolutely necessary group by fields.
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
                // Some of the fields in the group by may be required by parent, even if these fields
                // are unnecessary in terms of functional dependency.
                let required_indices =
                    merge_vectors(&simplest_groupby_indices, &group_by_reqs);
                get_at_indices(&aggregate.group_expr, &required_indices)
            } else {
                aggregate.group_expr.clone()
            };

            // Only use absolutely necessary aggregate expressions required by parent.
            let new_aggr_expr = get_at_indices(&aggregate.aggr_expr, &aggregate_reqs);
            let all_exprs_iter = new_group_bys.iter().chain(new_aggr_expr.iter());
            let necessary_indices =
                indices_referred_by_exprs(&aggregate.input, all_exprs_iter)?;

            let aggregate_input = if let Some(input) =
                remove_unnecessary_columns(&aggregate.input, _config, &necessary_indices)?
            {
                input
            } else {
                aggregate.input.as_ref().clone()
            };

            // Simplify input of the aggregation by adding a projection so that its input only contains
            // absolutely necessary columns for the aggregate expressions. Please no that we use aggregate.input.schema()
            // because necessary_indices refers to fields in this schema.
            let necessary_exprs =
                get_required_exprs(aggregate.input.schema(), &necessary_indices);
            let (aggregate_input, _is_added) =
                add_projection_on_top_if_helpful(aggregate_input, necessary_exprs, true)?;

            // Create new aggregate plan with updated input, and absolutely necessary fields.
            return Aggregate::try_new(
                Arc::new(aggregate_input),
                new_group_bys,
                new_aggr_expr,
            )
            .map(|aggregate| Some(LogicalPlan::Aggregate(aggregate)));
        }
        LogicalPlan::Window(window) => {
            // Split parent requirements to child and window expression sections.
            let n_input_fields = window.input.schema().fields().len();
            let (child_reqs, mut window_reqs): (Vec<usize>, Vec<usize>) =
                indices.iter().partition(|&&idx| idx < n_input_fields);
            // Offset window expr indices so that they point to valid indices at the `window.window_expr`
            window_reqs
                .iter_mut()
                .for_each(|idx| *idx -= n_input_fields);

            // Only use window expressions that are absolutely necessary by parent requirements.
            let new_window_expr = get_at_indices(&window.window_expr, &window_reqs);

            // All of the required column indices at the input of the window by parent, and window expression requirements.
            let required_indices = get_all_required_indices(
                &child_reqs,
                &window.input,
                new_window_expr.iter(),
            )?;
            let window_child = if let Some(new_window_child) =
                remove_unnecessary_columns(&window.input, _config, &required_indices)?
            {
                new_window_child
            } else {
                window.input.as_ref().clone()
            };
            // When no window expression is necessary, just use window input. (Remove window operator)
            return if new_window_expr.is_empty() {
                Ok(Some(window_child))
            } else {
                // Calculate required expressions at the input of the window.
                // Please note that we use `old_child`, because `required_indices` refers to `old_child`.
                let required_exprs =
                    get_required_exprs(window.input.schema(), &required_indices);
                let (window_child, _is_added) =
                    add_projection_on_top_if_helpful(window_child, required_exprs, true)?;
                let window = Window::try_new(new_window_expr, Arc::new(window_child))?;
                Ok(Some(LogicalPlan::Window(window)))
            };
        }
        LogicalPlan::Join(join) => {
            let left_len = join.left.schema().fields().len();
            let (left_req_indices, right_req_indices) =
                split_join_requirements(left_len, indices, &join.join_type);
            let exprs = plan.expressions();
            let left_indices =
                get_all_required_indices(&left_req_indices, &join.left, exprs.iter())?;
            let right_indices =
                get_all_required_indices(&right_req_indices, &join.right, exprs.iter())?;
            // Join benefits from small columns numbers at its input (decreases memory usage)
            // Hence each child benefits from projection.
            Some(vec![(left_indices, true), (right_indices, true)])
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_len = cross_join.left.schema().fields().len();
            let (left_child_indices, right_child_indices) =
                split_join_requirements(left_len, indices, &JoinType::Inner);
            // Join benefits from small columns numbers at its input (decreases memory usage)
            // Hence each child benefits from projection.
            Some(vec![
                (left_child_indices, true),
                (right_child_indices, true),
            ])
        }
        LogicalPlan::TableScan(table_scan) => {
            let projection_fields = table_scan.projected_schema.fields();
            let schema = table_scan.source.schema();
            let projection = indices
                .iter()
                .map(|&idx| {
                    schema.fields().iter().position(|field_source| {
                        projection_fields[idx].field() == field_source
                    })
                })
                .collect::<Option<Vec<_>>>();

            return Ok(Some(LogicalPlan::TableScan(TableScan::try_new(
                table_scan.table_name.clone(),
                table_scan.source.clone(),
                projection,
                table_scan.filters.clone(),
                table_scan.fetch,
            )?)));
        }
    };

    let child_required_indices =
        if let Some(child_required_indices) = child_required_indices {
            child_required_indices
        } else {
            // Stop iteration, cannot propagate requirement down below this operator.
            return Ok(None);
        };

    let new_inputs = izip!(child_required_indices, plan.inputs().into_iter())
        .map(|((required_indices, projection_beneficial), child)| {
            let (input, mut is_changed) = if let Some(new_input) =
                remove_unnecessary_columns(child, _config, &required_indices)?
            {
                (new_input, true)
            } else {
                (child.clone(), false)
            };
            let project_exprs = get_required_exprs(child.schema(), &required_indices);
            let (input, is_projection_added) = add_projection_on_top_if_helpful(
                input,
                project_exprs,
                projection_beneficial,
            )?;
            is_changed |= is_projection_added;
            Ok(is_changed.then_some(input))
        })
        .collect::<Result<Vec<Option<_>>>>()?;
    // All of the children are same in this case, no need to change plan
    if new_inputs.iter().all(|child| child.is_none()) {
        Ok(None)
    } else {
        // At least one of the children is changed.
        let new_inputs = izip!(new_inputs, plan.inputs())
            // If new_input is `None`, this means child is not changed. Hence use `old_child` during construction.
            .map(|(new_input, old_child)| new_input.unwrap_or_else(|| old_child.clone()))
            .collect::<Vec<_>>();
        let res = plan.with_new_inputs(&new_inputs)?;
        Ok(Some(res))
    }
}

/// Eliminates a `Projection` from a logical plan if it's unnecessary.
///
/// If the provided `plan` is a `LogicalPlan::Projection`, and the schema of the child plan
/// (input of the projection) matches the schema of the projection itself, then the
/// projection is unnecessary and can be removed. In such cases, this function returns
/// a `Transformed::Yes` variant containing the child plan without the projection.
///
/// If the `plan` is not a `LogicalPlan::Projection` or the schemas don't match, it returns
/// a `Transformed::No` variant containing the original `plan`.
///
/// # Arguments
///
/// * `plan` - The logical plan to be analyzed for projection elimination.
///
/// # Returns
///
/// A `Result` indicating whether the plan was transformed or not. The result contains
/// either the transformed plan or the original plan.
fn eliminate_projection(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(ref projection) => {
            let child_plan = projection.input.as_ref();
            if plan.schema() == child_plan.schema() {
                // If child schema and schema of the projection is same
                // Projection can be removed.
                Ok(Transformed::Yes(child_plan.clone()))
            } else {
                Ok(Transformed::No(plan))
            }
        }
        _ => Ok(Transformed::No(plan)),
    }
}
