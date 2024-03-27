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

//! Optimizer rule to prune unnecessary columns from intermediate schemas
//! inside the [`LogicalPlan`]. This rule:
//! - Removes unnecessary columns that do not appear at the output and/or are
//!   not used during any computation step.
//! - Adds projections to decrease table column size before operators that
//!   benefit from a smaller memory footprint at its input.
//! - Removes unnecessary [`LogicalPlan::Projection`]s from the [`LogicalPlan`].

use std::collections::HashSet;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use arrow::datatypes::SchemaRef;
use datafusion_common::{
    get_required_group_by_exprs_indices, Column, DFSchema, DFSchemaRef, JoinType, Result,
};
use datafusion_expr::expr::{Alias, ScalarFunction};
use datafusion_expr::{
    logical_plan::LogicalPlan, projection_schema, Aggregate, BinaryExpr, Cast, Distinct,
    Expr, Projection, TableScan, Window,
};

use datafusion_expr::utils::inspect_expr_pre;
use hashbrown::HashMap;
use itertools::{izip, Itertools};

/// A rule for optimizing logical plans by removing unused columns/fields.
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
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // All output fields are necessary:
        let indices = (0..plan.schema().fields().len()).collect::<Vec<_>>();
        optimize_projections(plan, config, &indices)
    }

    fn name(&self) -> &str {
        "optimize_projections"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
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
///   indices for downstream operations.
///
/// # Returns
///
/// A `Result` object with the following semantics:
///
/// - `Ok(Some(LogicalPlan))`: An optimized `LogicalPlan` without unnecessary
///   columns.
/// - `Ok(None)`: Signal that the given logical plan did not require any change.
/// - `Err(error)`: An error occured during the optimization process.
fn optimize_projections(
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
    indices: &[usize],
) -> Result<Option<LogicalPlan>> {
    // `child_required_indices` stores
    // - indices of the columns required for each child
    // - a flag indicating whether putting a projection above children is beneficial for the parent.
    // As an example LogicalPlan::Filter benefits from small tables. Hence for filter child this flag would be `true`.
    let child_required_indices: Vec<(Vec<usize>, bool)> = match plan {
        LogicalPlan::Sort(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Distinct(Distinct::On(_)) => {
            // Pass index requirements from the parent as well as column indices
            // that appear in this plan's expressions to its child. All these
            // operators benefit from "small" inputs, so the projection_beneficial
            // flag is `true`.
            let exprs = plan.expressions();
            plan.inputs()
                .into_iter()
                .map(|input| {
                    get_all_required_indices(indices, input, exprs.iter())
                        .map(|idxs| (idxs, true))
                })
                .collect::<Result<_>>()?
        }
        LogicalPlan::Limit(_) | LogicalPlan::Prepare(_) => {
            // Pass index requirements from the parent as well as column indices
            // that appear in this plan's expressions to its child. These operators
            // do not benefit from "small" inputs, so the projection_beneficial
            // flag is `false`.
            let exprs = plan.expressions();
            plan.inputs()
                .into_iter()
                .map(|input| {
                    get_all_required_indices(indices, input, exprs.iter())
                        .map(|idxs| (idxs, false))
                })
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
                .iter()
                .map(|input| ((0..input.schema().fields().len()).collect_vec(), false))
                .collect::<Vec<_>>()
        }
        LogicalPlan::EmptyRelation(_)
        | LogicalPlan::RecursiveQuery(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::DescribeTable(_) => {
            // These operators have no inputs, so stop the optimization process.
            // TODO: Add support for `LogicalPlan::Extension`.
            return Ok(None);
        }
        LogicalPlan::Projection(proj) => {
            return if let Some(proj) = merge_consecutive_projections(proj)? {
                Ok(Some(
                    rewrite_projection_given_requirements(&proj, config, indices)?
                        // Even if we cannot optimize the projection, merge if possible:
                        .unwrap_or_else(|| LogicalPlan::Projection(proj)),
                ))
            } else {
                rewrite_projection_given_requirements(proj, config, indices)
            };
        }
        LogicalPlan::Aggregate(aggregate) => {
            // Split parent requirements to GROUP BY and aggregate sections:
            let n_group_exprs = aggregate.group_expr_len()?;
            let (group_by_reqs, mut aggregate_reqs): (Vec<usize>, Vec<usize>) =
                indices.iter().partition(|&&idx| idx < n_group_exprs);
            // Offset aggregate indices so that they point to valid indices at
            // `aggregate.aggr_expr`:
            for idx in aggregate_reqs.iter_mut() {
                *idx -= n_group_exprs;
            }

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
                let required_indices =
                    merge_slices(&simplest_groupby_indices, &group_by_reqs);
                get_at_indices(&aggregate.group_expr, &required_indices)
            } else {
                aggregate.group_expr.clone()
            };

            // Only use the absolutely necessary aggregate expressions required
            // by the parent:
            let mut new_aggr_expr = get_at_indices(&aggregate.aggr_expr, &aggregate_reqs);

            // Aggregations always need at least one aggregate expression.
            // With a nested count, we don't require any column as input, but
            // still need to create a correct aggregate, which may be optimized
            // out later. As an example, consider the following query:
            //
            // SELECT COUNT(*) FROM (SELECT COUNT(*) FROM [...])
            //
            // which always returns 1.
            if new_aggr_expr.is_empty()
                && new_group_bys.is_empty()
                && !aggregate.aggr_expr.is_empty()
            {
                new_aggr_expr = vec![aggregate.aggr_expr[0].clone()];
            }

            let all_exprs_iter = new_group_bys.iter().chain(new_aggr_expr.iter());
            let schema = aggregate.input.schema();
            let necessary_indices = indices_referred_by_exprs(schema, all_exprs_iter)?;

            let aggregate_input = if let Some(input) =
                optimize_projections(&aggregate.input, config, &necessary_indices)?
            {
                input
            } else {
                aggregate.input.as_ref().clone()
            };

            // Simplify the input of the aggregation by adding a projection so
            // that its input only contains absolutely necessary columns for
            // the aggregate expressions. Note that necessary_indices refer to
            // fields in `aggregate.input.schema()`.
            let necessary_exprs = get_required_exprs(schema, &necessary_indices);
            let (aggregate_input, _) =
                add_projection_on_top_if_helpful(aggregate_input, necessary_exprs)?;

            // Create a new aggregate plan with the updated input and only the
            // absolutely necessary fields:
            return Aggregate::try_new(
                Box::new(aggregate_input),
                new_group_bys,
                new_aggr_expr,
            )
            .map(|aggregate| Some(LogicalPlan::Aggregate(aggregate)));
        }
        LogicalPlan::Window(window) => {
            // Split parent requirements to child and window expression sections:
            let n_input_fields = window.input.schema().fields().len();
            let (child_reqs, mut window_reqs): (Vec<usize>, Vec<usize>) =
                indices.iter().partition(|&&idx| idx < n_input_fields);
            // Offset window expression indices so that they point to valid
            // indices at `window.window_expr`:
            for idx in window_reqs.iter_mut() {
                *idx -= n_input_fields;
            }

            // Only use window expressions that are absolutely necessary according
            // to parent requirements:
            let new_window_expr = get_at_indices(&window.window_expr, &window_reqs);

            // Get all the required column indices at the input, either by the
            // parent or window expression requirements.
            let required_indices = get_all_required_indices(
                &child_reqs,
                &window.input,
                new_window_expr.iter(),
            )?;
            let window_child = if let Some(new_window_child) =
                optimize_projections(&window.input, config, &required_indices)?
            {
                new_window_child
            } else {
                window.input.as_ref().clone()
            };

            return if new_window_expr.is_empty() {
                // When no window expression is necessary, use the input directly:
                Ok(Some(window_child))
            } else {
                // Calculate required expressions at the input of the window.
                // Please note that we use `old_child`, because `required_indices`
                // refers to `old_child`.
                let required_exprs =
                    get_required_exprs(window.input.schema(), &required_indices);
                let (window_child, _) =
                    add_projection_on_top_if_helpful(window_child, required_exprs)?;
                Window::try_new(new_window_expr, Box::new(window_child))
                    .map(|window| Some(LogicalPlan::Window(window)))
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
            // Joins benefit from "small" input tables (lower memory usage).
            // Therefore, each child benefits from projection:
            vec![(left_indices, true), (right_indices, true)]
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_len = cross_join.left.schema().fields().len();
            let (left_child_indices, right_child_indices) =
                split_join_requirements(left_len, indices, &JoinType::Inner);
            // Joins benefit from "small" input tables (lower memory usage).
            // Therefore, each child benefits from projection:
            vec![(left_child_indices, true), (right_child_indices, true)]
        }
        LogicalPlan::TableScan(table_scan) => {
            let schema = table_scan.source.schema();
            // Get indices referred to in the original (schema with all fields)
            // given projected indices.
            let projection = with_indices(&table_scan.projection, schema, |map| {
                indices.iter().map(|&idx| map[idx]).collect()
            });

            return TableScan::try_new(
                table_scan.table_name.clone(),
                table_scan.source.clone(),
                Some(projection),
                table_scan.filters.clone(),
                table_scan.fetch,
            )
            .map(|table| Some(LogicalPlan::TableScan(table)));
        }
    };

    let new_inputs = izip!(child_required_indices, plan.inputs().into_iter())
        .map(|((required_indices, projection_beneficial), child)| {
            let (input, is_changed) = if let Some(new_input) =
                optimize_projections(child, config, &required_indices)?
            {
                (new_input, true)
            } else {
                (child.clone(), false)
            };
            let project_exprs = get_required_exprs(child.schema(), &required_indices);
            let (input, proj_added) = if projection_beneficial {
                add_projection_on_top_if_helpful(input, project_exprs)?
            } else {
                (input, false)
            };
            Ok((is_changed || proj_added).then_some(input))
        })
        .collect::<Result<Vec<_>>>()?;
    if new_inputs.iter().all(|child| child.is_none()) {
        // All children are the same in this case, no need to change the plan:
        Ok(None)
    } else {
        // At least one of the children is changed:
        let new_inputs = izip!(new_inputs, plan.inputs())
            // If new_input is `None`, this means child is not changed, so use
            // `old_child` during construction:
            .map(|(new_input, old_child)| new_input.unwrap_or_else(|| old_child.clone()))
            .collect();
        let exprs = plan.expressions();
        plan.with_new_exprs(exprs, new_inputs).map(Some)
    }
}

/// This function applies the given function `f` to the projection indices
/// `proj_indices` if they exist. Otherwise, applies `f` to a default set
/// of indices according to `schema`.
fn with_indices<F>(
    proj_indices: &Option<Vec<usize>>,
    schema: SchemaRef,
    mut f: F,
) -> Vec<usize>
where
    F: FnMut(&[usize]) -> Vec<usize>,
{
    match proj_indices {
        Some(indices) => f(indices.as_slice()),
        None => {
            let range: Vec<usize> = (0..schema.fields.len()).collect();
            f(range.as_slice())
        }
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
fn merge_consecutive_projections(proj: &Projection) -> Result<Option<Projection>> {
    let LogicalPlan::Projection(prev_projection) = proj.input.as_ref() else {
        return Ok(None);
    };

    // Count usages (referrals) of each projection expression in its input fields:
    let mut column_referral_map = HashMap::<Column, usize>::new();
    for columns in proj.expr.iter().flat_map(|expr| expr.to_columns()) {
        for col in columns.into_iter() {
            *column_referral_map.entry(col.clone()).or_default() += 1;
        }
    }

    // If an expression is non-trivial and appears more than once, consecutive
    // projections will benefit from a compute-once approach. For details, see:
    // https://github.com/apache/arrow-datafusion/issues/8296
    if column_referral_map.into_iter().any(|(col, usage)| {
        usage > 1
            && !is_expr_trivial(
                &prev_projection.expr
                    [prev_projection.schema.index_of_column(&col).unwrap()],
            )
    }) {
        return Ok(None);
    }

    // If all the expression of the top projection can be rewritten, do so and
    // create a new projection:
    let new_exprs = proj
        .expr
        .iter()
        .map(|expr| rewrite_expr(expr, prev_projection))
        .collect::<Result<Option<Vec<_>>>>()?;
    if let Some(new_exprs) = new_exprs {
        let new_exprs = new_exprs
            .into_iter()
            .zip(proj.expr.iter())
            .map(|(new_expr, old_expr)| {
                new_expr.alias_if_changed(old_expr.name_for_alias()?)
            })
            .collect::<Result<Vec<_>>>()?;
        Projection::try_new(new_exprs, prev_projection.input.clone()).map(Some)
    } else {
        Ok(None)
    }
}

/// Trim the given expression by removing any unnecessary layers of aliasing.
/// If the expression is an alias, the function returns the underlying expression.
/// Otherwise, it returns the given expression as is.
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
fn trim_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => trim_expr(*alias.expr),
        _ => expr,
    }
}

// Check whether `expr` is trivial; i.e. it doesn't imply any computation.
fn is_expr_trivial(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_))
}

// Exit early when there is no rewrite to do.
macro_rules! rewrite_expr_with_check {
    ($expr:expr, $input:expr) => {
        if let Some(value) = rewrite_expr($expr, $input)? {
            value
        } else {
            return Ok(None);
        }
    };
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
fn rewrite_expr(expr: &Expr, input: &Projection) -> Result<Option<Expr>> {
    let result = match expr {
        Expr::Column(col) => {
            // Find index of column:
            let idx = input.schema.index_of_column(col)?;
            input.expr[idx].clone()
        }
        Expr::BinaryExpr(binary) => Expr::BinaryExpr(BinaryExpr::new(
            Box::new(trim_expr(rewrite_expr_with_check!(&binary.left, input))),
            binary.op,
            Box::new(trim_expr(rewrite_expr_with_check!(&binary.right, input))),
        )),
        Expr::Alias(alias) => Expr::Alias(Alias::new(
            trim_expr(rewrite_expr_with_check!(&alias.expr, input)),
            alias.relation.clone(),
            alias.name.clone(),
        )),
        Expr::Literal(_) => expr.clone(),
        Expr::Cast(cast) => {
            let new_expr = rewrite_expr_with_check!(&cast.expr, input);
            Expr::Cast(Cast::new(Box::new(new_expr), cast.data_type.clone()))
        }
        Expr::ScalarFunction(scalar_fn) => {
            return Ok(scalar_fn
                .args
                .iter()
                .map(|expr| rewrite_expr(expr, input))
                .collect::<Result<Option<_>>>()?
                .map(|new_args| {
                    Expr::ScalarFunction(ScalarFunction::new_func_def(
                        scalar_fn.func_def.clone(),
                        new_args,
                    ))
                }));
        }
        // Unsupported type for consecutive projection merge analysis.
        _ => return Ok(None),
    };
    Ok(Some(result))
}

/// Accumulates outer-referenced columns by the
/// given expression, `expr`.
///
/// # Parameters
///
/// * `expr` - The expression to analyze for outer-referenced columns.
/// * `columns` - A mutable reference to a `HashSet<Column>` where detected
///   columns are collected.
fn outer_columns(expr: &Expr, columns: &mut HashSet<Column>) {
    // inspect_expr_pre doesn't handle subquery references, so find them explicitly
    inspect_expr_pre(expr, |expr| {
        match expr {
            Expr::OuterReferenceColumn(_, col) => {
                columns.insert(col.clone());
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
        Ok(()) as Result<()>
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
fn outer_columns_helper_multi<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    columns: &mut HashSet<Column>,
) {
    exprs.into_iter().for_each(|e| outer_columns(e, columns));
}

/// Generates the required expressions (columns) that reside at `indices` of
/// the given `input_schema`.
///
/// # Arguments
///
/// * `input_schema` - A reference to the input schema.
/// * `indices` - A slice of `usize` indices specifying required columns.
///
/// # Returns
///
/// A vector of `Expr::Column` expressions residing at `indices` of the `input_schema`.
fn get_required_exprs(input_schema: &Arc<DFSchema>, indices: &[usize]) -> Vec<Expr> {
    let fields = input_schema.fields();
    indices
        .iter()
        .map(|&idx| Expr::Column(fields[idx].qualified_column()))
        .collect()
}

/// Get indices of the fields referred to by any expression in `exprs` within
/// the given schema (`input_schema`).
///
/// # Arguments
///
/// * `input_schema`: The input schema to analyze for index requirements.
/// * `exprs`: An iterator of expressions for which we want to find necessary
///   field indices.
///
/// # Returns
///
/// A [`Result`] object containing the indices of all required fields in
/// `input_schema` to calculate all `exprs` successfully.
fn indices_referred_by_exprs<'a>(
    input_schema: &DFSchemaRef,
    exprs: impl Iterator<Item = &'a Expr>,
) -> Result<Vec<usize>> {
    let indices = exprs
        .map(|expr| indices_referred_by_expr(input_schema, expr))
        .collect::<Result<Vec<_>>>()?;
    Ok(indices
        .into_iter()
        .flatten()
        // Make sure no duplicate entries exist and indices are ordered:
        .sorted()
        .dedup()
        .collect())
}

/// Get indices of the fields referred to by the given expression `expr` within
/// the given schema (`input_schema`).
///
/// # Parameters
///
/// * `input_schema`: The input schema to analyze for index requirements.
/// * `expr`: An expression for which we want to find necessary field indices.
///
/// # Returns
///
/// A [`Result`] object containing the indices of all required fields in
/// `input_schema` to calculate `expr` successfully.
fn indices_referred_by_expr(
    input_schema: &DFSchemaRef,
    expr: &Expr,
) -> Result<Vec<usize>> {
    let mut cols = expr.to_columns()?;
    // Get outer-referenced (subquery) columns:
    outer_columns(expr, &mut cols);
    Ok(cols
        .iter()
        .flat_map(|col| input_schema.index_of_column(col))
        .collect())
}

/// Gets all required indices for the input; i.e. those required by the parent
/// and those referred to by `exprs`.
///
/// # Parameters
///
/// * `parent_required_indices` - A slice of indices required by the parent plan.
/// * `input` - The input logical plan to analyze for index requirements.
/// * `exprs` - An iterator of expressions used to determine required indices.
///
/// # Returns
///
/// A `Result` containing a vector of `usize` indices containing all the required
/// indices.
fn get_all_required_indices<'a>(
    parent_required_indices: &[usize],
    input: &LogicalPlan,
    exprs: impl Iterator<Item = &'a Expr>,
) -> Result<Vec<usize>> {
    indices_referred_by_exprs(input.schema(), exprs)
        .map(|indices| merge_slices(parent_required_indices, &indices))
}

/// Retrieves the expressions at specified indices within the given slice. Ignores
/// any invalid indices.
///
/// # Parameters
///
/// * `exprs` - A slice of expressions to index into.
/// * `indices` - A slice of indices specifying the positions of expressions sought.
///
/// # Returns
///
/// A vector of expressions corresponding to specified indices.
fn get_at_indices(exprs: &[Expr], indices: &[usize]) -> Vec<Expr> {
    indices
        .iter()
        // Indices may point to further places than `exprs` len.
        .filter_map(|&idx| exprs.get(idx).cloned())
        .collect()
}

/// Merges two slices into a single vector with sorted (ascending) and
/// deduplicated elements. For example, merging `[3, 2, 4]` and `[3, 6, 1]`
/// will produce `[1, 2, 3, 6]`.
fn merge_slices<T: Clone + Ord>(left: &[T], right: &[T]) -> Vec<T> {
    // Make sure to sort before deduping, which removes the duplicates:
    left.iter()
        .cloned()
        .chain(right.iter().cloned())
        .sorted()
        .dedup()
        .collect()
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
    indices: &[usize],
    join_type: &JoinType,
) -> (Vec<usize>, Vec<usize>) {
    match join_type {
        // In these cases requirements are split between left/right children:
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let (left_reqs, mut right_reqs): (Vec<usize>, Vec<usize>) =
                indices.iter().partition(|&&idx| idx < left_len);
            // Decrease right side indices by `left_len` so that they point to valid
            // positions within the right child:
            for idx in right_reqs.iter_mut() {
                *idx -= left_len;
            }
            (left_reqs, right_reqs)
        }
        // All requirements can be re-routed to left child directly.
        JoinType::LeftAnti | JoinType::LeftSemi => (indices.to_vec(), vec![]),
        // All requirements can be re-routed to right side directly.
        // No need to change index, join schema is right child schema.
        JoinType::RightSemi | JoinType::RightAnti => (vec![], indices.to_vec()),
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
/// A `Result` containing a tuple with two values: The resulting `LogicalPlan`
/// (with or without the added projection) and a `bool` flag indicating if a
/// projection was added (`true`) or not (`false`).
fn add_projection_on_top_if_helpful(
    plan: LogicalPlan,
    project_exprs: Vec<Expr>,
) -> Result<(LogicalPlan, bool)> {
    // Make sure projection decreases the number of columns, otherwise it is unnecessary.
    if project_exprs.len() >= plan.schema().fields().len() {
        Ok((plan, false))
    } else {
        Projection::try_new(project_exprs, Box::new(plan))
            .map(|proj| (LogicalPlan::Projection(proj), true))
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
    proj: &Projection,
    config: &dyn OptimizerConfig,
    indices: &[usize],
) -> Result<Option<LogicalPlan>> {
    let exprs_used = get_at_indices(&proj.expr, indices);
    let required_indices =
        indices_referred_by_exprs(proj.input.schema(), exprs_used.iter())?;
    return if let Some(input) =
        optimize_projections(&proj.input, config, &required_indices)?
    {
        if is_projection_unnecessary(&input, &exprs_used)? {
            Ok(Some(input))
        } else {
            Projection::try_new(exprs_used, Box::new(input))
                .map(|proj| Some(LogicalPlan::Projection(proj)))
        }
    } else if exprs_used.len() < proj.expr.len() {
        // Projection expression used is different than the existing projection.
        // In this case, even if the child doesn't change, we should update the
        // projection to use fewer columns:
        if is_projection_unnecessary(&proj.input, &exprs_used)? {
            Ok(Some(proj.input.as_ref().clone()))
        } else {
            Projection::try_new(exprs_used, proj.input.clone())
                .map(|proj| Some(LogicalPlan::Projection(proj)))
        }
    } else {
        // Projection doesn't change.
        Ok(None)
    };
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
    use std::sync::Arc;

    use crate::optimize_projections::OptimizeProjections;
    use crate::test::{assert_optimized_plan_eq, test_table_scan};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, TableReference};
    use datafusion_expr::{
        binary_expr, col, count, lit, logical_plan::builder::LogicalPlanBuilder, not,
        table_scan, try_cast, when, Expr, Like, LogicalPlan, Operator,
    };

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(OptimizeProjections::new()), plan, expected)
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

        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(Int32(1))]]\
        \n  Projection: \
        \n    Aggregate: groupBy=[[]], aggr=[[COUNT(Int32(1))]]\
        \n      TableScan: ?table? projection=[]";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn test_struct_field_push_down() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new_struct(
                "s",
                vec![
                    Field::new("x", DataType::Int64, false),
                    Field::new("y", DataType::Int64, false),
                ],
                false,
            ),
        ]));

        let table_scan = table_scan(TableReference::none(), &schema, None)?.build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("s").field("x")])?
            .build()?;
        let expected = "Projection: (?table?.s)[x]\
        \n  TableScan: ?table? projection=[s]";
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

    // Test outer projection isn't discarded despite the same schema as inner
    // https://github.com/apache/arrow-datafusion/issues/8942
    #[test]
    fn test_derived_column() -> Result<()> {
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

        let expected = "Projection: test.a, CASE WHEN test.a = Int32(1) THEN Int32(10) ELSE d END AS d\
        \n  Projection: test.a, Int32(0) AS d\
        \n    TableScan: test projection=[a]";
        assert_optimized_plan_equal(plan, expected)
    }
}
