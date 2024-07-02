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

//! EnforceSorting optimizer rule inspects the physical plan with respect
//! to local sorting requirements and does the following:
//! - Adds a [`SortExec`] when a requirement is not met,
//! - Removes an already-existing [`SortExec`] if it is possible to prove
//!   that this sort is unnecessary
//! The rule can work on valid *and* invalid physical plans with respect to
//! sorting requirements, but always produces a valid physical plan in this sense.
//!
//! A non-realistic but easy to follow example for sort removals: Assume that we
//! somehow get the fragment
//!
//! ```text
//! SortExec: expr=[nullable_col@0 ASC]
//!   SortExec: expr=[non_nullable_col@1 ASC]
//! ```
//!
//! in the physical plan. The first sort is unnecessary since its result is overwritten
//! by another [`SortExec`]. Therefore, this rule removes it from the physical plan.

use std::sync::Arc;

use super::utils::{add_sort_above, add_sort_above_with_check};
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::replace_with_order_preserving_variants::{
    replace_with_order_preserving_variants, OrderPreservationContext,
};
use crate::physical_optimizer::sort_pushdown::{
    assign_initial_requirements, pushdown_sorts, SortPushDown,
};
use crate::physical_optimizer::utils::{
    is_coalesce_partitions, is_limit, is_repartition, is_sort, is_sort_preserving_merge,
    is_union, is_window,
};
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::tree_node::PlanContext;
use crate::physical_plan::windows::{
    get_best_fitting_window, BoundedWindowAggExec, WindowAggExec,
};
use crate::physical_plan::{Distribution, ExecutionPlan, InputOrderMode};

use datafusion_common::plan_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion_physical_plan::ExecutionPlanProperties;

use itertools::izip;

/// This rule inspects [`SortExec`]'s in the given physical plan and removes the
/// ones it can prove unnecessary.
#[derive(Default)]
pub struct EnforceSorting {}

impl EnforceSorting {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// This object is used within the [`EnforceSorting`] rule to track the closest
/// [`SortExec`] descendant(s) for every child of a plan. The data attribute
/// stores whether the plan is a `SortExec` or is connected to a `SortExec`
/// via its children.
type PlanWithCorrespondingSort = PlanContext<bool>;

fn update_sort_ctx_children(
    mut node: PlanWithCorrespondingSort,
    data: bool,
) -> Result<PlanWithCorrespondingSort> {
    for child_node in node.children.iter_mut() {
        let plan = &child_node.plan;
        child_node.data = if is_sort(plan) {
            // Initiate connection:
            true
        } else if is_limit(plan) {
            // There is no sort linkage for this path, it starts at a limit.
            false
        } else {
            let is_spm = is_sort_preserving_merge(plan);
            let required_orderings = plan.required_input_ordering();
            let flags = plan.maintains_input_order();
            // Add parent node to the tree if there is at least one child with
            // a sort connection:
            izip!(flags, required_orderings).any(|(maintains, required_ordering)| {
                let propagates_ordering =
                    (maintains && required_ordering.is_none()) || is_spm;
                let connected_to_sort =
                    child_node.children.iter().any(|child| child.data);
                propagates_ordering && connected_to_sort
            })
        }
    }

    node.data = data;
    node.update_plan_from_children()
}

/// This object is used within the [`EnforceSorting`] rule to track the closest
/// [`CoalescePartitionsExec`] descendant(s) for every child of a plan. The data
/// attribute stores whether the plan is a `CoalescePartitionsExec` or is
/// connected to a `CoalescePartitionsExec` via its children.
type PlanWithCorrespondingCoalescePartitions = PlanContext<bool>;

fn update_coalesce_ctx_children(
    coalesce_context: &mut PlanWithCorrespondingCoalescePartitions,
) {
    let children = &coalesce_context.children;
    coalesce_context.data = if children.is_empty() {
        // Plan has no children, it cannot be a `CoalescePartitionsExec`.
        false
    } else if is_coalesce_partitions(&coalesce_context.plan) {
        // Initiate a connection:
        true
    } else {
        children.iter().enumerate().any(|(idx, node)| {
            // Only consider operators that don't require a single partition,
            // and connected to some `CoalescePartitionsExec`:
            node.data
                && !matches!(
                    coalesce_context.plan.required_input_distribution()[idx],
                    Distribution::SinglePartition
                )
        })
    };
}

/// The boolean flag `repartition_sorts` defined in the config indicates
/// whether we elect to transform [`CoalescePartitionsExec`] + [`SortExec`] cascades
/// into [`SortExec`] + [`SortPreservingMergeExec`] cascades, which enables us to
/// perform sorting in parallel.
impl PhysicalOptimizerRule for EnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_requirements = PlanWithCorrespondingSort::new_default(plan);
        // Execute a bottom-up traversal to enforce sorting requirements,
        // remove unnecessary sorts, and optimize sort-sensitive operators:
        let adjusted = plan_requirements.transform_up(ensure_sorting)?.data;
        let new_plan = if config.optimizer.repartition_sorts {
            let plan_with_coalesce_partitions =
                PlanWithCorrespondingCoalescePartitions::new_default(adjusted.plan);
            let parallel = plan_with_coalesce_partitions
                .transform_up(parallelize_sorts)
                .data()?;
            parallel.plan
        } else {
            adjusted.plan
        };

        let plan_with_pipeline_fixer = OrderPreservationContext::new_default(new_plan);
        let updated_plan = plan_with_pipeline_fixer
            .transform_up(|plan_with_pipeline_fixer| {
                replace_with_order_preserving_variants(
                    plan_with_pipeline_fixer,
                    false,
                    true,
                    config,
                )
            })
            .data()?;

        // Execute a top-down traversal to exploit sort push-down opportunities
        // missed by the bottom-up traversal:
        let mut sort_pushdown = SortPushDown::new_default(updated_plan.plan);
        assign_initial_requirements(&mut sort_pushdown);
        let adjusted = sort_pushdown.transform_down(pushdown_sorts)?.data;

        adjusted
            .plan
            .transform_up(|plan| Ok(Transformed::yes(replace_with_partial_sort(plan)?)))
            .data()
    }

    fn name(&self) -> &str {
        "EnforceSorting"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn replace_with_partial_sort(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_any = plan.as_any();
    if let Some(sort_plan) = plan_any.downcast_ref::<SortExec>() {
        let child = Arc::clone(sort_plan.children()[0]);
        if !child.execution_mode().is_unbounded() {
            return Ok(plan);
        }

        // here we're trying to find the common prefix for sorted columns that is required for the
        // sort and already satisfied by the given ordering
        let child_eq_properties = child.equivalence_properties();
        let sort_req = PhysicalSortRequirement::from_sort_exprs(sort_plan.expr());

        let mut common_prefix_length = 0;
        while child_eq_properties
            .ordering_satisfy_requirement(&sort_req[0..common_prefix_length + 1])
        {
            common_prefix_length += 1;
        }
        if common_prefix_length > 0 {
            return Ok(Arc::new(
                PartialSortExec::new(
                    sort_plan.expr().to_vec(),
                    Arc::clone(sort_plan.input()),
                    common_prefix_length,
                )
                .with_preserve_partitioning(sort_plan.preserve_partitioning())
                .with_fetch(sort_plan.fetch()),
            ));
        }
    }
    Ok(plan)
}

/// This function turns plans of the form
/// ```text
///      "SortExec: expr=\[a@0 ASC\]",
///      "  CoalescePartitionsExec",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
/// to
/// ```text
///      "SortPreservingMergeExec: \[a@0 ASC\]",
///      "  SortExec: expr=\[a@0 ASC\]",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
/// by following connections from [`CoalescePartitionsExec`]s to [`SortExec`]s.
/// By performing sorting in parallel, we can increase performance in some scenarios.
fn parallelize_sorts(
    mut requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<Transformed<PlanWithCorrespondingCoalescePartitions>> {
    update_coalesce_ctx_children(&mut requirements);

    if requirements.children.is_empty() || !requirements.children[0].data {
        // We only take an action when the plan is either a `SortExec`, a
        // `SortPreservingMergeExec` or a `CoalescePartitionsExec`, and they
        // all have a single child. Therefore, if the first child has no
        // connection, we can return immediately.
        Ok(Transformed::no(requirements))
    } else if (is_sort(&requirements.plan)
        || is_sort_preserving_merge(&requirements.plan))
        && requirements.plan.output_partitioning().partition_count() <= 1
    {
        // Take the initial sort expressions and requirements
        let (sort_exprs, fetch) = get_sort_exprs(&requirements.plan)?;
        let sort_reqs = PhysicalSortRequirement::from_sort_exprs(sort_exprs);
        let sort_exprs = sort_exprs.to_vec();

        // If there is a connection between a `CoalescePartitionsExec` and a
        // global sort that satisfy the requirements (i.e. intermediate
        // executors don't require single partition), then we can replace
        // the `CoalescePartitionsExec` + `SortExec` cascade with a `SortExec`
        // + `SortPreservingMergeExec` cascade to parallelize sorting.
        requirements = remove_corresponding_coalesce_in_sub_plan(requirements)?;
        // We also need to remove the self node since `remove_corresponding_coalesce_in_sub_plan`
        // deals with the children and their children and so on.
        requirements = requirements.children.swap_remove(0);

        requirements = add_sort_above_with_check(requirements, sort_reqs, fetch);

        let spm =
            SortPreservingMergeExec::new(sort_exprs, Arc::clone(&requirements.plan));
        Ok(Transformed::yes(
            PlanWithCorrespondingCoalescePartitions::new(
                Arc::new(spm.with_fetch(fetch)),
                false,
                vec![requirements],
            ),
        ))
    } else if is_coalesce_partitions(&requirements.plan) {
        // There is an unnecessary `CoalescePartitionsExec` in the plan.
        // This will handle the recursive `CoalescePartitionsExec` plans.
        requirements = remove_corresponding_coalesce_in_sub_plan(requirements)?;
        // For the removal of self node which is also a `CoalescePartitionsExec`.
        requirements = requirements.children.swap_remove(0);

        Ok(Transformed::yes(
            PlanWithCorrespondingCoalescePartitions::new(
                Arc::new(CoalescePartitionsExec::new(Arc::clone(&requirements.plan))),
                false,
                vec![requirements],
            ),
        ))
    } else {
        Ok(Transformed::yes(requirements))
    }
}

/// This function enforces sorting requirements and makes optimizations without
/// violating these requirements whenever possible.
fn ensure_sorting(
    mut requirements: PlanWithCorrespondingSort,
) -> Result<Transformed<PlanWithCorrespondingSort>> {
    requirements = update_sort_ctx_children(requirements, false)?;

    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    if requirements.children.is_empty() {
        return Ok(Transformed::no(requirements));
    }
    let maybe_requirements = analyze_immediate_sort_removal(requirements);
    requirements = if !maybe_requirements.transformed {
        maybe_requirements.data
    } else {
        return Ok(maybe_requirements);
    };

    let plan = &requirements.plan;
    let mut updated_children = vec![];
    for (idx, (required_ordering, mut child)) in plan
        .required_input_ordering()
        .into_iter()
        .zip(requirements.children.into_iter())
        .enumerate()
    {
        let physical_ordering = child.plan.output_ordering();

        if let Some(required) = required_ordering {
            let eq_properties = child.plan.equivalence_properties();
            if !eq_properties.ordering_satisfy_requirement(&required) {
                // Make sure we preserve the ordering requirements:
                if physical_ordering.is_some() {
                    child = update_child_to_remove_unnecessary_sort(idx, child, plan)?;
                }
                child = add_sort_above(child, required, None);
                child = update_sort_ctx_children(child, true)?;
            }
        } else if physical_ordering.is_none()
            || !plan.maintains_input_order()[idx]
            || is_union(plan)
        {
            // We have a `SortExec` whose effect may be neutralized by another
            // order-imposing operator, remove this sort:
            child = update_child_to_remove_unnecessary_sort(idx, child, plan)?;
        }
        updated_children.push(child);
    }
    requirements.children = updated_children;
    // For window expressions, we can remove some sorts when we can
    // calculate the result in reverse:
    let child_node = &requirements.children[0];
    if is_window(plan) && child_node.data {
        return adjust_window_sort_removal(requirements).map(Transformed::yes);
    } else if is_sort_preserving_merge(plan)
        && child_node.plan.output_partitioning().partition_count() <= 1
    {
        // This `SortPreservingMergeExec` is unnecessary, input already has a
        // single partition.
        let child_node = requirements.children.swap_remove(0);
        return Ok(Transformed::yes(child_node));
    }

    update_sort_ctx_children(requirements, false).map(Transformed::yes)
}

/// Analyzes a given [`SortExec`] (`plan`) to determine whether its input
/// already has a finer ordering than it enforces.
fn analyze_immediate_sort_removal(
    mut node: PlanWithCorrespondingSort,
) -> Transformed<PlanWithCorrespondingSort> {
    if let Some(sort_exec) = node.plan.as_any().downcast_ref::<SortExec>() {
        let sort_input = sort_exec.input();
        // If this sort is unnecessary, we should remove it:
        if sort_input
            .equivalence_properties()
            .ordering_satisfy(sort_exec.properties().output_ordering().unwrap_or(&[]))
        {
            node.plan = if !sort_exec.preserve_partitioning()
                && sort_input.output_partitioning().partition_count() > 1
            {
                // Replace the sort with a sort-preserving merge:
                let expr = sort_exec.expr().to_vec();
                Arc::new(SortPreservingMergeExec::new(expr, Arc::clone(sort_input))) as _
            } else {
                // Remove the sort:
                node.children = node.children.swap_remove(0).children;
                Arc::clone(sort_input)
            };
            for child in node.children.iter_mut() {
                child.data = false;
            }
            node.data = false;
            return Transformed::yes(node);
        }
    }
    Transformed::no(node)
}

/// Adjusts a [`WindowAggExec`] or a [`BoundedWindowAggExec`] to determine
/// whether it may allow removing a sort.
fn adjust_window_sort_removal(
    mut window_tree: PlanWithCorrespondingSort,
) -> Result<PlanWithCorrespondingSort> {
    // Window operators have a single child we need to adjust:
    let child_node = remove_corresponding_sort_from_sub_plan(
        window_tree.children.swap_remove(0),
        matches!(
            window_tree.plan.required_input_distribution()[0],
            Distribution::SinglePartition
        ),
    )?;
    window_tree.children.push(child_node);

    let plan = window_tree.plan.as_any();
    let child_plan = &window_tree.children[0].plan;
    let (window_expr, new_window) =
        if let Some(exec) = plan.downcast_ref::<WindowAggExec>() {
            let window_expr = exec.window_expr();
            let new_window =
                get_best_fitting_window(window_expr, child_plan, &exec.partition_keys)?;
            (window_expr, new_window)
        } else if let Some(exec) = plan.downcast_ref::<BoundedWindowAggExec>() {
            let window_expr = exec.window_expr();
            let new_window =
                get_best_fitting_window(window_expr, child_plan, &exec.partition_keys)?;
            (window_expr, new_window)
        } else {
            return plan_err!("Expected WindowAggExec or BoundedWindowAggExec");
        };

    window_tree.plan = if let Some(new_window) = new_window {
        // We were able to change the window to accommodate the input, use it:
        new_window
    } else {
        // We were unable to change the window to accommodate the input, so we
        // will insert a sort.
        let reqs = window_tree
            .plan
            .required_input_ordering()
            .swap_remove(0)
            .unwrap_or_default();

        // Satisfy the ordering requirement so that the window can run:
        let mut child_node = window_tree.children.swap_remove(0);
        child_node = add_sort_above(child_node, reqs, None);
        let child_plan = Arc::clone(&child_node.plan);
        window_tree.children.push(child_node);

        if window_expr.iter().all(|e| e.uses_bounded_memory()) {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr.to_vec(),
                child_plan,
                window_expr[0].partition_by().to_vec(),
                InputOrderMode::Sorted,
            )?) as _
        } else {
            Arc::new(WindowAggExec::try_new(
                window_expr.to_vec(),
                child_plan,
                window_expr[0].partition_by().to_vec(),
            )?) as _
        }
    };

    window_tree.data = false;
    Ok(window_tree)
}

/// Removes the [`CoalescePartitionsExec`] from the plan in `node`.
fn remove_corresponding_coalesce_in_sub_plan(
    mut requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<PlanWithCorrespondingCoalescePartitions> {
    let plan = &requirements.plan;
    let children = &mut requirements.children;
    if is_coalesce_partitions(&children[0].plan) {
        // We can safely use the 0th index since we have a `CoalescePartitionsExec`.
        let mut new_child_node = children[0].children.swap_remove(0);
        while new_child_node.plan.output_partitioning() == plan.output_partitioning()
            && is_repartition(&new_child_node.plan)
            && is_repartition(plan)
        {
            new_child_node = new_child_node.children.swap_remove(0)
        }
        children[0] = new_child_node;
    } else {
        requirements.children = requirements
            .children
            .into_iter()
            .map(|node| {
                if node.data {
                    remove_corresponding_coalesce_in_sub_plan(node)
                } else {
                    Ok(node)
                }
            })
            .collect::<Result<_>>()?;
    }

    requirements.update_plan_from_children()
}

/// Updates child to remove the unnecessary sort below it.
fn update_child_to_remove_unnecessary_sort(
    child_idx: usize,
    mut node: PlanWithCorrespondingSort,
    parent: &Arc<dyn ExecutionPlan>,
) -> Result<PlanWithCorrespondingSort> {
    if node.data {
        let requires_single_partition = matches!(
            parent.required_input_distribution()[child_idx],
            Distribution::SinglePartition
        );
        node = remove_corresponding_sort_from_sub_plan(node, requires_single_partition)?;
    }
    node.data = false;
    Ok(node)
}

/// Removes the sort from the plan in `node`.
fn remove_corresponding_sort_from_sub_plan(
    mut node: PlanWithCorrespondingSort,
    requires_single_partition: bool,
) -> Result<PlanWithCorrespondingSort> {
    // A `SortExec` is always at the bottom of the tree.
    if is_sort(&node.plan) {
        node = node.children.swap_remove(0);
    } else {
        let mut any_connection = false;
        let required_dist = node.plan.required_input_distribution();
        node.children = node
            .children
            .into_iter()
            .enumerate()
            .map(|(idx, child)| {
                if child.data {
                    any_connection = true;
                    remove_corresponding_sort_from_sub_plan(
                        child,
                        matches!(required_dist[idx], Distribution::SinglePartition),
                    )
                } else {
                    Ok(child)
                }
            })
            .collect::<Result<_>>()?;
        if any_connection || node.children.is_empty() {
            node = update_sort_ctx_children(node, false)?;
        }

        // Replace with variants that do not preserve order.
        if is_sort_preserving_merge(&node.plan) {
            node.children = node.children.swap_remove(0).children;
            node.plan = Arc::clone(node.plan.children().swap_remove(0));
        } else if let Some(repartition) =
            node.plan.as_any().downcast_ref::<RepartitionExec>()
        {
            node.plan = Arc::new(RepartitionExec::try_new(
                Arc::clone(&node.children[0].plan),
                repartition.properties().output_partitioning().clone(),
            )?) as _;
        }
    };
    // Deleting a merging sort may invalidate distribution requirements.
    // Ensure that we stay compliant with such requirements:
    if requires_single_partition && node.plan.output_partitioning().partition_count() > 1
    {
        // If there is existing ordering, to preserve ordering use
        // `SortPreservingMergeExec` instead of a `CoalescePartitionsExec`.
        let plan = Arc::clone(&node.plan);
        let plan = if let Some(ordering) = plan.output_ordering() {
            Arc::new(SortPreservingMergeExec::new(ordering.to_vec(), plan)) as _
        } else {
            Arc::new(CoalescePartitionsExec::new(plan)) as _
        };
        node = PlanWithCorrespondingSort::new(plan, false, vec![node]);
        node = update_sort_ctx_children(node, false)?;
    }
    Ok(node)
}

/// Converts an [ExecutionPlan] trait object to a [PhysicalSortExpr] slice when possible.
fn get_sort_exprs(
    sort_any: &Arc<dyn ExecutionPlan>,
) -> Result<(&[PhysicalSortExpr], Option<usize>)> {
    if let Some(sort_exec) = sort_any.as_any().downcast_ref::<SortExec>() {
        Ok((sort_exec.expr(), sort_exec.fetch()))
    } else if let Some(spm) = sort_any.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        Ok((spm.expr(), spm.fetch()))
    } else {
        plan_err!("Given ExecutionPlan is not a SortExec or a SortPreservingMergeExec")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_optimizer::enforce_distribution::EnforceDistribution;
    use crate::physical_optimizer::test_utils::{
        aggregate_exec, bounded_window_exec, check_integrity, coalesce_batches_exec,
        coalesce_partitions_exec, filter_exec, global_limit_exec, hash_join_exec,
        limit_exec, local_limit_exec, memory_exec, parquet_exec, parquet_exec_sorted,
        repartition_exec, sort_exec, sort_expr, sort_expr_options, sort_merge_join_exec,
        sort_preserving_merge_exec, spr_repartition_exec, union_exec,
    };
    use crate::physical_plan::{displayable, get_plan_string, Partitioning};
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::{csv_exec_ordered, csv_exec_sorted, stream_exec_ordered};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::expressions::{col, Column, NotExpr};

    use rstest::rstest;

    fn create_test_schema() -> Result<SchemaRef> {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));
        Ok(schema)
    }

    fn create_test_schema2() -> Result<SchemaRef> {
        let col_a = Field::new("col_a", DataType::Int32, true);
        let col_b = Field::new("col_b", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![col_a, col_b]));
        Ok(schema)
    }

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema3() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    /// Runs the sort enforcement optimizer and asserts the plan
    /// against the original and expected plans
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
    /// `$PLAN`: the plan to optimized
    /// `REPARTITION_SORTS`: Flag to set `config.options.optimizer.repartition_sorts` option.
    ///
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $REPARTITION_SORTS: expr) => {
            let config = SessionConfig::new().with_repartition_sorts($REPARTITION_SORTS);
            let session_ctx = SessionContext::new_with_config(config);
            let state = session_ctx.state();

            // This file has 4 rules that use tree node, apply these rules as in the
            // EnforSorting::optimize implementation
            // After these operations tree nodes should be in a consistent state.
            // This code block makes sure that these rules doesn't violate tree node integrity.
            {
                let plan_requirements = PlanWithCorrespondingSort::new_default($PLAN.clone());
                let adjusted = plan_requirements
                    .transform_up(ensure_sorting)
                    .data()
                    .and_then(check_integrity)?;
                // TODO: End state payloads will be checked here.

                let new_plan = if state.config_options().optimizer.repartition_sorts {
                    let plan_with_coalesce_partitions =
                        PlanWithCorrespondingCoalescePartitions::new_default(adjusted.plan);
                    let parallel = plan_with_coalesce_partitions
                        .transform_up(parallelize_sorts)
                        .data()
                        .and_then(check_integrity)?;
                    // TODO: End state payloads will be checked here.
                    parallel.plan
                } else {
                    adjusted.plan
                };

                let plan_with_pipeline_fixer = OrderPreservationContext::new_default(new_plan);
                let updated_plan = plan_with_pipeline_fixer
                    .transform_up(|plan_with_pipeline_fixer| {
                        replace_with_order_preserving_variants(
                            plan_with_pipeline_fixer,
                            false,
                            true,
                            state.config_options(),
                        )
                    })
                    .data()
                    .and_then(check_integrity)?;
                // TODO: End state payloads will be checked here.

                let mut sort_pushdown = SortPushDown::new_default(updated_plan.plan);
                assign_initial_requirements(&mut sort_pushdown);
                sort_pushdown
                    .transform_down(pushdown_sorts)
                    .data()
                    .and_then(check_integrity)?;
                // TODO: End state payloads will be checked here.
            }

            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

            let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES
                .iter().map(|s| *s).collect();

            // Run the actual optimizer
            let optimized_physical_plan =
                EnforceSorting::new().optimize(physical_plan, state.config_options())?;

            // Get string representation of the plan
            let actual = get_plan_string(&optimized_physical_plan);
            assert_eq!(
                expected_optimized_lines, actual,
                "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

        };
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
        let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input);

        let expected_input = [
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort_window_multilayer() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs = vec![sort_expr_options(
            "non_nullable_col",
            &source.schema(),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )];
        let sort = sort_exec(sort_exprs.clone(), source);
        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let coalesce_batches = coalesce_batches_exec(sort);

        let window_agg =
            bounded_window_exec("non_nullable_col", sort_exprs, coalesce_batches);

        let sort_exprs = vec![sort_expr_options(
            "non_nullable_col",
            &window_agg.schema(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )];

        let sort = sort_exec(sort_exprs.clone(), window_agg);

        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let filter = filter_exec(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            sort,
        );

        let physical_plan = bounded_window_exec("non_nullable_col", sort_exprs, filter);

        let expected_input = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "        CoalesceBatchesExec: target_batch_size=128",
            "          SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
            "            MemoryExec: partitions=1, partition_sizes=[0]"];

        let expected_optimized = ["WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      CoalesceBatchesExec: target_batch_size=128",
            "        SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
            "          MemoryExec: partitions=1, partition_sizes=[0]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_required_sort() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];

        let physical_plan = sort_preserving_merge_exec(sort_exprs, source);

        let expected_input = [
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort1() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), spm);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
        let expected_input = [
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "        MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort2() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort2 = sort_exec(sort_exprs.clone(), spm);
        let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort3 = sort_exec(sort_exprs, spm2);
        let physical_plan = repartition_exec(repartition_exec(sort3));

        let expected_input = [
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "          SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "            SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "              MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = [
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort3() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let repartition_exec = repartition_exec(spm);
        let sort2 = Arc::new(
            SortExec::new(sort_exprs.clone(), repartition_exec)
                .with_preserve_partitioning(true),
        ) as _;
        let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

        let physical_plan = aggregate_exec(spm2);

        // When removing a `SortPreservingMergeExec`, make sure that partitioning
        // requirements are not violated. In some cases, we may need to replace
        // it with a `CoalescePartitionsExec` instead of directly removing it.
        let expected_input = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "          SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort4() -> Result<()> {
        let schema = create_test_schema()?;
        let source1 = repartition_exec(memory_exec(&schema));

        let source2 = repartition_exec(memory_exec(&schema));
        let union = union_exec(vec![source1, source2]);

        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        // let sort = sort_exec(sort_exprs.clone(), union);
        let sort = Arc::new(
            SortExec::new(sort_exprs.clone(), union).with_preserve_partitioning(true),
        ) as _;
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let filter = filter_exec(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            spm,
        );

        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let physical_plan = sort_exec(sort_exprs, filter);

        // When removing a `SortPreservingMergeExec`, make sure that partitioning
        // requirements are not violated. In some cases, we may need to replace
        // it with a `CoalescePartitionsExec` instead of directly removing it.
        let expected_input = ["SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "        UnionExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]"];

        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "    FilterExec: NOT non_nullable_col@1",
            "      UnionExec",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[0]",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[0]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort5() -> Result<()> {
        let left_schema = create_test_schema2()?;
        let right_schema = create_test_schema3()?;
        let left_input = memory_exec(&left_schema);
        let parquet_sort_exprs = vec![sort_expr("a", &right_schema)];
        let right_input = parquet_exec_sorted(&right_schema, parquet_sort_exprs);

        let on = vec![(
            Arc::new(Column::new_with_schema("col_a", &left_schema)?) as _,
            Arc::new(Column::new_with_schema("c", &right_schema)?) as _,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(vec![sort_expr("a", &join.schema())], join);

        let expected_input = ["SortExec: expr=[a@2 ASC], preserve_partitioning=[false]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]"];

        let expected_optimized = ["HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_spm1() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let input = sort_preserving_merge_exec(
            vec![sort_expr("non_nullable_col", &schema)],
            source,
        );
        let input2 = sort_preserving_merge_exec(
            vec![sort_expr("non_nullable_col", &schema)],
            input,
        );
        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("nullable_col", &schema)], input2);

        let expected_input = [
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_do_not_remove_sort_with_limit() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort = sort_exec(sort_exprs.clone(), source1);
        let limit = limit_exec(sort);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, limit]);
        let repartition = repartition_exec(union);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, repartition);

        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "    UnionExec",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "      GlobalLimitExec: skip=0, fetch=100",
            "        LocalLimitExec: fetch=100",
            "          SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "            ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];

        // We should keep the bottom `SortExec`.
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "      UnionExec",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "        GlobalLimitExec: skip=0, fetch=100",
            "          LocalLimitExec: fetch=100",
            "            SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "              ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_change_wrong_sorting() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort = sort_exec(vec![sort_exprs[0].clone()], source);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
        let expected_input = [
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_change_wrong_sorting2() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let spm1 = sort_preserving_merge_exec(sort_exprs.clone(), source);
        let sort2 = sort_exec(vec![sort_exprs[0].clone()], spm1);
        let physical_plan =
            sort_preserving_merge_exec(vec![sort_exprs[1].clone()], sort2);

        let expected_input = [
            "SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = [
            "SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_sorted() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source1);

        let source2 = parquet_exec_sorted(&schema, sort_exprs.clone());

        let union = union_exec(vec![source2, sort]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

        // one input to the union is already sorted, one is not.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source1);

        let parquet_sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, sort]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

        // one input to the union is already sorted, one is not.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted2() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort = sort_exec(sort_exprs.clone(), source1);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, sort]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

        // Input is an invalid plan. In this case rule should add required sorting in appropriate places.
        // First ParquetExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy the
        // required ordering of SortPreservingMergeExec.
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];

        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted3() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        let sort2 = sort_exec(sort_exprs2, source1);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs.clone());

        let union = union_exec(vec![sort1, source2, sort2]);
        let physical_plan = sort_preserving_merge_exec(parquet_sort_exprs, union);

        // First input to the union is not Sorted (SortExec is finer than required ordering by the SortPreservingMergeExec above).
        // Second input to the union is already Sorted (matches with the required ordering by the SortPreservingMergeExec above).
        // Third input to the union is not Sorted (SortExec is matches required ordering by the SortPreservingMergeExec above).
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted4() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs2.clone(), source1.clone());
        let sort2 = sort_exec(sort_exprs2.clone(), source1);

        let source2 = parquet_exec_sorted(&schema, sort_exprs2);

        let union = union_exec(vec![sort1, source2, sort2]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs1, union);

        // Ordering requirement of the `SortPreservingMergeExec` is not met.
        // Should modify the plan to ensure that all three inputs to the
        // `UnionExec` satisfy the ordering, OR add a single sort after
        // the `UnionExec` (both of which are equally good for this example).
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted5() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr_options(
                "non_nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];
        let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort2 = sort_exec(sort_exprs2, source1);

        let union = union_exec(vec![sort1, sort2]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

        // The `UnionExec` doesn't preserve any of the inputs ordering in the
        // example below. However, we should be able to change the unnecessarily
        // fine `SortExec`s below with required `SortExec`s that are absolutely necessary.
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted6() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let repartition = repartition_exec(source1);
        let spm = sort_preserving_merge_exec(sort_exprs2, repartition);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs.clone());

        let union = union_exec(vec![sort1, source2, spm]);
        let physical_plan = sort_preserving_merge_exec(parquet_sort_exprs, union);

        // The plan is not valid as it is -- the input ordering requirement
        // of the `SortPreservingMergeExec` under the third child of the
        // `UnionExec` is not met. We should add a `SortExec` below it.
        // At the same time, this ordering requirement is unnecessarily fine.
        // The final plan should be valid AND the ordering of the third child
        // shouldn't be finer than necessary.
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        // Should adjust the requirement in the third input of the union so
        // that it is not unnecessarily fine.
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted7() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1.clone(), source1.clone());
        let sort2 = sort_exec(sort_exprs1, source1);

        let union = union_exec(vec![sort1, sort2]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

        // Union has unnecessarily fine ordering below it. We should be able to replace them with absolutely necessary ordering.
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        // Union preserves the inputs ordering and we should not change any of the SortExecs under UnionExec
        let expected_output = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_output, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted8() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![
            sort_expr_options(
                "nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
            sort_expr_options(
                "non_nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort2 = sort_exec(sort_exprs2, source1);

        let physical_plan = union_exec(vec![sort1, sort2]);

        // The `UnionExec` doesn't preserve any of the inputs ordering in the
        // example below.
        let expected_input = ["UnionExec",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "  SortExec: expr=[nullable_col@0 DESC NULLS LAST,non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        // Since `UnionExec` doesn't preserve ordering in the plan above.
        // We shouldn't keep SortExecs in the plan.
        let expected_optimized = ["UnionExec",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_multi_path_sort() -> Result<()> {
        let schema = create_test_schema()?;

        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        // reverse sorting of sort_exprs2
        let sort_exprs3 = vec![sort_expr_options(
            "nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )];
        let source1 = parquet_exec_sorted(&schema, sort_exprs1);
        let source2 = parquet_exec_sorted(&schema, sort_exprs2);
        let sort1 = sort_exec(sort_exprs3.clone(), source1);
        let sort2 = sort_exec(sort_exprs3.clone(), source2);

        let union = union_exec(vec![sort1, sort2]);
        let spm = sort_preserving_merge_exec(sort_exprs3.clone(), union);
        let physical_plan = bounded_window_exec("nullable_col", sort_exprs3, spm);

        // The `WindowAggExec` gets its sorting from multiple children jointly.
        // During the removal of `SortExec`s, it should be able to remove the
        // corresponding SortExecs together. Also, the inputs of these `SortExec`s
        // are not necessarily the same to be able to remove them.
        let expected_input = [
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortPreservingMergeExec: [nullable_col@0 DESC NULLS LAST]",
            "    UnionExec",
            "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
        let expected_optimized = [
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    UnionExec",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_multi_path_sort2() -> Result<()> {
        let schema = create_test_schema()?;

        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        let source1 = parquet_exec_sorted(&schema, sort_exprs2.clone());
        let source2 = parquet_exec_sorted(&schema, sort_exprs2.clone());
        let sort1 = sort_exec(sort_exprs1.clone(), source1);
        let sort2 = sort_exec(sort_exprs1.clone(), source2);

        let union = union_exec(vec![sort1, sort2]);
        let spm = Arc::new(SortPreservingMergeExec::new(sort_exprs1, union)) as _;
        let physical_plan = bounded_window_exec("nullable_col", sort_exprs2, spm);

        // The `WindowAggExec` can get its required sorting from the leaf nodes directly.
        // The unnecessary SortExecs should be removed
        let expected_input = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    UnionExec",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
        let expected_optimized = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    UnionExec",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted_with_limit() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr_options(
                "non_nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];
        let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1, source1.clone());

        let sort2 = sort_exec(sort_exprs2, source1);
        let limit = local_limit_exec(sort2);
        let limit = global_limit_exec(limit);

        let union = union_exec(vec![sort1, limit]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

        // Should not change the unnecessarily fine `SortExec`s because there is `LimitExec`
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    GlobalLimitExec: skip=0, fetch=100",
            "      LocalLimitExec: fetch=100",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
            "          ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    GlobalLimitExec: skip=0, fetch=100",
            "      LocalLimitExec: fetch=100",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
            "          ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_join_order_by_left() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;

        let left = parquet_exec(&left_schema);
        let right = parquet_exec(&right_schema);

        // Join on (nullable_col == col_a)
        let join_on = vec![(
            Arc::new(Column::new_with_schema("nullable_col", &left.schema()).unwrap())
                as _,
            Arc::new(Column::new_with_schema("col_a", &right.schema()).unwrap()) as _,
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];
        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let sort_exprs = vec![
                sort_expr("nullable_col", &join.schema()),
                sort_expr("non_nullable_col", &join.schema()),
            ];
            let physical_plan = sort_preserving_merge_exec(sort_exprs.clone(), join);

            let join_plan = format!(
                "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let join_plan2 = format!(
                "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
                join_plan2.as_str(),
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
            let expected_optimized = match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    // can push down the sort requirements and save 1 SortExec
                    vec![
                        join_plan.as_str(),
                        "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "  SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
                _ => {
                    // can not push down the sort requirements
                    vec![
                        "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
                        join_plan2.as_str(),
                        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
            };
            assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_join_order_by_right() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;

        let left = parquet_exec(&left_schema);
        let right = parquet_exec(&right_schema);

        // Join on (nullable_col == col_a)
        let join_on = vec![(
            Arc::new(Column::new_with_schema("nullable_col", &left.schema()).unwrap())
                as _,
            Arc::new(Column::new_with_schema("col_a", &right.schema()).unwrap()) as _,
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::RightAnti,
        ];
        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let sort_exprs = vec![
                sort_expr("col_a", &join.schema()),
                sort_expr("col_b", &join.schema()),
            ];
            let physical_plan = sort_preserving_merge_exec(sort_exprs, join);

            let join_plan = format!(
                "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let spm_plan = match join_type {
                JoinType::RightAnti => {
                    "SortPreservingMergeExec: [col_a@0 ASC,col_b@1 ASC]"
                }
                _ => "SortPreservingMergeExec: [col_a@2 ASC,col_b@3 ASC]",
            };
            let join_plan2 = format!(
                "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let expected_input = [spm_plan,
                join_plan2.as_str(),
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
            let expected_optimized = match join_type {
                JoinType::Inner | JoinType::Right | JoinType::RightAnti => {
                    // can push down the sort requirements and save 1 SortExec
                    vec![
                        join_plan.as_str(),
                        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "  SortExec: expr=[col_a@0 ASC,col_b@1 ASC], preserve_partitioning=[false]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
                _ => {
                    // can not push down the sort requirements for Left and Full join.
                    vec![
                        "SortExec: expr=[col_a@2 ASC,col_b@3 ASC], preserve_partitioning=[false]",
                        join_plan2.as_str(),
                        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
            };
            assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_join_complex_order_by() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;

        let left = parquet_exec(&left_schema);
        let right = parquet_exec(&right_schema);

        // Join on (nullable_col == col_a)
        let join_on = vec![(
            Arc::new(Column::new_with_schema("nullable_col", &left.schema()).unwrap())
                as _,
            Arc::new(Column::new_with_schema("col_a", &right.schema()).unwrap()) as _,
        )];

        let join = sort_merge_join_exec(left, right, &join_on, &JoinType::Inner);

        // order by (col_b, col_a)
        let sort_exprs1 = vec![
            sort_expr("col_b", &join.schema()),
            sort_expr("col_a", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs1, join.clone());

        let expected_input = ["SortPreservingMergeExec: [col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];

        // can not push down the sort requirements, need to add SortExec
        let expected_optimized = ["SortExec: expr=[col_b@3 ASC,col_a@2 ASC], preserve_partitioning=[false]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        // order by (nullable_col, col_b, col_a)
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &join.schema()),
            sort_expr("col_b", &join.schema()),
            sort_expr("col_a", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs2, join);

        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC,col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];

        // can not push down the sort requirements, need to add SortExec
        let expected_optimized = ["SortExec: expr=[nullable_col@0 ASC,col_b@3 ASC,col_a@2 ASC], preserve_partitioning=[false]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_sort_window_exec() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];

        let sort1 = sort_exec(sort_exprs1.clone(), source);
        let window_agg1 =
            bounded_window_exec("non_nullable_col", sort_exprs1.clone(), sort1);
        let window_agg2 =
            bounded_window_exec("non_nullable_col", sort_exprs2, window_agg1);
        // let filter_exec = sort_exec;
        let physical_plan =
            bounded_window_exec("non_nullable_col", sort_exprs1, window_agg2);

        let expected_input = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "        MemoryExec: partitions=1, partition_sizes=[0]"];

        let expected_optimized = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "        MemoryExec: partitions=1, partition_sizes=[0]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_multilayer_coalesce_partitions() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let repartition = repartition_exec(source1);
        let coalesce = Arc::new(CoalescePartitionsExec::new(repartition)) as _;
        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let filter = filter_exec(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            coalesce,
        );
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let physical_plan = sort_exec(sort_exprs, filter);

        // CoalescePartitionsExec and SortExec are not directly consecutive. In this case
        // we should be able to parallelize Sorting also (given that executors in between don't require)
        // single partition.
        let expected_input = ["SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  FilterExec: NOT non_nullable_col@1",
            "    CoalescePartitionsExec",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
            "    FilterExec: NOT non_nullable_col@1",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    // With new change in SortEnforcement EnforceSorting->EnforceDistribution->EnforceSorting
    // should produce same result with EnforceDistribution+EnforceSorting
    // This enables us to use EnforceSorting possibly before EnforceDistribution
    // Given that it will be called at least once after last EnforceDistribution. The reason is that
    // EnforceDistribution may invalidate ordering invariant.
    async fn test_commutativity() -> Result<()> {
        let schema = create_test_schema()?;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let memory_exec = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let window = bounded_window_exec("nullable_col", sort_exprs.clone(), memory_exec);
        let repartition = repartition_exec(window);

        let orig_plan =
            Arc::new(SortExec::new(sort_exprs, repartition)) as Arc<dyn ExecutionPlan>;
        let actual = get_plan_string(&orig_plan);
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_eq!(
            expected_input, actual,
            "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_input:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let mut plan = orig_plan.clone();
        let rules = vec![
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        ];
        for rule in rules {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let first_plan = plan.clone();

        let mut plan = orig_plan.clone();
        let rules = vec![
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        ];
        for rule in rules {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let second_plan = plan.clone();

        assert_eq!(get_plan_string(&first_plan), get_plan_string(&second_plan));
        Ok(())
    }

    #[tokio::test]
    async fn test_coalesce_propagate() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let repartition = repartition_exec(source);
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(repartition));
        let repartition = repartition_exec(coalesce_partitions);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        // Add local sort
        let sort = Arc::new(
            SortExec::new(sort_exprs.clone(), repartition)
                .with_preserve_partitioning(true),
        ) as _;
        let spm = sort_preserving_merge_exec(sort_exprs.clone(), sort);
        let sort = sort_exec(sort_exprs, spm);

        let physical_plan = sort.clone();
        // Sort Parallelize rule should end Coalesce + Sort linkage when Sort is Global Sort
        // Also input plan is not valid as it is. We need to add SortExec before SortPreservingMergeExec.
        let expected_input = ["SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CoalescePartitionsExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]"];
        let expected_optimized = [
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_ordering_bounded() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs);
        let repartition_rr = repartition_exec(source);
        let repartition_hash = Arc::new(RepartitionExec::try_new(
            repartition_rr,
            Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
        )?) as _;
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

        let expected_input = ["SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=false"];
        let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=false"];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_lost_ordering_unbounded_bounded(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        // create either bounded or unbounded source
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_ordered(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec(source);
        let repartition_hash = Arc::new(RepartitionExec::try_new(
            repartition_rr,
            Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
        )?) as _;
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = vec![
            "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]",
        ];
        let expected_input_bounded = vec![
            "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = vec![
            "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=true",
        ];
        let expected_optimized_bounded_parallelize_sort = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=true",
        ];
        let (expected_input, expected_optimized, expected_optimized_sort_parallelize) =
            if source_unbounded {
                (
                    expected_input_unbounded,
                    expected_optimized_unbounded.clone(),
                    expected_optimized_unbounded,
                )
            } else {
                (
                    expected_input_bounded,
                    expected_optimized_bounded,
                    expected_optimized_bounded_parallelize_sort,
                )
            };
        assert_optimized!(
            expected_input,
            expected_optimized,
            physical_plan.clone(),
            false
        );
        assert_optimized!(
            expected_input,
            expected_optimized_sort_parallelize,
            physical_plan,
            true
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_do_not_pushdown_through_spm() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema), sort_expr("b", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs.clone());
        let repartition_rr = repartition_exec(source);
        let spm = sort_preserving_merge_exec(sort_exprs, repartition_rr);
        let physical_plan = sort_exec(vec![sort_expr("b", &schema)], spm);

        let expected_input = ["SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [a@0 ASC,b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
        let expected_optimized = ["SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [a@0 ASC,b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
        assert_optimized!(expected_input, expected_optimized, physical_plan, false);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_through_spm() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema), sort_expr("b", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs.clone());
        let repartition_rr = repartition_exec(source);
        let spm = sort_preserving_merge_exec(sort_exprs, repartition_rr);
        let physical_plan = sort_exec(
            vec![
                sort_expr("a", &schema),
                sort_expr("b", &schema),
                sort_expr("c", &schema),
            ],
            spm,
        );

        let expected_input = ["SortExec: expr=[a@0 ASC,b@1 ASC,c@2 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [a@0 ASC,b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
        let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC,b@1 ASC]",
            "  SortExec: expr=[a@0 ASC,b@1 ASC,c@2 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
        assert_optimized!(expected_input, expected_optimized, physical_plan, false);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_multi_layer_requirement() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema), sort_expr("b", &schema)];
        let source = csv_exec_sorted(&schema, vec![]);
        let sort = sort_exec(sort_exprs.clone(), source);
        let repartition = repartition_exec(sort);
        let repartition = spr_repartition_exec(repartition);
        let spm = sort_preserving_merge_exec(sort_exprs.clone(), repartition);

        let physical_plan = bounded_window_exec("a", sort_exprs, spm);

        let expected_input = [
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortPreservingMergeExec: [a@0 ASC,b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC,b@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortExec: expr=[a@0 ASC,b@1 ASC], preserve_partitioning=[false]",
            "          CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        let expected_optimized = [
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortExec: expr=[a@0 ASC,b@1 ASC], preserve_partitioning=[false]",
            "    CoalescePartitionsExec",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, false);

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_with_partial_sort() -> Result<()> {
        let schema = create_test_schema3()?;
        let input_sort_exprs = vec![sort_expr("a", &schema)];
        let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs);

        let physical_plan = sort_exec(
            vec![sort_expr("a", &schema), sort_expr("c", &schema)],
            unbounded_input,
        );

        let expected_input = [
            "SortExec: expr=[a@0 ASC,c@2 ASC], preserve_partitioning=[false]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]"
        ];
        let expected_optimized = [
            "PartialSortExec: expr=[a@0 ASC,c@2 ASC], common_prefix_length=[1]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_with_partial_sort2() -> Result<()> {
        let schema = create_test_schema3()?;
        let input_sort_exprs = vec![sort_expr("a", &schema), sort_expr("c", &schema)];
        let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs);

        let physical_plan = sort_exec(
            vec![
                sort_expr("a", &schema),
                sort_expr("c", &schema),
                sort_expr("d", &schema),
            ],
            unbounded_input,
        );

        let expected_input = [
            "SortExec: expr=[a@0 ASC,c@2 ASC,d@3 ASC], preserve_partitioning=[false]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC, c@2 ASC]"
        ];
        // let optimized
        let expected_optimized = [
            "PartialSortExec: expr=[a@0 ASC,c@2 ASC,d@3 ASC], common_prefix_length=[2]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC, c@2 ASC]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_replaced_with_partial_sort_for_bounded_input() -> Result<()> {
        let schema = create_test_schema3()?;
        let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
        let parquet_input = parquet_exec_sorted(&schema, input_sort_exprs);

        let physical_plan = sort_exec(
            vec![
                sort_expr("a", &schema),
                sort_expr("b", &schema),
                sort_expr("c", &schema),
            ],
            parquet_input,
        );
        let expected_input = [
            "SortExec: expr=[a@0 ASC,b@1 ASC,c@2 ASC], preserve_partitioning=[false]",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[b@1 ASC, c@2 ASC]"
        ];
        let expected_no_change = expected_input;
        assert_optimized!(expected_input, expected_no_change, physical_plan, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_replaced_with_partial_sort_for_unbounded_input() -> Result<()> {
        let schema = create_test_schema3()?;
        let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
        let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs);

        let physical_plan = sort_exec(
            vec![
                sort_expr("a", &schema),
                sort_expr("b", &schema),
                sort_expr("c", &schema),
            ],
            unbounded_input,
        );
        let expected_input = [
            "SortExec: expr=[a@0 ASC,b@1 ASC,c@2 ASC], preserve_partitioning=[false]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
        ];
        let expected_no_change = expected_input;
        assert_optimized!(expected_input, expected_no_change, physical_plan, true);
        Ok(())
    }
}
