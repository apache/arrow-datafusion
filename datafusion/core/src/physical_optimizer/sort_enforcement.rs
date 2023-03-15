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
//! - Adds a [SortExec] when a requirement is not met,
//! - Removes an already-existing [SortExec] if it is possible to prove
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
//! by another SortExec. Therefore, this rule removes it from the physical plan.
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::utils::add_sort_above;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::JoinSide;
use crate::physical_plan::joins::SortMergeJoinExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::tree_node::TreeNodeRewritable;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};
use arrow::datatypes::SchemaRef;
use datafusion_common::{reverse_sort_options, DataFusionError};
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{
    create_sort_expr_from_requirement, ordering_satisfy, ordering_satisfy_requirement,
    ordering_satisfy_requirement_concrete, requirements_compatible,
};
use datafusion_physical_expr::{
    new_sort_requirements, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirements,
};
use itertools::{concat, izip};
use std::iter::zip;
use std::ops::Deref;
use std::sync::Arc;

/// This rule inspects `SortExec`'s in the given physical plan and removes the
/// ones it can prove unnecessary.
#[derive(Default)]
pub struct EnforceSorting {}

impl EnforceSorting {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Checks whether the given executor is a limit;
/// i.e. either a `LocalLimitExec` or a `GlobalLimitExec`.
fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>() || plan.as_any().is::<LocalLimitExec>()
}

/// Checks whether the given executor is a widnow;
/// i.e. either a `WindowAggExec` or a `BoundedWindowAggExec`.
fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<WindowAggExec>() || plan.as_any().is::<BoundedWindowAggExec>()
}

/// Checks whether the given executor is a `SortExec`.
fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortExec>()
}

/// Checks whether the given executor is a `SortPreservingMergeExec`.
fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortPreservingMergeExec>()
}

/// This object implements a tree that we use while keeping track of paths
/// leading to `SortExec`s.
#[derive(Debug, Clone)]
struct ExecTree {
    /// The `ExecutionPlan` associated with this node
    pub plan: Arc<dyn ExecutionPlan>,
    /// Child index of the plan in its parent
    pub idx: usize,
    /// Children of the plan that would need updating if we remove leaf executors
    pub children: Vec<ExecTree>,
}

impl ExecTree {
    /// Create new Exec tree
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        idx: usize,
        children: Vec<ExecTree>,
    ) -> Self {
        ExecTree {
            plan,
            idx,
            children,
        }
    }

    /// This function returns the executors at the leaves of the tree.
    fn get_leaves(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        if self.children.is_empty() {
            vec![self.plan.clone()]
        } else {
            concat(self.children.iter().map(|e| e.get_leaves()))
        }
    }
}

/// This object is used within the [EnforceSorting] rule to track the closest
/// `SortExec` descendant(s) for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingSort {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a subtree of `ExecutionPlan`s starting from the
    // child until the `SortExec`(s) -- could be multiple for n-ary plans like
    // Union -- that determine the output ordering of the child. If the child
    // has no connection to any sort, simply store None (and not a subtree).
    sort_onwards: Vec<Option<ExecTree>>,
}

impl PlanWithCorrespondingSort {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PlanWithCorrespondingSort {
            plan,
            sort_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<PlanWithCorrespondingSort>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect::<Vec<_>>();
        let sort_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                let plan = &item.plan;
                // Leaves of `sort_onwards` are `SortExec` operators, which impose
                // an ordering. This tree collects all the intermediate executors
                // that maintain this ordering. If we just saw a order imposing
                // operator, we reset the tree and start accumulating.
                if is_sort(plan) {
                    return Some(ExecTree::new(item.plan, idx, vec![]));
                } else if is_limit(plan) {
                    // There is no sort linkage for this path, it starts at a limit.
                    return None;
                }
                let is_spm = is_sort_preserving_merge(plan);
                let required_orderings = plan.required_input_ordering();
                let flags = plan.maintains_input_order();
                let children = izip!(flags, item.sort_onwards, required_orderings)
                    .filter_map(|(maintains, element, required_ordering)| {
                        if (required_ordering.is_none() && maintains) || is_spm {
                            element
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<ExecTree>>();
                if !children.is_empty() {
                    // Add parent node to the tree if there is at least one
                    // child with a subtree:
                    Some(ExecTree::new(item.plan, idx, children))
                } else {
                    // There is no sort linkage for this child, do nothing.
                    None
                }
            })
            .collect();

        let plan = with_new_children_if_necessary(parent_plan, children_plans)?;
        Ok(PlanWithCorrespondingSort { plan, sort_onwards })
    }

    pub fn children(&self) -> Vec<PlanWithCorrespondingSort> {
        self.plan
            .children()
            .into_iter()
            .map(|child| PlanWithCorrespondingSort::new(child))
            .collect()
    }
}

impl TreeNodeRewritable for PlanWithCorrespondingSort {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            PlanWithCorrespondingSort::new_from_children_nodes(children_nodes, self.plan)
        }
    }
}

/// This object is used within the [EnforceSorting] rule to track the closest
/// `CoalescePartitionsExec` descendant(s) for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingCoalescePartitions {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a subtree of `ExecutionPlan`s starting from the
    // child until the `CoalescePartitionsExec`(s) -- could be multiple for
    // n-ary plans like Union -- that affect the output partitioning of the
    // child. If the child has no connection to any `CoalescePartitionsExec`,
    // simply store None (and not a subtree).
    coalesce_onwards: Vec<Option<ExecTree>>,
}

impl PlanWithCorrespondingCoalescePartitions {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PlanWithCorrespondingCoalescePartitions {
            plan,
            coalesce_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<PlanWithCorrespondingCoalescePartitions>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let coalesce_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                // Leaves of the `coalesce_onwards` tree are `CoalescePartitionsExec`
                // operators. This tree collects all the intermediate executors that
                // maintain a single partition. If we just saw a `CoalescePartitionsExec`
                // operator, we reset the tree and start accumulating.
                let plan = item.plan;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if plan.as_any().is::<CoalescePartitionsExec>() {
                    Some(ExecTree::new(plan, idx, vec![]))
                } else {
                    let children = item
                        .coalesce_onwards
                        .into_iter()
                        .flatten()
                        .filter(|item| {
                            // Only consider operators that don't require a
                            // single partition.
                            !matches!(
                                plan.required_input_distribution()[item.idx],
                                Distribution::SinglePartition
                            )
                        })
                        .collect::<Vec<_>>();
                    if children.is_empty() {
                        None
                    } else {
                        Some(ExecTree::new(plan, idx, children))
                    }
                }
            })
            .collect();
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?;
        Ok(PlanWithCorrespondingCoalescePartitions {
            plan,
            coalesce_onwards,
        })
    }

    pub fn children(&self) -> Vec<PlanWithCorrespondingCoalescePartitions> {
        self.plan
            .children()
            .into_iter()
            .map(|child| PlanWithCorrespondingCoalescePartitions::new(child))
            .collect()
    }
}

impl TreeNodeRewritable for PlanWithCorrespondingCoalescePartitions {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            PlanWithCorrespondingCoalescePartitions::new_from_children_nodes(
                children_nodes,
                self.plan,
            )
        }
    }
}

/// This is a "data class" we use within the [TopDownEnforceSorting] rule
#[derive(Debug, Clone)]
struct TopDownSortPushDown {
    /// Current plan
    plan: Arc<dyn ExecutionPlan>,
    /// Whether the plan could impact the final result ordering
    impact_result_ordering: bool,
    /// Parent has the SinglePartition requirement to children
    satisfy_single_distribution: bool,
    /// Parent required sort ordering
    required_ordering: Option<Vec<PhysicalSortRequirements>>,
    /// The adjusted request sort ordering to children.
    /// By default they are the same as the plan's required input ordering, but can be adjusted based on parent required sort ordering properties.
    adjusted_request_ordering: Vec<Option<Vec<PhysicalSortRequirements>>>,
}

impl TopDownSortPushDown {
    pub fn init(plan: Arc<dyn ExecutionPlan>) -> Self {
        let impact_result_ordering = plan.output_ordering().is_some()
            || plan.output_partitioning().partition_count() <= 1
            || is_limit(&plan);
        let request_ordering = plan.required_input_ordering();
        TopDownSortPushDown {
            plan,
            impact_result_ordering,
            satisfy_single_distribution: false,
            required_ordering: None,
            adjusted_request_ordering: request_ordering,
        }
    }

    pub fn new_without_impact_result_ordering(plan: Arc<dyn ExecutionPlan>) -> Self {
        let request_ordering = plan.required_input_ordering();
        TopDownSortPushDown {
            plan,
            impact_result_ordering: false,
            satisfy_single_distribution: false,
            required_ordering: None,
            adjusted_request_ordering: request_ordering,
        }
    }

    pub fn children(&self) -> Vec<TopDownSortPushDown> {
        let plan_children = self.plan.children();
        assert_eq!(plan_children.len(), self.adjusted_request_ordering.len());

        izip!(
            plan_children.into_iter(),
            self.adjusted_request_ordering.clone().into_iter(),
            self.plan.maintains_input_order().into_iter(),
            self.plan.required_input_distribution().into_iter(),
        )
        .map(
            |(child, from_parent, maintains_input_order, required_dist)| {
                let child_satisfy_single_distribution =
                    matches!(required_dist, Distribution::SinglePartition);
                let child_impact_result_ordering = if is_limit(&self.plan) {
                    true
                } else {
                    maintains_input_order && self.impact_result_ordering
                };
                let child_request_ordering = child.required_input_ordering();
                TopDownSortPushDown {
                    plan: child,
                    impact_result_ordering: child_impact_result_ordering,
                    satisfy_single_distribution: child_satisfy_single_distribution,
                    required_ordering: from_parent,
                    adjusted_request_ordering: child_request_ordering,
                }
            },
        )
        .collect()
    }
}

impl TreeNodeRewritable for TopDownSortPushDown {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let new_children = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;

            let children_plans = new_children
                .iter()
                .map(|elem| elem.plan.clone())
                .collect::<Vec<_>>();
            let plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(TopDownSortPushDown {
                plan,
                impact_result_ordering: self.impact_result_ordering,
                satisfy_single_distribution: self.satisfy_single_distribution,
                required_ordering: self.required_ordering,
                adjusted_request_ordering: self.adjusted_request_ordering,
            })
        }
    }
}

/// The boolean flag `repartition_sorts` defined in the config indicates
/// whether we elect to transform CoalescePartitionsExec + SortExec cascades
/// into SortExec + SortPreservingMergeExec cascades, which enables us to
/// perform sorting in parallel.
impl PhysicalOptimizerRule for EnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_requirements = PlanWithCorrespondingSort::new(plan);
        let adjusted = plan_requirements.transform_up(&ensure_sorting)?;
        let new_plan = if config.optimizer.repartition_sorts {
            let plan_with_coalesce_partitions =
                PlanWithCorrespondingCoalescePartitions::new(adjusted.plan);
            let parallel =
                plan_with_coalesce_partitions.transform_up(&parallelize_sorts)?;
            parallel.plan
        } else {
            adjusted.plan
        };
        // Execute a Top-Down process(Preorder Traversal) to ensure the sort requirements:
        let sort_pushdown = TopDownSortPushDown::init(new_plan);
        let adjusted = sort_pushdown.transform_down(&pushdown_sorts)?;
        Ok(adjusted.plan)
    }

    fn name(&self) -> &str {
        "EnforceSorting"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function turns plans of the form
///      "SortExec: expr=[a@0 ASC]",
///      "  CoalescePartitionsExec",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// to
///      "SortPreservingMergeExec: [a@0 ASC]",
///      "  SortExec: expr=[a@0 ASC]",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// by following connections from `CoalescePartitionsExec`s to `SortExec`s.
/// By performing sorting in parallel, we can increase performance in some scenarios.
fn parallelize_sorts(
    requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<Option<PlanWithCorrespondingCoalescePartitions>> {
    let plan = requirements.plan;
    let mut coalesce_onwards = requirements.coalesce_onwards;
    if plan.children().is_empty()
        // We only do action when plan is either SortExec, SortPreservingMergeExec or CoalescePartitionsExec
        // all of them have single child. If 0th child is `None` we can immediately return.
        || coalesce_onwards[0].is_none()
    {
        return Ok(None);
    }
    // We know that `plan` has children, so `coalesce_onwards` is non-empty.
    if (is_sort(&plan) || is_sort_preserving_merge(&plan))
        // Make sure that Sort is actually global sort
        && plan.output_partitioning().partition_count() <= 1
    {
        // If there is a connection between a `CoalescePartitionsExec` and a
        // Global Sort that satisfy the requirements (i.e. intermediate
        // executors  don't require single partition), then we can
        // replace the `CoalescePartitionsExec`+ GlobalSort cascade with
        // the `SortExec` + `SortPreservingMergeExec`
        // cascade to parallelize sorting.
        let mut prev_layer = plan.clone();
        update_child_to_remove_coalesce(&mut prev_layer, &mut coalesce_onwards[0])?;
        let sort_exprs = get_sort_exprs(&plan)?;
        add_sort_above(&mut prev_layer, sort_exprs.to_vec())?;
        let spm = SortPreservingMergeExec::new(sort_exprs.to_vec(), prev_layer);
        return Ok(Some(PlanWithCorrespondingCoalescePartitions {
            plan: Arc::new(spm),
            coalesce_onwards: vec![None],
        }));
    } else if plan.as_any().is::<CoalescePartitionsExec>() {
        // There is an unnecessary `CoalescePartitionExec` in the plan.
        let mut prev_layer = plan.clone();
        update_child_to_remove_coalesce(&mut prev_layer, &mut coalesce_onwards[0])?;
        let new_plan = plan.with_new_children(vec![prev_layer])?;
        return Ok(Some(PlanWithCorrespondingCoalescePartitions {
            plan: new_plan,
            coalesce_onwards: vec![None],
        }));
    }

    Ok(Some(PlanWithCorrespondingCoalescePartitions {
        plan,
        coalesce_onwards,
    }))
}

/// This function enforces sorting requirements and makes optimizations without
/// violating these requirements whenever possible.
fn ensure_sorting(
    requirements: PlanWithCorrespondingSort,
) -> Result<Option<PlanWithCorrespondingSort>> {
    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    let plan = requirements.plan;
    let mut children = plan.children();
    if children.is_empty() {
        return Ok(None);
    }
    let mut sort_onwards = requirements.sort_onwards;
    if let Some(result) = analyze_immediate_sort_removal(&plan, &sort_onwards) {
        return Ok(Some(result));
    }
    for (idx, (child, sort_onwards, required_ordering)) in izip!(
        children.iter_mut(),
        sort_onwards.iter_mut(),
        plan.required_input_ordering()
    )
    .enumerate()
    {
        let physical_ordering = child.output_ordering();
        match (required_ordering, physical_ordering) {
            (Some(required_ordering), Some(physical_ordering)) => {
                if !ordering_satisfy_requirement_concrete(
                    physical_ordering,
                    &required_ordering,
                    || child.equivalence_properties(),
                ) {
                    // Make sure we preserve the ordering requirements:
                    update_child_to_remove_unnecessary_sort(
                        child,
                        sort_onwards,
                        &plan,
                        idx,
                    )?;
                    let sort_expr = create_sort_expr_from_requirement(&required_ordering);
                    add_sort_above(child, sort_expr)?;
                    if is_sort(child) {
                        *sort_onwards = Some(ExecTree::new(child.clone(), idx, vec![]));
                    } else {
                        *sort_onwards = None;
                    }
                }
            }
            (Some(required), None) => {
                // Ordering requirement is not met, we should add a `SortExec` to the plan.
                let sort_expr = create_sort_expr_from_requirement(&required);
                add_sort_above(child, sort_expr)?;
                *sort_onwards = Some(ExecTree::new(child.clone(), idx, vec![]));
            }
            (None, Some(_)) => {
                // We have a `SortExec` whose effect may be neutralized by
                // another order-imposing operator. Remove this sort.
                if !plan.maintains_input_order()[idx] {
                    update_child_to_remove_unnecessary_sort(
                        child,
                        sort_onwards,
                        &plan,
                        idx,
                    )?;
                }
            }
            (None, None) => {}
        }
    }
    // For window expressions, we can remove some sorts when we can
    // calculate the result in reverse:
    if is_window(&plan) {
        if let Some(tree) = &mut sort_onwards[0] {
            if let Some(result) = analyze_window_sort_removal(tree, &plan)? {
                return Ok(Some(result));
            }
        }
    } else if is_sort_preserving_merge(&plan)
        && children[0].output_partitioning().partition_count() <= 1
    {
        // sort preserving merge can removed. Input already has single partition
        return Ok(Some(PlanWithCorrespondingSort {
            plan: children[0].clone(),
            sort_onwards: vec![sort_onwards[0].clone()],
        }));
    }
    Ok(Some(PlanWithCorrespondingSort {
        plan: plan.with_new_children(children)?,
        sort_onwards,
    }))
}

fn pushdown_sorts(
    requirements: TopDownSortPushDown,
) -> Result<Option<TopDownSortPushDown>> {
    let plan = &requirements.plan;
    let parent_required = requirements.required_ordering.as_deref();
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let mut new_plan = plan.clone();
        if !ordering_satisfy_requirement(plan.output_ordering(), parent_required, || {
            plan.equivalence_properties()
        }) {
            // If the current plan is a SortExec, modify current SortExec to satisfy the parent requirements
            let parent_required_expr =
                create_sort_expr_from_requirement(parent_required.unwrap());
            new_plan = sort_exec.input.clone();
            add_sort_above(&mut new_plan, parent_required_expr)?;
        };
        let required_ordering = new_sort_requirements(new_plan.output_ordering());
        let child = &new_plan.children()[0];
        if let Some(adjusted) =
            pushdown_requirement_to_children(child, required_ordering.as_deref())?
        {
            // Can push down requirements
            Ok(Some(TopDownSortPushDown {
                plan: child.clone(),
                required_ordering,
                adjusted_request_ordering: adjusted,
                ..requirements
            }))
        } else {
            // Can not push down requirements
            Ok(Some(
                TopDownSortPushDown::new_without_impact_result_ordering(new_plan),
            ))
        }
    } else {
        // Executors other than SortExec
        if ordering_satisfy_requirement(plan.output_ordering(), parent_required, || {
            plan.equivalence_properties()
        }) {
            Ok(Some(TopDownSortPushDown {
                required_ordering: None,
                ..requirements
            }))
        } else {
            // Can not satisfy the parent requirements, check whether the requirements can be pushed down. If not, add new SortExec.
            let parent_required_expr =
                create_sort_expr_from_requirement(parent_required.unwrap());
            if let Some(adjusted) = pushdown_requirement_to_children(
                plan,
                requirements.required_ordering.as_deref(),
            )? {
                Ok(Some(TopDownSortPushDown {
                    plan: plan.clone(),
                    adjusted_request_ordering: adjusted,
                    ..requirements
                }))
            } else {
                // Can not push down requirements, add new SortExec
                let mut new_plan = plan.clone();
                add_sort_above(&mut new_plan, parent_required_expr)?;
                Ok(Some(
                    TopDownSortPushDown::new_without_impact_result_ordering(new_plan),
                ))
            }
        }
    }
}

/// Analyzes a given `SortExec` (`plan`) to determine whether its input already
/// has a finer ordering than this `SortExec` enforces.
fn analyze_immediate_sort_removal(
    plan: &Arc<dyn ExecutionPlan>,
    sort_onwards: &[Option<ExecTree>],
) -> Option<PlanWithCorrespondingSort> {
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let sort_input = sort_exec.input().clone();
        // If this sort is unnecessary, we should remove it:
        if ordering_satisfy(
            sort_input.output_ordering(),
            sort_exec.output_ordering(),
            || sort_input.equivalence_properties(),
        ) {
            // Since we know that a `SortExec` has exactly one child,
            // we can use the zero index safely:
            return Some(
                if !sort_exec.preserve_partitioning()
                    && sort_input.output_partitioning().partition_count() > 1
                {
                    // Replace the sort with a sort-preserving merge:
                    let new_plan: Arc<dyn ExecutionPlan> =
                        Arc::new(SortPreservingMergeExec::new(
                            sort_exec.expr().to_vec(),
                            sort_input,
                        ));
                    let new_tree = ExecTree::new(
                        new_plan.clone(),
                        0,
                        sort_onwards.iter().flat_map(|e| e.clone()).collect(),
                    );
                    PlanWithCorrespondingSort {
                        plan: new_plan,
                        sort_onwards: vec![Some(new_tree)],
                    }
                } else {
                    // Remove the sort:
                    PlanWithCorrespondingSort {
                        plan: sort_input,
                        sort_onwards: sort_onwards.to_vec(),
                    }
                },
            );
        }
    }
    None
}

/// Analyzes a [WindowAggExec] or a [BoundedWindowAggExec] to determine whether
/// it may allow removing a sort.
fn analyze_window_sort_removal(
    sort_tree: &mut ExecTree,
    window_exec: &Arc<dyn ExecutionPlan>,
) -> Result<Option<PlanWithCorrespondingSort>> {
    let (window_expr, partition_keys) = if let Some(exec) =
        window_exec.as_any().downcast_ref::<BoundedWindowAggExec>()
    {
        (exec.window_expr(), &exec.partition_keys)
    } else if let Some(exec) = window_exec.as_any().downcast_ref::<WindowAggExec>() {
        (exec.window_expr(), &exec.partition_keys)
    } else {
        return Err(DataFusionError::Plan(
            "Expects to receive either WindowAggExec of BoundedWindowAggExec".to_string(),
        ));
    };

    let mut first_should_reverse = None;
    let mut physical_ordering_common = vec![];
    for sort_any in sort_tree.get_leaves() {
        let sort_output_ordering = sort_any.output_ordering();
        // Variable `sort_any` will either be a `SortExec` or a
        // `SortPreservingMergeExec`, and both have a single child.
        // Therefore, we can use the 0th index without loss of generality.
        let sort_input = sort_any.children()[0].clone();
        let physical_ordering = sort_input.output_ordering();
        // TODO: Once we can ensure that required ordering information propagates with
        //       the necessary lineage information, compare `physical_ordering` and the
        //       ordering required by the window executor instead of `sort_output_ordering`.
        //       This will enable us to handle cases such as (a,b) -> Sort -> (a,b,c) -> Required(a,b).
        //       Currently, we can not remove such sorts.
        let required_ordering = sort_output_ordering.ok_or_else(|| {
            DataFusionError::Plan("A SortExec should have output ordering".to_string())
        })?;
        if let Some(physical_ordering) = physical_ordering {
            if physical_ordering_common.is_empty()
                || physical_ordering.len() < physical_ordering_common.len()
            {
                physical_ordering_common = physical_ordering.to_vec();
            }
            let (can_skip_sorting, should_reverse) = can_skip_sort(
                window_expr[0].partition_by(),
                required_ordering,
                &sort_input.schema(),
                physical_ordering,
            )?;
            if !can_skip_sorting {
                return Ok(None);
            }
            if let Some(first_should_reverse) = first_should_reverse {
                if first_should_reverse != should_reverse {
                    return Ok(None);
                }
            } else {
                first_should_reverse = Some(should_reverse);
            }
        } else {
            // If there is no physical ordering, there is no way to remove a
            // sort, so immediately return.
            return Ok(None);
        }
    }
    let new_window_expr = if first_should_reverse.unwrap() {
        window_expr
            .iter()
            .map(|e| e.get_reverse_expr())
            .collect::<Option<Vec<_>>>()
    } else {
        Some(window_expr.to_vec())
    };
    if let Some(window_expr) = new_window_expr {
        let requires_single_partition = matches!(
            window_exec.required_input_distribution()[sort_tree.idx],
            Distribution::SinglePartition
        );
        let new_child = remove_corresponding_sort_from_sub_plan(
            sort_tree,
            requires_single_partition,
        )?;
        let new_schema = new_child.schema();

        let uses_bounded_memory = window_expr.iter().all(|e| e.uses_bounded_memory());
        // If all window expressions can run with bounded memory, choose the
        // bounded window variant:
        let new_plan = if uses_bounded_memory {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr,
                new_child,
                new_schema,
                partition_keys.to_vec(),
                Some(physical_ordering_common),
            )?) as _
        } else {
            Arc::new(WindowAggExec::try_new(
                window_expr,
                new_child,
                new_schema,
                partition_keys.to_vec(),
                Some(physical_ordering_common),
            )?) as _
        };
        return Ok(Some(PlanWithCorrespondingSort::new(new_plan)));
    }
    Ok(None)
}

/// Updates child to remove the unnecessary `CoalescePartitions` below it.
fn update_child_to_remove_coalesce(
    child: &mut Arc<dyn ExecutionPlan>,
    coalesce_onwards: &mut Option<ExecTree>,
) -> Result<()> {
    if let Some(coalesce_onwards) = coalesce_onwards {
        *child = remove_corresponding_coalesce_in_sub_plan(coalesce_onwards)?;
    }
    Ok(())
}

/// Removes the `CoalescePartitions` from the plan in `coalesce_onwards`.
fn remove_corresponding_coalesce_in_sub_plan(
    coalesce_onwards: &mut ExecTree,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(
        if coalesce_onwards
            .plan
            .as_any()
            .is::<CoalescePartitionsExec>()
        {
            // We can safely use the 0th index since we have a `CoalescePartitionsExec`.
            coalesce_onwards.plan.children()[0].clone()
        } else {
            let plan = coalesce_onwards.plan.clone();
            let mut children = plan.children();
            for item in &mut coalesce_onwards.children {
                children[item.idx] = remove_corresponding_coalesce_in_sub_plan(item)?;
            }
            plan.with_new_children(children)?
        },
    )
}

/// Updates child to remove the unnecessary sorting below it.
fn update_child_to_remove_unnecessary_sort(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Option<ExecTree>,
    parent: &Arc<dyn ExecutionPlan>,
    child_idx: usize,
) -> Result<()> {
    if let Some(sort_onwards) = sort_onwards {
        let requires_single_partition = matches!(
            parent.required_input_distribution()[sort_onwards.idx],
            Distribution::SinglePartition
        );
        *child = remove_corresponding_sort_from_sub_plan(
            sort_onwards,
            requires_single_partition,
        )?;
    }
    *sort_onwards = None;
    // Deleting sort may invalidate distribution
    let requires_single_partition = matches!(
        parent.required_input_distribution()[child_idx],
        Distribution::SinglePartition
    );
    if requires_single_partition && child.output_partitioning().partition_count() > 1 {
        *child = Arc::new(CoalescePartitionsExec::new(child.clone())) as _;
    }
    Ok(())
}

/// Removes the sort from the plan in `sort_onwards`.
fn remove_corresponding_sort_from_sub_plan(
    sort_onwards: &mut ExecTree,
    requires_single_partition: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // A `SortExec` is always at the bottom of the tree.
    if is_sort(&sort_onwards.plan) {
        Ok(sort_onwards.plan.children()[0].clone())
    } else {
        let plan = &sort_onwards.plan;
        let mut children = plan.children();
        for item in &mut sort_onwards.children {
            let requires_single_partition = matches!(
                plan.required_input_distribution()[item.idx],
                Distribution::SinglePartition
            );
            children[item.idx] =
                remove_corresponding_sort_from_sub_plan(item, requires_single_partition)?;
        }
        if is_sort_preserving_merge(plan) {
            let child = &children[0];
            if requires_single_partition
                && child.output_partitioning().partition_count() > 1
            {
                Ok(Arc::new(CoalescePartitionsExec::new(child.clone())))
            } else {
                Ok(child.clone())
            }
        } else {
            plan.clone().with_new_children(children)
        }
    }
}

/// Converts an [ExecutionPlan] trait object to a [PhysicalSortExpr] slice when possible.
fn get_sort_exprs(sort_any: &Arc<dyn ExecutionPlan>) -> Result<&[PhysicalSortExpr]> {
    if let Some(sort_exec) = sort_any.as_any().downcast_ref::<SortExec>() {
        Ok(sort_exec.expr())
    } else if let Some(sort_preserving_merge_exec) =
        sort_any.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        Ok(sort_preserving_merge_exec.expr())
    } else {
        Err(DataFusionError::Plan(
            "Given ExecutionPlan is not a SortExec or a SortPreservingMergeExec"
                .to_string(),
        ))
    }
}

#[derive(Debug)]
/// This structure stores extra column information required to remove unnecessary sorts.
pub struct ColumnInfo {
    is_aligned: bool,
    reverse: bool,
    is_partition: bool,
}

/// Compares physical ordering and required ordering of all `PhysicalSortExpr`s and returns a tuple.
/// The first element indicates whether these `PhysicalSortExpr`s can be removed from the physical plan.
/// The second element is a flag indicating whether we should reverse the sort direction in order to
/// remove physical sort expressions from the plan.
pub fn can_skip_sort(
    partition_keys: &[Arc<dyn PhysicalExpr>],
    required: &[PhysicalSortExpr],
    input_schema: &SchemaRef,
    physical_ordering: &[PhysicalSortExpr],
) -> Result<(bool, bool)> {
    if required.len() > physical_ordering.len() {
        return Ok((false, false));
    }
    let mut col_infos = vec![];
    for (sort_expr, physical_expr) in zip(required, physical_ordering) {
        let column = sort_expr.expr.clone();
        let is_partition = partition_keys.iter().any(|e| e.eq(&column));
        let (is_aligned, reverse) =
            check_alignment(input_schema, physical_expr, sort_expr);
        col_infos.push(ColumnInfo {
            is_aligned,
            reverse,
            is_partition,
        });
    }
    let partition_by_sections = col_infos
        .iter()
        .filter(|elem| elem.is_partition)
        .collect::<Vec<_>>();
    let can_skip_partition_bys = if partition_by_sections.is_empty() {
        true
    } else {
        let first_reverse = partition_by_sections[0].reverse;
        let can_skip_partition_bys = partition_by_sections
            .iter()
            .all(|c| c.is_aligned && c.reverse == first_reverse);
        can_skip_partition_bys
    };
    let order_by_sections = col_infos
        .iter()
        .filter(|elem| !elem.is_partition)
        .collect::<Vec<_>>();
    let (can_skip_order_bys, should_reverse_order_bys) = if order_by_sections.is_empty() {
        (true, false)
    } else {
        let first_reverse = order_by_sections[0].reverse;
        let can_skip_order_bys = order_by_sections
            .iter()
            .all(|c| c.is_aligned && c.reverse == first_reverse);
        (can_skip_order_bys, first_reverse)
    };
    let can_skip = can_skip_order_bys && can_skip_partition_bys;
    Ok((can_skip, should_reverse_order_bys))
}

/// Compares `physical_ordering` and `required` ordering, returns a tuple
/// indicating (1) whether this column requires sorting, and (2) whether we
/// should reverse the window expression in order to avoid sorting.
fn check_alignment(
    input_schema: &SchemaRef,
    physical_ordering: &PhysicalSortExpr,
    required: &PhysicalSortExpr,
) -> (bool, bool) {
    if required.expr.eq(&physical_ordering.expr) {
        let nullable = required.expr.nullable(input_schema).unwrap();
        let physical_opts = physical_ordering.options;
        let required_opts = required.options;
        let is_reversed = if nullable {
            physical_opts == reverse_sort_options(required_opts)
        } else {
            // If the column is not nullable, NULLS FIRST/LAST is not important.
            physical_opts.descending != required_opts.descending
        };
        let can_skip = !nullable || is_reversed || (physical_opts == required_opts);
        (can_skip, is_reversed)
    } else {
        (false, false)
    }
}

fn pushdown_requirement_to_children(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: Option<&[PhysicalSortRequirements]>,
) -> Result<Option<Vec<Option<Vec<PhysicalSortRequirements>>>>> {
    let maintains_input_order = plan.maintains_input_order();
    if is_window(plan) {
        let required_input_ordering = plan.required_input_ordering();
        let request_child = required_input_ordering[0].as_deref();
        let child_plan = plan.children()[0].clone();
        match determine_children_requirement(parent_required, request_child, child_plan) {
            RequirementsCompatibility::Satisfy => {
                Ok(Some(vec![request_child.map(|r| r.to_vec())]))
            }
            RequirementsCompatibility::Compatible(adjusted) => Ok(Some(vec![adjusted])),
            RequirementsCompatibility::NonCompatible => Ok(None),
        }
    } else if plan.as_any().is::<UnionExec>() {
        // UnionExec does not have real sort requirements for its input. Here we change the adjusted_request_ordering to UnionExec's output ordering and
        // propagate the sort requirements down to correct the unnecessary descendant SortExec under the UnionExec
        Ok(Some(vec![
            parent_required.map(|elem| elem.to_vec());
            plan.children().len()
        ]))
    } else if let Some(smj) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        // If the current plan is SortMergeJoinExec
        let left_columns_len = smj.left.schema().fields().len();
        let parent_required_expr =
            create_sort_expr_from_requirement(parent_required.unwrap());
        let expr_source_side =
            expr_source_sides(&parent_required_expr, smj.join_type, left_columns_len);
        match expr_source_side {
            Some(JoinSide::Left) if maintains_input_order[0] => {
                try_pushdown_requirements_to_join(
                    plan,
                    parent_required,
                    parent_required_expr,
                    JoinSide::Left,
                )
            }
            Some(JoinSide::Right) if maintains_input_order[1] => {
                let new_right_required = match smj.join_type {
                    JoinType::Inner | JoinType::Right => {
                        shift_right_required(parent_required.unwrap(), left_columns_len)?
                    }
                    JoinType::RightSemi | JoinType::RightAnti => {
                        parent_required.unwrap().to_vec()
                    }
                    _ => Err(DataFusionError::Plan(
                        "Unexpected SortMergeJoin type here".to_string(),
                    ))?,
                };
                try_pushdown_requirements_to_join(
                    plan,
                    Some(new_right_required.deref()),
                    parent_required_expr,
                    JoinSide::Right,
                )
            }
            _ => {
                // Can not decide the expr side for SortMergeJoinExec, can not push down
                Ok(None)
            }
        }
    } else if maintains_input_order.is_empty()
        || !maintains_input_order.iter().any(|o| *o)
        || plan.as_any().is::<RepartitionExec>()
        || plan.as_any().is::<FilterExec>()
        // TODO: Add support for Projection push down
        || plan.as_any().is::<ProjectionExec>()
        || is_limit(plan)
    {
        // If the current plan is a leaf node or can not maintain any of the input ordering, can not pushed down requirements.
        // For RepartitionExec, we always choose to not push down the sort requirements even the RepartitionExec(input_partition=1) could maintain input ordering.
        // For RepartitionExec, we always choose to not push down the sort requirements even the RepartitionExec(input_partition=1) could maintain input ordering.
        // Pushing down is not beneficial
        Ok(None)
    } else {
        Ok(Some(vec![
            parent_required.map(|elem| elem.to_vec());
            plan.children().len()
        ]))
    }
    // TODO: Add support for Projection push down
}

/// Determine the children requirements
/// If the children requirements are more specific, do not push down the parent requirements
/// If the the parent requirements are more specific, push down the parent requirements
/// If they are not compatible, need to add Sort.
fn determine_children_requirement(
    parent_required: Option<&[PhysicalSortRequirements]>,
    request_child: Option<&[PhysicalSortRequirements]>,
    child_plan: Arc<dyn ExecutionPlan>,
) -> RequirementsCompatibility {
    if requirements_compatible(request_child, parent_required, || {
        child_plan.equivalence_properties()
    }) {
        // request child requirements are more specific, no need to push down the parent requirements
        RequirementsCompatibility::Satisfy
    } else if requirements_compatible(parent_required, request_child, || {
        child_plan.equivalence_properties()
    }) {
        // parent requirements are more specific, adjust the request child requirements and push down the new requirements
        let adjusted = parent_required.map(|r| r.to_vec());
        RequirementsCompatibility::Compatible(adjusted)
    } else {
        RequirementsCompatibility::NonCompatible
    }
}

fn try_pushdown_requirements_to_join(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: Option<&[PhysicalSortRequirements]>,
    sort_expr: Vec<PhysicalSortExpr>,
    push_side: JoinSide,
) -> Result<Option<Vec<Option<Vec<PhysicalSortRequirements>>>>> {
    let child_idx = match push_side {
        JoinSide::Left => 0,
        JoinSide::Right => 1,
    };
    let required_input_ordering = plan.required_input_ordering();
    let request_child = required_input_ordering[child_idx].as_deref();
    let child_plan = plan.children()[child_idx].clone();
    match determine_children_requirement(parent_required, request_child, child_plan) {
        RequirementsCompatibility::Satisfy => Ok(None),
        RequirementsCompatibility::Compatible(adjusted) => {
            let new_adjusted = match push_side {
                JoinSide::Left => {
                    vec![adjusted, required_input_ordering[1].clone()]
                }
                JoinSide::Right => {
                    vec![required_input_ordering[0].clone(), adjusted]
                }
            };
            Ok(Some(new_adjusted))
        }
        RequirementsCompatibility::NonCompatible => {
            // Can not push down, add new SortExec
            let mut new_plan = plan.clone();
            add_sort_above(&mut new_plan, sort_expr)?;
            Ok(None)
        }
    }
}

fn expr_source_sides(
    required_exprs: &[PhysicalSortExpr],
    join_type: JoinType,
    left_columns_len: usize,
) -> Option<JoinSide> {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let all_column_sides = required_exprs
                .iter()
                .filter_map(|r| {
                    if let Some(col) = r.expr.as_any().downcast_ref::<Column>() {
                        if col.index() < left_columns_len {
                            Some(JoinSide::Left)
                        } else {
                            Some(JoinSide::Right)
                        }
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            // If the exprs are all coming from one side, the requirements can be pushed down
            if all_column_sides.len() != required_exprs.len() {
                None
            } else if all_column_sides
                .iter()
                .all(|side| matches!(side, JoinSide::Left))
            {
                Some(JoinSide::Left)
            } else if all_column_sides
                .iter()
                .all(|side| matches!(side, JoinSide::Right))
            {
                Some(JoinSide::Right)
            } else {
                None
            }
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            if required_exprs
                .iter()
                .filter_map(|r| {
                    if r.expr.as_any().downcast_ref::<Column>().is_some() {
                        Some(JoinSide::Left)
                    } else {
                        None
                    }
                })
                .count()
                != required_exprs.len()
            {
                None
            } else {
                Some(JoinSide::Left)
            }
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            if required_exprs
                .iter()
                .filter_map(|r| {
                    if r.expr.as_any().downcast_ref::<Column>().is_some() {
                        Some(JoinSide::Right)
                    } else {
                        None
                    }
                })
                .count()
                != required_exprs.len()
            {
                None
            } else {
                Some(JoinSide::Right)
            }
        }
    }
}

fn shift_right_required(
    parent_required: &[PhysicalSortRequirements],
    left_columns_len: usize,
) -> Result<Vec<PhysicalSortRequirements>> {
    let new_right_required: Vec<PhysicalSortRequirements> = parent_required
        .iter()
        .filter_map(|r| {
            if let Some(col) = r.expr.as_any().downcast_ref::<Column>() {
                if col.index() >= left_columns_len {
                    Some(PhysicalSortRequirements {
                        expr: Arc::new(Column::new(
                            col.name(),
                            col.index() - left_columns_len,
                        )) as Arc<dyn PhysicalExpr>,
                        sort_options: r.sort_options,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if new_right_required.len() != parent_required.len() {
        Err(DataFusionError::Plan(
            "Expect to shift all the parent required column indexes for SortMergeJoin"
                .to_string(),
        ))
    } else {
        Ok(new_right_required)
    }
}

/// Define the Requirements Compatibility
#[derive(Debug)]
pub enum RequirementsCompatibility {
    /// Requirements satisfy
    Satisfy,
    /// Requirements compatible
    Compatible(Option<Vec<PhysicalSortRequirements>>),
    /// Requirements not compatible
    NonCompatible,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_plan::aggregates::PhysicalGroupBy;
    use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::joins::utils::JoinOn;
    use crate::physical_plan::joins::SortMergeJoinExec;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::windows::create_window_expr;
    use crate::physical_plan::{displayable, Partitioning};
    use crate::prelude::SessionContext;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{Result, Statistics};
    use datafusion_expr::JoinType;
    use datafusion_expr::{AggregateFunction, WindowFrame, WindowFunction};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::expressions::{col, NotExpr};
    use datafusion_physical_expr::PhysicalSortExpr;
    use std::sync::Arc;

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

    // Util function to get string representation of a physical plan
    fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let formatted = displayable(plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        actual.iter().map(|elem| elem.to_string()).collect()
    }

    #[tokio::test]
    async fn test_is_column_aligned_nullable() -> Result<()> {
        let schema = create_test_schema()?;
        let params = vec![
            ((true, true), (false, false), (true, true)),
            ((true, true), (false, true), (false, false)),
            ((true, true), (true, false), (false, false)),
            ((true, false), (false, true), (true, true)),
            ((true, false), (false, false), (false, false)),
            ((true, false), (true, true), (false, false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            (is_aligned_expected, reverse_expected),
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let (is_aligned, reverse) =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(is_aligned, is_aligned_expected);
            assert_eq!(reverse, reverse_expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_column_aligned_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), (true, true)),
            ((true, true), (false, true), (true, true)),
            ((true, true), (true, false), (true, false)),
            ((true, false), (false, true), (true, true)),
            ((true, false), (false, false), (true, true)),
            ((true, false), (true, true), (true, false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            (is_aligned_expected, reverse_expected),
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let (is_aligned, reverse) =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(is_aligned, is_aligned_expected);
            assert_eq!(reverse, reverse_expected);
        }

        Ok(())
    }

    /// Runs the sort enforcement optimizer and asserts the plan
    /// against the original and expected plans
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
    /// `$PLAN`: the plan to optimized
    ///
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr) => {
            let session_ctx = SessionContext::new();
            let state = session_ctx.state();

            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent().to_string();
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

        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  SortExec: expr=[non_nullable_col@1 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        let window_agg = bounded_window_exec("non_nullable_col", sort_exprs, sort);

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

        // let filter_exec = sort_exec;
        let physical_plan = bounded_window_exec("non_nullable_col", sort_exprs, filter);

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], global=true",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "        SortExec: expr=[non_nullable_col@1 DESC], global=true",
            "          MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: expr=[non_nullable_col@1 DESC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_required_sort() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];

        let physical_plan = sort_preserving_merge_exec(sort_exprs, source);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        let expected_input = vec![
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "          SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "            SortExec: expr=[non_nullable_col@1 ASC], global=true",
            "              MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let sort2 = Arc::new(SortExec::new_with_partitioning(
            sort_exprs.clone(),
            repartition_exec,
            true,
            None,
        )) as _;
        let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

        let physical_plan = aggregate_exec(spm2);

        // When removing a `SortPreservingMergeExec`, make sure that partitioning
        // requirements are not violated. In some cases, we may need to replace
        // it with a `CoalescePartitionsExec` instead of directly removing it.
        let expected_input = vec![
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=false",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "          SortExec: expr=[non_nullable_col@1 ASC], global=true",
            "            MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort4() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), spm);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], global=true",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: expr=[nullable_col@0 ASC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort5() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
        let input2 = sort_exec(
            vec![
                sort_expr("nullable_col", &schema),
                sort_expr("non_nullable_col", &schema),
            ],
            input,
        );
        let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input2);

        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "    SortExec: expr=[non_nullable_col@1 ASC], global=true",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        // Keep the middle SortExec
        let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort6() -> Result<()> {
        let schema = create_test_schema()?;
        let source1 = repartition_exec(memory_exec(&schema));

        let source2 = repartition_exec(memory_exec(&schema));
        let union = union_exec(vec![source1, source2]);

        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        // let sort = sort_exec(sort_exprs.clone(), union);
        let sort = Arc::new(SortExec::new_with_partitioning(
            sort_exprs.clone(),
            union,
            true,
            None,
        )) as _;
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
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      SortExec: expr=[non_nullable_col@1 ASC], global=false",
            "        UnionExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "            MemoryExec: partitions=0, partition_sizes=[]",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "            MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=false",
            "    FilterExec: NOT non_nullable_col@1",
            "      UnionExec",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "          MemoryExec: partitions=0, partition_sizes=[]",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "          MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input);

        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_spm2() -> Result<()> {
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

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "    UnionExec",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "      GlobalLimitExec: skip=0, fetch=100",
            "        LocalLimitExec: fetch=100",
            "          SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "            ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        // We should keep the bottom `SortExec`.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=false",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "      UnionExec",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "        GlobalLimitExec: skip=0, fetch=100",
            "          LocalLimitExec: fetch=100",
            "            SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "              ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        let expected_input = vec![
            "SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], global=true",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[non_nullable_col@1 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        // First ParquetExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy required ordering
        // of SortPreservingMergeExec.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // Should adjust the requirement in the third input of the union so
        // that it is not unnecessarily fine.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC], global=false",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        // Union preserves the inputs ordering and we should not change any of the SortExecs under UnionExec
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_input, physical_plan);
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
        let expected_input = vec![
            "UnionExec",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "  SortExec: expr=[nullable_col@0 DESC NULLS LAST,non_nullable_col@1 DESC NULLS LAST], global=true",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // Since `UnionExec` doesn't preserve ordering in the plan above.
        // We shouldn't keep SortExecs in the plan.
        let expected_optimized = vec![
            "UnionExec",
            "  ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "  ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    GlobalLimitExec: skip=0, fetch=100",
            "      LocalLimitExec: fetch=100",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], global=true",
            "          ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    GlobalLimitExec: skip=0, fetch=100",
            "      LocalLimitExec: fetch=100",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], global=true",
            "          ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            Column::new_with_schema("nullable_col", &left.schema()).unwrap(),
            Column::new_with_schema("col_a", &right.schema()).unwrap(),
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

            let join_plan =
                format!("SortMergeJoin: join_type={join_type}, on=[(Column {{ name: \"nullable_col\", index: 0 }}, Column {{ name: \"col_a\", index: 0 }})]");
            let join_plan2 =
                format!("  SortMergeJoin: join_type={join_type}, on=[(Column {{ name: \"nullable_col\", index: 0 }}, Column {{ name: \"col_a\", index: 0 }})]");
            let expected_input = vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
                join_plan2.as_str(),
                "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
            ];
            let expected_optimized = match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    // can push down the sort requirements and save 1 SortExec
                    vec![
                        join_plan.as_str(),
                        "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
                        "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "  SortExec: expr=[col_a@0 ASC], global=true",
                        "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
                _ => {
                    // can not push down the sort requirements
                    vec![
                        "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
                        join_plan2.as_str(),
                        "    SortExec: expr=[nullable_col@0 ASC], global=true",
                        "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "    SortExec: expr=[col_a@0 ASC], global=true",
                        "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
            };
            assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            Column::new_with_schema("nullable_col", &left.schema()).unwrap(),
            Column::new_with_schema("col_a", &right.schema()).unwrap(),
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

            let join_plan =
                format!("SortMergeJoin: join_type={join_type}, on=[(Column {{ name: \"nullable_col\", index: 0 }}, Column {{ name: \"col_a\", index: 0 }})]");
            let spm_plan = match join_type {
                JoinType::RightAnti => {
                    "SortPreservingMergeExec: [col_a@0 ASC,col_b@1 ASC]"
                }
                _ => "SortPreservingMergeExec: [col_a@2 ASC,col_b@3 ASC]",
            };
            let join_plan2 =
                format!("  SortMergeJoin: join_type={join_type}, on=[(Column {{ name: \"nullable_col\", index: 0 }}, Column {{ name: \"col_a\", index: 0 }})]");
            let expected_input = vec![
                spm_plan,
                join_plan2.as_str(),
                "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
            ];
            let expected_optimized = match join_type {
                JoinType::Inner | JoinType::Right | JoinType::RightAnti => {
                    // can push down the sort requirements and save 1 SortExec
                    vec![
                        join_plan.as_str(),
                        "  SortExec: expr=[nullable_col@0 ASC], global=true",
                        "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "  SortExec: expr=[col_a@0 ASC,col_b@1 ASC], global=true",
                        "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
                _ => {
                    // can not push down the sort requirements for Left and Full join.
                    vec![
                        "SortExec: expr=[col_a@2 ASC,col_b@3 ASC], global=true",
                        join_plan2.as_str(),
                        "    SortExec: expr=[nullable_col@0 ASC], global=true",
                        "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "    SortExec: expr=[col_a@0 ASC], global=true",
                        "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
            };
            assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            Column::new_with_schema("nullable_col", &left.schema()).unwrap(),
            Column::new_with_schema("col_a", &right.schema()).unwrap(),
        )];

        let join = sort_merge_join_exec(left, right, &join_on, &JoinType::Inner);

        // order by (col_b, col_a)
        let sort_exprs1 = vec![
            sort_expr("col_b", &join.schema()),
            sort_expr("col_a", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs1, join.clone());

        let expected_input = vec![
            "SortPreservingMergeExec: [col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(Column { name: \"nullable_col\", index: 0 }, Column { name: \"col_a\", index: 0 })]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
        ];

        // can not push down the sort requirements, need to add SortExec
        let expected_optimized = vec![
            "SortExec: expr=[col_b@3 ASC,col_a@2 ASC], global=true",
            "  SortMergeJoin: join_type=Inner, on=[(Column { name: \"nullable_col\", index: 0 }, Column { name: \"col_a\", index: 0 })]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[col_a@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);

        // order by (nullable_col, col_b, col_a)
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &join.schema()),
            sort_expr("col_b", &join.schema()),
            sort_expr("col_a", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs2, join);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(Column { name: \"nullable_col\", index: 0 }, Column { name: \"col_a\", index: 0 })]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
        ];

        // can not push down the sort requirements, need to add SortExec
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC,col_b@3 ASC,col_a@2 ASC], global=true",
            "  SortMergeJoin: join_type=Inner, on=[(Column { name: \"nullable_col\", index: 0 }, Column { name: \"col_a\", index: 0 })]",
            "    SortExec: expr=[nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[col_a@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[col_a, col_b]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);

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

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: expr=[nullable_col@0 ASC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let physical_plan = bounded_window_exec("nullable_col", sort_exprs3, union);

        // The `WindowAggExec` gets its sorting from multiple children jointly.
        // During the removal of `SortExec`s, it should be able to remove the
        // corresponding SortExecs together. Also, the inputs of these `SortExec`s
        // are not necessarily the same to be able to remove them.
        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 DESC NULLS LAST], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 DESC NULLS LAST], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  UnionExec",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        // reverse sorting of sort_exprs2
        let reversed_sort_exprs2 = vec![sort_expr_options(
            "nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )];
        let source1 = parquet_exec_sorted(&schema, sort_exprs1);
        let source2 = parquet_exec_sorted(&schema, sort_exprs2.clone());
        let sort1 = sort_exec(reversed_sort_exprs2.clone(), source1);
        let sort2 = sort_exec(reversed_sort_exprs2, source2);

        let union = union_exec(vec![sort1, sort2]);
        let coalesce = Arc::new(CoalescePartitionsExec::new(union)) as _;
        let physical_plan = bounded_window_exec("nullable_col", sort_exprs2, coalesce);

        // The `WindowAggExec` can get its required sorting from the leaf nodes directly.
        // The unnecessary SortExecs should be removed
        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  CoalescePartitionsExec",
            "    UnionExec",
            "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], global=true",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], global=true",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    UnionExec",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  FilterExec: NOT non_nullable_col@1",
            "    CoalescePartitionsExec",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], global=false",
            "    FilterExec: NOT non_nullable_col@1",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let sort = Arc::new(SortExec::new_with_partitioning(
            sort_exprs.clone(),
            repartition,
            true,
            None,
        )) as _;
        let spm = sort_preserving_merge_exec(sort_exprs.clone(), sort);
        let sort = sort_exec(sort_exprs, spm);

        let physical_plan = sort.clone();
        // Sort Parallelize rule should end Coalesce + Sort linkage when Sort is Global Sort
        // Also input plan is not valid as it is. We need to add SortExec before SortPreservingMergeExec.
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], global=true",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC], global=false",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CoalescePartitionsExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "            MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], global=false",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        let orig_plan = Arc::new(SortExec::new_with_partitioning(
            sort_exprs,
            repartition,
            false,
            None,
        )) as Arc<dyn ExecutionPlan>;

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

    /// make PhysicalSortExpr with default options
    fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        sort_expr_options(name, schema, SortOptions::default())
    }

    /// PhysicalSortExpr with specified options
    fn sort_expr_options(
        name: &str,
        schema: &Schema,
        options: SortOptions,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options,
        }
    }

    fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(MemoryExec::try_new(&[], schema.clone(), None).unwrap())
    }

    fn sort_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortExec::try_new(sort_exprs, input, None).unwrap())
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    fn filter_exec(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
    }

    fn bounded_window_exec(
        col_name: &str,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs: Vec<_> = sort_exprs.into_iter().collect();
        let schema = input.schema();

        Arc::new(
            BoundedWindowAggExec::try_new(
                vec![create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Count),
                    "count".to_owned(),
                    &[col(col_name, &schema).unwrap()],
                    &[],
                    &sort_exprs,
                    Arc::new(WindowFrame::new(true)),
                    schema.as_ref(),
                )
                .unwrap()],
                input.clone(),
                input.schema(),
                vec![],
                Some(sort_exprs),
            )
            .unwrap(),
        )
    }

    /// Create a non sorted parquet exec
    fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    // Created a sorted parquet exec
    fn parquet_exec_sorted(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Arc<ParquetExec> {
        let sort_exprs = sort_exprs.into_iter().collect();

        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(sort_exprs),
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        global_limit_exec(local_limit_exec(input))
    }

    fn local_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(LocalLimitExec::new(input, 100))
    }

    fn global_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(input, 0, Some(100)))
    }

    fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap(),
        )
    }

    fn aggregate_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                vec![],
                input,
                schema,
            )
            .unwrap(),
        )
    }

    fn sort_merge_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: &JoinOn,
        join_type: &JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                join_on.clone(),
                *join_type,
                vec![SortOptions::default(); join_on.len()],
                false,
            )
            .unwrap(),
        )
    }
}
