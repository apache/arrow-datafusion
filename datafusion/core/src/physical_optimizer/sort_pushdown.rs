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

use std::sync::Arc;

use super::utils::add_sort_above;
use crate::physical_optimizer::utils::{
    is_limit, is_sort_preserving_merge, is_union, is_window,
};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::calculate_join_output_ordering;
use crate::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::tree_node::PlanContext;
use crate::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use datafusion_common::tree_node::Transformed;
use datafusion_common::{plan_err, JoinSide, Result};
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    LexRequirementRef, PhysicalSortExpr, PhysicalSortRequirement,
};

/// This is a "data class" we use within the [`EnforceSorting`] rule to push
/// down [`SortExec`] in the plan. In some cases, we can reduce the total
/// computational cost by pushing down `SortExec`s through some executors. The
/// object carries the parent required ordering as its data.
///
/// [`EnforceSorting`]: crate::physical_optimizer::enforce_sorting::EnforceSorting
pub type SortPushDown = PlanContext<Option<Vec<PhysicalSortRequirement>>>;

/// Assigns the ordering requirement of the root node to the its children.
pub fn assign_initial_requirements(node: &mut SortPushDown) {
    let reqs = node.plan.required_input_ordering();
    for (child, requirement) in node.children.iter_mut().zip(reqs) {
        child.data = requirement;
    }
}

pub(crate) fn pushdown_sorts(
    mut requirements: SortPushDown,
) -> Result<Transformed<SortPushDown>> {
    let plan = &requirements.plan;
    let parent_reqs = requirements.data.as_deref().unwrap_or(&[]);
    let satisfy_parent = plan
        .equivalence_properties()
        .ordering_satisfy_requirement(parent_reqs);

    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let required_ordering = plan
            .output_ordering()
            .map(PhysicalSortRequirement::from_sort_exprs)
            .unwrap_or_default();

        if !satisfy_parent {
            // Make sure this `SortExec` satisfies parent requirements:
            let fetch = sort_exec.fetch();
            let sort_reqs = requirements.data.unwrap_or_default();
            requirements = requirements.children.swap_remove(0);
            requirements = add_sort_above(requirements, sort_reqs, fetch);
        };

        // We can safely get the 0th index as we are dealing with a `SortExec`.
        let mut child = requirements.children.swap_remove(0);
        if let Some(adjusted) =
            pushdown_requirement_to_children(&child.plan, &required_ordering)?
        {
            for (grand_child, order) in child.children.iter_mut().zip(adjusted) {
                grand_child.data = order;
            }
            // Can push down requirements
            child.data = None;
            return Ok(Transformed::yes(child));
        } else {
            // Can not push down requirements
            requirements.children = vec![child];
            assign_initial_requirements(&mut requirements);
        }
    } else if satisfy_parent {
        // For non-sort operators, immediately return if parent requirements are met:
        let reqs = plan.required_input_ordering();
        for (child, order) in requirements.children.iter_mut().zip(reqs) {
            child.data = order;
        }
    } else if let Some(adjusted) = pushdown_requirement_to_children(plan, parent_reqs)? {
        // Can not satisfy the parent requirements, check whether we can push
        // requirements down:
        for (child, order) in requirements.children.iter_mut().zip(adjusted) {
            child.data = order;
        }
        requirements.data = None;
    } else {
        // Can not push down requirements, add new `SortExec`:
        let sort_reqs = requirements.data.clone().unwrap_or_default();
        requirements = add_sort_above(requirements, sort_reqs, None);
        assign_initial_requirements(&mut requirements);
    }
    Ok(Transformed::yes(requirements))
}

fn pushdown_requirement_to_children(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: LexRequirementRef,
) -> Result<Option<Vec<Option<Vec<PhysicalSortRequirement>>>>> {
    let maintains_input_order = plan.maintains_input_order();
    if is_window(plan) {
        let required_input_ordering = plan.required_input_ordering();
        let request_child = required_input_ordering[0].as_deref().unwrap_or(&[]);
        let child_plan = plan.children().swap_remove(0).clone();
        match determine_children_requirement(parent_required, request_child, child_plan) {
            RequirementsCompatibility::Satisfy => {
                let req = (!request_child.is_empty()).then(|| request_child.to_vec());
                Ok(Some(vec![req]))
            }
            RequirementsCompatibility::Compatible(adjusted) => Ok(Some(vec![adjusted])),
            RequirementsCompatibility::NonCompatible => Ok(None),
        }
    } else if is_union(plan) {
        // UnionExec does not have real sort requirements for its input. Here we change the adjusted_request_ordering to UnionExec's output ordering and
        // propagate the sort requirements down to correct the unnecessary descendant SortExec under the UnionExec
        let req = (!parent_required.is_empty()).then(|| parent_required.to_vec());
        Ok(Some(vec![req; plan.children().len()]))
    } else if let Some(smj) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        // If the current plan is SortMergeJoinExec
        let left_columns_len = smj.left().schema().fields().len();
        let parent_required_expr =
            PhysicalSortRequirement::to_sort_exprs(parent_required.iter().cloned());
        match expr_source_side(&parent_required_expr, smj.join_type(), left_columns_len) {
            Some(JoinSide::Left) => try_pushdown_requirements_to_join(
                smj,
                parent_required,
                parent_required_expr,
                JoinSide::Left,
            ),
            Some(JoinSide::Right) => {
                let right_offset =
                    smj.schema().fields.len() - smj.right().schema().fields.len();
                let new_right_required =
                    shift_right_required(parent_required, right_offset)?;
                let new_right_required_expr =
                    PhysicalSortRequirement::to_sort_exprs(new_right_required);
                try_pushdown_requirements_to_join(
                    smj,
                    parent_required,
                    new_right_required_expr,
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
        || plan.as_any().is::<HashJoinExec>()
    {
        // If the current plan is a leaf node or can not maintain any of the input ordering, can not pushed down requirements.
        // For RepartitionExec, we always choose to not push down the sort requirements even the RepartitionExec(input_partition=1) could maintain input ordering.
        // Pushing down is not beneficial
        Ok(None)
    } else if is_sort_preserving_merge(plan) {
        let new_ordering =
            PhysicalSortRequirement::to_sort_exprs(parent_required.to_vec());
        let mut spm_eqs = plan.equivalence_properties().clone();
        // Sort preserving merge will have new ordering, one requirement above is pushed down to its below.
        spm_eqs = spm_eqs.with_reorder(new_ordering);
        // Do not push-down through SortPreservingMergeExec when
        // ordering requirement invalidates requirement of sort preserving merge exec.
        if !spm_eqs.ordering_satisfy(plan.output_ordering().unwrap_or(&[])) {
            Ok(None)
        } else {
            // Can push-down through SortPreservingMergeExec, because parent requirement is finer
            // than SortPreservingMergeExec output ordering.
            let req = (!parent_required.is_empty()).then(|| parent_required.to_vec());
            Ok(Some(vec![req]))
        }
    } else {
        Ok(Some(
            maintains_input_order
                .into_iter()
                .map(|flag| {
                    (flag && !parent_required.is_empty())
                        .then(|| parent_required.to_vec())
                })
                .collect(),
        ))
    }
    // TODO: Add support for Projection push down
}

/// Determine children requirements:
/// - If children requirements are more specific, do not push down parent
///   requirements.
/// - If parent requirements are more specific, push down parent requirements.
/// - If they are not compatible, need to add a sort.
fn determine_children_requirement(
    parent_required: LexRequirementRef,
    request_child: LexRequirementRef,
    child_plan: Arc<dyn ExecutionPlan>,
) -> RequirementsCompatibility {
    if child_plan
        .equivalence_properties()
        .requirements_compatible(request_child, parent_required)
    {
        // Child requirements are more specific, no need to push down.
        RequirementsCompatibility::Satisfy
    } else if child_plan
        .equivalence_properties()
        .requirements_compatible(parent_required, request_child)
    {
        // Parent requirements are more specific, adjust child's requirements
        // and push down the new requirements:
        let adjusted = (!parent_required.is_empty()).then(|| parent_required.to_vec());
        RequirementsCompatibility::Compatible(adjusted)
    } else {
        RequirementsCompatibility::NonCompatible
    }
}
fn try_pushdown_requirements_to_join(
    smj: &SortMergeJoinExec,
    parent_required: LexRequirementRef,
    sort_expr: Vec<PhysicalSortExpr>,
    push_side: JoinSide,
) -> Result<Option<Vec<Option<Vec<PhysicalSortRequirement>>>>> {
    let left_eq_properties = smj.left().equivalence_properties();
    let right_eq_properties = smj.right().equivalence_properties();
    let mut smj_required_orderings = smj.required_input_ordering();
    let right_requirement = smj_required_orderings.swap_remove(1);
    let left_requirement = smj_required_orderings.swap_remove(0);
    let left_ordering = smj.left().output_ordering().unwrap_or(&[]);
    let right_ordering = smj.right().output_ordering().unwrap_or(&[]);
    let (new_left_ordering, new_right_ordering) = match push_side {
        JoinSide::Left => {
            let left_eq_properties =
                left_eq_properties.clone().with_reorder(sort_expr.clone());
            if left_eq_properties
                .ordering_satisfy_requirement(&left_requirement.unwrap_or_default())
            {
                // After re-ordering requirement is still satisfied
                (sort_expr.as_slice(), right_ordering)
            } else {
                return Ok(None);
            }
        }
        JoinSide::Right => {
            let right_eq_properties =
                right_eq_properties.clone().with_reorder(sort_expr.clone());
            if right_eq_properties
                .ordering_satisfy_requirement(&right_requirement.unwrap_or_default())
            {
                // After re-ordering requirement is still satisfied
                (left_ordering, sort_expr.as_slice())
            } else {
                return Ok(None);
            }
        }
    };
    let join_type = smj.join_type();
    let probe_side = SortMergeJoinExec::probe_side(&join_type);
    let new_output_ordering = calculate_join_output_ordering(
        new_left_ordering,
        new_right_ordering,
        join_type,
        smj.on(),
        smj.left().schema().fields.len(),
        &smj.maintains_input_order(),
        Some(probe_side),
    );
    let mut smj_eqs = smj.properties().equivalence_properties().clone();
    // smj will have this ordering when its input changes.
    smj_eqs = smj_eqs.with_reorder(new_output_ordering.unwrap_or_default());
    let should_pushdown = smj_eqs.ordering_satisfy_requirement(parent_required);
    Ok(should_pushdown.then(|| {
        let mut required_input_ordering = smj.required_input_ordering();
        let new_req = Some(PhysicalSortRequirement::from_sort_exprs(&sort_expr));
        match push_side {
            JoinSide::Left => {
                required_input_ordering[0] = new_req;
            }
            JoinSide::Right => {
                required_input_ordering[1] = new_req;
            }
        }
        required_input_ordering
    }))
}

fn expr_source_side(
    required_exprs: &[PhysicalSortExpr],
    join_type: JoinType,
    left_columns_len: usize,
) -> Option<JoinSide> {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let all_column_sides = required_exprs
                .iter()
                .filter_map(|r| {
                    r.expr.as_any().downcast_ref::<Column>().map(|col| {
                        if col.index() < left_columns_len {
                            JoinSide::Left
                        } else {
                            JoinSide::Right
                        }
                    })
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
        JoinType::LeftSemi | JoinType::LeftAnti => required_exprs
            .iter()
            .all(|e| e.expr.as_any().downcast_ref::<Column>().is_some())
            .then_some(JoinSide::Left),
        JoinType::RightSemi | JoinType::RightAnti => required_exprs
            .iter()
            .all(|e| e.expr.as_any().downcast_ref::<Column>().is_some())
            .then_some(JoinSide::Right),
    }
}

fn shift_right_required(
    parent_required: LexRequirementRef,
    left_columns_len: usize,
) -> Result<Vec<PhysicalSortRequirement>> {
    let new_right_required = parent_required
        .iter()
        .filter_map(|r| {
            let col = r.expr.as_any().downcast_ref::<Column>()?;
            col.index().checked_sub(left_columns_len).map(|offset| {
                r.clone()
                    .with_expr(Arc::new(Column::new(col.name(), offset)))
            })
        })
        .collect::<Vec<_>>();
    if new_right_required.len() == parent_required.len() {
        Ok(new_right_required)
    } else {
        plan_err!(
            "Expect to shift all the parent required column indexes for SortMergeJoin"
        )
    }
}

/// Define the Requirements Compatibility
#[derive(Debug)]
enum RequirementsCompatibility {
    /// Requirements satisfy
    Satisfy,
    /// Requirements compatible
    Compatible(Option<Vec<PhysicalSortRequirement>>),
    /// Requirements not compatible
    NonCompatible,
}
