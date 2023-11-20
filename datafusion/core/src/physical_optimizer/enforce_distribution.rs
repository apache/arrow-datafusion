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

//! EnforceDistribution optimizer rule inspects the physical plan with respect
//! to distribution requirements and adds [`RepartitionExec`]s to satisfy them
//! when necessary. If increasing parallelism is beneficial (and also desirable
//! according to the configuration), this rule increases partition counts in
//! the physical plan.

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::utils::{
    add_sort_above, get_children_exectrees, get_plan_string, is_coalesce_partitions,
    is_repartition, is_sort_preserving_merge, ExecTree,
};
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, PartitionMode, SortMergeJoinExec,
};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::{can_interleave, InterleaveExec, UnionExec};
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::{
    with_new_children_if_necessary, Distribution, ExecutionPlan, Partitioning,
};

use arrow::compute::SortOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_expr::logical_plan::JoinType;
use datafusion_physical_expr::expressions::{Column, NoOp};
use datafusion_physical_expr::utils::map_columns_before_projection;
use datafusion_physical_expr::{
    physical_exprs_equal, EquivalenceProperties, PhysicalExpr,
};
use datafusion_physical_plan::unbounded_output;
use datafusion_physical_plan::windows::{get_best_fitting_window, BoundedWindowAggExec};

use itertools::izip;

use super::utils::is_recursive_query;

/// The `EnforceDistribution` rule ensures that distribution requirements are
/// met. In doing so, this rule will increase the parallelism in the plan by
/// introducing repartitioning operators to the physical plan.
///
/// For example, given an input such as:
///
///
/// ```text
/// ┌─────────────────────────────────┐
/// │                                 │
/// │          ExecutionPlan          │
/// │                                 │
/// └─────────────────────────────────┘
///             ▲         ▲
///             │         │
///       ┌─────┘         └─────┐
///       │                     │
///       │                     │
///       │                     │
/// ┌───────────┐         ┌───────────┐
/// │           │         │           │
/// │ batch A1  │         │ batch B1  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A2  │         │ batch B2  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A3  │         │ batch B3  │
/// │           │         │           │
/// └───────────┘         └───────────┘
///
///      Input                 Input
///        A                     B
/// ```
///
/// This rule will attempt to add a `RepartitionExec` to increase parallelism
/// (to 3, in this case) and create the following arrangement:
///
/// ```text
///     ┌─────────────────────────────────┐
///     │                                 │
///     │          ExecutionPlan          │
///     │                                 │
///     └─────────────────────────────────┘
///               ▲      ▲       ▲            Input now has 3
///               │      │       │             partitions
///       ┌───────┘      │       └───────┐
///       │              │               │
///       │              │               │
/// ┌───────────┐  ┌───────────┐   ┌───────────┐
/// │           │  │           │   │           │
/// │ batch A1  │  │ batch A3  │   │ batch B3  │
/// │           │  │           │   │           │
/// ├───────────┤  ├───────────┤   ├───────────┤
/// │           │  │           │   │           │
/// │ batch B2  │  │ batch B1  │   │ batch A2  │
/// │           │  │           │   │           │
/// └───────────┘  └───────────┘   └───────────┘
///       ▲              ▲               ▲
///       │              │               │
///       └─────────┐    │    ┌──────────┘
///                 │    │    │
///                 │    │    │
///     ┌─────────────────────────────────┐   batches are
///     │       RepartitionExec(3)        │   repartitioned
///     │           RoundRobin            │
///     │                                 │
///     └─────────────────────────────────┘
///                 ▲         ▲
///                 │         │
///           ┌─────┘         └─────┐
///           │                     │
///           │                     │
///           │                     │
///     ┌───────────┐         ┌───────────┐
///     │           │         │           │
///     │ batch A1  │         │ batch B1  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A2  │         │ batch B2  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A3  │         │ batch B3  │
///     │           │         │           │
///     └───────────┘         └───────────┘
///
///
///      Input                 Input
///        A                     B
/// ```
///
/// The `EnforceDistribution` rule
/// - is idempotent; i.e. it can be applied multiple times, each time producing
///   the same result.
/// - always produces a valid plan in terms of distribution requirements. Its
///   input plan can be valid or invalid with respect to distribution requirements,
///   but the output plan will always be valid.
/// - produces a valid plan in terms of ordering requirements, *if* its input is
///   a valid plan in terms of ordering requirements. If the input plan is invalid,
///   this rule does not attempt to fix it as doing so is the responsibility of the
///   `EnforceSorting` rule.
///
/// Note that distribution requirements are met in the strictest way. This may
/// result in more than strictly necessary [`RepartitionExec`]s in the plan, but
/// meeting the requirements in the strictest way may help avoid possible data
/// skew in joins.
///
/// For example for a hash join with keys (a, b, c), the required Distribution(a, b, c)
/// can be satisfied by several alternative partitioning ways: (a, b, c), (a, b),
/// (a, c), (b, c), (a), (b), (c) and ( ).
///
/// This rule only chooses the exact match and satisfies the Distribution(a, b, c)
/// by a HashPartition(a, b, c).
#[derive(Default)]
pub struct EnforceDistribution {}

impl EnforceDistribution {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnforceDistribution {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let top_down_join_key_reordering = config.optimizer.top_down_join_key_reordering;

        let adjusted = if top_down_join_key_reordering {
            // Run a top-down process to adjust input key ordering recursively
            let plan_requirements = PlanWithKeyRequirements::new(plan);
            let adjusted =
                plan_requirements.transform_down(&adjust_input_keys_ordering)?;
            adjusted.plan
        } else {
            // Run a bottom-up process
            plan.transform_up(&|plan| {
                Ok(Transformed::Yes(reorder_join_keys_to_inputs(plan)?))
            })?
        };

        let distribution_context = DistributionContext::new(adjusted);
        // Distribution enforcement needs to be applied bottom-up.
        let distribution_context =
            distribution_context.transform_up(&|distribution_context| {
                ensure_distribution(distribution_context, config)
            })?;
        Ok(distribution_context.plan)
    }

    fn name(&self) -> &str {
        "EnforceDistribution"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// When the physical planner creates the Joins, the ordering of join keys is from the original query.
/// That might not match with the output partitioning of the join node's children
/// A Top-Down process will use this method to adjust children's output partitioning based on the parent key reordering requirements:
///
/// Example:
///     TopJoin on (a, b, c)
///         bottom left join on(b, a, c)
///         bottom right join on(c, b, a)
///
///  Will be adjusted to:
///     TopJoin on (a, b, c)
///         bottom left join on(a, b, c)
///         bottom right join on(a, b, c)
///
/// Example:
///     TopJoin on (a, b, c)
///         Agg1 group by (b, a, c)
///         Agg2 group by (c, b, a)
///
/// Will be adjusted to:
///     TopJoin on (a, b, c)
///          Projection(b, a, c)
///             Agg1 group by (a, b, c)
///          Projection(c, b, a)
///             Agg2 group by (a, b, c)
///
/// Following is the explanation of the reordering process:
///
/// 1) If the current plan is Partitioned HashJoin, SortMergeJoin, check whether the requirements can be satisfied by adjusting join keys ordering:
///    Requirements can not be satisfied, clear the current requirements, generate new requirements(to pushdown) based on the current join keys, return the unchanged plan.
///    Requirements is already satisfied, clear the current requirements, generate new requirements(to pushdown) based on the current join keys, return the unchanged plan.
///    Requirements can be satisfied by adjusting keys ordering, clear the current requiements, generate new requirements(to pushdown) based on the adjusted join keys, return the changed plan.
///
/// 2) If the current plan is Aggregation, check whether the requirements can be satisfied by adjusting group by keys ordering:
///    Requirements can not be satisfied, clear all the requirements, return the unchanged plan.
///    Requirements is already satisfied, clear all the requirements, return the unchanged plan.
///    Requirements can be satisfied by adjusting keys ordering, clear all the requirements, return the changed plan.
///
/// 3) If the current plan is RepartitionExec, CoalescePartitionsExec or WindowAggExec, clear all the requirements, return the unchanged plan
/// 4) If the current plan is Projection, transform the requirements to the columns before the Projection and push down requirements
/// 5) For other types of operators, by default, pushdown the parent requirements to children.
///
fn adjust_input_keys_ordering(
    requirements: PlanWithKeyRequirements,
) -> Result<Transformed<PlanWithKeyRequirements>> {
    let parent_required = requirements.required_key_ordering.clone();
    let plan_any = requirements.plan.as_any();
    let transformed = if let Some(HashJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        mode,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<HashJoinExec>()
    {
        match mode {
            PartitionMode::Partitioned => {
                let join_constructor =
                    |new_conditions: (Vec<(Column, Column)>, Vec<SortOptions>)| {
                        Ok(Arc::new(HashJoinExec::try_new(
                            left.clone(),
                            right.clone(),
                            new_conditions.0,
                            filter.clone(),
                            join_type,
                            PartitionMode::Partitioned,
                            *null_equals_null,
                        )?) as Arc<dyn ExecutionPlan>)
                    };
                Some(reorder_partitioned_join_keys(
                    requirements.plan.clone(),
                    &parent_required,
                    on,
                    vec![],
                    &join_constructor,
                )?)
            }
            PartitionMode::CollectLeft => {
                let new_right_request = match join_type {
                    JoinType::Inner | JoinType::Right => shift_right_required(
                        &parent_required,
                        left.schema().fields().len(),
                    ),
                    JoinType::RightSemi | JoinType::RightAnti => {
                        Some(parent_required.clone())
                    }
                    JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::Full => None,
                };

                // Push down requirements to the right side
                Some(PlanWithKeyRequirements {
                    plan: requirements.plan.clone(),
                    required_key_ordering: vec![],
                    request_key_ordering: vec![None, new_right_request],
                })
            }
            PartitionMode::Auto => {
                // Can not satisfy, clear the current requirements and generate new empty requirements
                Some(PlanWithKeyRequirements::new(requirements.plan.clone()))
            }
        }
    } else if let Some(CrossJoinExec { left, .. }) =
        plan_any.downcast_ref::<CrossJoinExec>()
    {
        let left_columns_len = left.schema().fields().len();
        // Push down requirements to the right side
        Some(PlanWithKeyRequirements {
            plan: requirements.plan.clone(),
            required_key_ordering: vec![],
            request_key_ordering: vec![
                None,
                shift_right_required(&parent_required, left_columns_len),
            ],
        })
    } else if let Some(SortMergeJoinExec {
        left,
        right,
        on,
        join_type,
        sort_options,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<SortMergeJoinExec>()
    {
        let join_constructor =
            |new_conditions: (Vec<(Column, Column)>, Vec<SortOptions>)| {
                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left.clone(),
                    right.clone(),
                    new_conditions.0,
                    *join_type,
                    new_conditions.1,
                    *null_equals_null,
                )?) as Arc<dyn ExecutionPlan>)
            };
        Some(reorder_partitioned_join_keys(
            requirements.plan.clone(),
            &parent_required,
            on,
            sort_options.clone(),
            &join_constructor,
        )?)
    } else if let Some(aggregate_exec) = plan_any.downcast_ref::<AggregateExec>() {
        if !parent_required.is_empty() {
            match aggregate_exec.mode() {
                AggregateMode::FinalPartitioned => Some(reorder_aggregate_keys(
                    requirements.plan.clone(),
                    &parent_required,
                    aggregate_exec,
                )?),
                _ => Some(PlanWithKeyRequirements::new(requirements.plan.clone())),
            }
        } else {
            // Keep everything unchanged
            None
        }
    } else if let Some(proj) = plan_any.downcast_ref::<ProjectionExec>() {
        let expr = proj.expr();
        // For Projection, we need to transform the requirements to the columns before the Projection
        // And then to push down the requirements
        // Construct a mapping from new name to the the orginal Column
        let new_required = map_columns_before_projection(&parent_required, expr);
        if new_required.len() == parent_required.len() {
            Some(PlanWithKeyRequirements {
                plan: requirements.plan.clone(),
                required_key_ordering: vec![],
                request_key_ordering: vec![Some(new_required.clone())],
            })
        } else {
            // Can not satisfy, clear the current requirements and generate new empty requirements
            Some(PlanWithKeyRequirements::new(requirements.plan.clone()))
        }
    } else if plan_any.downcast_ref::<RepartitionExec>().is_some()
        || plan_any.downcast_ref::<CoalescePartitionsExec>().is_some()
        || plan_any.downcast_ref::<WindowAggExec>().is_some()
    {
        Some(PlanWithKeyRequirements::new(requirements.plan.clone()))
    } else {
        // By default, push down the parent requirements to children
        let children_len = requirements.plan.children().len();
        Some(PlanWithKeyRequirements {
            plan: requirements.plan.clone(),
            required_key_ordering: vec![],
            request_key_ordering: vec![Some(parent_required.clone()); children_len],
        })
    };
    Ok(if let Some(transformed) = transformed {
        Transformed::Yes(transformed)
    } else {
        Transformed::No(requirements)
    })
}

fn reorder_partitioned_join_keys<F>(
    join_plan: Arc<dyn ExecutionPlan>,
    parent_required: &[Arc<dyn PhysicalExpr>],
    on: &[(Column, Column)],
    sort_options: Vec<SortOptions>,
    join_constructor: &F,
) -> Result<PlanWithKeyRequirements>
where
    F: Fn((Vec<(Column, Column)>, Vec<SortOptions>)) -> Result<Arc<dyn ExecutionPlan>>,
{
    let join_key_pairs = extract_join_keys(on);
    if let Some((
        JoinKeyPairs {
            left_keys,
            right_keys,
        },
        new_positions,
    )) = try_reorder(
        join_key_pairs.clone(),
        parent_required,
        &join_plan.equivalence_properties(),
    ) {
        if !new_positions.is_empty() {
            let new_join_on = new_join_conditions(&left_keys, &right_keys);
            let mut new_sort_options: Vec<SortOptions> = vec![];
            for idx in 0..sort_options.len() {
                new_sort_options.push(sort_options[new_positions[idx]])
            }

            Ok(PlanWithKeyRequirements {
                plan: join_constructor((new_join_on, new_sort_options))?,
                required_key_ordering: vec![],
                request_key_ordering: vec![Some(left_keys), Some(right_keys)],
            })
        } else {
            Ok(PlanWithKeyRequirements {
                plan: join_plan,
                required_key_ordering: vec![],
                request_key_ordering: vec![Some(left_keys), Some(right_keys)],
            })
        }
    } else {
        Ok(PlanWithKeyRequirements {
            plan: join_plan,
            required_key_ordering: vec![],
            request_key_ordering: vec![
                Some(join_key_pairs.left_keys),
                Some(join_key_pairs.right_keys),
            ],
        })
    }
}

fn reorder_aggregate_keys(
    agg_plan: Arc<dyn ExecutionPlan>,
    parent_required: &[Arc<dyn PhysicalExpr>],
    agg_exec: &AggregateExec,
) -> Result<PlanWithKeyRequirements> {
    let output_columns = agg_exec
        .group_by()
        .expr()
        .iter()
        .enumerate()
        .map(|(index, (_col, name))| Column::new(name, index))
        .collect::<Vec<_>>();

    let output_exprs = output_columns
        .iter()
        .map(|c| Arc::new(c.clone()) as _)
        .collect::<Vec<_>>();

    if parent_required.len() != output_exprs.len()
        || !agg_exec.group_by().null_expr().is_empty()
        || physical_exprs_equal(&output_exprs, parent_required)
    {
        Ok(PlanWithKeyRequirements::new(agg_plan))
    } else {
        let new_positions = expected_expr_positions(&output_exprs, parent_required);
        match new_positions {
            None => Ok(PlanWithKeyRequirements::new(agg_plan)),
            Some(positions) => {
                let new_partial_agg = if let Some(agg_exec) =
                    agg_exec.input().as_any().downcast_ref::<AggregateExec>()
                {
                    if matches!(agg_exec.mode(), &AggregateMode::Partial) {
                        let group_exprs = agg_exec.group_by().expr();
                        let new_group_exprs = positions
                            .into_iter()
                            .map(|idx| group_exprs[idx].clone())
                            .collect();
                        let new_partial_group_by =
                            PhysicalGroupBy::new_single(new_group_exprs);
                        Some(Arc::new(AggregateExec::try_new(
                            AggregateMode::Partial,
                            new_partial_group_by,
                            agg_exec.aggr_expr().to_vec(),
                            agg_exec.filter_expr().to_vec(),
                            agg_exec.order_by_expr().to_vec(),
                            agg_exec.input().clone(),
                            agg_exec.input_schema.clone(),
                        )?))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(partial_agg) = new_partial_agg {
                    // Build new group expressions that correspond to the output of partial_agg
                    let group_exprs = partial_agg.group_expr().expr();
                    let new_final_group = partial_agg.output_group_expr();
                    let new_group_by = PhysicalGroupBy::new_single(
                        new_final_group
                            .iter()
                            .enumerate()
                            .map(|(idx, expr)| (expr.clone(), group_exprs[idx].1.clone()))
                            .collect(),
                    );

                    let new_final_agg = Arc::new(AggregateExec::try_new(
                        AggregateMode::FinalPartitioned,
                        new_group_by,
                        agg_exec.aggr_expr().to_vec(),
                        agg_exec.filter_expr().to_vec(),
                        agg_exec.order_by_expr().to_vec(),
                        partial_agg,
                        agg_exec.input_schema(),
                    )?);

                    // Need to create a new projection to change the expr ordering back
                    let agg_schema = new_final_agg.schema();
                    let mut proj_exprs = output_columns
                        .iter()
                        .map(|col| {
                            let name = col.name();
                            (
                                Arc::new(Column::new(
                                    name,
                                    agg_schema.index_of(name).unwrap(),
                                )) as _,
                                name.to_owned(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let agg_fields = agg_schema.fields();
                    for (idx, field) in
                        agg_fields.iter().enumerate().skip(output_columns.len())
                    {
                        let name = field.name();
                        proj_exprs
                            .push((Arc::new(Column::new(name, idx)) as _, name.clone()))
                    }
                    // TODO merge adjacent Projections if there are
                    Ok(PlanWithKeyRequirements::new(Arc::new(
                        ProjectionExec::try_new(proj_exprs, new_final_agg)?,
                    )))
                } else {
                    Ok(PlanWithKeyRequirements::new(agg_plan))
                }
            }
        }
    }
}

fn shift_right_required(
    parent_required: &[Arc<dyn PhysicalExpr>],
    left_columns_len: usize,
) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
    let new_right_required = parent_required
        .iter()
        .filter_map(|r| {
            if let Some(col) = r.as_any().downcast_ref::<Column>() {
                let idx = col.index();
                if idx >= left_columns_len {
                    let result = Column::new(col.name(), idx - left_columns_len);
                    Some(Arc::new(result) as _)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // if the parent required are all comming from the right side, the requirements can be pushdown
    (new_right_required.len() == parent_required.len()).then_some(new_right_required)
}

/// When the physical planner creates the Joins, the ordering of join keys is from the original query.
/// That might not match with the output partitioning of the join node's children
/// This method will try to change the ordering of the join keys to match with the
/// partitioning of the join nodes' children. If it can not match with both sides, it will try to
/// match with one, either the left side or the right side.
///
/// Example:
///     TopJoin on (a, b, c)
///         bottom left join on(b, a, c)
///         bottom right join on(c, b, a)
///
///  Will be adjusted to:
///     TopJoin on (b, a, c)
///         bottom left join on(b, a, c)
///         bottom right join on(c, b, a)
///
/// Compared to the Top-Down reordering process, this Bottom-Up approach is much simpler, but might not reach a best result.
/// The Bottom-Up approach will be useful in future if we plan to support storage partition-wised Joins.
/// In that case, the datasources/tables might be pre-partitioned and we can't adjust the key ordering of the datasources
/// and then can't apply the Top-Down reordering process.
pub(crate) fn reorder_join_keys_to_inputs(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_any = plan.as_any();
    if let Some(HashJoinExec {
        left,
        right,
        on,
        filter,
        join_type,
        mode,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<HashJoinExec>()
    {
        if matches!(mode, PartitionMode::Partitioned) {
            let join_key_pairs = extract_join_keys(on);
            if let Some((
                JoinKeyPairs {
                    left_keys,
                    right_keys,
                },
                new_positions,
            )) = reorder_current_join_keys(
                join_key_pairs,
                Some(left.output_partitioning()),
                Some(right.output_partitioning()),
                &left.equivalence_properties(),
                &right.equivalence_properties(),
            ) {
                if !new_positions.is_empty() {
                    let new_join_on = new_join_conditions(&left_keys, &right_keys);
                    return Ok(Arc::new(HashJoinExec::try_new(
                        left.clone(),
                        right.clone(),
                        new_join_on,
                        filter.clone(),
                        join_type,
                        PartitionMode::Partitioned,
                        *null_equals_null,
                    )?));
                }
            }
        }
    } else if let Some(SortMergeJoinExec {
        left,
        right,
        on,
        join_type,
        sort_options,
        null_equals_null,
        ..
    }) = plan_any.downcast_ref::<SortMergeJoinExec>()
    {
        let join_key_pairs = extract_join_keys(on);
        if let Some((
            JoinKeyPairs {
                left_keys,
                right_keys,
            },
            new_positions,
        )) = reorder_current_join_keys(
            join_key_pairs,
            Some(left.output_partitioning()),
            Some(right.output_partitioning()),
            &left.equivalence_properties(),
            &right.equivalence_properties(),
        ) {
            if !new_positions.is_empty() {
                let new_join_on = new_join_conditions(&left_keys, &right_keys);
                let new_sort_options = (0..sort_options.len())
                    .map(|idx| sort_options[new_positions[idx]])
                    .collect();
                return Ok(Arc::new(SortMergeJoinExec::try_new(
                    left.clone(),
                    right.clone(),
                    new_join_on,
                    *join_type,
                    new_sort_options,
                    *null_equals_null,
                )?));
            }
        }
    }
    Ok(plan)
}

/// Reorder the current join keys ordering based on either left partition or right partition
fn reorder_current_join_keys(
    join_keys: JoinKeyPairs,
    left_partition: Option<Partitioning>,
    right_partition: Option<Partitioning>,
    left_equivalence_properties: &EquivalenceProperties,
    right_equivalence_properties: &EquivalenceProperties,
) -> Option<(JoinKeyPairs, Vec<usize>)> {
    match (left_partition, right_partition.clone()) {
        (Some(Partitioning::Hash(left_exprs, _)), _) => {
            try_reorder(join_keys.clone(), &left_exprs, left_equivalence_properties)
                .or_else(|| {
                    reorder_current_join_keys(
                        join_keys,
                        None,
                        right_partition,
                        left_equivalence_properties,
                        right_equivalence_properties,
                    )
                })
        }
        (_, Some(Partitioning::Hash(right_exprs, _))) => {
            try_reorder(join_keys, &right_exprs, right_equivalence_properties)
        }
        _ => None,
    }
}

fn try_reorder(
    join_keys: JoinKeyPairs,
    expected: &[Arc<dyn PhysicalExpr>],
    equivalence_properties: &EquivalenceProperties,
) -> Option<(JoinKeyPairs, Vec<usize>)> {
    let eq_groups = equivalence_properties.eq_group();
    let mut normalized_expected = vec![];
    let mut normalized_left_keys = vec![];
    let mut normalized_right_keys = vec![];
    if join_keys.left_keys.len() != expected.len() {
        return None;
    }
    if physical_exprs_equal(expected, &join_keys.left_keys)
        || physical_exprs_equal(expected, &join_keys.right_keys)
    {
        return Some((join_keys, vec![]));
    } else if !equivalence_properties.eq_group().is_empty() {
        normalized_expected = expected
            .iter()
            .map(|e| eq_groups.normalize_expr(e.clone()))
            .collect::<Vec<_>>();
        assert_eq!(normalized_expected.len(), expected.len());

        normalized_left_keys = join_keys
            .left_keys
            .iter()
            .map(|e| eq_groups.normalize_expr(e.clone()))
            .collect::<Vec<_>>();
        assert_eq!(join_keys.left_keys.len(), normalized_left_keys.len());

        normalized_right_keys = join_keys
            .right_keys
            .iter()
            .map(|e| eq_groups.normalize_expr(e.clone()))
            .collect::<Vec<_>>();
        assert_eq!(join_keys.right_keys.len(), normalized_right_keys.len());

        if physical_exprs_equal(&normalized_expected, &normalized_left_keys)
            || physical_exprs_equal(&normalized_expected, &normalized_right_keys)
        {
            return Some((join_keys, vec![]));
        }
    }

    let new_positions = expected_expr_positions(&join_keys.left_keys, expected)
        .or_else(|| expected_expr_positions(&join_keys.right_keys, expected))
        .or_else(|| expected_expr_positions(&normalized_left_keys, &normalized_expected))
        .or_else(|| {
            expected_expr_positions(&normalized_right_keys, &normalized_expected)
        });

    if let Some(positions) = new_positions {
        let mut new_left_keys = vec![];
        let mut new_right_keys = vec![];
        for pos in positions.iter() {
            new_left_keys.push(join_keys.left_keys[*pos].clone());
            new_right_keys.push(join_keys.right_keys[*pos].clone());
        }
        Some((
            JoinKeyPairs {
                left_keys: new_left_keys,
                right_keys: new_right_keys,
            },
            positions,
        ))
    } else {
        None
    }
}

/// Return the expected expressions positions.
/// For example, the current expressions are ['c', 'a', 'a', b'], the expected expressions are ['b', 'c', 'a', 'a'],
///
/// This method will return a Vec [3, 0, 1, 2]
fn expected_expr_positions(
    current: &[Arc<dyn PhysicalExpr>],
    expected: &[Arc<dyn PhysicalExpr>],
) -> Option<Vec<usize>> {
    if current.is_empty() || expected.is_empty() {
        return None;
    }
    let mut indexes: Vec<usize> = vec![];
    let mut current = current.to_vec();
    for expr in expected.iter() {
        // Find the position of the expected expr in the current expressions
        if let Some(expected_position) = current.iter().position(|e| e.eq(expr)) {
            current[expected_position] = Arc::new(NoOp::new());
            indexes.push(expected_position);
        } else {
            return None;
        }
    }
    Some(indexes)
}

fn extract_join_keys(on: &[(Column, Column)]) -> JoinKeyPairs {
    let (left_keys, right_keys) = on
        .iter()
        .map(|(l, r)| (Arc::new(l.clone()) as _, Arc::new(r.clone()) as _))
        .unzip();
    JoinKeyPairs {
        left_keys,
        right_keys,
    }
}

fn new_join_conditions(
    new_left_keys: &[Arc<dyn PhysicalExpr>],
    new_right_keys: &[Arc<dyn PhysicalExpr>],
) -> Vec<(Column, Column)> {
    new_left_keys
        .iter()
        .zip(new_right_keys.iter())
        .map(|(l_key, r_key)| {
            (
                l_key.as_any().downcast_ref::<Column>().unwrap().clone(),
                r_key.as_any().downcast_ref::<Column>().unwrap().clone(),
            )
        })
        .collect()
}

/// Updates `dist_onward` such that, to keep track of
/// `input` in the `exec_tree`.
///
/// # Arguments
///
/// * `input`: Current execution plan
/// * `dist_onward`: It keeps track of executors starting from a distribution
///    changing operator (e.g Repartition, SortPreservingMergeExec, etc.)
///    until child of `input` (`input` should have single child).
/// * `input_idx`: index of the `input`, for its parent.
///
fn update_distribution_onward(
    input: Arc<dyn ExecutionPlan>,
    dist_onward: &mut Option<ExecTree>,
    input_idx: usize,
) {
    // Update the onward tree if there is an active branch
    if let Some(exec_tree) = dist_onward {
        // When we add a new operator to change distribution
        // we add RepartitionExec, SortPreservingMergeExec, CoalescePartitionsExec
        // in this case, we need to update exec tree idx such that exec tree is now child of these
        // operators (change the 0, since all of the operators have single child).
        exec_tree.idx = 0;
        *exec_tree = ExecTree::new(input, input_idx, vec![exec_tree.clone()]);
    } else {
        *dist_onward = Some(ExecTree::new(input, input_idx, vec![]));
    }
}

/// Adds RoundRobin repartition operator to the plan increase parallelism.
///
/// # Arguments
///
/// * `input`: Current execution plan
/// * `n_target`: desired target partition number, if partition number of the
///    current executor is less than this value. Partition number will be increased.
/// * `dist_onward`: It keeps track of executors starting from a distribution
///    changing operator (e.g Repartition, SortPreservingMergeExec, etc.)
///    until `input` plan.
/// * `input_idx`: index of the `input`, for its parent.
///
/// # Returns
///
/// A [Result] object that contains new execution plan, where desired partition number
/// is achieved by adding RoundRobin Repartition.
fn add_roundrobin_on_top(
    input: Arc<dyn ExecutionPlan>,
    n_target: usize,
    dist_onward: &mut Option<ExecTree>,
    input_idx: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Adding repartition is helpful
    if input.output_partitioning().partition_count() < n_target {
        // When there is an existing ordering, we preserve ordering
        // during repartition. This will be un-done in the future
        // If any of the following conditions is true
        // - Preserving ordering is not helpful in terms of satisfying ordering requirements
        // - Usage of order preserving variants is not desirable
        // (determined by flag `config.optimizer.bounded_order_preserving_variants`)
        let partitioning = Partitioning::RoundRobinBatch(n_target);
        let repartition =
            RepartitionExec::try_new(input, partitioning)?.with_preserve_order();

        // update distribution onward with new operator
        let new_plan = Arc::new(repartition) as Arc<dyn ExecutionPlan>;
        update_distribution_onward(new_plan.clone(), dist_onward, input_idx);
        Ok(new_plan)
    } else {
        // Partition is not helpful, we already have desired number of partitions.
        Ok(input)
    }
}

/// Adds a hash repartition operator:
/// - to increase parallelism, and/or
/// - to satisfy requirements of the subsequent operators.
/// Repartition(Hash) is added on top of operator `input`.
///
/// # Arguments
///
/// * `input`: Current execution plan
/// * `hash_exprs`: Stores Physical Exprs that are used during hashing.
/// * `n_target`: desired target partition number, if partition number of the
///    current executor is less than this value. Partition number will be increased.
/// * `dist_onward`: It keeps track of executors starting from a distribution
///    changing operator (e.g Repartition, SortPreservingMergeExec, etc.)
///    until `input` plan.
/// * `input_idx`: index of the `input`, for its parent.
///
/// # Returns
///
/// A [`Result`] object that contains new execution plan, where desired distribution is
/// satisfied by adding Hash Repartition.
fn add_hash_on_top(
    input: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    // Repartition(Hash) will have `n_target` partitions at the output.
    n_target: usize,
    // Stores executors starting from Repartition(RoundRobin) until
    // current executor. When Repartition(Hash) is added, `dist_onward`
    // is updated such that it stores connection from Repartition(RoundRobin)
    // until Repartition(Hash).
    dist_onward: &mut Option<ExecTree>,
    input_idx: usize,
    repartition_beneficial_stats: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    if n_target == input.output_partitioning().partition_count() && n_target == 1 {
        // In this case adding a hash repartition is unnecessary as the hash
        // requirement is implicitly satisfied.
        return Ok(input);
    }
    let satisfied = input
        .output_partitioning()
        .satisfy(Distribution::HashPartitioned(hash_exprs.clone()), || {
            input.equivalence_properties()
        });
    // Add hash repartitioning when:
    // - The hash distribution requirement is not satisfied, or
    // - We can increase parallelism by adding hash partitioning.
    if !satisfied || n_target > input.output_partitioning().partition_count() {
        // When there is an existing ordering, we preserve ordering during
        // repartition. This will be rolled back in the future if any of the
        // following conditions is true:
        // - Preserving ordering is not helpful in terms of satisfying ordering
        //   requirements.
        // - Usage of order preserving variants is not desirable (per the flag
        //   `config.optimizer.bounded_order_preserving_variants`).
        let mut new_plan = if repartition_beneficial_stats {
            // Since hashing benefits from partitioning, add a round-robin repartition
            // before it:
            add_roundrobin_on_top(input, n_target, dist_onward, 0)?
        } else {
            input
        };
        let partitioning = Partitioning::Hash(hash_exprs, n_target);
        let repartition = RepartitionExec::try_new(new_plan, partitioning)?
            // preserve any ordering if possible
            .with_preserve_order();
        new_plan = Arc::new(repartition) as _;

        // update distribution onward with new operator
        update_distribution_onward(new_plan.clone(), dist_onward, input_idx);
        Ok(new_plan)
    } else {
        Ok(input)
    }
}

/// Adds a `SortPreservingMergeExec` operator on top of input executor:
/// - to satisfy single distribution requirement.
///
/// # Arguments
///
/// * `input`: Current execution plan
/// * `dist_onward`: It keeps track of executors starting from a distribution
///    changing operator (e.g Repartition, SortPreservingMergeExec, etc.)
///    until `input` plan.
/// * `input_idx`: index of the `input`, for its parent.
///
/// # Returns
///
/// New execution plan, where desired single
/// distribution is satisfied by adding `SortPreservingMergeExec`.
fn add_spm_on_top(
    input: Arc<dyn ExecutionPlan>,
    dist_onward: &mut Option<ExecTree>,
    input_idx: usize,
) -> Arc<dyn ExecutionPlan> {
    // Add SortPreservingMerge only when partition count is larger than 1.
    if input.output_partitioning().partition_count() > 1 {
        // When there is an existing ordering, we preserve ordering
        // during decreasıng partıtıons. This will be un-done in the future
        // If any of the following conditions is true
        // - Preserving ordering is not helpful in terms of satisfying ordering requirements
        // - Usage of order preserving variants is not desirable
        // (determined by flag `config.optimizer.bounded_order_preserving_variants`)
        let should_preserve_ordering = input.output_ordering().is_some();
        let new_plan: Arc<dyn ExecutionPlan> = if should_preserve_ordering {
            let existing_ordering = input.output_ordering().unwrap_or(&[]);
            Arc::new(SortPreservingMergeExec::new(
                existing_ordering.to_vec(),
                input,
            )) as _
        } else {
            Arc::new(CoalescePartitionsExec::new(input)) as _
        };

        // update repartition onward with new operator
        update_distribution_onward(new_plan.clone(), dist_onward, input_idx);
        new_plan
    } else {
        input
    }
}

/// Updates the physical plan inside `distribution_context` so that distribution
/// changing operators are removed from the top. If they are necessary, they will
/// be added in subsequent stages.
///
/// Assume that following plan is given:
/// ```text
/// "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
/// "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "    ParquetExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC]",
/// ```
///
/// Since `RepartitionExec`s change the distribution, this function removes
/// them and returns following plan:
///
/// ```text
/// "ParquetExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC]",
/// ```
fn remove_dist_changing_operators(
    distribution_context: DistributionContext,
) -> Result<DistributionContext> {
    let DistributionContext {
        mut plan,
        mut distribution_onwards,
        has_recursive_ancestor,
    } = distribution_context;

    // Remove any distribution changing operators at the beginning:
    // Note that they will be re-inserted later on if necessary or helpful.
    while is_repartition(&plan)
        || is_coalesce_partitions(&plan)
        || is_sort_preserving_merge(&plan)
    {
        // All of above operators have a single child. When we remove the top
        // operator, we take the first child.
        plan = plan.children().swap_remove(0);
        distribution_onwards =
            get_children_exectrees(plan.children().len(), &distribution_onwards[0]);
    }

    // Create a plan with the updated children:
    Ok(DistributionContext {
        plan,
        distribution_onwards,
        has_recursive_ancestor,
    })
}

/// Updates the physical plan `input` by using `dist_onward` replace order preserving operator variants
/// with their corresponding operators that do not preserve order. It is a wrapper for `replace_order_preserving_variants_helper`
fn replace_order_preserving_variants(
    input: &mut Arc<dyn ExecutionPlan>,
    dist_onward: &mut Option<ExecTree>,
) -> Result<()> {
    if let Some(dist_onward) = dist_onward {
        *input = replace_order_preserving_variants_helper(dist_onward)?;
    }
    *dist_onward = None;
    Ok(())
}

/// Updates the physical plan inside `ExecTree` if preserving ordering while changing partitioning
/// is not helpful or desirable.
///
/// Assume that following plan is given:
/// ```text
/// "SortPreservingMergeExec: \[a@0 ASC]"
/// "  SortPreservingRepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
/// "    SortPreservingRepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "      ParquetExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC]",
/// ```
///
/// This function converts plan above (inside `ExecTree`) to the following:
///
/// ```text
/// "CoalescePartitionsExec"
/// "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
/// "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
/// "      ParquetExec: file_groups={2 groups: \[\[x], \[y]]}, projection=\[a, b, c, d, e], output_ordering=\[a@0 ASC]",
/// ```
fn replace_order_preserving_variants_helper(
    exec_tree: &ExecTree,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut updated_children = exec_tree.plan.children();
    for child in &exec_tree.children {
        updated_children[child.idx] = replace_order_preserving_variants_helper(child)?;
    }
    if is_sort_preserving_merge(&exec_tree.plan) {
        return Ok(Arc::new(CoalescePartitionsExec::new(
            updated_children.swap_remove(0),
        )));
    }
    if let Some(repartition) = exec_tree.plan.as_any().downcast_ref::<RepartitionExec>() {
        if repartition.preserve_order() {
            return Ok(Arc::new(
                // new RepartitionExec don't preserve order
                RepartitionExec::try_new(
                    updated_children.swap_remove(0),
                    repartition.partitioning().clone(),
                )?,
            ));
        }
    }
    exec_tree.plan.clone().with_new_children(updated_children)
}

/// This function checks whether we need to add additional data exchange
/// operators to satisfy distribution requirements. Since this function
/// takes care of such requirements, we should avoid manually adding data
/// exchange operators in other places.
fn ensure_distribution(
    dist_context: DistributionContext,
    config: &ConfigOptions,
) -> Result<Transformed<DistributionContext>> {
    if dist_context.has_recursive_ancestor {
        return Ok(Transformed::No(dist_context));
    }
    let target_partitions = config.execution.target_partitions;
    // When `false`, round robin repartition will not be added to increase parallelism
    let enable_round_robin = config.optimizer.enable_round_robin_repartition;
    let repartition_file_scans = config.optimizer.repartition_file_scans;
    let batch_size = config.execution.batch_size;
    let is_unbounded = unbounded_output(&dist_context.plan);
    // Use order preserving variants either of the conditions true
    // - it is desired according to config
    // - when plan is unbounded
    let order_preserving_variants_desirable =
        is_unbounded || config.optimizer.prefer_existing_sort;

    if dist_context.plan.children().is_empty() {
        return Ok(Transformed::No(dist_context));
    }

    // Remove unnecessary repartition from the physical plan if any
    let DistributionContext {
        mut plan,
        mut distribution_onwards,
        has_recursive_ancestor,
    } = remove_dist_changing_operators(dist_context)?;

    if let Some(exec) = plan.as_any().downcast_ref::<WindowAggExec>() {
        if let Some(updated_window) = get_best_fitting_window(
            exec.window_expr(),
            exec.input(),
            &exec.partition_keys,
        )? {
            plan = updated_window;
        }
    } else if let Some(exec) = plan.as_any().downcast_ref::<BoundedWindowAggExec>() {
        if let Some(updated_window) = get_best_fitting_window(
            exec.window_expr(),
            exec.input(),
            &exec.partition_keys,
        )? {
            plan = updated_window;
        }
    };
    let n_children = plan.children().len();
    // This loop iterates over all the children to:
    // - Increase parallelism for every child if it is beneficial.
    // - Satisfy the distribution requirements of every child, if it is not
    //   already satisfied.
    // We store the updated children in `new_children`.
    let new_children = izip!(
        plan.children().into_iter(),
        plan.required_input_distribution().iter(),
        plan.required_input_ordering().iter(),
        distribution_onwards.iter_mut(),
        plan.benefits_from_input_partitioning(),
        plan.maintains_input_order(),
        0..n_children
    )
    .map(
        |(
            mut child,
            requirement,
            required_input_ordering,
            dist_onward,
            would_benefit,
            maintains,
            child_idx,
        )| {
            // Don't need to apply when the returned row count is not greater than 1:
            let num_rows = child.statistics()?.num_rows;
            let repartition_beneficial_stats = if num_rows.is_exact().unwrap_or(false) {
                num_rows
                    .get_value()
                    .map(|value| value > &batch_size)
                    .unwrap_or(true)
            } else {
                true
            };
            if enable_round_robin
                // Operator benefits from partitioning (e.g. filter):
                && (would_benefit && repartition_beneficial_stats)
                // Unless partitioning doesn't increase the partition count, it is not beneficial:
                && child.output_partitioning().partition_count() < target_partitions
            {
                // When `repartition_file_scans` is set, attempt to increase
                // parallelism at the source.
                if repartition_file_scans {
                    if let Some(new_child) =
                        child.repartitioned(target_partitions, config)?
                    {
                        child = new_child;
                    }
                }
                // Increase parallelism by adding round-robin repartitioning
                // on top of the operator. Note that we only do this if the
                // partition count is not already equal to the desired partition
                // count.
                child = add_roundrobin_on_top(
                    child,
                    target_partitions,
                    dist_onward,
                    child_idx,
                )?;
            }

            // Satisfy the distribution requirement if it is unmet.
            match requirement {
                Distribution::SinglePartition => {
                    child = add_spm_on_top(child, dist_onward, child_idx);
                }
                Distribution::HashPartitioned(exprs) => {
                    child = add_hash_on_top(
                        child,
                        exprs.to_vec(),
                        target_partitions,
                        dist_onward,
                        child_idx,
                        repartition_beneficial_stats,
                    )?;
                }
                Distribution::UnspecifiedDistribution => {}
            };

            // There is an ordering requirement of the operator:
            if let Some(required_input_ordering) = required_input_ordering {
                // Either:
                // - Ordering requirement cannot be satisfied by preserving ordering through repartitions, or
                // - using order preserving variant is not desirable.
                let ordering_satisfied = child
                    .equivalence_properties()
                    .ordering_satisfy_requirement(required_input_ordering);
                if !ordering_satisfied || !order_preserving_variants_desirable {
                    replace_order_preserving_variants(&mut child, dist_onward)?;
                    // If ordering requirements were satisfied before repartitioning,
                    // make sure ordering requirements are still satisfied after.
                    if ordering_satisfied {
                        // Make sure to satisfy ordering requirement:
                        add_sort_above(&mut child, required_input_ordering, None);
                    }
                }
                // Stop tracking distribution changing operators
                *dist_onward = None;
            } else {
                // no ordering requirement
                match requirement {
                    // Operator requires specific distribution.
                    Distribution::SinglePartition | Distribution::HashPartitioned(_) => {
                        // Since there is no ordering requirement, preserving ordering is pointless
                        replace_order_preserving_variants(&mut child, dist_onward)?;
                    }
                    Distribution::UnspecifiedDistribution => {
                        // Since ordering is lost, trying to preserve ordering is pointless
                        if !maintains {
                            replace_order_preserving_variants(&mut child, dist_onward)?;
                        }
                    }
                }
            }
            Ok(child)
        },
    )
    .collect::<Result<Vec<_>>>()?;

    let new_distribution_context = DistributionContext {
        plan: if plan.as_any().is::<UnionExec>() && can_interleave(&new_children) {
            // Add a special case for [`UnionExec`] since we want to "bubble up"
            // hash-partitioned data. So instead of
            //
            // Agg:
            //   Repartition (hash):
            //     Union:
            //       - Agg:
            //           Repartition (hash):
            //             Data
            //       - Agg:
            //           Repartition (hash):
            //             Data
            //
            // we can use:
            //
            // Agg:
            //   Interleave:
            //     - Agg:
            //         Repartition (hash):
            //           Data
            //     - Agg:
            //         Repartition (hash):
            //           Data
            Arc::new(InterleaveExec::try_new(new_children)?)
        } else {
            plan.with_new_children(new_children)?
        },
        distribution_onwards,
        has_recursive_ancestor,
    };
    Ok(Transformed::Yes(new_distribution_context))
}

/// A struct to keep track of distribution changing executors
/// (`RepartitionExec`, `SortPreservingMergeExec`, `CoalescePartitionsExec`),
/// and their associated parents inside `plan`. Using this information,
/// we can optimize distribution of the plan if/when necessary.
#[derive(Debug, Clone)]
struct DistributionContext {
    plan: Arc<dyn ExecutionPlan>,
    /// Keep track of associations for each child of the plan. If `None`,
    /// there is no distribution changing operator in its descendants.
    distribution_onwards: Vec<Option<ExecTree>>,
    // keep track of whether we've encountered a RecursiveQuery
    has_recursive_ancestor: bool,
}

impl DistributionContext {
    /// Creates an empty context.
    /// Only use this method at the root of the plan.
    /// All other contexts should be created using `new_descendent`.
    fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        DistributionContext {
            has_recursive_ancestor: is_recursive_query(&plan),
            plan,
            distribution_onwards: vec![None; length],
        }
    }

    /// Creates a new context from a descendent plan.
    /// Importantly, this function propagates the `has_recursive_ancestor` flag.
    fn new_descendent(&self, descendent_plan: Arc<dyn ExecutionPlan>) -> Self {
        let ancestor = self;

        let mut new_ctx = Self::new(descendent_plan);
        new_ctx.has_recursive_ancestor |= ancestor.has_recursive_ancestor;
        new_ctx
    }

    /// Creates a new context from a descendent context.
    /// Importantly, this function propagates the `has_recursive_ancestor` flag.
    fn new_descendent_from_ctx(&self, ctx: Self) -> Self {
        let ancestor = self;

        let mut ctx = ctx;
        ctx.has_recursive_ancestor |= ancestor.has_recursive_ancestor;
        ctx
    }

    /// Constructs a new context from children contexts.
    fn new_from_children_nodes(
        children_nodes: Vec<DistributionContext>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let distribution_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, context)| {
                let DistributionContext {
                    plan,
                    // The `distribution_onwards` tree keeps track of operators
                    // that change distribution, or preserves the existing
                    // distribution (starting from an operator that change distribution).
                    distribution_onwards,
                    ..
                } = context;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if distribution_onwards[0].is_none() {
                    if let Some(repartition) =
                        plan.as_any().downcast_ref::<RepartitionExec>()
                    {
                        match repartition.partitioning() {
                            Partitioning::RoundRobinBatch(_)
                            | Partitioning::Hash(_, _) => {
                                // Start tracking operators starting from this repartition (either roundrobin or hash):
                                return Some(ExecTree::new(plan, idx, vec![]));
                            }
                            _ => {}
                        }
                    } else if plan.as_any().is::<SortPreservingMergeExec>()
                        || plan.as_any().is::<CoalescePartitionsExec>()
                    {
                        // Start tracking operators starting from this sort preserving merge:
                        return Some(ExecTree::new(plan, idx, vec![]));
                    }
                    None
                } else {
                    // Propagate children distribution tracking to the above
                    let new_distribution_onwards = izip!(
                        plan.required_input_distribution().iter(),
                        distribution_onwards.into_iter()
                    )
                    .flat_map(|(required_dist, distribution_onwards)| {
                        if let Some(distribution_onwards) = distribution_onwards {
                            // Operator can safely propagate the distribution above.
                            // This is similar to maintaining order in the EnforceSorting rule.
                            if let Distribution::UnspecifiedDistribution = required_dist {
                                return Some(distribution_onwards);
                            }
                        }
                        None
                    })
                    .collect::<Vec<_>>();
                    // Either:
                    // - None of the children has a connection to an operator that modifies distribution, or
                    // - The current operator requires distribution at its input so doesn't propagate it above.
                    if new_distribution_onwards.is_empty() {
                        None
                    } else {
                        Some(ExecTree::new(plan, idx, new_distribution_onwards))
                    }
                }
            })
            .collect();
        Ok(DistributionContext {
            has_recursive_ancestor: is_recursive_query(&parent_plan),
            plan: with_new_children_if_necessary(parent_plan, children_plans)?.into(),
            distribution_onwards,
        })
    }

    /// Computes distribution tracking contexts for every child of the plan.
    fn children(&self) -> Vec<DistributionContext> {
        self.plan
            .children()
            .into_iter()
            .map(|child| self.new_descendent(child))
            .collect()
    }
}

impl TreeNode for DistributionContext {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.children() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

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

            DistributionContext::new_from_children_nodes(
                children_nodes,
                self.plan.clone(),
            )
            .map(|ctx| self.new_descendent_from_ctx(ctx))
        }
    }
}

/// implement Display method for `DistributionContext` struct.
impl fmt::Display for DistributionContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let plan_string = get_plan_string(&self.plan);
        write!(f, "plan: {:?}", plan_string)?;
        write!(
            f,
            "has_recursive_ancestor: {:?}",
            self.has_recursive_ancestor,
        )?;
        for (idx, child) in self.distribution_onwards.iter().enumerate() {
            if let Some(child) = child {
                write!(f, "idx:{:?}, exec_tree:{}", idx, child)?;
            }
        }
        write!(f, "")
    }
}

#[derive(Debug, Clone)]
struct JoinKeyPairs {
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
}

#[derive(Debug, Clone)]
struct PlanWithKeyRequirements {
    plan: Arc<dyn ExecutionPlan>,
    /// Parent required key ordering
    required_key_ordering: Vec<Arc<dyn PhysicalExpr>>,
    /// The request key ordering to children
    request_key_ordering: Vec<Option<Vec<Arc<dyn PhysicalExpr>>>>,
}

impl PlanWithKeyRequirements {
    fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let children_len = plan.children().len();
        PlanWithKeyRequirements {
            plan,
            required_key_ordering: vec![],
            request_key_ordering: vec![None; children_len],
        }
    }

    fn children(&self) -> Vec<PlanWithKeyRequirements> {
        let plan_children = self.plan.children();
        assert_eq!(plan_children.len(), self.request_key_ordering.len());
        plan_children
            .into_iter()
            .zip(self.request_key_ordering.clone())
            .map(|(child, required)| {
                let from_parent = required.unwrap_or_default();
                let length = child.children().len();
                PlanWithKeyRequirements {
                    plan: child,
                    required_key_ordering: from_parent,
                    request_key_ordering: vec![None; length],
                }
            })
            .collect()
    }
}

impl TreeNode for PlanWithKeyRequirements {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        let children = self.children();
        for child in children {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if !children.is_empty() {
            let new_children: Result<Vec<_>> =
                children.into_iter().map(transform).collect();

            let children_plans = new_children?
                .into_iter()
                .map(|child| child.plan)
                .collect::<Vec<_>>();
            let new_plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(PlanWithKeyRequirements {
                plan: new_plan.into(),
                required_key_ordering: self.required_key_ordering,
                request_key_ordering: self.request_key_ordering,
            })
        } else {
            Ok(self)
        }
    }
}

/// Since almost all of these tests explicitly use `ParquetExec` they only run with the parquet  feature flag on
#[cfg(feature = "parquet")]
#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Deref;

    use super::*;
    use crate::datasource::file_format::file_compression_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::ParquetExec;
    use crate::datasource::physical_plan::{CsvExec, FileScanConfig};
    use crate::physical_optimizer::enforce_sorting::EnforceSorting;
    use crate::physical_optimizer::output_requirements::OutputRequirements;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::joins::{
        utils::JoinOn, HashJoinExec, PartitionMode, SortMergeJoinExec,
    };
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::{displayable, DisplayAs, DisplayFormatType, Statistics};

    use crate::physical_optimizer::test_utils::{
        coalesce_partitions_exec, repartition_exec,
    };
    use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use crate::physical_plan::sorts::sort::SortExec;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::ScalarValue;
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion_physical_expr::{
        expressions, expressions::binary, expressions::lit, expressions::Column,
        LexOrdering, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
    };

    /// Models operators like BoundedWindowExec that require an input
    /// ordering but is easy to construct
    #[derive(Debug)]
    struct SortRequiredExec {
        input: Arc<dyn ExecutionPlan>,
        expr: LexOrdering,
    }

    impl SortRequiredExec {
        fn new(input: Arc<dyn ExecutionPlan>) -> Self {
            let expr = input.output_ordering().unwrap_or(&[]).to_vec();
            Self { input, expr }
        }

        fn new_with_requirement(
            input: Arc<dyn ExecutionPlan>,
            requirement: Vec<PhysicalSortExpr>,
        ) -> Self {
            Self {
                input,
                expr: requirement,
            }
        }
    }

    impl DisplayAs for SortRequiredExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(
                f,
                "SortRequiredExec: [{}]",
                PhysicalSortExpr::format_list(&self.expr)
            )
        }
    }

    impl ExecutionPlan for SortRequiredExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }

        fn output_partitioning(&self) -> crate::physical_plan::Partitioning {
            self.input.output_partitioning()
        }

        fn benefits_from_input_partitioning(&self) -> Vec<bool> {
            vec![false]
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            self.input.output_ordering()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.input.clone()]
        }

        // model that it requires the output ordering of its input
        fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
            vec![self
                .output_ordering()
                .map(PhysicalSortRequirement::from_sort_exprs)]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq!(children.len(), 1);
            let child = children.pop().unwrap();
            Ok(Arc::new(Self::new_with_requirement(
                child,
                self.expr.clone(),
            )))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<crate::execution::context::TaskContext>,
        ) -> Result<crate::physical_plan::SendableRecordBatchStream> {
            unreachable!();
        }

        fn statistics(&self) -> Result<Statistics> {
            self.input.statistics()
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]))
    }

    fn parquet_exec() -> Arc<ParquetExec> {
        parquet_exec_with_sort(vec![])
    }

    pub(crate) fn parquet_exec_with_sort(
        output_ordering: Vec<Vec<PhysicalSortExpr>>,
    ) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::new_unknown(&schema()),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    fn parquet_exec_multiple() -> Arc<ParquetExec> {
        parquet_exec_multiple_sorted(vec![])
    }

    // Created a sorted parquet exec with multiple files
    fn parquet_exec_multiple_sorted(
        output_ordering: Vec<Vec<PhysicalSortExpr>>,
    ) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![
                    vec![PartitionedFile::new("x".to_string(), 100)],
                    vec![PartitionedFile::new("y".to_string(), 100)],
                ],
                statistics: Statistics::new_unknown(&schema()),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    fn csv_exec() -> Arc<CsvExec> {
        csv_exec_with_sort(vec![])
    }

    fn csv_exec_with_sort(output_ordering: Vec<Vec<PhysicalSortExpr>>) -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::new_unknown(&schema()),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering,
                infinite_source: false,
            },
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    fn csv_exec_multiple() -> Arc<CsvExec> {
        csv_exec_multiple_sorted(vec![])
    }

    // Created a sorted parquet exec with multiple files
    fn csv_exec_multiple_sorted(
        output_ordering: Vec<Vec<PhysicalSortExpr>>,
    ) -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema(),
                file_groups: vec![
                    vec![PartitionedFile::new("x".to_string(), 100)],
                    vec![PartitionedFile::new("y".to_string(), 100)],
                ],
                statistics: Statistics::new_unknown(&schema()),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering,
                infinite_source: false,
            },
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    fn projection_exec_with_alias(
        input: Arc<dyn ExecutionPlan>,
        alias_pairs: Vec<(String, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut exprs = vec![];
        for (column, alias) in alias_pairs.iter() {
            exprs.push((col(column, &input.schema()).unwrap(), alias.to_string()));
        }
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn aggregate_exec_with_alias(
        input: Arc<dyn ExecutionPlan>,
        alias_pairs: Vec<(String, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let mut group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
        for (column, alias) in alias_pairs.iter() {
            group_by_expr
                .push((col(column, &input.schema()).unwrap(), alias.to_string()));
        }
        let group_by = PhysicalGroupBy::new_single(group_by_expr.clone());

        let final_group_by_expr = group_by_expr
            .iter()
            .enumerate()
            .map(|(index, (_col, name))| {
                (
                    Arc::new(expressions::Column::new(name, index))
                        as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect::<Vec<_>>();
        let final_grouping = PhysicalGroupBy::new_single(final_group_by_expr);

        Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                final_grouping,
                vec![],
                vec![],
                vec![],
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        group_by,
                        vec![],
                        vec![],
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn hash_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_on: &JoinOn,
        join_type: &JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on.clone(),
                None,
                join_type,
                PartitionMode::Partitioned,
                false,
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

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let predicate = Arc::new(BinaryExpr::new(
            col("c", &schema()).unwrap(),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        ));
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
    }

    fn sort_exec(
        sort_exprs: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let new_sort = SortExec::new(sort_exprs, input)
            .with_preserve_partitioning(preserve_partitioning);
        Arc::new(new_sort)
    }

    fn sort_preserving_merge_exec(
        sort_exprs: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            0,
            Some(100),
        ))
    }

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    fn sort_required_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortRequiredExec::new(input))
    }

    fn sort_required_exec_with_req(
        input: Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortRequiredExec::new_with_requirement(input, sort_exprs))
    }

    pub(crate) fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn ensure_distribution_helper(
        plan: Arc<dyn ExecutionPlan>,
        target_partitions: usize,
        bounded_order_preserving_variants: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let distribution_context = DistributionContext::new(plan);
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = target_partitions;
        config.optimizer.enable_round_robin_repartition = false;
        config.optimizer.repartition_file_scans = false;
        config.optimizer.repartition_file_min_size = 1024;
        config.optimizer.prefer_existing_sort = bounded_order_preserving_variants;
        ensure_distribution(distribution_context, &config).map(|item| item.into().plan)
    }

    /// Test whether plan matches with expected plan
    macro_rules! plans_matches_expected {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );
        }
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr, $FIRST_ENFORCE_DIST: expr) => {
            assert_optimized!($EXPECTED_LINES, $PLAN, $FIRST_ENFORCE_DIST, false, 10, false, 1024);
        };

        ($EXPECTED_LINES: expr, $PLAN: expr, $FIRST_ENFORCE_DIST: expr, $BOUNDED_ORDER_PRESERVING_VARIANTS: expr) => {
            assert_optimized!($EXPECTED_LINES, $PLAN, $FIRST_ENFORCE_DIST, $BOUNDED_ORDER_PRESERVING_VARIANTS, 10, false, 1024);
        };

        ($EXPECTED_LINES: expr, $PLAN: expr, $FIRST_ENFORCE_DIST: expr, $BOUNDED_ORDER_PRESERVING_VARIANTS: expr, $TARGET_PARTITIONS: expr, $REPARTITION_FILE_SCANS: expr, $REPARTITION_FILE_MIN_SIZE: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            let mut config = ConfigOptions::new();
            config.execution.target_partitions = $TARGET_PARTITIONS;
            config.optimizer.repartition_file_scans = $REPARTITION_FILE_SCANS;
            config.optimizer.repartition_file_min_size = $REPARTITION_FILE_MIN_SIZE;
            config.optimizer.prefer_existing_sort = $BOUNDED_ORDER_PRESERVING_VARIANTS;

            // NOTE: These tests verify the joint `EnforceDistribution` + `EnforceSorting` cascade
            //       because they were written prior to the separation of `BasicEnforcement` into
            //       `EnforceSorting` and `EnforceDistribution`.
            // TODO: Orthogonalize the tests here just to verify `EnforceDistribution` and create
            //       new tests for the cascade.

            // Add the ancillary output requirements operator at the start:
            let optimizer = OutputRequirements::new_add_mode();
            let optimized = optimizer.optimize($PLAN.clone(), &config)?;

            let optimized = if $FIRST_ENFORCE_DIST {
                // Run enforce distribution rule first:
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                // The rule should be idempotent.
                // Re-running this rule shouldn't introduce unnecessary operators.
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                // Run the enforce sorting rule:
                let optimizer = EnforceSorting::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                optimized
            } else {
                // Run the enforce sorting rule first:
                let optimizer = EnforceSorting::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                // Run enforce distribution rule:
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                // The rule should be idempotent.
                // Re-running this rule shouldn't introduce unnecessary operators.
                let optimizer = EnforceDistribution::new();
                let optimized = optimizer.optimize(optimized, &config)?;
                optimized
            };

            // Remove the ancillary output requirements operator when done:
            let optimizer = OutputRequirements::new_remove_mode();
            let optimized = optimizer.optimize(optimized, &config)?;

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    macro_rules! assert_plan_txt {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();
            // Now format correctly
            let plan = displayable($PLAN.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn multi_hash_joins() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
            ("d".to_string(), "d1".to_string()),
            ("e".to_string(), "e1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        // Join on (a == b1)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        for join_type in join_types {
            let join = hash_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let join_plan = format!(
                "HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(a@0, b1@1)]"
            );

            match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    // Join on (a == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("a", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];
                    let top_join = hash_join_exec(
                        join.clone(),
                        parquet_exec(),
                        &top_join_on,
                        &join_type,
                    );
                    let top_join_plan =
                        format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(a@0, c@2)]");

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs
                        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // Should include 4 RepartitionExecs
                        _ => vec![
                            top_join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            join_plan.as_str(),
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                    };
                    assert_optimized!(expected, top_join.clone(), true);
                    assert_optimized!(expected, top_join, false);
                }
                JoinType::RightSemi | JoinType::RightAnti => {}
            }

            match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::RightSemi
                | JoinType::RightAnti => {
                    // This time we use (b1 == c) for top join
                    // Join on (b1 == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("b1", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];

                    let top_join =
                        hash_join_exec(join, parquet_exec(), &top_join_on, &join_type);
                    let top_join_plan = match join_type {
                        JoinType::RightSemi | JoinType::RightAnti =>
                            format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(b1@1, c@2)]"),
                        _ =>
                            format!("HashJoinExec: mode=Partitioned, join_type={join_type}, on=[(b1@6, c@2)]"),
                    };

                    let expected = match join_type {
                        // Should include 3 RepartitionExecs
                        JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti =>
                            vec![
                                top_join_plan.as_str(),
                                join_plan.as_str(),
                                "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            ],
                        // Should include 4 RepartitionExecs
                        _ =>
                            vec![
                                top_join_plan.as_str(),
                                "RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                                join_plan.as_str(),
                                "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                                "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            ],
                    };
                    assert_optimized!(expected, top_join.clone(), true);
                    assert_optimized!(expected, top_join, false);
                }
                JoinType::LeftSemi | JoinType::LeftAnti => {}
            }
        }

        Ok(())
    }

    #[test]
    fn multi_joins_after_alias() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Projection(a as a1, a as a2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("a".to_string(), "a2".to_string()),
        ];
        let projection = projection_exec_with_alias(join, alias_pairs);

        // Join on (a1 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a1", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(
            projection.clone(),
            right.clone(),
            &top_join_on,
            &JoinType::Inner,
        );

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, c@2)]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, top_join.clone(), true);
        assert_optimized!(expected, top_join, false);

        // Join on (a2 == c)
        let top_join_on = vec![(
            Column::new_with_schema("a2", &projection.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection, right, &top_join_on, &JoinType::Inner);

        // Output partition need to respect the Alias and should not introduce additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a2@1, c@2)]",
            "ProjectionExec: expr=[a@0 as a1, a@0 as a2]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join.clone(), true);
        assert_optimized!(expected, top_join, false);
        Ok(())
    }

    #[test]
    fn multi_joins_after_multi_alias() -> Result<()> {
        let left = parquet_exec();
        let right = parquet_exec();

        // Join on (a == b)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b", &schema()).unwrap(),
        )];

        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Projection(c as c1)
        let alias_pairs: Vec<(String, String)> =
            vec![("c".to_string(), "c1".to_string())];
        let projection = projection_exec_with_alias(join, alias_pairs);

        // Projection(c1 as a)
        let alias_pairs: Vec<(String, String)> =
            vec![("c1".to_string(), "a".to_string())];
        let projection2 = projection_exec_with_alias(projection, alias_pairs);

        // Join on (a == c)
        let top_join_on = vec![(
            Column::new_with_schema("a", &projection2.schema()).unwrap(),
            Column::new_with_schema("c", &schema()).unwrap(),
        )];

        let top_join = hash_join_exec(projection2, right, &top_join_on, &JoinType::Inner);

        // The Column 'a' has different meaning now after the two Projections
        // The original Output partition can not satisfy the Join requirements and need to add an additional RepartitionExec
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, c@2)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "ProjectionExec: expr=[c1@0 as a]",
            "ProjectionExec: expr=[c@2 as c1]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@1)]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b@1], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, top_join.clone(), true);
        assert_optimized!(expected, top_join, false);
        Ok(())
    }

    #[test]
    fn join_after_agg_alias() -> Result<()> {
        // group by (a as a1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );
        // group by (a as a2)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a2".to_string())],
        );

        // Join on (a1 == a2)
        let join_on = vec![(
            Column::new_with_schema("a1", &left.schema()).unwrap(),
            Column::new_with_schema("a2", &right.schema()).unwrap(),
        )];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a1@0, a2@0)]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
            "RepartitionExec: partitioning=Hash([a2@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a2], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join.clone(), true);
        assert_optimized!(expected, join, false);
        Ok(())
    }

    #[test]
    fn hash_join_key_ordering() -> Result<()> {
        // group by (a as a1, b as b1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("a".to_string(), "a1".to_string()),
                ("b".to_string(), "b1".to_string()),
            ],
        );
        // group by (b, a)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("b".to_string(), "b".to_string()),
                ("a".to_string(), "a".to_string()),
            ],
        );

        // Join on (b1 == b && a1 == a)
        let join_on = vec![
            (
                Column::new_with_schema("b1", &left.schema()).unwrap(),
                Column::new_with_schema("b", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a1", &left.schema()).unwrap(),
                Column::new_with_schema("a", &right.schema()).unwrap(),
            ),
        ];
        let join = hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b1@1, b@0), (a1@0, a@1)]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join.clone(), true);
        assert_optimized!(expected, join, false);
        Ok(())
    }

    #[test]
    fn multi_hash_join_key_ordering() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // Join on (a == a1 and b == b1 and c == c1)
        let join_on = vec![
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_left_join =
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner);

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let bottom_left_projection =
            projection_exec_with_alias(bottom_left_join, alias_pairs);

        // Join on (c == c1 and b == b1 and a == a1)
        let join_on = vec![
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_right_join =
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("c", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap(),
            ),
        ];

        let top_join = hash_join_exec(
            bottom_left_projection.clone(),
            bottom_right_join,
            &top_join_on,
            &JoinType::Inner,
        );

        let predicate: Arc<dyn PhysicalExpr> = binary(
            col("c", top_join.schema().deref())?,
            Operator::Gt,
            lit(1i64),
            top_join.schema().deref(),
        )?;

        let filter_top_join: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, top_join)?);

        // The bottom joins' join key ordering is adjusted based on the top join. And the top join should not introduce additional RepartitionExec
        let expected = &[
            "FilterExec: c@6 > 1",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(B@2, b1@6), (C@3, c@2), (AA@1, a1@5)]",
            "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]",
            "RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, b1@1), (c@2, c1@2), (a@0, a1@0)]",
            "RepartitionExec: partitioning=Hash([b@1, c@2, a@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "RepartitionExec: partitioning=Hash([b1@1, c1@2, a1@0], 10), input_partitions=10",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, filter_top_join.clone(), true);
        assert_optimized!(expected, filter_top_join, false);
        Ok(())
    }

    #[test]
    fn reorder_join_keys_to_left_input() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // Join on (a == a1 and b == b1 and c == c1)
        let join_on = vec![
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
        ];

        let bottom_left_join = ensure_distribution_helper(
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner),
            10,
            true,
        )?;

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let bottom_left_projection =
            projection_exec_with_alias(bottom_left_join, alias_pairs);

        // Join on (c == c1 and b == b1 and a == a1)
        let join_on = vec![
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_right_join = ensure_distribution_helper(
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner),
            10,
            true,
        )?;

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("c", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap(),
            ),
        ];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let top_join = hash_join_exec(
                bottom_left_projection.clone(),
                bottom_right_join.clone(),
                &top_join_on,
                &join_type,
            );
            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={:?}, on=[(AA@1, a1@5), (B@2, b1@6), (C@3, c@2)]", &join_type);

            let reordered = reorder_join_keys_to_inputs(top_join)?;

            // The top joins' join key ordering is adjusted based on the children inputs.
            let expected = &[
                top_join_plan.as_str(),
                "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1), (c@2, c1@2)]",
                "RepartitionExec: partitioning=Hash([a@0, b@1, c@2], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([a1@0, b1@1, c1@2], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]",
                "RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            ];

            assert_plan_txt!(expected, reordered);
        }

        Ok(())
    }

    #[test]
    fn reorder_join_keys_to_right_input() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // Join on (a == a1 and b == b1)
        let join_on = vec![
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_left_join = ensure_distribution_helper(
            hash_join_exec(left.clone(), right.clone(), &join_on, &JoinType::Inner),
            10,
            true,
        )?;

        // Projection(a as A, a as AA, b as B, c as C)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "A".to_string()),
            ("a".to_string(), "AA".to_string()),
            ("b".to_string(), "B".to_string()),
            ("c".to_string(), "C".to_string()),
        ];
        let bottom_left_projection =
            projection_exec_with_alias(bottom_left_join, alias_pairs);

        // Join on (c == c1 and b == b1 and a == a1)
        let join_on = vec![
            (
                Column::new_with_schema("c", &schema()).unwrap(),
                Column::new_with_schema("c1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("b", &schema()).unwrap(),
                Column::new_with_schema("b1", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a", &schema()).unwrap(),
                Column::new_with_schema("a1", &right.schema()).unwrap(),
            ),
        ];
        let bottom_right_join = ensure_distribution_helper(
            hash_join_exec(left, right.clone(), &join_on, &JoinType::Inner),
            10,
            true,
        )?;

        // Join on (B == b1 and C == c and AA = a1)
        let top_join_on = vec![
            (
                Column::new_with_schema("B", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("b1", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("C", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("c", &bottom_right_join.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("AA", &bottom_left_projection.schema()).unwrap(),
                Column::new_with_schema("a1", &bottom_right_join.schema()).unwrap(),
            ),
        ];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let top_join = hash_join_exec(
                bottom_left_projection.clone(),
                bottom_right_join.clone(),
                &top_join_on,
                &join_type,
            );
            let top_join_plan =
                format!("HashJoinExec: mode=Partitioned, join_type={:?}, on=[(C@3, c@2), (B@2, b1@6), (AA@1, a1@5)]", &join_type);

            let reordered = reorder_join_keys_to_inputs(top_join)?;

            // The top joins' join key ordering is adjusted based on the children inputs.
            let expected = &[
                top_join_plan.as_str(),
                "ProjectionExec: expr=[a@0 as A, a@0 as AA, b@1 as B, c@2 as C]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a1@0), (b@1, b1@1)]",
                "RepartitionExec: partitioning=Hash([a@0, b@1], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([a1@0, b1@1], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c1@2), (b@1, b1@1), (a@0, a1@0)]",
                "RepartitionExec: partitioning=Hash([c@2, b@1, a@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                "RepartitionExec: partitioning=Hash([c1@2, b1@1, a1@0], 10), input_partitions=10",
                "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1]",
                "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            ];

            assert_plan_txt!(expected, reordered);
        }

        Ok(())
    }

    #[test]
    fn multi_smj_joins() -> Result<()> {
        let left = parquet_exec();
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c1".to_string()),
            ("d".to_string(), "d1".to_string()),
            ("e".to_string(), "e1".to_string()),
        ];
        let right = projection_exec_with_alias(parquet_exec(), alias_pairs);

        // SortMergeJoin does not support RightSemi and RightAnti join now
        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];

        // Join on (a == b1)
        let join_on = vec![(
            Column::new_with_schema("a", &schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let join_plan =
                format!("SortMergeJoin: join_type={join_type}, on=[(a@0, b1@1)]");

            // Top join on (a == c)
            let top_join_on = vec![(
                Column::new_with_schema("a", &join.schema()).unwrap(),
                Column::new_with_schema("c", &schema()).unwrap(),
            )];
            let top_join = sort_merge_join_exec(
                join.clone(),
                parquet_exec(),
                &top_join_on,
                &join_type,
            );
            let top_join_plan =
                format!("SortMergeJoin: join_type={join_type}, on=[(a@0, c@2)]");

            let expected = match join_type {
                // Should include 6 RepartitionExecs (3 hash, 3 round-robin), 3 SortExecs
                JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti =>
                    vec![
                        top_join_plan.as_str(),
                        join_plan.as_str(),
                        "SortExec: expr=[a@0 ASC]",
                        "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[b1@1 ASC]",
                        "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[c@2 ASC]",
                        "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    ],
                // Should include 7 RepartitionExecs (4 hash, 3 round-robin), 4 SortExecs
                // Since ordering of the left child is not preserved after SortMergeJoin
                // when mode is Right, RgihtSemi, RightAnti, Full
                // - We need to add one additional SortExec after SortMergeJoin in contrast the test cases
                //   when mode is Inner, Left, LeftSemi, LeftAnti
                // Similarly, since partitioning of the left side is not preserved
                // when mode is Right, RgihtSemi, RightAnti, Full
                // - We need to add one additional Hash Repartition after SortMergeJoin in contrast the test
                //   cases when mode is Inner, Left, LeftSemi, LeftAnti
                _ => vec![
                        top_join_plan.as_str(),
                        // Below 2 operators are differences introduced, when join mode is changed
                        "SortExec: expr=[a@0 ASC]",
                        "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        join_plan.as_str(),
                        "SortExec: expr=[a@0 ASC]",
                        "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[b1@1 ASC]",
                        "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortExec: expr=[c@2 ASC]",
                        "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected, top_join.clone(), true, true);

            let expected_first_sort_enforcement = match join_type {
                // Should include 6 RepartitionExecs (3 hash, 3 round-robin), 3 SortExecs
                JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti =>
                    vec![
                        top_join_plan.as_str(),
                        join_plan.as_str(),
                        "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, sort_exprs=a@0 ASC",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "SortExec: expr=[a@0 ASC]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, sort_exprs=b1@1 ASC",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "SortExec: expr=[b1@1 ASC]",
                        "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, sort_exprs=c@2 ASC",
                        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                        "SortExec: expr=[c@2 ASC]",
                        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    ],
                // Should include 8 RepartitionExecs (4 hash, 8 round-robin), 4 SortExecs
                // Since ordering of the left child is not preserved after SortMergeJoin
                // when mode is Right, RgihtSemi, RightAnti, Full
                // - We need to add one additional SortExec after SortMergeJoin in contrast the test cases
                //   when mode is Inner, Left, LeftSemi, LeftAnti
                // Similarly, since partitioning of the left side is not preserved
                // when mode is Right, RgihtSemi, RightAnti, Full
                // - We need to add one additional Hash Repartition and Roundrobin repartition after
                //   SortMergeJoin in contrast the test cases when mode is Inner, Left, LeftSemi, LeftAnti
                _ => vec![
                    top_join_plan.as_str(),
                    // Below 4 operators are differences introduced, when join mode is changed
                    "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, sort_exprs=a@0 ASC",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[a@0 ASC]",
                    "CoalescePartitionsExec",
                    join_plan.as_str(),
                    "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, sort_exprs=a@0 ASC",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[a@0 ASC]",
                    "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, sort_exprs=b1@1 ASC",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[b1@1 ASC]",
                    "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                    "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                    "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, sort_exprs=c@2 ASC",
                    "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                    "SortExec: expr=[c@2 ASC]",
                    "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                ],
            };
            assert_optimized!(expected_first_sort_enforcement, top_join, false, true);

            match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    // This time we use (b1 == c) for top join
                    // Join on (b1 == c)
                    let top_join_on = vec![(
                        Column::new_with_schema("b1", &join.schema()).unwrap(),
                        Column::new_with_schema("c", &schema()).unwrap(),
                    )];
                    let top_join = sort_merge_join_exec(
                        join,
                        parquet_exec(),
                        &top_join_on,
                        &join_type,
                    );
                    let top_join_plan =
                        format!("SortMergeJoin: join_type={join_type}, on=[(b1@6, c@2)]");

                    let expected = match join_type {
                        // Should include 6 RepartitionExecs(3 hash, 3 round-robin) and 3 SortExecs
                        JoinType::Inner | JoinType::Right => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "SortExec: expr=[a@0 ASC]",
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[b1@1 ASC]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[c@2 ASC]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // Should include 7 RepartitionExecs (4 hash, 3 round-robin) and 4 SortExecs
                        JoinType::Left | JoinType::Full => vec![
                            top_join_plan.as_str(),
                            "SortExec: expr=[b1@6 ASC]",
                            "RepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10",
                            join_plan.as_str(),
                            "SortExec: expr=[a@0 ASC]",
                            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[b1@1 ASC]",
                            "RepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortExec: expr=[c@2 ASC]",
                            "RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // this match arm cannot be reached
                        _ => unreachable!()
                    };
                    assert_optimized!(expected, top_join.clone(), true, true);

                    let expected_first_sort_enforcement = match join_type {
                        // Should include 6 RepartitionExecs (3 of them preserves order) and 3 SortExecs
                        JoinType::Inner | JoinType::Right => vec![
                            top_join_plan.as_str(),
                            join_plan.as_str(),
                            "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, sort_exprs=a@0 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[a@0 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, sort_exprs=b1@1 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[b1@1 ASC]",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, sort_exprs=c@2 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[c@2 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // Should include 8 RepartitionExecs (4 of them preserves order) and 4 SortExecs
                        JoinType::Left | JoinType::Full => vec![
                            top_join_plan.as_str(),
                            "SortPreservingRepartitionExec: partitioning=Hash([b1@6], 10), input_partitions=10, sort_exprs=b1@6 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[b1@6 ASC]",
                            "CoalescePartitionsExec",
                            join_plan.as_str(),
                            "SortPreservingRepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10, sort_exprs=a@0 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[a@0 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([b1@1], 10), input_partitions=10, sort_exprs=b1@1 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[b1@1 ASC]",
                            "ProjectionExec: expr=[a@0 as a1, b@1 as b1, c@2 as c1, d@3 as d1, e@4 as e1]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                            "SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, sort_exprs=c@2 ASC",
                            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
                            "SortExec: expr=[c@2 ASC]",
                            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
                        ],
                        // this match arm cannot be reached
                        _ => unreachable!()
                    };
                    assert_optimized!(
                        expected_first_sort_enforcement,
                        top_join,
                        false,
                        true
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[test]
    fn smj_join_key_ordering() -> Result<()> {
        // group by (a as a1, b as b1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("a".to_string(), "a1".to_string()),
                ("b".to_string(), "b1".to_string()),
            ],
        );
        //Projection(a1 as a3, b1 as b3)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a1".to_string(), "a3".to_string()),
            ("b1".to_string(), "b3".to_string()),
        ];
        let left = projection_exec_with_alias(left, alias_pairs);

        // group by (b, a)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![
                ("b".to_string(), "b".to_string()),
                ("a".to_string(), "a".to_string()),
            ],
        );

        //Projection(a as a2, b as b2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a2".to_string()),
            ("b".to_string(), "b2".to_string()),
        ];
        let right = projection_exec_with_alias(right, alias_pairs);

        // Join on (b3 == b2 && a3 == a2)
        let join_on = vec![
            (
                Column::new_with_schema("b3", &left.schema()).unwrap(),
                Column::new_with_schema("b2", &right.schema()).unwrap(),
            ),
            (
                Column::new_with_schema("a3", &left.schema()).unwrap(),
                Column::new_with_schema("a2", &right.schema()).unwrap(),
            ),
        ];
        let join = sort_merge_join_exec(left, right.clone(), &join_on, &JoinType::Inner);

        // Only two RepartitionExecs added
        let expected = &[
            "SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]",
            "SortExec: expr=[b3@1 ASC,a3@0 ASC]",
            "ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "SortExec: expr=[b2@1 ASC,a2@0 ASC]",
            "ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, join.clone(), true, true);

        let expected_first_sort_enforcement = &[
            "SortMergeJoin: join_type=Inner, on=[(b3@1, b2@1), (a3@0, a2@0)]",
            "SortPreservingRepartitionExec: partitioning=Hash([b3@1, a3@0], 10), input_partitions=10, sort_exprs=b3@1 ASC,a3@0 ASC",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[b3@1 ASC,a3@0 ASC]",
            "CoalescePartitionsExec",
            "ProjectionExec: expr=[a1@0 as a3, b1@1 as b3]",
            "ProjectionExec: expr=[a1@1 as a1, b1@0 as b1]",
            "AggregateExec: mode=FinalPartitioned, gby=[b1@0 as b1, a1@1 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([b1@0, a1@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b1, a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "SortPreservingRepartitionExec: partitioning=Hash([b2@1, a2@0], 10), input_partitions=10, sort_exprs=b2@1 ASC,a2@0 ASC",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[b2@1 ASC,a2@0 ASC]",
            "CoalescePartitionsExec",
            "ProjectionExec: expr=[a@1 as a2, b@0 as b2]",
            "AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, a@1 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([b@0, a@1], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[b@1 as b, a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected_first_sort_enforcement, join, false, true);
        Ok(())
    }

    #[test]
    fn merge_does_not_need_sort() -> Result<()> {
        // see https://github.com/apache/arrow-datafusion/issues/4331
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        // Scan some sorted parquet files
        let exec = parquet_exec_multiple_sorted(vec![sort_key.clone()]);

        // CoalesceBatchesExec to mimic behavior after a filter
        let exec = Arc::new(CoalesceBatchesExec::new(exec, 4096));

        // Merge from multiple parquet files and keep the data sorted
        let exec = Arc::new(SortPreservingMergeExec::new(sort_key, exec));

        // The optimizer should not add an additional SortExec as the
        // data is already sorted
        let expected = &[
            "SortPreservingMergeExec: [a@0 ASC]",
            "CoalesceBatchesExec: target_batch_size=4096",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];
        assert_optimized!(expected, exec, true);
        // In this case preserving ordering through order preserving operators is not desirable
        // (according to flag: bounded_order_preserving_variants)
        // hence in this case ordering lost during CoalescePartitionsExec and re-introduced with
        // SortExec at the top.
        let expected = &[
            "SortExec: expr=[a@0 ASC]",
            "CoalescePartitionsExec",
            "CoalesceBatchesExec: target_batch_size=4096",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];
        assert_optimized!(expected, exec, false);
        Ok(())
    }

    #[test]
    fn union_to_interleave() -> Result<()> {
        // group by (a as a1)
        let left = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );
        // group by (a as a2)
        let right = aggregate_exec_with_alias(
            parquet_exec(),
            vec![("a".to_string(), "a1".to_string())],
        );

        //  Union
        let plan = Arc::new(UnionExec::new(vec![left, right]));

        // final agg
        let plan =
            aggregate_exec_with_alias(plan, vec![("a1".to_string(), "a2".to_string())]);

        // Only two RepartitionExecs added, no final RepartitionExec required
        let expected = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a2@0 as a2], aggr=[]",
            "AggregateExec: mode=Partial, gby=[a1@0 as a2], aggr=[]",
            "InterleaveExec",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "AggregateExec: mode=FinalPartitioned, gby=[a1@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=Hash([a1@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a1], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan = aggregate_exec_with_alias(parquet_exec(), alias);

        let expected = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan = aggregate_exec_with_alias(filter_exec(parquet_exec()), alias);

        let expected = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]

    fn repartition_unsorted_limit() -> Result<()> {
        let plan = limit_exec(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c@2 = 0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan = limit_exec(sort_exec(sort_key, parquet_exec(), false));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit_with_filter() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan =
            sort_required_exec(filter_exec(sort_exec(sort_key, parquet_exec(), false)));

        let expected = &[
            "SortRequiredExec: [c@2 ASC]",
            "FilterExec: c@2 = 0",
            // We can use repartition here, ordering requirement by SortRequiredExec
            // is still satisfied.
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan = aggregate_exec_with_alias(
            limit_exec(filter_exec(limit_exec(parquet_exec()))),
            alias,
        );

        let expected = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c@2 = 0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_union() -> Result<()> {
        let plan = union_exec(vec![parquet_exec(); 5]);

        let expected = &[
            "UnionExec",
            // Expect no repartition of ParquetExec
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_through_sort_preserving_merge() -> Result<()> {
        // sort preserving merge with non-sorted input
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan = sort_preserving_merge_exec(sort_key, parquet_exec());

        // need resort as the data was not sorted correctly
        let expected = &[
            "SortExec: expr=[c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);

        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge() -> Result<()> {
        // sort preserving merge already sorted input,
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan = sort_preserving_merge_exec(
            sort_key.clone(),
            parquet_exec_multiple_sorted(vec![sort_key]),
        );

        // should not sort (as the data was already sorted)
        // should not repartition, since increased parallelism is not beneficial for SortPReservingMerge
        let expected = &[
            "SortPreservingMergeExec: [c@2 ASC]",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected = &[
            "SortExec: expr=[c@2 ASC]",
            "CoalescePartitionsExec",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge_with_union() -> Result<()> {
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = union_exec(vec![parquet_exec_with_sort(vec![sort_key.clone()]); 2]);
        let plan = sort_preserving_merge_exec(sort_key, input);

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c@2 ASC]",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected = &[
            "SortExec: expr=[c@2 ASC]",
            "CoalescePartitionsExec",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort() -> Result<()> {
        //  SortRequired
        //    Parquet(sorted)
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan =
            sort_required_exec(filter_exec(parquet_exec_with_sort(vec![sort_key])));

        // during repartitioning ordering is preserved
        let expected = &[
            "SortRequiredExec: [c@2 ASC]",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true, true);
        assert_optimized!(expected, plan, false, true);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort_more_complex() -> Result<()> {
        // model a more complicated scenario where one child of a union can be repartitioned for performance
        // but the other can not be
        //
        // Union
        //  SortRequired
        //    Parquet(sorted)
        //  Filter
        //    Parquet(unsorted)

        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input1 = sort_required_exec(parquet_exec_with_sort(vec![sort_key]));
        let input2 = filter_exec(parquet_exec());
        let plan = union_exec(vec![input1, input2]);

        // should not repartition below the SortRequired as that
        // branch doesn't benefit from increased parallelism
        let expected = &[
            "UnionExec",
            // union input 1: no repartitioning
            "SortRequiredExec: [c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
            // union input 2: should repartition
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_with_projection() -> Result<()> {
        let schema = schema();
        let proj_exprs = vec![(
            Arc::new(BinaryExpr::new(
                col("a", &schema).unwrap(),
                Operator::Plus,
                col("b", &schema).unwrap(),
            )) as Arc<dyn PhysicalExpr>,
            "sum".to_string(),
        )];
        // non sorted input
        let proj = Arc::new(ProjectionExec::try_new(proj_exprs, parquet_exec())?);
        let sort_key = vec![PhysicalSortExpr {
            expr: col("sum", &proj.schema()).unwrap(),
            options: SortOptions::default(),
        }];
        let plan = sort_preserving_merge_exec(sort_key, proj);

        let expected = &[
            "SortPreservingMergeExec: [sum@0 ASC]",
            "SortExec: expr=[sum@0 ASC]",
            // Since this projection is not trivial, increasing parallelism is beneficial
            "ProjectionExec: expr=[a@0 + b@1 as sum]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortExec: expr=[sum@0 ASC]",
            "CoalescePartitionsExec",
            // Since this projection is not trivial, increasing parallelism is beneficial
            "ProjectionExec: expr=[a@0 + b@1 as sum]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_transitively_with_projection() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let alias = vec![
            ("a".to_string(), "a".to_string()),
            ("b".to_string(), "b".to_string()),
            ("c".to_string(), "c".to_string()),
        ];
        // sorted input
        let plan = sort_required_exec(projection_exec_with_alias(
            parquet_exec_multiple_sorted(vec![sort_key]),
            alias,
        ));

        let expected = &[
            "SortRequiredExec: [c@2 ASC]",
            // Since this projection is trivial, increasing parallelism is not beneficial
            "ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan = sort_preserving_merge_exec(
            sort_key.clone(),
            sort_exec(
                sort_key,
                projection_exec_with_alias(parquet_exec(), alias),
                true,
            ),
        );

        let expected = &[
            "SortExec: expr=[c@2 ASC]",
            // Since this projection is trivial, increasing parallelism is not beneficial
            "ProjectionExec: expr=[a@0 as a]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_filter() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan = sort_exec(sort_key, filter_exec(parquet_exec()), false);

        let expected = &[
            "SortPreservingMergeExec: [a@0 ASC]",
            "SortExec: expr=[a@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortExec: expr=[a@0 ASC]",
            "CoalescePartitionsExec",
            "FilterExec: c@2 = 0",
            // Expect repartition on the input of the filter (as it can benefit from additional parallelism)
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    #[cfg(feature = "parquet")]
    fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan = sort_exec(
            sort_key,
            projection_exec_with_alias(
                filter_exec(parquet_exec()),
                vec![
                    ("a".to_string(), "a".to_string()),
                    ("b".to_string(), "b".to_string()),
                    ("c".to_string(), "c".to_string()),
                ],
            ),
            false,
        );

        let expected = &[
            "SortPreservingMergeExec: [c@2 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: expr=[c@2 ASC]",
            "ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
            "FilterExec: c@2 = 0",
            // repartition is lowest down
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortExec: expr=[c@2 ASC]",
            "CoalescePartitionsExec",
            "ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c]",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    fn parallelization_single_partition() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan_parquet = aggregate_exec_with_alias(parquet_exec(), alias.clone());
        let plan_csv = aggregate_exec_with_alias(csv_exec(), alias);

        let expected_parquet = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "ParquetExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "CsvExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, false, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, false, 2, true, 10);
        Ok(())
    }

    #[test]
    /// CsvExec on compressed csv file will not be partitioned
    /// (Not able to decompress chunked csv file)
    fn parallelization_compressed_csv() -> Result<()> {
        let compression_types = [
            FileCompressionType::GZIP,
            FileCompressionType::BZIP2,
            FileCompressionType::XZ,
            FileCompressionType::ZSTD,
            FileCompressionType::UNCOMPRESSED,
        ];

        let expected_not_partitioned = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        let expected_partitioned = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "CsvExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        for compression_type in compression_types {
            let expected = if compression_type.is_compressed() {
                &expected_not_partitioned[..]
            } else {
                &expected_partitioned[..]
            };

            let plan = aggregate_exec_with_alias(
                Arc::new(CsvExec::new(
                    FileScanConfig {
                        object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                        file_schema: schema(),
                        file_groups: vec![vec![PartitionedFile::new(
                            "x".to_string(),
                            100,
                        )]],
                        statistics: Statistics::new_unknown(&schema()),
                        projection: None,
                        limit: None,
                        table_partition_cols: vec![],
                        output_ordering: vec![],
                        infinite_source: false,
                    },
                    false,
                    b',',
                    b'"',
                    None,
                    compression_type,
                )),
                vec![("a".to_string(), "a".to_string())],
            );

            assert_optimized!(expected, plan, true, false, 2, true, 10);
        }
        Ok(())
    }

    #[test]
    fn parallelization_two_partitions() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan_parquet =
            aggregate_exec_with_alias(parquet_exec_multiple(), alias.clone());
        let plan_csv = aggregate_exec_with_alias(csv_exec_multiple(), alias.clone());

        let expected_parquet = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            // Plan already has two partitions
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 2), input_partitions=2",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            // Plan already has two partitions
            "CsvExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, false, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, false, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_two_partitions_into_four() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan_parquet =
            aggregate_exec_with_alias(parquet_exec_multiple(), alias.clone());
        let plan_csv = aggregate_exec_with_alias(csv_exec_multiple(), alias.clone());

        let expected_parquet = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            // Multiple source files splitted across partitions
            "ParquetExec: file_groups={4 groups: [[x:0..50], [x:50..100], [y:0..50], [y:50..100]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = [
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            // Multiple source files splitted across partitions
            "CsvExec: file_groups={4 groups: [[x:0..50], [x:50..100], [y:0..50], [y:50..100]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, false, 4, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, false, 4, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_sorted_limit() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan_parquet = limit_exec(sort_exec(sort_key.clone(), parquet_exec(), false));
        let plan_csv = limit_exec(sort_exec(sort_key.clone(), csv_exec(), false));

        let expected_parquet = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c@2 ASC]",
            // Doesn't parallelize for SortExec without preserve_partitioning
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c@2 ASC]",
            // Doesn't parallelize for SortExec without preserve_partitioning
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_limit_with_filter() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let plan_parquet = limit_exec(filter_exec(sort_exec(
            sort_key.clone(),
            parquet_exec(),
            false,
        )));
        let plan_csv =
            limit_exec(filter_exec(sort_exec(sort_key.clone(), csv_exec(), false)));

        let expected_parquet = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c@2 = 0",
            // even though data is sorted, we can use repartition here. Since
            // ordering is not used in subsequent stages anyway.
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[c@2 ASC]",
            // SortExec doesn't benefit from input partitioning
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c@2 = 0",
            // even though data is sorted, we can use repartition here. Since
            // ordering is not used in subsequent stages anyway.
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[c@2 ASC]",
            // SortExec doesn't benefit from input partitioning
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_limit() -> Result<()> {
        let alias = vec![("a".to_string(), "a".to_string())];
        let plan_parquet = aggregate_exec_with_alias(
            limit_exec(filter_exec(limit_exec(parquet_exec()))),
            alias.clone(),
        );
        let plan_csv = aggregate_exec_with_alias(
            limit_exec(filter_exec(limit_exec(csv_exec()))),
            alias.clone(),
        );

        let expected_parquet = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c@2 = 0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            // Limit doesn't benefit from input partitioning - no parallelism
            "LocalLimitExec: fetch=100",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=10",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c@2 = 0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            // Limit doesn't benefit from input partitioning - no parallelism
            "LocalLimitExec: fetch=100",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_union_inputs() -> Result<()> {
        let plan_parquet = union_exec(vec![parquet_exec(); 5]);
        let plan_csv = union_exec(vec![csv_exec(); 5]);

        let expected_parquet = &[
            "UnionExec",
            // Union doesn't benefit from input partitioning - no parallelism
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        let expected_csv = &[
            "UnionExec",
            // Union doesn't benefit from input partitioning - no parallelism
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_prior_to_sort_preserving_merge() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        // sort preserving merge already sorted input,
        let plan_parquet = sort_preserving_merge_exec(
            sort_key.clone(),
            parquet_exec_with_sort(vec![sort_key.clone()]),
        );
        let plan_csv = sort_preserving_merge_exec(
            sort_key.clone(),
            csv_exec_with_sort(vec![sort_key.clone()]),
        );

        // parallelization is not beneficial for SortPreservingMerge
        let expected_parquet = &[
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        let expected_csv = &[
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_sort_preserving_merge_with_union() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let input_parquet =
            union_exec(vec![parquet_exec_with_sort(vec![sort_key.clone()]); 2]);
        let input_csv = union_exec(vec![csv_exec_with_sort(vec![sort_key.clone()]); 2]);
        let plan_parquet = sort_preserving_merge_exec(sort_key.clone(), input_parquet);
        let plan_csv = sort_preserving_merge_exec(sort_key.clone(), input_csv);

        // should not repartition (union doesn't benefit from increased parallelism)
        // should not sort (as the data was already sorted)
        let expected_parquet = &[
            "SortPreservingMergeExec: [c@2 ASC]",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        let expected_csv = &[
            "SortPreservingMergeExec: [c@2 ASC]",
            "UnionExec",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_does_not_benefit() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        //  SortRequired
        //    Parquet(sorted)
        let plan_parquet =
            sort_required_exec(parquet_exec_with_sort(vec![sort_key.clone()]));
        let plan_csv = sort_required_exec(csv_exec_with_sort(vec![sort_key]));

        // no parallelization, because SortRequiredExec doesn't benefit from increased parallelism
        let expected_parquet = &[
            "SortRequiredExec: [c@2 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        let expected_csv = &[
            "SortRequiredExec: [c@2 ASC]",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_transitively_with_projection_parquet() -> Result<()> {
        // sorted input
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        //Projection(a as a2, b as b2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a2".to_string()),
            ("c".to_string(), "c2".to_string()),
        ];
        let proj_parquet = projection_exec_with_alias(
            parquet_exec_with_sort(vec![sort_key.clone()]),
            alias_pairs.clone(),
        );
        let sort_key_after_projection = vec![PhysicalSortExpr {
            expr: col("c2", &proj_parquet.schema()).unwrap(),
            options: SortOptions::default(),
        }];
        let plan_parquet =
            sort_preserving_merge_exec(sort_key_after_projection, proj_parquet);
        let expected = &[
            "SortPreservingMergeExec: [c2@1 ASC]",
            "  ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        plans_matches_expected!(expected, &plan_parquet);

        // data should not be repartitioned / resorted
        let expected_parquet = &[
            "ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_transitively_with_projection_csv() -> Result<()> {
        // sorted input
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        //Projection(a as a2, b as b2)
        let alias_pairs: Vec<(String, String)> = vec![
            ("a".to_string(), "a2".to_string()),
            ("c".to_string(), "c2".to_string()),
        ];

        let proj_csv =
            projection_exec_with_alias(csv_exec_with_sort(vec![sort_key]), alias_pairs);
        let sort_key_after_projection = vec![PhysicalSortExpr {
            expr: col("c2", &proj_csv.schema()).unwrap(),
            options: SortOptions::default(),
        }];
        let plan_csv = sort_preserving_merge_exec(sort_key_after_projection, proj_csv);
        let expected = &[
            "SortPreservingMergeExec: [c2@1 ASC]",
            "  ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], has_header=false",
        ];
        plans_matches_expected!(expected, &plan_csv);

        // data should not be repartitioned / resorted
        let expected_csv = &[
            "ProjectionExec: expr=[a@0 as a2, c@2 as c2]",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC], has_header=false",
        ];

        assert_optimized!(expected_csv, plan_csv, true);
        Ok(())
    }

    #[test]
    fn remove_redundant_roundrobins() -> Result<()> {
        let input = parquet_exec();
        let repartition = repartition_exec(repartition_exec(input));
        let physical_plan = repartition_exec(filter_exec(repartition));
        let expected = &[
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  FilterExec: c@2 = 0",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        plans_matches_expected!(expected, &physical_plan);

        let expected = &[
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, physical_plan.clone(), true);
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }

    #[test]
    fn preserve_ordering_through_repartition() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
        let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

        let expected = &[
            "SortPreservingMergeExec: [c@2 ASC]",
            "FilterExec: c@2 = 0",
            "SortPreservingRepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, sort_exprs=c@2 ASC",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        // last flag sets config.optimizer.bounded_order_preserving_variants
        assert_optimized!(expected, physical_plan.clone(), true, true);
        assert_optimized!(expected, physical_plan, false, true);

        Ok(())
    }

    #[test]
    fn do_not_preserve_ordering_through_repartition() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
        let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

        let expected = &[
            "SortPreservingMergeExec: [a@0 ASC]",
            "SortExec: expr=[a@0 ASC]",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];

        assert_optimized!(expected, physical_plan.clone(), true);

        let expected = &[
            "SortExec: expr=[a@0 ASC]",
            "CoalescePartitionsExec",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }

    #[test]
    fn no_need_for_sort_after_filter() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
        let physical_plan = sort_preserving_merge_exec(sort_key, filter_exec(input));

        let expected = &[
            // After CoalescePartitionsExec c is still constant. Hence c@2 ASC ordering is already satisfied.
            "CoalescePartitionsExec",
            // Since after this stage c is constant. c@2 ASC ordering is already satisfied.
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, physical_plan.clone(), true);
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }

    #[test]
    fn do_not_preserve_ordering_through_repartition2() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec_multiple_sorted(vec![sort_key]);

        let sort_req = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let physical_plan = sort_preserving_merge_exec(sort_req, filter_exec(input));

        let expected = &[
            "SortPreservingMergeExec: [a@0 ASC]",
            "SortExec: expr=[a@0 ASC]",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, physical_plan.clone(), true);

        let expected = &[
            "SortExec: expr=[a@0 ASC]",
            "CoalescePartitionsExec",
            "SortExec: expr=[a@0 ASC]",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }

    #[test]
    fn do_not_preserve_ordering_through_repartition3() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec_multiple_sorted(vec![sort_key]);
        let physical_plan = filter_exec(input);

        let expected = &[
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        assert_optimized!(expected, physical_plan.clone(), true);
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }

    #[test]
    fn do_not_put_sort_when_input_is_invalid() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec();
        let physical_plan = sort_required_exec_with_req(filter_exec(input), sort_key);
        let expected = &[
            // Ordering requirement of sort required exec is NOT satisfied
            // by existing ordering at the source.
            "SortRequiredExec: [a@0 ASC]",
            "FilterExec: c@2 = 0",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        assert_plan_txt!(expected, physical_plan);

        let expected = &[
            "SortRequiredExec: [a@0 ASC]",
            // Since at the start of the rule ordering requirement is not satisfied
            // EnforceDistribution rule doesn't satisfy this requirement either.
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        config.optimizer.enable_round_robin_repartition = true;
        config.optimizer.prefer_existing_sort = false;
        let distribution_plan =
            EnforceDistribution::new().optimize(physical_plan, &config)?;
        assert_plan_txt!(expected, distribution_plan);

        Ok(())
    }

    #[test]
    fn put_sort_when_input_is_valid() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("a", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let input = parquet_exec_multiple_sorted(vec![sort_key.clone()]);
        let physical_plan = sort_required_exec_with_req(filter_exec(input), sort_key);

        let expected = &[
            // Ordering requirement of sort required exec is satisfied
            // by existing ordering at the source.
            "SortRequiredExec: [a@0 ASC]",
            "FilterExec: c@2 = 0",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];
        assert_plan_txt!(expected, physical_plan);

        let expected = &[
            "SortRequiredExec: [a@0 ASC]",
            // Since at the start of the rule ordering requirement is satisfied
            // EnforceDistribution rule satisfy this requirement also.
            // ordering is re-satisfied by introduction of SortExec.
            "SortExec: expr=[a@0 ASC]",
            "FilterExec: c@2 = 0",
            // ordering is lost here
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];

        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        config.optimizer.enable_round_robin_repartition = true;
        config.optimizer.prefer_existing_sort = false;
        let distribution_plan =
            EnforceDistribution::new().optimize(physical_plan, &config)?;
        assert_plan_txt!(expected, distribution_plan);

        Ok(())
    }

    #[test]
    fn do_not_add_unnecessary_hash() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let alias = vec![("a".to_string(), "a".to_string())];
        let input = parquet_exec_with_sort(vec![sort_key]);
        let physical_plan = aggregate_exec_with_alias(input, alias.clone());

        let expected = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        // Make sure target partition number is 1. In this case hash repartition is unnecessary
        assert_optimized!(expected, physical_plan.clone(), true, false, 1, false, 1024);
        assert_optimized!(expected, physical_plan, false, false, 1, false, 1024);

        Ok(())
    }

    #[test]
    fn do_not_add_unnecessary_hash2() -> Result<()> {
        let schema = schema();
        let sort_key = vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let alias = vec![("a".to_string(), "a".to_string())];
        let input = parquet_exec_multiple_sorted(vec![sort_key]);
        let aggregate = aggregate_exec_with_alias(input, alias.clone());
        let physical_plan = aggregate_exec_with_alias(aggregate, alias.clone());

        let expected = &[
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            // Since hash requirements of this operator is satisfied. There shouldn't be
            // a hash repartition here
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=4",
            "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[a, b, c, d, e], output_ordering=[c@2 ASC]",
        ];

        // Make sure target partition number is larger than 2 (e.g partition number at the source).
        assert_optimized!(expected, physical_plan.clone(), true, false, 4, false, 1024);
        assert_optimized!(expected, physical_plan, false, false, 4, false, 1024);

        Ok(())
    }

    #[test]
    fn optimize_away_unnecessary_repartition() -> Result<()> {
        let physical_plan = coalesce_partitions_exec(repartition_exec(parquet_exec()));
        let expected = &[
            "CoalescePartitionsExec",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        plans_matches_expected!(expected, physical_plan.clone());

        let expected =
            &["ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]"];

        assert_optimized!(expected, physical_plan.clone(), true);
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }

    #[test]
    fn optimize_away_unnecessary_repartition2() -> Result<()> {
        let physical_plan = filter_exec(repartition_exec(coalesce_partitions_exec(
            filter_exec(repartition_exec(parquet_exec())),
        )));
        let expected = &[
            "FilterExec: c@2 = 0",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    CoalescePartitionsExec",
            "      FilterExec: c@2 = 0",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];
        plans_matches_expected!(expected, physical_plan.clone());

        let expected = &[
            "FilterExec: c@2 = 0",
            "FilterExec: c@2 = 0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e]",
        ];

        assert_optimized!(expected, physical_plan.clone(), true);
        assert_optimized!(expected, physical_plan, false);

        Ok(())
    }
}
