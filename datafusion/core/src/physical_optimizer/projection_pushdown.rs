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

//! This file implements the `ProjectionPushdown` physical optimization rule.
//! The function [`remove_unnecessary_projections`] tries to push down all
//! projections one by one if the operator below is amenable to this. If a
//! projection reaches a source, it can even dissappear from the plan entirely.

use std::sync::Arc;

use super::output_requirements::OutputRequirementExec;
use super::PhysicalOptimizerRule;
use crate::datasource::physical_plan::CsvExec;
use crate::error::Result;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
    SymmetricHashJoinExec,
};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::{Distribution, ExecutionPlan};

use arrow_schema::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::JoinSide;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    Partitioning, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_physical_plan::streaming::StreamingTableExec;
use datafusion_physical_plan::union::UnionExec;

use itertools::Itertools;

/// This rule inspects [`ProjectionExec`]'s in the given physical plan and tries to
/// remove or swap with its child.
#[derive(Default)]
pub struct ProjectionPushdown {}

impl ProjectionPushdown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for ProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&remove_unnecessary_projections)
    }

    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function checks if `plan` is a [`ProjectionExec`], and inspects its
/// input(s) to test whether it can push `plan` under its input(s). This function
/// will operate on the entire tree and may ultimately remove `plan` entirely
/// by leveraging source providers with built-in projection capabilities.
pub fn remove_unnecessary_projections(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let maybe_modified = if let Some(projection) =
        plan.as_any().downcast_ref::<ProjectionExec>()
    {
        // If the projection does not cause any change on the input, we can
        // safely remove it:
        if is_projection_removable(projection) {
            return Ok(Transformed::Yes(projection.input().clone()));
        }
        // If it does, check if we can push it under its child(ren):
        let input = projection.input().as_any();
        if let Some(csv) = input.downcast_ref::<CsvExec>() {
            try_swapping_with_csv(projection, csv)
        } else if let Some(memory) = input.downcast_ref::<MemoryExec>() {
            try_swapping_with_memory(projection, memory)?
        } else if let Some(child_projection) = input.downcast_ref::<ProjectionExec>() {
            let maybe_unified = try_unifying_projections(projection, child_projection)?;
            return if let Some(new_plan) = maybe_unified {
                // To unify 3 or more sequential projections:
                remove_unnecessary_projections(new_plan)
            } else {
                Ok(Transformed::No(plan))
            };
        } else if let Some(output_req) = input.downcast_ref::<OutputRequirementExec>() {
            try_swapping_with_output_req(projection, output_req)?
        } else if input.is::<CoalescePartitionsExec>() {
            try_swapping_with_coalesce_partitions(projection)?
        } else if let Some(filter) = input.downcast_ref::<FilterExec>() {
            try_swapping_with_filter(projection, filter)?
        } else if let Some(repartition) = input.downcast_ref::<RepartitionExec>() {
            try_swapping_with_repartition(projection, repartition)?
        } else if let Some(sort) = input.downcast_ref::<SortExec>() {
            try_swapping_with_sort(projection, sort)?
        } else if let Some(spm) = input.downcast_ref::<SortPreservingMergeExec>() {
            try_swapping_with_sort_preserving_merge(projection, spm)?
        } else if let Some(union) = input.downcast_ref::<UnionExec>() {
            try_pushdown_through_union(projection, union)?
        } else if let Some(hash_join) = input.downcast_ref::<HashJoinExec>() {
            try_pushdown_through_hash_join(projection, hash_join)?
        } else if let Some(cross_join) = input.downcast_ref::<CrossJoinExec>() {
            try_swapping_with_cross_join(projection, cross_join)?
        } else if let Some(nl_join) = input.downcast_ref::<NestedLoopJoinExec>() {
            try_swapping_with_nested_loop_join(projection, nl_join)?
        } else if let Some(sm_join) = input.downcast_ref::<SortMergeJoinExec>() {
            try_swapping_with_sort_merge_join(projection, sm_join)?
        } else if let Some(sym_join) = input.downcast_ref::<SymmetricHashJoinExec>() {
            try_swapping_with_sym_hash_join(projection, sym_join)?
        } else if let Some(ste) = input.downcast_ref::<StreamingTableExec>() {
            try_swapping_with_streaming_table(projection, ste)?
        } else {
            // If the input plan of the projection is not one of the above, we
            // conservatively assume that pushing the projection down may hurt.
            // When adding new operators, consider adding them here if you
            // think pushing projections under them is beneficial.
            None
        }
    } else {
        return Ok(Transformed::No(plan));
    };

    Ok(maybe_modified.map_or(Transformed::No(plan), Transformed::Yes))
}

/// Tries to embed `projection` to its input (`csv`). If possible, returns
/// [`CsvExec`] as the top plan. Otherwise, returns `None`.
fn try_swapping_with_csv(
    projection: &ProjectionExec,
    csv: &CsvExec,
) -> Option<Arc<dyn ExecutionPlan>> {
    // If there is any non-column or alias-carrier expression, Projection should not be removed.
    // This process can be moved into CsvExec, but it would be an overlap of their responsibility.
    all_alias_free_columns(projection.expr()).then(|| {
        let mut file_scan = csv.base_config().clone();
        let new_projections =
            new_projections_for_columns(projection, &file_scan.projection);
        file_scan.projection = Some(new_projections);

        Arc::new(CsvExec::new(
            file_scan,
            csv.has_header(),
            csv.delimiter(),
            csv.quote(),
            csv.escape(),
            csv.file_compression_type,
        )) as _
    })
}

/// Tries to embed `projection` to its input (`memory`). If possible, returns
/// [`MemoryExec`] as the top plan. Otherwise, returns `None`.
fn try_swapping_with_memory(
    projection: &ProjectionExec,
    memory: &MemoryExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If there is any non-column or alias-carrier expression, Projection should not be removed.
    // This process can be moved into MemoryExec, but it would be an overlap of their responsibility.
    all_alias_free_columns(projection.expr())
        .then(|| {
            let new_projections =
                new_projections_for_columns(projection, memory.projection());

            MemoryExec::try_new(
                memory.partitions(),
                memory.original_schema(),
                Some(new_projections),
            )
            .map(|e| Arc::new(e) as _)
        })
        .transpose()
}

/// Tries to embed `projection` to its input (`streaming table`).
/// If possible, returns [`StreamingTableExec`] as the top plan. Otherwise,
/// returns `None`.
fn try_swapping_with_streaming_table(
    projection: &ProjectionExec,
    streaming_table: &StreamingTableExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if !all_alias_free_columns(projection.expr()) {
        return Ok(None);
    }

    let streaming_table_projections = streaming_table
        .projection()
        .as_ref()
        .map(|i| i.as_ref().to_vec());
    let new_projections =
        new_projections_for_columns(projection, &streaming_table_projections);

    let mut lex_orderings = vec![];
    for lex_ordering in streaming_table.projected_output_ordering().into_iter() {
        let mut orderings = vec![];
        for order in lex_ordering {
            let Some(new_ordering) = update_expr(&order.expr, projection.expr(), false)?
            else {
                return Ok(None);
            };
            orderings.push(PhysicalSortExpr {
                expr: new_ordering,
                options: order.options,
            });
        }
        lex_orderings.push(orderings);
    }

    StreamingTableExec::try_new(
        streaming_table.partition_schema().clone(),
        streaming_table.partitions().clone(),
        Some(&new_projections),
        lex_orderings,
        streaming_table.is_infinite(),
    )
    .map(|e| Some(Arc::new(e) as _))
}

/// Unifies `projection` with its input (which is also a [`ProjectionExec`]).
/// Two consecutive projections can always merge into a single projection.
fn try_unifying_projections(
    projection: &ProjectionExec,
    child: &ProjectionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut projected_exprs = vec![];
    for (expr, alias) in projection.expr() {
        // If there is no match in the input projection, we cannot unify these
        // projections. This case will arise if the projection expression contains
        // a `PhysicalExpr` variant `update_expr` doesn't support.
        let Some(expr) = update_expr(expr, child.expr(), true)? else {
            return Ok(None);
        };
        projected_exprs.push((expr, alias.clone()));
    }

    ProjectionExec::try_new(projected_exprs, child.input().clone())
        .map(|e| Some(Arc::new(e) as _))
}

/// Tries to swap `projection` with its input (`output_req`). If possible,
/// performs the swap and returns [`OutputRequirementExec`] as the top plan.
/// Otherwise, returns `None`.
fn try_swapping_with_output_req(
    projection: &ProjectionExec,
    output_req: &OutputRequirementExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection does not narrow the the schema, we should not try to push it down:
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }

    let mut updated_sort_reqs = vec![];
    // None or empty_vec can be treated in the same way.
    if let Some(reqs) = &output_req.required_input_ordering()[0] {
        for req in reqs {
            let Some(new_expr) = update_expr(&req.expr, projection.expr(), false)? else {
                return Ok(None);
            };
            updated_sort_reqs.push(PhysicalSortRequirement {
                expr: new_expr,
                options: req.options,
            });
        }
    }

    let dist_req = match &output_req.required_input_distribution()[0] {
        Distribution::HashPartitioned(exprs) => {
            let mut updated_exprs = vec![];
            for expr in exprs {
                let Some(new_expr) = update_expr(expr, projection.expr(), false)? else {
                    return Ok(None);
                };
                updated_exprs.push(new_expr);
            }
            Distribution::HashPartitioned(updated_exprs)
        }
        dist => dist.clone(),
    };

    make_with_child(projection, &output_req.input())
        .map(|input| {
            OutputRequirementExec::new(
                input,
                (!updated_sort_reqs.is_empty()).then_some(updated_sort_reqs),
                dist_req,
            )
        })
        .map(|e| Some(Arc::new(e) as _))
}

/// Tries to swap `projection` with its input, which is known to be a
/// [`CoalescePartitionsExec`]. If possible, performs the swap and returns
/// [`CoalescePartitionsExec`] as the top plan. Otherwise, returns `None`.
fn try_swapping_with_coalesce_partitions(
    projection: &ProjectionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection does not narrow the the schema, we should not try to push it down:
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }
    // CoalescePartitionsExec always has a single child, so zero indexing is safe.
    make_with_child(projection, &projection.input().children()[0])
        .map(|e| Some(Arc::new(CoalescePartitionsExec::new(e)) as _))
}

/// Tries to swap `projection` with its input (`filter`). If possible, performs
/// the swap and returns [`FilterExec`] as the top plan. Otherwise, returns `None`.
fn try_swapping_with_filter(
    projection: &ProjectionExec,
    filter: &FilterExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection does not narrow the the schema, we should not try to push it down:
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }
    // Each column in the predicate expression must exist after the projection.
    let Some(new_predicate) = update_expr(filter.predicate(), projection.expr(), false)?
    else {
        return Ok(None);
    };

    // TODO: fix projection
    FilterExec::try_new(
        new_predicate,
        None,
        make_with_child(projection, filter.input())?,
    )
    .map(|e| Some(Arc::new(e) as _))
}

/// Tries to swap the projection with its input [`RepartitionExec`]. If it can be done,
/// it returns the new swapped version having the [`RepartitionExec`] as the top plan.
/// Otherwise, it returns None.
fn try_swapping_with_repartition(
    projection: &ProjectionExec,
    repartition: &RepartitionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection does not narrow the the schema, we should not try to push it down.
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }

    // If pushdown is not beneficial or applicable, break it.
    if projection.benefits_from_input_partitioning()[0] || !all_columns(projection.expr())
    {
        return Ok(None);
    }

    let new_projection = make_with_child(projection, repartition.input())?;

    let new_partitioning = match repartition.partitioning() {
        Partitioning::Hash(partitions, size) => {
            let mut new_partitions = vec![];
            for partition in partitions {
                let Some(new_partition) =
                    update_expr(partition, projection.expr(), false)?
                else {
                    return Ok(None);
                };
                new_partitions.push(new_partition);
            }
            Partitioning::Hash(new_partitions, *size)
        }
        others => others.clone(),
    };

    Ok(Some(Arc::new(RepartitionExec::try_new(
        new_projection,
        new_partitioning,
    )?)))
}

/// Tries to swap the projection with its input [`SortExec`]. If it can be done,
/// it returns the new swapped version having the [`SortExec`] as the top plan.
/// Otherwise, it returns None.
fn try_swapping_with_sort(
    projection: &ProjectionExec,
    sort: &SortExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection does not narrow the the schema, we should not try to push it down.
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }

    let mut updated_exprs = vec![];
    for sort in sort.expr() {
        let Some(new_expr) = update_expr(&sort.expr, projection.expr(), false)? else {
            return Ok(None);
        };
        updated_exprs.push(PhysicalSortExpr {
            expr: new_expr,
            options: sort.options,
        });
    }

    Ok(Some(Arc::new(
        SortExec::new(updated_exprs, make_with_child(projection, sort.input())?)
            .with_fetch(sort.fetch()),
    )))
}

/// Tries to swap the projection with its input [`SortPreservingMergeExec`].
/// If this is possible, it returns the new [`SortPreservingMergeExec`] whose
/// child is a projection. Otherwise, it returns None.
fn try_swapping_with_sort_preserving_merge(
    projection: &ProjectionExec,
    spm: &SortPreservingMergeExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection does not narrow the the schema, we should not try to push it down.
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }

    let mut updated_exprs = vec![];
    for sort in spm.expr() {
        let Some(updated_expr) = update_expr(&sort.expr, projection.expr(), false)?
        else {
            return Ok(None);
        };
        updated_exprs.push(PhysicalSortExpr {
            expr: updated_expr,
            options: sort.options,
        });
    }

    Ok(Some(Arc::new(
        SortPreservingMergeExec::new(
            updated_exprs,
            make_with_child(projection, spm.input())?,
        )
        .with_fetch(spm.fetch()),
    )))
}

/// Tries to push `projection` down through `union`. If possible, performs the
/// pushdown and returns a new [`UnionExec`] as the top plan which has projections
/// as its children. Otherwise, returns `None`.
fn try_pushdown_through_union(
    projection: &ProjectionExec,
    union: &UnionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // If the projection doesn't narrow the schema, we shouldn't try to push it down.
    if projection.expr().len() >= projection.input().schema().fields().len() {
        return Ok(None);
    }

    let new_children = union
        .children()
        .into_iter()
        .map(|child| make_with_child(projection, &child))
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(Arc::new(UnionExec::new(new_children))))
}

/// Tries to push `projection` down through `hash_join`. If possible, performs the
/// pushdown and returns a new [`HashJoinExec`] as the top plan which has projections
/// as its children. Otherwise, returns `None`.
fn try_pushdown_through_hash_join(
    projection: &ProjectionExec,
    hash_join: &HashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Convert projected expressions to columns. We can not proceed if this is
    // not possible.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
        hash_join.left().schema().fields().len(),
        &projection_as_columns,
    );

    if !join_allows_pushdown(
        &projection_as_columns,
        hash_join.schema(),
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let Some(new_on) = update_join_on(
        &projection_as_columns[0..=far_right_left_col_ind as _],
        &projection_as_columns[far_left_right_col_ind as _..],
        hash_join.on(),
    ) else {
        return Ok(None);
    };

    let new_filter = if let Some(filter) = hash_join.filter() {
        match update_join_filter(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            filter,
            hash_join.left(),
            hash_join.right(),
        ) {
            Some(updated_filter) => Some(updated_filter),
            None => return Ok(None),
        }
    } else {
        None
    };

    let (new_left, new_right) = new_join_children(
        projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        hash_join.left(),
        hash_join.right(),
    )?;

    Ok(Some(Arc::new(HashJoinExec::try_new(
        Arc::new(new_left),
        Arc::new(new_right),
        new_on,
        new_filter,
        hash_join.join_type(),
        *hash_join.partition_mode(),
        hash_join.null_equals_null,
    )?)))
}

/// Tries to swap the projection with its input [`CrossJoinExec`]. If it can be done,
/// it returns the new swapped version having the [`CrossJoinExec`] as the top plan.
/// Otherwise, it returns None.
fn try_swapping_with_cross_join(
    projection: &ProjectionExec,
    cross_join: &CrossJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
        cross_join.left().schema().fields().len(),
        &projection_as_columns,
    );

    if !join_allows_pushdown(
        &projection_as_columns,
        cross_join.schema(),
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let (new_left, new_right) = new_join_children(
        projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        cross_join.left(),
        cross_join.right(),
    )?;

    Ok(Some(Arc::new(CrossJoinExec::new(
        Arc::new(new_left),
        Arc::new(new_right),
    ))))
}

/// Tries to swap the projection with its input [`NestedLoopJoinExec`]. If it can be done,
/// it returns the new swapped version having the [`NestedLoopJoinExec`] as the top plan.
/// Otherwise, it returns None.
fn try_swapping_with_nested_loop_join(
    projection: &ProjectionExec,
    nl_join: &NestedLoopJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
        nl_join.left().schema().fields().len(),
        &projection_as_columns,
    );

    if !join_allows_pushdown(
        &projection_as_columns,
        nl_join.schema(),
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let new_filter = if let Some(filter) = nl_join.filter() {
        match update_join_filter(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            filter,
            nl_join.left(),
            nl_join.right(),
        ) {
            Some(updated_filter) => Some(updated_filter),
            None => return Ok(None),
        }
    } else {
        None
    };

    let (new_left, new_right) = new_join_children(
        projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        nl_join.left(),
        nl_join.right(),
    )?;

    Ok(Some(Arc::new(NestedLoopJoinExec::try_new(
        Arc::new(new_left),
        Arc::new(new_right),
        new_filter,
        nl_join.join_type(),
    )?)))
}

/// Tries to swap the projection with its input [`SortMergeJoinExec`]. If it can be done,
/// it returns the new swapped version having the [`SortMergeJoinExec`] as the top plan.
/// Otherwise, it returns None.
fn try_swapping_with_sort_merge_join(
    projection: &ProjectionExec,
    sm_join: &SortMergeJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
        sm_join.left().schema().fields().len(),
        &projection_as_columns,
    );

    if !join_allows_pushdown(
        &projection_as_columns,
        sm_join.schema(),
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let Some(new_on) = update_join_on(
        &projection_as_columns[0..=far_right_left_col_ind as _],
        &projection_as_columns[far_left_right_col_ind as _..],
        sm_join.on(),
    ) else {
        return Ok(None);
    };

    let (new_left, new_right) = new_join_children(
        projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        &sm_join.children()[0],
        &sm_join.children()[1],
    )?;

    Ok(Some(Arc::new(SortMergeJoinExec::try_new(
        Arc::new(new_left),
        Arc::new(new_right),
        new_on,
        sm_join.join_type,
        sm_join.sort_options.clone(),
        sm_join.null_equals_null,
    )?)))
}

/// Tries to swap the projection with its input [`SymmetricHashJoinExec`]. If it can be done,
/// it returns the new swapped version having the [`SymmetricHashJoinExec`] as the top plan.
/// Otherwise, it returns None.
fn try_swapping_with_sym_hash_join(
    projection: &ProjectionExec,
    sym_join: &SymmetricHashJoinExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
        sym_join.left().schema().fields().len(),
        &projection_as_columns,
    );

    if !join_allows_pushdown(
        &projection_as_columns,
        sym_join.schema(),
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let Some(new_on) = update_join_on(
        &projection_as_columns[0..=far_right_left_col_ind as _],
        &projection_as_columns[far_left_right_col_ind as _..],
        sym_join.on(),
    ) else {
        return Ok(None);
    };

    let new_filter = if let Some(filter) = sym_join.filter() {
        match update_join_filter(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            filter,
            sym_join.left(),
            sym_join.right(),
        ) {
            Some(updated_filter) => Some(updated_filter),
            None => return Ok(None),
        }
    } else {
        None
    };

    let (new_left, new_right) = new_join_children(
        projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        sym_join.left(),
        sym_join.right(),
    )?;

    Ok(Some(Arc::new(SymmetricHashJoinExec::try_new(
        Arc::new(new_left),
        Arc::new(new_right),
        new_on,
        new_filter,
        sym_join.join_type(),
        sym_join.null_equals_null(),
        sym_join.partition_mode(),
    )?)))
}

/// Compare the inputs and outputs of the projection. If the projection causes
/// any change in the fields, it returns `false`.
fn is_projection_removable(projection: &ProjectionExec) -> bool {
    all_alias_free_columns(projection.expr()) && {
        let schema = projection.schema();
        let input_schema = projection.input().schema();
        let fields = schema.fields();
        let input_fields = input_schema.fields();
        fields.len() == input_fields.len()
            && fields
                .iter()
                .zip(input_fields.iter())
                .all(|(out, input)| out.eq(input))
    }
}

/// Given the expression set of a projection, checks if the projection causes
/// any renaming or constructs a non-`Column` physical expression.
fn all_alias_free_columns(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> bool {
    exprs.iter().all(|(expr, alias)| {
        expr.as_any()
            .downcast_ref::<Column>()
            .map(|column| column.name() == alias)
            .unwrap_or(false)
    })
}

/// Updates a source provider's projected columns according to the given
/// projection operator's expressions. To use this function safely, one must
/// ensure that all expressions are `Column` expressions without aliases.
fn new_projections_for_columns(
    projection: &ProjectionExec,
    source: &Option<Vec<usize>>,
) -> Vec<usize> {
    projection
        .expr()
        .iter()
        .filter_map(|(expr, _)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .and_then(|expr| source.as_ref().map(|proj| proj[expr.index()]))
        })
        .collect()
}

/// The function operates in two modes:
///
/// 1) When `sync_with_child` is `true`:
///
///    The function updates the indices of `expr` if the expression resides
///    in the input plan. For instance, given the expressions `a@1 + b@2`
///    and `c@0` with the input schema `c@2, a@0, b@1`, the expressions are
///    updated to `a@0 + b@1` and `c@2`.
///
/// 2) When `sync_with_child` is `false`:
///
///    The function determines how the expression would be updated if a projection
///    was placed before the plan associated with the expression. If the expression
///    cannot be rewritten after the projection, it returns `None`. For example,
///    given the expressions `c@0`, `a@1` and `b@2`, and the [`ProjectionExec`] with
///    an output schema of `a, c_new`, then `c@0` becomes `c_new@1`, `a@1` becomes
///    `a@0`, but `b@2` results in `None` since the projection does not include `b`.
fn update_expr(
    expr: &Arc<dyn PhysicalExpr>,
    projected_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    sync_with_child: bool,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    #[derive(Debug, PartialEq)]
    enum RewriteState {
        /// The expression is unchanged.
        Unchanged,
        /// Some part of the expression has been rewritten
        RewrittenValid,
        /// Some part of the expression has been rewritten, but some column
        /// references could not be.
        RewrittenInvalid,
    }

    let mut state = RewriteState::Unchanged;

    let new_expr = expr
        .clone()
        .transform_up_mut(&mut |expr: Arc<dyn PhysicalExpr>| {
            if state == RewriteState::RewrittenInvalid {
                return Ok(Transformed::No(expr));
            }

            let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::No(expr));
            };
            if sync_with_child {
                state = RewriteState::RewrittenValid;
                // Update the index of `column`:
                Ok(Transformed::Yes(projected_exprs[column.index()].0.clone()))
            } else {
                // default to invalid, in case we can't find the relevant column
                state = RewriteState::RewrittenInvalid;
                // Determine how to update `column` to accommodate `projected_exprs`
                projected_exprs
                    .iter()
                    .enumerate()
                    .find_map(|(index, (projected_expr, alias))| {
                        projected_expr.as_any().downcast_ref::<Column>().and_then(
                            |projected_column| {
                                column.name().eq(projected_column.name()).then(|| {
                                    state = RewriteState::RewrittenValid;
                                    Arc::new(Column::new(alias, index)) as _
                                })
                            },
                        )
                    })
                    .map_or_else(
                        || Ok(Transformed::No(expr)),
                        |c| Ok(Transformed::Yes(c)),
                    )
            }
        });

    new_expr.map(|e| (state == RewriteState::RewrittenValid).then_some(e))
}

/// Creates a new [`ProjectionExec`] instance with the given child plan and
/// projected expressions.
fn make_with_child(
    projection: &ProjectionExec,
    child: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    ProjectionExec::try_new(projection.expr().to_vec(), child.clone())
        .map(|e| Arc::new(e) as _)
}

/// Returns `true` if all the expressions in the argument are `Column`s.
fn all_columns(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> bool {
    exprs.iter().all(|(expr, _)| expr.as_any().is::<Column>())
}

/// Downcasts all the expressions in `exprs` to `Column`s. If any of the given
/// expressions is not a `Column`, returns `None`.
fn physical_to_column_exprs(
    exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Option<Vec<(Column, String)>> {
    exprs
        .iter()
        .map(|(expr, alias)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|col| (col.clone(), alias.clone()))
        })
        .collect()
}

/// Returns the last index before encountering a column coming from the right table when traveling
/// through the projection from left to right, and the last index before encountering a column
/// coming from the left table when traveling through the projection from right to left.
/// If there is no column in the projection coming from the left side, it returns (-1, ...),
/// if there is no column in the projection coming from the right side, it returns (..., projection length).
fn join_table_borders(
    left_table_column_count: usize,
    projection_as_columns: &[(Column, String)],
) -> (i32, i32) {
    let far_right_left_col_ind = projection_as_columns
        .iter()
        .enumerate()
        .take_while(|(_, (projection_column, _))| {
            projection_column.index() < left_table_column_count
        })
        .last()
        .map(|(index, _)| index as i32)
        .unwrap_or(-1);

    let far_left_right_col_ind = projection_as_columns
        .iter()
        .enumerate()
        .rev()
        .take_while(|(_, (projection_column, _))| {
            projection_column.index() >= left_table_column_count
        })
        .last()
        .map(|(index, _)| index as i32)
        .unwrap_or(projection_as_columns.len() as i32);

    (far_right_left_col_ind, far_left_right_col_ind)
}

/// Tries to update the equi-join `Column`'s of a join as if the the input of
/// the join was replaced by a projection.
fn update_join_on(
    proj_left_exprs: &[(Column, String)],
    proj_right_exprs: &[(Column, String)],
    hash_join_on: &[(Column, Column)],
) -> Option<Vec<(Column, Column)>> {
    let (left_idx, right_idx): (Vec<_>, Vec<_>) = hash_join_on
        .iter()
        .map(|(left, right)| (left, right))
        .unzip();

    let new_left_columns = new_columns_for_join_on(&left_idx, proj_left_exprs);
    let new_right_columns = new_columns_for_join_on(&right_idx, proj_right_exprs);

    match (new_left_columns, new_right_columns) {
        (Some(left), Some(right)) => Some(left.into_iter().zip(right).collect()),
        _ => None,
    }
}

/// This function generates a new set of columns to be used in a hash join
/// operation based on a set of equi-join conditions (`hash_join_on`) and a
/// list of projection expressions (`projection_exprs`).
fn new_columns_for_join_on(
    hash_join_on: &[&Column],
    projection_exprs: &[(Column, String)],
) -> Option<Vec<Column>> {
    let new_columns = hash_join_on
        .iter()
        .filter_map(|on| {
            projection_exprs
                .iter()
                .enumerate()
                .find(|(_, (proj_column, _))| on.name() == proj_column.name())
                .map(|(index, (_, alias))| Column::new(alias, index))
        })
        .collect::<Vec<_>>();
    (new_columns.len() == hash_join_on.len()).then_some(new_columns)
}

/// Tries to update the column indices of a [`JoinFilter`] as if the the input of
/// the join was replaced by a projection.
fn update_join_filter(
    projection_left_exprs: &[(Column, String)],
    projection_right_exprs: &[(Column, String)],
    join_filter: &JoinFilter,
    join_left: &Arc<dyn ExecutionPlan>,
    join_right: &Arc<dyn ExecutionPlan>,
) -> Option<JoinFilter> {
    let mut new_left_indices = new_indices_for_join_filter(
        join_filter,
        JoinSide::Left,
        projection_left_exprs,
        join_left.schema(),
    )
    .into_iter();
    let mut new_right_indices = new_indices_for_join_filter(
        join_filter,
        JoinSide::Right,
        projection_right_exprs,
        join_right.schema(),
    )
    .into_iter();

    // Check if all columns match:
    (new_right_indices.len() + new_left_indices.len()
        == join_filter.column_indices().len())
    .then(|| {
        JoinFilter::new(
            join_filter.expression().clone(),
            join_filter
                .column_indices()
                .iter()
                .map(|col_idx| ColumnIndex {
                    index: if col_idx.side == JoinSide::Left {
                        new_left_indices.next().unwrap()
                    } else {
                        new_right_indices.next().unwrap()
                    },
                    side: col_idx.side,
                })
                .collect(),
            join_filter.schema().clone(),
        )
    })
}

/// This function determines and returns a vector of indices representing the
/// positions of columns in `projection_exprs` that are involved in `join_filter`,
/// and correspond to a particular side (`join_side`) of the join operation.
fn new_indices_for_join_filter(
    join_filter: &JoinFilter,
    join_side: JoinSide,
    projection_exprs: &[(Column, String)],
    join_child_schema: SchemaRef,
) -> Vec<usize> {
    join_filter
        .column_indices()
        .iter()
        .filter(|col_idx| col_idx.side == join_side)
        .filter_map(|col_idx| {
            projection_exprs.iter().position(|(col, _)| {
                col.name() == join_child_schema.fields()[col_idx.index].name()
            })
        })
        .collect()
}

/// Checks three conditions for pushing a projection down through a join:
/// - Projection must narrow the join output schema.
/// - Columns coming from left/right tables must be collected at the left/right
///   sides of the output table.
/// - Left or right table is not lost after the projection.
fn join_allows_pushdown(
    projection_as_columns: &[(Column, String)],
    join_schema: SchemaRef,
    far_right_left_col_ind: i32,
    far_left_right_col_ind: i32,
) -> bool {
    // Projection must narrow the join output:
    projection_as_columns.len() < join_schema.fields().len()
    // Are the columns from different tables mixed?
    && (far_right_left_col_ind + 1 == far_left_right_col_ind)
    // Left or right table is not lost after the projection.
    && far_right_left_col_ind >= 0
    && far_left_right_col_ind < projection_as_columns.len() as i32
}

/// If pushing down the projection over this join's children seems possible,
/// this function constructs the new [`ProjectionExec`]s that will come on top
/// of the original children of the join.
fn new_join_children(
    projection_as_columns: Vec<(Column, String)>,
    far_right_left_col_ind: i32,
    far_left_right_col_ind: i32,
    left_child: &Arc<dyn ExecutionPlan>,
    right_child: &Arc<dyn ExecutionPlan>,
) -> Result<(ProjectionExec, ProjectionExec)> {
    let new_left = ProjectionExec::try_new(
        projection_as_columns[0..=far_right_left_col_ind as _]
            .iter()
            .map(|(col, alias)| {
                (
                    Arc::new(Column::new(col.name(), col.index())) as _,
                    alias.clone(),
                )
            })
            .collect_vec(),
        left_child.clone(),
    )?;
    let left_size = left_child.schema().fields().len() as i32;
    let new_right = ProjectionExec::try_new(
        projection_as_columns[far_left_right_col_ind as _..]
            .iter()
            .map(|(col, alias)| {
                (
                    Arc::new(Column::new(
                        col.name(),
                        // Align projected expressions coming from the right
                        // table with the new right child projection:
                        (col.index() as i32 - left_size) as _,
                    )) as _,
                    alias.clone(),
                )
            })
            .collect_vec(),
        right_child.clone(),
    )?;

    Ok((new_left, new_right))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datasource::file_format::file_compression_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::physical_plan::{CsvExec, FileScanConfig};
    use crate::physical_optimizer::output_requirements::OutputRequirementExec;
    use crate::physical_optimizer::projection_pushdown::{
        join_table_borders, update_expr, ProjectionPushdown,
    };
    use crate::physical_optimizer::PhysicalOptimizerRule;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use crate::physical_plan::joins::StreamJoinPartitionMode;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::{get_plan_string, ExecutionPlan};

    use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{JoinSide, JoinType, Result, ScalarValue, Statistics};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_expr::{ColumnarValue, Operator};
    use datafusion_physical_expr::expressions::{
        BinaryExpr, CaseExpr, CastExpr, Column, Literal, NegativeExpr,
    };
    use datafusion_physical_expr::{
        Distribution, Partitioning, PhysicalExpr, PhysicalSortExpr,
        PhysicalSortRequirement, ScalarFunctionExpr,
    };
    use datafusion_physical_plan::joins::SymmetricHashJoinExec;
    use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion_physical_plan::union::UnionExec;

    use itertools::Itertools;

    #[test]
    fn test_update_matching_exprs() -> Result<()> {
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 3)),
                Operator::Divide,
                Arc::new(Column::new("e", 5)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 3)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f", 4)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(|_: &[ColumnarValue]| unimplemented!("not implemented")),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 0)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 0)),
                        Operator::Divide,
                        Arc::new(Column::new("b", 1)),
                    )),
                ],
                DataType::Int32,
                None,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d", 2))),
                vec![
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 2)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 5)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 5)),
                            Operator::Plus,
                            Arc::new(Column::new("d", 2)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 3)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 5)),
                ))),
            )?),
        ];
        let child: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("c", 2)), "c".to_owned()),
            (Arc::new(Column::new("b", 1)), "b".to_owned()),
            (Arc::new(Column::new("d", 3)), "d".to_owned()),
            (Arc::new(Column::new("a", 0)), "a".to_owned()),
            (Arc::new(Column::new("f", 5)), "f".to_owned()),
            (Arc::new(Column::new("e", 4)), "e".to_owned()),
        ];

        let expected_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Divide,
                Arc::new(Column::new("e", 4)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 0)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f", 5)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(|_: &[ColumnarValue]| unimplemented!("not implemented")),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 2)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Divide,
                        Arc::new(Column::new("b", 1)),
                    )),
                ],
                DataType::Int32,
                None,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d", 3))),
                vec![
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 3)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 4)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 4)),
                            Operator::Plus,
                            Arc::new(Column::new("d", 3)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 4)),
                ))),
            )?),
        ];

        for (expr, expected_expr) in exprs.into_iter().zip(expected_exprs.into_iter()) {
            assert!(update_expr(&expr, &child, true)?
                .unwrap()
                .eq(&expected_expr));
        }

        Ok(())
    }

    #[test]
    fn test_update_projected_exprs() -> Result<()> {
        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 3)),
                Operator::Divide,
                Arc::new(Column::new("e", 5)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 3)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f", 4)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(|_: &[ColumnarValue]| unimplemented!("not implemented")),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 0)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 0)),
                        Operator::Divide,
                        Arc::new(Column::new("b", 1)),
                    )),
                ],
                DataType::Int32,
                None,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d", 2))),
                vec![
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 2)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 5)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 5)),
                            Operator::Plus,
                            Arc::new(Column::new("d", 2)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 3)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 5)),
                ))),
            )?),
        ];
        let projected_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_owned()),
            (Arc::new(Column::new("b", 1)), "b_new".to_owned()),
            (Arc::new(Column::new("c", 2)), "c".to_owned()),
            (Arc::new(Column::new("d", 3)), "d_new".to_owned()),
            (Arc::new(Column::new("e", 4)), "e".to_owned()),
            (Arc::new(Column::new("f", 5)), "f_new".to_owned()),
        ];

        let expected_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Divide,
                Arc::new(Column::new("e", 4)),
            )),
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 0)),
                DataType::Float32,
                None,
            )),
            Arc::new(NegativeExpr::new(Arc::new(Column::new("f_new", 5)))),
            Arc::new(ScalarFunctionExpr::new(
                "scalar_expr",
                Arc::new(|_: &[ColumnarValue]| unimplemented!("not implemented")),
                vec![
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b_new", 1)),
                        Operator::Divide,
                        Arc::new(Column::new("c", 2)),
                    )),
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Divide,
                        Arc::new(Column::new("b_new", 1)),
                    )),
                ],
                DataType::Int32,
                None,
            )),
            Arc::new(CaseExpr::try_new(
                Some(Arc::new(Column::new("d_new", 3))),
                vec![
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d_new", 3)),
                            Operator::Plus,
                            Arc::new(Column::new("e", 4)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("e", 4)),
                            Operator::Plus,
                            Arc::new(Column::new("d_new", 3)),
                        )) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Modulo,
                    Arc::new(Column::new("e", 4)),
                ))),
            )?),
        ];

        for (expr, expected_expr) in exprs.into_iter().zip(expected_exprs.into_iter()) {
            assert!(update_expr(&expr, &projected_exprs, false)?
                .unwrap()
                .eq(&expected_expr));
        }

        Ok(())
    }

    #[test]
    fn test_join_table_borders() -> Result<()> {
        let projections = vec![
            (Column::new("b", 1), "b".to_owned()),
            (Column::new("c", 2), "c".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("d", 3), "d".to_owned()),
            (Column::new("c", 2), "c".to_owned()),
            (Column::new("f", 5), "f".to_owned()),
            (Column::new("h", 7), "h".to_owned()),
            (Column::new("g", 6), "g".to_owned()),
        ];
        let left_table_column_count = 5;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (4, 5)
        );

        let left_table_column_count = 8;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (7, 8)
        );

        let left_table_column_count = 1;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (-1, 0)
        );

        let projections = vec![
            (Column::new("a", 0), "a".to_owned()),
            (Column::new("b", 1), "b".to_owned()),
            (Column::new("d", 3), "d".to_owned()),
            (Column::new("g", 6), "g".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("f", 5), "f".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("h", 7), "h".to_owned()),
        ];
        let left_table_column_count = 5;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (2, 7)
        );

        let left_table_column_count = 7;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (6, 7)
        );

        Ok(())
    }

    fn create_simple_csv_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
        ]));
        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::new_unknown(&schema),
                projection: Some(vec![0, 1, 2, 3, 4]),
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![vec![]],
                infinite_source: false,
            },
            false,
            0,
            0,
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    fn create_projecting_csv_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]));
        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::new_unknown(&schema),
                projection: Some(vec![3, 2, 1]),
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![vec![]],
                infinite_source: false,
            },
            false,
            0,
            0,
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    fn create_projecting_memory_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
        ]));

        Arc::new(MemoryExec::try_new(&[], schema, Some(vec![2, 0, 3, 4])).unwrap())
    }

    #[test]
    fn test_csv_after_projection() -> Result<()> {
        let csv = create_projecting_csv_exec();
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("b", 2)), "b".to_string()),
                (Arc::new(Column::new("d", 0)), "d".to_string()),
            ],
            csv.clone(),
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[b@2 as b, d@0 as d]",
                "  CsvExec: file_groups={1 group: [[x]]}, projection=[d, c, b], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "CsvExec: file_groups={1 group: [[x]]}, projection=[b, d], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_memory_after_projection() -> Result<()> {
        let memory = create_projecting_memory_exec();
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("d", 2)), "d".to_string()),
                (Arc::new(Column::new("e", 3)), "e".to_string()),
                (Arc::new(Column::new("a", 1)), "a".to_string()),
            ],
            memory.clone(),
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[d@2 as d, e@3 as e, a@1 as a]",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = ["MemoryExec: partitions=0, partition_sizes=[]"];
        assert_eq!(get_plan_string(&after_optimize), expected);
        assert_eq!(
            after_optimize
                .clone()
                .as_any()
                .downcast_ref::<MemoryExec>()
                .unwrap()
                .projection()
                .clone()
                .unwrap(),
            vec![3, 4, 0]
        );

        Ok(())
    }

    #[test]
    fn test_streaming_table_after_projection() -> Result<()> {
        struct DummyStreamPartition {
            schema: SchemaRef,
        }
        impl PartitionStream for DummyStreamPartition {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }
            fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
                unreachable!()
            }
        }

        let streaming_table = StreamingTableExec::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Int32, true),
                Field::new("d", DataType::Int32, true),
                Field::new("e", DataType::Int32, true),
            ])),
            vec![Arc::new(DummyStreamPartition {
                schema: Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Int32, true),
                    Field::new("c", DataType::Int32, true),
                    Field::new("d", DataType::Int32, true),
                    Field::new("e", DataType::Int32, true),
                ])),
            }) as _],
            Some(&vec![0_usize, 2, 4, 3]),
            vec![
                vec![
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("e", 2)),
                        options: SortOptions::default(),
                    },
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("a", 0)),
                        options: SortOptions::default(),
                    },
                ],
                vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options: SortOptions::default(),
                }],
            ]
            .into_iter(),
            true,
        )?;
        let projection = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("d", 3)), "d".to_string()),
                (Arc::new(Column::new("e", 2)), "e".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
            ],
            Arc::new(streaming_table) as _,
        )?) as _;

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let result = after_optimize
            .as_any()
            .downcast_ref::<StreamingTableExec>()
            .unwrap();
        assert_eq!(
            result.partition_schema(),
            &Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Int32, true),
                Field::new("d", DataType::Int32, true),
                Field::new("e", DataType::Int32, true),
            ]))
        );
        assert_eq!(
            result.projection().clone().unwrap().to_vec(),
            vec![3_usize, 4, 0]
        );
        assert_eq!(
            result.projected_schema(),
            &Schema::new(vec![
                Field::new("d", DataType::Int32, true),
                Field::new("e", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ])
        );
        assert_eq!(
            result.projected_output_ordering().into_iter().collect_vec(),
            vec![
                vec![
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("e", 1)),
                        options: SortOptions::default(),
                    },
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("a", 2)),
                        options: SortOptions::default(),
                    },
                ],
                vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 0)),
                    options: SortOptions::default(),
                }],
            ]
        );
        assert!(result.is_infinite());

        Ok(())
    }

    #[test]
    fn test_projection_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let child_projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("e", 4)), "new_e".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
                (Arc::new(Column::new("b", 1)), "new_b".to_string()),
            ],
            csv.clone(),
        )?);
        let top_projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("new_b", 3)), "new_b".to_string()),
                (
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 0)),
                        Operator::Plus,
                        Arc::new(Column::new("new_e", 1)),
                    )),
                    "binary".to_string(),
                ),
                (Arc::new(Column::new("new_b", 3)), "newest_b".to_string()),
            ],
            child_projection.clone(),
        )?);

        let initial = get_plan_string(&top_projection);
        let expected_initial = [
            "ProjectionExec: expr=[new_b@3 as new_b, c@0 + new_e@1 as binary, new_b@3 as newest_b]",
            "  ProjectionExec: expr=[c@2 as c, e@4 as new_e, a@0 as a, b@1 as new_b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(top_projection, &ConfigOptions::new())?;

        let expected = [
            "ProjectionExec: expr=[b@1 as new_b, c@2 + e@4 as binary, b@1 as newest_b]",
            "  CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_output_req_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let sort_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            csv.clone(),
            Some(vec![
                PhysicalSortRequirement {
                    expr: Arc::new(Column::new("b", 1)),
                    options: Some(SortOptions::default()),
                },
                PhysicalSortRequirement {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Plus,
                        Arc::new(Column::new("a", 0)),
                    )),
                    options: Some(SortOptions::default()),
                },
            ]),
            Distribution::HashPartitioned(vec![
                Arc::new(Column::new("a", 0)),
                Arc::new(Column::new("b", 1)),
            ]),
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            sort_req.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  OutputRequirementExec",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected: [&str; 3] = [
            "OutputRequirementExec",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];

        assert_eq!(get_plan_string(&after_optimize), expected);
        let expected_reqs = vec![
            PhysicalSortRequirement {
                expr: Arc::new(Column::new("b", 2)),
                options: Some(SortOptions::default()),
            },
            PhysicalSortRequirement {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("c", 0)),
                    Operator::Plus,
                    Arc::new(Column::new("new_a", 1)),
                )),
                options: Some(SortOptions::default()),
            },
        ];
        assert_eq!(
            after_optimize
                .as_any()
                .downcast_ref::<OutputRequirementExec>()
                .unwrap()
                .required_input_ordering()[0]
                .clone()
                .unwrap(),
            expected_reqs
        );
        let expected_distribution: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("new_a", 1)),
            Arc::new(Column::new("b", 2)),
        ];
        if let Distribution::HashPartitioned(vec) = after_optimize
            .as_any()
            .downcast_ref::<OutputRequirementExec>()
            .unwrap()
            .required_input_distribution()[0]
            .clone()
        {
            assert!(vec
                .iter()
                .zip(expected_distribution)
                .all(|(actual, expected)| actual.eq(&expected)));
        } else {
            panic!("Expected HashPartitioned distribution!");
        };

        Ok(())
    }

    #[test]
    fn test_coalesce_partitions_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let coalesce_partitions: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(csv));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("b", 1)), "b".to_string()),
                (Arc::new(Column::new("a", 0)), "a_new".to_string()),
                (Arc::new(Column::new("d", 3)), "d".to_string()),
            ],
            coalesce_partitions,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[b@1 as b, a@0 as a_new, d@3 as d]",
                "  CoalescePartitionsExec",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
                "CoalescePartitionsExec",
                "  ProjectionExec: expr=[b@1 as b, a@0 as a_new, d@3 as d]",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_filter_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("b", 1)),
                Operator::Minus,
                Arc::new(Column::new("a", 0)),
            )),
            Operator::Gt,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("d", 3)),
                Operator::Minus,
                Arc::new(Column::new("a", 0)),
            )),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, None, csv)?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("a", 0)), "a_new".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
                (Arc::new(Column::new("d", 3)), "d".to_string()),
            ],
            filter.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[a@0 as a_new, b@1 as b, d@3 as d]",
                "  FilterExec: b@1 - a@0 > d@3 - a@0",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
                "FilterExec: b@1 - a_new@0 > d@2 - a_new@0",
                "  ProjectionExec: expr=[a@0 as a_new, b@1 as b, d@3 as d]",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_join_after_projection() -> Result<()> {
        let left_csv = create_simple_csv_exec();
        let right_csv = create_simple_csv_exec();

        let join: Arc<dyn ExecutionPlan> = Arc::new(SymmetricHashJoinExec::try_new(
            left_csv,
            right_csv,
            vec![(Column::new("b", 1), Column::new("c", 2))],
            // b_left-(1+a_right)<=a_right+c_left
            Some(JoinFilter::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("b_left_inter", 0)),
                        Operator::Minus,
                        Arc::new(BinaryExpr::new(
                            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                            Operator::Plus,
                            Arc::new(Column::new("a_right_inter", 1)),
                        )),
                    )),
                    Operator::LtEq,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("a_right_inter", 1)),
                        Operator::Plus,
                        Arc::new(Column::new("c_left_inter", 2)),
                    )),
                )),
                vec![
                    ColumnIndex {
                        index: 1,
                        side: JoinSide::Left,
                    },
                    ColumnIndex {
                        index: 0,
                        side: JoinSide::Right,
                    },
                    ColumnIndex {
                        index: 2,
                        side: JoinSide::Left,
                    },
                ],
                Schema::new(vec![
                    Field::new("b_left_inter", DataType::Int32, true),
                    Field::new("a_right_inter", DataType::Int32, true),
                    Field::new("c_left_inter", DataType::Int32, true),
                ]),
            )),
            &JoinType::Inner,
            true,
            StreamJoinPartitionMode::SinglePartition,
        )?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c_from_left".to_string()),
                (Arc::new(Column::new("b", 1)), "b_from_left".to_string()),
                (Arc::new(Column::new("a", 0)), "a_from_left".to_string()),
                (Arc::new(Column::new("a", 5)), "a_from_right".to_string()),
                (Arc::new(Column::new("c", 7)), "c_from_right".to_string()),
            ],
            join,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left, a@5 as a_from_right, c@7 as c_from_right]", 
            "  SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b@1, c@2)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "SymmetricHashJoinExec: mode=SinglePartition, join_type=Inner, on=[(b_from_left@1, c_from_right@1)], filter=b_left_inter@0 - 1 + a_right_inter@1 <= a_right_inter@1 + c_left_inter@2", 
            "  ProjectionExec: expr=[c@2 as c_from_left, b@1 as b_from_left, a@0 as a_from_left]", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", 
            "  ProjectionExec: expr=[a@0 as a_from_right, c@2 as c_from_right]", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        let expected_filter_col_ind = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
        ];

        assert_eq!(
            expected_filter_col_ind,
            after_optimize
                .as_any()
                .downcast_ref::<SymmetricHashJoinExec>()
                .unwrap()
                .filter()
                .unwrap()
                .column_indices()
        );

        Ok(())
    }

    #[test]
    fn test_repartition_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let repartition: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            csv,
            Partitioning::Hash(
                vec![
                    Arc::new(Column::new("a", 0)),
                    Arc::new(Column::new("b", 1)),
                    Arc::new(Column::new("d", 3)),
                ],
                6,
            ),
        )?);
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("b", 1)), "b_new".to_string()),
                (Arc::new(Column::new("a", 0)), "a".to_string()),
                (Arc::new(Column::new("d", 3)), "d_new".to_string()),
            ],
            repartition,
        )?);
        let initial = get_plan_string(&projection);
        let expected_initial = [
                "ProjectionExec: expr=[b@1 as b_new, a@0 as a, d@3 as d_new]",
                "  RepartitionExec: partitioning=Hash([a@0, b@1, d@3], 6), input_partitions=1",
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
                "RepartitionExec: partitioning=Hash([a@1, b_new@0, d_new@2], 6), input_partitions=1", 
                "  ProjectionExec: expr=[b@1 as b_new, a@0 as a, d@3 as d_new]", 
                "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        assert_eq!(
            after_optimize
                .as_any()
                .downcast_ref::<RepartitionExec>()
                .unwrap()
                .partitioning()
                .clone(),
            Partitioning::Hash(
                vec![
                    Arc::new(Column::new("a", 1)),
                    Arc::new(Column::new("b_new", 0)),
                    Arc::new(Column::new("d_new", 2)),
                ],
                6,
            ),
        );

        Ok(())
    }

    #[test]
    fn test_sort_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let sort_req: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Plus,
                        Arc::new(Column::new("a", 0)),
                    )),
                    options: SortOptions::default(),
                },
            ],
            csv.clone(),
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            sort_req.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  SortExec: expr=[b@1 ASC,c@2 + a@0 ASC]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "SortExec: expr=[b@2 ASC,c@0 + new_a@1 ASC]",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_sort_preserving_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let sort_req: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c", 2)),
                        Operator::Plus,
                        Arc::new(Column::new("a", 0)),
                    )),
                    options: SortOptions::default(),
                },
            ],
            csv.clone(),
        ));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            sort_req.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "  SortPreservingMergeExec: [b@1 ASC,c@2 + a@0 ASC]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "SortPreservingMergeExec: [b@2 ASC,c@0 + new_a@1 ASC]",
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]",
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn test_union_after_projection() -> Result<()> {
        let csv = create_simple_csv_exec();
        let union: Arc<dyn ExecutionPlan> =
            Arc::new(UnionExec::new(vec![csv.clone(), csv.clone(), csv]));
        let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![
                (Arc::new(Column::new("c", 2)), "c".to_string()),
                (Arc::new(Column::new("a", 0)), "new_a".to_string()),
                (Arc::new(Column::new("b", 1)), "b".to_string()),
            ],
            union.clone(),
        )?);

        let initial = get_plan_string(&projection);
        let expected_initial = [
            "ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]", 
            "  UnionExec", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
            ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            ProjectionPushdown::new().optimize(projection, &ConfigOptions::new())?;

        let expected = [
            "UnionExec", 
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", 
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false", 
            "  ProjectionExec: expr=[c@2 as c, a@0 as new_a, b@1 as b]", 
            "    CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }
}
