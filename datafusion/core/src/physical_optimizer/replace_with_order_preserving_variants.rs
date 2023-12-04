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

//! Optimizer rule that replaces executors that lose ordering with their
//! order-preserving variants when it is helpful; either in terms of
//! performance or to accommodate unbounded streams by fixing the pipeline.

use std::sync::Arc;

use crate::error::Result;
use crate::physical_optimizer::utils::{is_coalesce_partitions, is_sort, ExecTree};
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use super::utils::is_repartition;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_physical_plan::unbounded_output;

/// For a given `plan`, this object carries the information one needs from its
/// descendants to decide whether it is beneficial to replace order-losing (but
/// somewhat faster) variants of certain operators with their order-preserving
/// (but somewhat slower) cousins.
#[derive(Debug, Clone)]
pub(crate) struct OrderPreservationContext {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    ordering_onwards: Vec<Option<ExecTree>>,
}

impl OrderPreservationContext {
    /// Creates a "default" order-preservation context.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        OrderPreservationContext {
            plan,
            ordering_onwards: vec![None; length],
        }
    }

    /// Creates a new order-preservation context from those of children nodes.
    pub fn new_from_children_nodes(
        children_nodes: Vec<OrderPreservationContext>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let ordering_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                // `ordering_onwards` tree keeps track of executors that maintain
                // ordering, (or that can maintain ordering with the replacement of
                // its variant)
                let plan = item.plan;
                let children = plan.children();
                let ordering_onwards = item.ordering_onwards;
                if children.is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if ordering_onwards[0].is_none()
                    && ((is_repartition(&plan) && !plan.maintains_input_order()[0])
                        || (is_coalesce_partitions(&plan)
                            && children[0].output_ordering().is_some()))
                {
                    Some(ExecTree::new(plan, idx, vec![]))
                } else {
                    let children = ordering_onwards
                        .into_iter()
                        .flatten()
                        .filter(|item| {
                            // Only consider operators that maintains ordering
                            plan.maintains_input_order()[item.idx]
                                || is_coalesce_partitions(&plan)
                                || is_repartition(&plan)
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
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?.into();
        Ok(OrderPreservationContext {
            plan,
            ordering_onwards,
        })
    }

    /// Computes order-preservation contexts for every child of the plan.
    pub fn children(&self) -> Vec<OrderPreservationContext> {
        self.plan
            .children()
            .into_iter()
            .map(OrderPreservationContext::new)
            .collect()
    }
}

impl TreeNode for OrderPreservationContext {
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
            OrderPreservationContext::new_from_children_nodes(children_nodes, self.plan)
        }
    }
}

/// Calculates the updated plan by replacing executors that lose ordering
/// inside the `ExecTree` with their order-preserving variants. This will
/// generate an alternative plan, which will be accepted or rejected later on
/// depending on whether it helps us remove a `SortExec`.
fn get_updated_plan(
    exec_tree: &ExecTree,
    // Flag indicating that it is desirable to replace `RepartitionExec`s with
    // `SortPreservingRepartitionExec`s:
    is_spr_better: bool,
    // Flag indicating that it is desirable to replace `CoalescePartitionsExec`s
    // with `SortPreservingMergeExec`s:
    is_spm_better: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = exec_tree.plan.clone();

    let mut children = plan.children();
    // Update children and their descendants in the given tree:
    for item in &exec_tree.children {
        children[item.idx] = get_updated_plan(item, is_spr_better, is_spm_better)?;
    }
    // Construct the plan with updated children:
    let mut plan = plan.with_new_children(children)?;

    // When a `RepartitionExec` doesn't preserve ordering, replace it with
    // a `SortPreservingRepartitionExec` if appropriate:
    if is_repartition(&plan) && !plan.maintains_input_order()[0] && is_spr_better {
        let child = plan.children().swap_remove(0);
        let repartition = RepartitionExec::try_new(child, plan.output_partitioning())?
            .with_preserve_order();
        plan = Arc::new(repartition) as _
    }
    // When the input of a `CoalescePartitionsExec` has an ordering, replace it
    // with a `SortPreservingMergeExec` if appropriate:
    let mut children = plan.children();
    if is_coalesce_partitions(&plan)
        && children[0].output_ordering().is_some()
        && is_spm_better
    {
        let child = children.swap_remove(0);
        plan = Arc::new(SortPreservingMergeExec::new(
            child.output_ordering().unwrap_or(&[]).to_vec(),
            child,
        )) as _
    }
    Ok(plan)
}

/// The `replace_with_order_preserving_variants` optimizer sub-rule tries to
/// remove `SortExec`s from the physical plan by replacing operators that do
/// not preserve ordering with their order-preserving variants; i.e. by replacing
/// `RepartitionExec`s with `SortPreservingRepartitionExec`s or by replacing
/// `CoalescePartitionsExec`s with `SortPreservingMergeExec`s.
///
/// If this replacement is helpful for removing a `SortExec`, it updates the plan.
/// Otherwise, it leaves the plan unchanged.
///
/// Note: this optimizer sub-rule will only produce `SortPreservingRepartitionExec`s
/// if the query is bounded or if the config option `bounded_order_preserving_variants`
/// is set to `true`.
///
/// The algorithm flow is simply like this:
/// 1. Visit nodes of the physical plan bottom-up and look for `SortExec` nodes.
/// 1_1. During the traversal, build an `ExecTree` to keep track of operators
///      that maintain ordering (or can maintain ordering when replaced by an
///      order-preserving variant) until a `SortExec` is found.
/// 2. When a `SortExec` is found, update the child of the `SortExec` by replacing
///    operators that do not preserve ordering in the `ExecTree` with their order
///    preserving variants.
/// 3. Check if the `SortExec` is still necessary in the updated plan by comparing
///    its input ordering with the output ordering it imposes. We do this because
///    replacing operators that lose ordering with their order-preserving variants
///    enables us to preserve the previously lost ordering at the input of `SortExec`.
/// 4. If the `SortExec` in question turns out to be unnecessary, remove it and use
///     updated plan. Otherwise, use the original plan.
/// 5. Continue the bottom-up traversal until another `SortExec` is seen, or the traversal
///    is complete.
pub(crate) fn replace_with_order_preserving_variants(
    requirements: OrderPreservationContext,
    // A flag indicating that replacing `RepartitionExec`s with
    // `SortPreservingRepartitionExec`s is desirable when it helps
    // to remove a `SortExec` from the plan. If this flag is `false`,
    // this replacement should only be made to fix the pipeline (streaming).
    is_spr_better: bool,
    // A flag indicating that replacing `CoalescePartitionsExec`s with
    // `SortPreservingMergeExec`s is desirable when it helps to remove
    // a `SortExec` from the plan. If this flag is `false`, this replacement
    // should only be made to fix the pipeline (streaming).
    is_spm_better: bool,
    config: &ConfigOptions,
) -> Result<Transformed<OrderPreservationContext>> {
    let plan = &requirements.plan;
    let ordering_onwards = &requirements.ordering_onwards;
    if is_sort(plan) {
        let exec_tree = if let Some(exec_tree) = &ordering_onwards[0] {
            exec_tree
        } else {
            return Ok(Transformed::No(requirements));
        };
        // For unbounded cases, replace with the order-preserving variant in
        // any case, as doing so helps fix the pipeline.
        // Also do the replacement if opted-in via config options.
        let use_order_preserving_variant =
            config.optimizer.prefer_existing_sort || unbounded_output(plan);
        let updated_sort_input = get_updated_plan(
            exec_tree,
            is_spr_better || use_order_preserving_variant,
            is_spm_better || use_order_preserving_variant,
        )?;
        // If this sort is unnecessary, we should remove it and update the plan:
        if updated_sort_input
            .equivalence_properties()
            .ordering_satisfy(plan.output_ordering().unwrap_or(&[]))
        {
            return Ok(Transformed::Yes(OrderPreservationContext {
                plan: updated_sort_input,
                ordering_onwards: vec![None],
            }));
        }
    }

    Ok(Transformed::No(requirements))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datasource::file_format::file_compression_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::physical_plan::{CsvExec, FileScanConfig};
    use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::joins::{HashJoinExec, PartitionMode};
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::{displayable, get_plan_string, Partitioning};
    use crate::prelude::SessionConfig;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::tree_node::TreeNode;
    use datafusion_common::{Result, Statistics};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::{JoinType, Operator};
    use datafusion_physical_expr::expressions::{self, col, Column};
    use datafusion_physical_expr::PhysicalSortExpr;

    /// Runs the `replace_with_order_preserving_variants` sub-rule and asserts the plan
    /// against the original and expected plans.
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
    /// `$PLAN`: the plan to optimized
    /// `$ALLOW_BOUNDED`: whether to allow the plan to be optimized for bounded cases
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr) => {
            assert_optimized!(
                $EXPECTED_PLAN_LINES,
                $EXPECTED_OPTIMIZED_PLAN_LINES,
                $PLAN,
                false
            );
        };
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $ALLOW_BOUNDED: expr) => {
            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

            let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES.iter().map(|s| *s).collect();

            // Run the rule top-down
            // let optimized_physical_plan = physical_plan.transform_down(&replace_repartition_execs)?;
            let config = SessionConfig::new().with_prefer_existing_sort($ALLOW_BOUNDED);
            let plan_with_pipeline_fixer = OrderPreservationContext::new(physical_plan);
            let parallel = plan_with_pipeline_fixer.transform_up(&|plan_with_pipeline_fixer| replace_with_order_preserving_variants(plan_with_pipeline_fixer, false, false, config.options()))?;
            let optimized_physical_plan = parallel.plan;

            // Get string representation of the plan
            let actual = get_plan_string(&optimized_physical_plan);
            assert_eq!(
                expected_optimized_lines, actual,
                "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );
        };
    }

    #[tokio::test]
    // Searches for a simple sort and a repartition just after it, the second repartition with 1 input partition should not be affected
    async fn test_replace_multiple_input_repartition_1() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition = repartition_exec_hash(repartition_exec_round_robin(source));
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_inter_children_change_only() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr_default("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let sort = sort_exec(
            vec![sort_expr_default("a", &coalesce_partitions.schema())],
            coalesce_partitions,
            false,
        );
        let repartition_rr2 = repartition_exec_round_robin(sort);
        let repartition_hash2 = repartition_exec_hash(repartition_rr2);
        let filter = filter_exec(repartition_hash2);
        let sort2 =
            sort_exec(vec![sort_expr_default("a", &filter.schema())], filter, true);

        let physical_plan = sort_preserving_merge_exec(
            vec![sort_expr_default("a", &sort2.schema())],
            sort2,
        );

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC], has_header=true",
        ];

        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  FilterExec: c@1 > 3",
            "    SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortPreservingMergeExec: [a@0 ASC]",
            "          SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_multiple_input_repartition_2() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let filter = filter_exec(repartition_rr);
        let repartition_hash = repartition_exec_hash(filter);
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_multiple_input_repartition_with_extra_steps() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);
        let sort = sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_multiple_input_repartition_with_extra_steps_2() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let coalesce_batches_exec_1 = coalesce_batches_exec(repartition_rr);
        let repartition_hash = repartition_exec_hash(coalesce_batches_exec_1);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches_exec_2 = coalesce_batches_exec(filter);
        let sort =
            sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec_2, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_replacing_when_no_need_to_preserve_sorting() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);

        let physical_plan: Arc<dyn ExecutionPlan> =
            coalesce_partitions_exec(coalesce_batches_exec);

        let expected_input = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_multiple_replacable_repartitions() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches = coalesce_batches_exec(filter);
        let repartition_hash_2 = repartition_exec_hash(coalesce_batches);
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash_2, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_replace_with_different_orderings() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let sort = sort_exec(
            vec![sort_expr_default("c", &repartition_hash.schema())],
            repartition_hash,
            true,
        );

        let physical_plan = sort_preserving_merge_exec(
            vec![sort_expr_default("c", &sort.schema())],
            sort,
        );

        let expected_input = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan =
            sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions, false);

        let expected_input = [
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_and_kept_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let sort = sort_exec(
            vec![sort_expr_default("c", &coalesce_partitions.schema())],
            coalesce_partitions,
            false,
        );
        let repartition_rr2 = repartition_exec_round_robin(sort);
        let repartition_hash2 = repartition_exec_hash(repartition_rr2);
        let filter = filter_exec(repartition_hash2);
        let sort2 =
            sort_exec(vec![sort_expr_default("c", &filter.schema())], filter, true);

        let physical_plan = sort_preserving_merge_exec(
            vec![sort_expr_default("c", &sort2.schema())],
            sort2,
        );

        let expected_input = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        let expected_optimized = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  FilterExec: c@1 > 3",
            "    SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=c@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[c@1 ASC]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_multiple_child_trees() -> Result<()> {
        let schema = create_test_schema()?;

        let left_sort_exprs = vec![sort_expr("a", &schema)];
        let left_source = csv_exec_sorted(&schema, left_sort_exprs, true);
        let left_repartition_rr = repartition_exec_round_robin(left_source);
        let left_repartition_hash = repartition_exec_hash(left_repartition_rr);
        let left_coalesce_partitions =
            Arc::new(CoalesceBatchesExec::new(left_repartition_hash, 4096));

        let right_sort_exprs = vec![sort_expr("a", &schema)];
        let right_source = csv_exec_sorted(&schema, right_sort_exprs, true);
        let right_repartition_rr = repartition_exec_round_robin(right_source);
        let right_repartition_hash = repartition_exec_hash(right_repartition_rr);
        let right_coalesce_partitions =
            Arc::new(CoalesceBatchesExec::new(right_repartition_hash, 4096));

        let hash_join_exec =
            hash_join_exec(left_coalesce_partitions, right_coalesce_partitions);
        let sort = sort_exec(
            vec![sort_expr_default("a", &hash_join_exec.schema())],
            hash_join_exec,
            true,
        );

        let physical_plan = sort_preserving_merge_exec(
            vec![sort_expr_default("a", &sort.schema())],
            sort,
        );

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_bounded_input() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, false);
        let repartition = repartition_exec_hash(repartition_exec_round_robin(source));
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    // End test cases
    // Start test helpers

    fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        let sort_opts = SortOptions {
            nulls_first: false,
            descending: false,
        };
        sort_expr_options(name, schema, sort_opts)
    }

    fn sort_expr_default(name: &str, schema: &Schema) -> PhysicalSortExpr {
        let sort_opts = SortOptions::default();
        sort_expr_options(name, schema, sort_opts)
    }

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

    fn sort_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(
            SortExec::new(sort_exprs, input)
                .with_preserve_partitioning(preserve_partitioning),
        )
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    fn repartition_exec_round_robin(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(8)).unwrap(),
        )
    }

    fn repartition_exec_hash(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let input_schema = input.schema();
        Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col("c", &input_schema).unwrap()], 8),
            )
            .unwrap(),
        )
    }

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let input_schema = input.schema();
        let predicate = expressions::binary(
            col("c", &input_schema).unwrap(),
            Operator::Gt,
            expressions::lit(3i32),
            &input_schema,
        )
        .unwrap();
        Arc::new(FilterExec::try_new(predicate, None, input).unwrap())
    }

    fn coalesce_batches_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalesceBatchesExec::new(input, 8192))
    }

    fn coalesce_partitions_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalescePartitionsExec::new(input))
    }

    fn hash_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let left_on = col("c", &left.schema()).unwrap();
        let right_on = col("c", &right.schema()).unwrap();
        let left_col = left_on.as_any().downcast_ref::<Column>().unwrap();
        let right_col = right_on.as_any().downcast_ref::<Column>().unwrap();
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(left_col.clone(), right_col.clone())],
                None,
                &JoinType::Inner,
                PartitionMode::Partitioned,
                false,
            )
            .unwrap(),
        )
    }

    fn create_test_schema() -> Result<SchemaRef> {
        let column_a = Field::new("a", DataType::Int32, false);
        let column_b = Field::new("b", DataType::Int32, false);
        let column_c = Field::new("c", DataType::Int32, false);
        let column_d = Field::new("d", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![column_a, column_b, column_c, column_d]));

        Ok(schema)
    }

    // creates a csv exec source for the test purposes
    // projection and has_header parameters are given static due to testing needs
    fn csv_exec_sorted(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        infinite_source: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        let projection: Vec<usize> = vec![0, 2, 3];

        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new(
                    "file_path".to_string(),
                    100,
                )]],
                statistics: Statistics::new_unknown(schema),
                projection: Some(projection),
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![sort_exprs],
                infinite_source,
            },
            true,
            0,
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }
}
