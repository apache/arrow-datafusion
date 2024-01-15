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

use std::borrow::Cow;
use std::sync::Arc;

use super::utils::is_repartition;
use crate::error::Result;
use crate::physical_optimizer::utils::{is_coalesce_partitions, is_sort};
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, VisitRecursionIterator,
};
use datafusion_physical_plan::unbounded_output;

/// For a given `plan`, this object carries the information one needs from its
/// descendants to decide whether it is beneficial to replace order-losing (but
/// somewhat faster) variants of certain operators with their order-preserving
/// (but somewhat slower) cousins.
#[derive(Debug, Clone)]
pub(crate) struct OrderPreservationContext {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    ordering_connection: bool,
    children_nodes: Vec<Self>,
}

impl OrderPreservationContext {
    /// Creates an empty context tree. Each node has `false` connections.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let children = plan.children();
        Self {
            plan,
            ordering_connection: false,
            children_nodes: children.into_iter().map(Self::new).collect(),
        }
    }

    /// Creates a new order-preservation context from those of children nodes.
    pub fn update_children(mut self) -> Result<Self> {
        for node in self.children_nodes.iter_mut() {
            let plan = node.plan.clone();
            let children = plan.children();
            let maintains_input_order = plan.maintains_input_order();
            let inspect_child = |idx| {
                maintains_input_order[idx]
                    || is_coalesce_partitions(&plan)
                    || is_repartition(&plan)
            };

            // We cut the path towards nodes that do not maintain ordering.
            for (idx, c) in node.children_nodes.iter_mut().enumerate() {
                c.ordering_connection &= inspect_child(idx);
            }

            node.ordering_connection = if children.is_empty() {
                false
            } else if !node.children_nodes[0].ordering_connection
                && ((is_repartition(&plan) && !maintains_input_order[0])
                    || (is_coalesce_partitions(&plan)
                        && children[0].output_ordering().is_some()))
            {
                // We either have a RepartitionExec or a CoalescePartitionsExec
                // and they lose their input ordering, so initiate connection:
                true
            } else {
                // Maintain connection if there is a child with a connection,
                // and operator can possibly maintain that connection (either
                // in its current form or when we replace it with the corresponding
                // order preserving operator).
                node.children_nodes
                    .iter()
                    .enumerate()
                    .any(|(idx, c)| c.ordering_connection && inspect_child(idx))
            }
        }

        self.plan = with_new_children_if_necessary(
            self.plan,
            self.children_nodes.iter().map(|c| c.plan.clone()).collect(),
        )?
        .into();
        self.ordering_connection = false;
        Ok(self)
    }
}

impl TreeNode for OrderPreservationContext {
    fn children_nodes(&self) -> Vec<Cow<Self>> {
        self.children_nodes.iter().map(Cow::Borrowed).collect()
    }

    fn map_children<F>(mut self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        if !self.children_nodes.is_empty() {
            self.children_nodes = self
                .children_nodes
                .into_iter()
                .map(transform)
                .collect::<Result<_>>()?;
            self.plan = with_new_children_if_necessary(
                self.plan,
                self.children_nodes.iter().map(|c| c.plan.clone()).collect(),
            )?
            .into();
        }
        Ok(self)
    }

    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        if !self.children_nodes.is_empty() {
            let tnr = self.children_nodes.iter_mut().for_each_till_continue(f)?;
            self.plan = with_new_children_if_necessary(
                self.plan.clone(),
                self.children_nodes.iter().map(|c| c.plan.clone()).collect(),
            )?
            .into();
            Ok(tnr)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    }
}

/// Calculates the updated plan by replacing operators that lose ordering
/// inside `sort_input` with their order-preserving variants. This will
/// generate an alternative plan, which will be accepted or rejected later on
/// depending on whether it helps us remove a `SortExec`.
fn get_updated_plan(
    mut sort_input: OrderPreservationContext,
    // Flag indicating that it is desirable to replace `RepartitionExec`s with
    // `SortPreservingRepartitionExec`s:
    is_spr_better: bool,
    // Flag indicating that it is desirable to replace `CoalescePartitionsExec`s
    // with `SortPreservingMergeExec`s:
    is_spm_better: bool,
) -> Result<OrderPreservationContext> {
    let updated_children = sort_input
        .children_nodes
        .clone()
        .into_iter()
        .map(|item| {
            // Update children and their descendants in the given tree if the connection is open:
            if item.ordering_connection {
                get_updated_plan(item, is_spr_better, is_spm_better)
            } else {
                Ok(item)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    sort_input.plan = sort_input
        .plan
        .with_new_children(updated_children.iter().map(|c| c.plan.clone()).collect())?;
    sort_input.ordering_connection = false;
    sort_input.children_nodes = updated_children;

    // When a `RepartitionExec` doesn't preserve ordering, replace it with
    // a sort-preserving variant if appropriate:
    if is_repartition(&sort_input.plan)
        && !sort_input.plan.maintains_input_order()[0]
        && is_spr_better
    {
        let child = sort_input.plan.children().swap_remove(0);
        let repartition =
            RepartitionExec::try_new(child, sort_input.plan.output_partitioning())?
                .with_preserve_order();
        sort_input.plan = Arc::new(repartition) as _;
        sort_input.children_nodes[0].ordering_connection = true;
    } else if is_coalesce_partitions(&sort_input.plan) && is_spm_better {
        // When the input of a `CoalescePartitionsExec` has an ordering, replace it
        // with a `SortPreservingMergeExec` if appropriate:
        if let Some(ordering) = sort_input.children_nodes[0]
            .plan
            .output_ordering()
            .map(|o| o.to_vec())
        {
            // Now we can mutate `new_node.children_nodes` safely
            let child = sort_input.children_nodes.clone().swap_remove(0);
            sort_input.plan =
                Arc::new(SortPreservingMergeExec::new(ordering, child.plan)) as _;
            sort_input.children_nodes[0].ordering_connection = true;
        }
    }

    Ok(sort_input)
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
/// 1_1. During the traversal, keep track of operators that maintain ordering
///    (or can maintain ordering when replaced by an order-preserving variant) until
///    a `SortExec` is found.
/// 2. When a `SortExec` is found, update the child of the `SortExec` by replacing
///    operators that do not preserve ordering in the tree with their order
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
    let mut requirements = requirements.update_children()?;
    if !(is_sort(&requirements.plan)
        && requirements.children_nodes[0].ordering_connection)
    {
        return Ok(Transformed::No(requirements));
    }

    // For unbounded cases, replace with the order-preserving variant in
    // any case, as doing so helps fix the pipeline.
    // Also do the replacement if opted-in via config options.
    let use_order_preserving_variant =
        config.optimizer.prefer_existing_sort || unbounded_output(&requirements.plan);

    let mut updated_sort_input = get_updated_plan(
        requirements.children_nodes.clone().swap_remove(0),
        is_spr_better || use_order_preserving_variant,
        is_spm_better || use_order_preserving_variant,
    )?;

    // If this sort is unnecessary, we should remove it and update the plan:
    if updated_sort_input
        .plan
        .equivalence_properties()
        .ordering_satisfy(requirements.plan.output_ordering().unwrap_or(&[]))
    {
        for child in updated_sort_input.children_nodes.iter_mut() {
            child.ordering_connection = false;
        }
        Ok(Transformed::Yes(updated_sort_input))
    } else {
        for child in requirements.children_nodes.iter_mut() {
            child.ordering_connection = false;
        }
        Ok(Transformed::Yes(requirements))
    }
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
    use crate::test::TestStreamPartition;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::tree_node::TreeNode;
    use datafusion_common::{Result, Statistics};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::{JoinType, Operator};
    use datafusion_physical_expr::expressions::{self, col, Column};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_plan::streaming::StreamingTableExec;

    use rstest::rstest;

    /// Runs the `replace_with_order_preserving_variants` sub-rule and asserts
    /// the plan against the original and expected plans for both bounded and
    /// unbounded cases.
    ///
    /// # Parameters
    ///
    /// * `EXPECTED_UNBOUNDED_PLAN_LINES`: Expected input unbounded plan.
    /// * `EXPECTED_BOUNDED_PLAN_LINES`: Expected input bounded plan.
    /// * `EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES`: Optimized plan, which is
    ///   the same regardless of the value of the `prefer_existing_sort` flag.
    /// * `EXPECTED_BOUNDED_OPTIMIZED_PLAN_LINES`: Optimized plan when the flag
    ///   `prefer_existing_sort` is `false` for bounded cases.
    /// * `EXPECTED_BOUNDED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES`: Optimized plan
    ///   when the flag `prefer_existing_sort` is `true` for bounded cases.
    /// * `$PLAN`: The plan to optimize.
    /// * `$SOURCE_UNBOUNDED`: Whether the given plan contains an unbounded source.
    macro_rules! assert_optimized_in_all_boundedness_situations {
        ($EXPECTED_UNBOUNDED_PLAN_LINES: expr,  $EXPECTED_BOUNDED_PLAN_LINES: expr, $EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES: expr, $EXPECTED_BOUNDED_OPTIMIZED_PLAN_LINES: expr, $EXPECTED_BOUNDED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $SOURCE_UNBOUNDED: expr) => {
            if $SOURCE_UNBOUNDED {
                assert_optimized_prefer_sort_on_off!(
                    $EXPECTED_UNBOUNDED_PLAN_LINES,
                    $EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES,
                    $EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES,
                    $PLAN
                );
            } else {
                assert_optimized_prefer_sort_on_off!(
                    $EXPECTED_BOUNDED_PLAN_LINES,
                    $EXPECTED_BOUNDED_OPTIMIZED_PLAN_LINES,
                    $EXPECTED_BOUNDED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES,
                    $PLAN
                );
            }
        };
    }

    /// Runs the `replace_with_order_preserving_variants` sub-rule and asserts
    /// the plan against the original and expected plans.
    ///
    /// # Parameters
    ///
    /// * `$EXPECTED_PLAN_LINES`: Expected input plan.
    /// * `EXPECTED_OPTIMIZED_PLAN_LINES`: Optimized plan when the flag
    ///   `prefer_existing_sort` is `false`.
    /// * `EXPECTED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES`: Optimized plan when
    ///   the flag `prefer_existing_sort` is `true`.
    /// * `$PLAN`: The plan to optimize.
    macro_rules! assert_optimized_prefer_sort_on_off {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $EXPECTED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr) => {
            assert_optimized!(
                $EXPECTED_PLAN_LINES,
                $EXPECTED_OPTIMIZED_PLAN_LINES,
                $PLAN.clone(),
                false
            );
            assert_optimized!(
                $EXPECTED_PLAN_LINES,
                $EXPECTED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES,
                $PLAN,
                true
            );
        };
    }

    /// Runs the `replace_with_order_preserving_variants` sub-rule and asserts
    /// the plan against the original and expected plans.
    ///
    /// # Parameters
    ///
    /// * `$EXPECTED_PLAN_LINES`: Expected input plan.
    /// * `$EXPECTED_OPTIMIZED_PLAN_LINES`: Expected optimized plan.
    /// * `$PLAN`: The plan to optimize.
    /// * `$PREFER_EXISTING_SORT`: Value of the `prefer_existing_sort` flag.
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $PREFER_EXISTING_SORT: expr) => {
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
            let config = SessionConfig::new().with_prefer_existing_sort($PREFER_EXISTING_SORT);
            let plan_with_pipeline_fixer = OrderPreservationContext::new(physical_plan);
            let parallel = plan_with_pipeline_fixer.transform_up_old(&|plan_with_pipeline_fixer| replace_with_order_preserving_variants(plan_with_pipeline_fixer, false, false, config.options()))?;
            let optimized_physical_plan = parallel.plan;

            // Get string representation of the plan
            let actual = get_plan_string(&optimized_physical_plan);
            assert_eq!(
                expected_optimized_lines, actual,
                "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );
        };
    }

    #[rstest]
    #[tokio::test]
    // Searches for a simple sort and a repartition just after it, the second repartition with 1 input partition should not be affected
    async fn test_replace_multiple_input_repartition_1(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition = repartition_exec_hash(repartition_exec_round_robin(source));
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_inter_children_change_only(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr_default("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
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

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortPreservingMergeExec: [a@0 ASC]",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortPreservingMergeExec: [a@0 ASC]",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_replace_multiple_input_repartition_2(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec_round_robin(source);
        let filter = filter_exec(repartition_rr);
        let repartition_hash = repartition_exec_hash(filter);
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded =  [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded =  [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded =  [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_replace_multiple_input_repartition_with_extra_steps(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);
        let sort = sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_replace_multiple_input_repartition_with_extra_steps_2(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec_round_robin(source);
        let coalesce_batches_exec_1 = coalesce_batches_exec(repartition_rr);
        let repartition_hash = repartition_exec_hash(coalesce_batches_exec_1);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches_exec_2 = coalesce_batches_exec(filter);
        let sort =
            sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec_2, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_not_replacing_when_no_need_to_preserve_sorting(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);

        let physical_plan: Arc<dyn ExecutionPlan> =
            coalesce_partitions_exec(coalesce_batches_exec);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results same with and without flag, because there is no executor  with ordering requirement
        let expected_optimized_bounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = expected_optimized_bounded;

        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_multiple_replacable_repartitions(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash);
        let coalesce_batches = coalesce_batches_exec(filter);
        let repartition_hash_2 = repartition_exec_hash(coalesce_batches);
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash_2, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_not_replace_with_different_orderings(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
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

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results same with and without flag, because ordering requirement of the executor is different than the existing ordering.
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = expected_optimized_bounded;

        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_lost_ordering(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan =
            sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions, false);

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_lost_and_kept_ordering(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = if source_unbounded {
            stream_exec_ordered(&schema, sort_exprs)
        } else {
            csv_exec_sorted(&schema, sort_exprs)
        };
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

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=c@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[c@1 ASC]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results with and without flag
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=c@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[c@1 ASC]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_multiple_child_trees(
        #[values(false, true)] source_unbounded: bool,
    ) -> Result<()> {
        let schema = create_test_schema()?;

        let left_sort_exprs = vec![sort_expr("a", &schema)];
        let left_source = if source_unbounded {
            stream_exec_ordered(&schema, left_sort_exprs)
        } else {
            csv_exec_sorted(&schema, left_sort_exprs)
        };
        let left_repartition_rr = repartition_exec_round_robin(left_source);
        let left_repartition_hash = repartition_exec_hash(left_repartition_rr);
        let left_coalesce_partitions =
            Arc::new(CoalesceBatchesExec::new(left_repartition_hash, 4096));

        let right_sort_exprs = vec![sort_expr("a", &schema)];
        let right_source = if source_unbounded {
            stream_exec_ordered(&schema, right_sort_exprs)
        } else {
            csv_exec_sorted(&schema, right_sort_exprs)
        };
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

        // Expected inputs unbounded and bounded
        let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        // Expected unbounded result (same for with and without flag)
        let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

        // Expected bounded results same with and without flag, because ordering get lost during intermediate executor anyway. Hence no need to preserve
        // existing ordering.
        let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized_bounded_sort_preserve = expected_optimized_bounded;

        assert_optimized_in_all_boundedness_situations!(
            expected_input_unbounded,
            expected_input_bounded,
            expected_optimized_unbounded,
            expected_optimized_bounded,
            expected_optimized_bounded_sort_preserve,
            physical_plan,
            source_unbounded
        );
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
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
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

    // creates a stream exec source for the test purposes
    fn stream_exec_ordered(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        let projection: Vec<usize> = vec![0, 2, 3];

        Arc::new(
            StreamingTableExec::try_new(
                schema.clone(),
                vec![Arc::new(TestStreamPartition {
                    schema: schema.clone(),
                }) as _],
                Some(&projection),
                vec![sort_exprs],
                true,
            )
            .unwrap(),
        )
    }

    // creates a csv exec source for the test purposes
    // projection and has_header parameters are given static due to testing needs
    fn csv_exec_sorted(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
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
            },
            true,
            0,
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }
}
