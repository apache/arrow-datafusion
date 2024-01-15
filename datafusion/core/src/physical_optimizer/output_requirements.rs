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

//! The GlobalOrderRequire optimizer rule either:
//! - Adds an auxiliary `OutputRequirementExec` operator to keep track of global
//!   ordering and distribution requirement across rules, or
//! - Removes the auxiliary `OutputRequirementExec` operator from the physical plan.
//!   Since the `OutputRequirementExec` operator is only a helper operator, it
//!   shouldn't occur in the final plan (i.e. the executed plan).

use std::sync::Arc;

use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};

use arrow_schema::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, Statistics};
use datafusion_physical_expr::{
    Distribution, LexRequirement, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

/// This rule either adds or removes [`OutputRequirements`]s to/from the physical
/// plan according to its `mode` attribute, which is set by the constructors
/// `new_add_mode` and `new_remove_mode`. With this rule, we can keep track of
/// the global requirements (ordering and distribution) across rules.
///
/// The primary usecase of this node and rule is to specify and preserve the desired output
/// ordering and distribution the entire plan. When sending to a single client, a single partition may
/// be desirable, but when sending to a multi-partitioned writer, keeping multiple partitions may be
/// better.
#[derive(Debug)]
pub struct OutputRequirements {
    mode: RuleMode,
}

impl OutputRequirements {
    /// Create a new rule which works in `Add` mode; i.e. it simply adds a
    /// top-level [`OutputRequirementExec`] into the physical plan to keep track
    /// of global ordering and distribution requirements if there are any.
    /// Note that this rule should run at the beginning.
    pub fn new_add_mode() -> Self {
        Self {
            mode: RuleMode::Add,
        }
    }

    /// Create a new rule which works in `Remove` mode; i.e. it simply removes
    /// the top-level [`OutputRequirementExec`] from the physical plan if there is
    /// any. We do this because a `OutputRequirementExec` is an ancillary,
    /// non-executable operator whose sole purpose is to track global
    /// requirements during optimization. Therefore, a
    /// `OutputRequirementExec` should not appear in the final plan.
    pub fn new_remove_mode() -> Self {
        Self {
            mode: RuleMode::Remove,
        }
    }
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Hash)]
enum RuleMode {
    Add,
    Remove,
}

/// An ancillary, non-executable operator whose sole purpose is to track global
/// requirements during optimization. It imposes
/// - the ordering requirement in its `order_requirement` attribute.
/// - the distribution requirement in its `dist_requirement` attribute.
///
/// See [`OutputRequirements`] for more details
#[derive(Debug)]
pub(crate) struct OutputRequirementExec {
    input: Arc<dyn ExecutionPlan>,
    order_requirement: Option<LexRequirement>,
    dist_requirement: Distribution,
}

impl OutputRequirementExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        requirements: Option<LexRequirement>,
        dist_requirement: Distribution,
    ) -> Self {
        Self {
            input,
            order_requirement: requirements,
            dist_requirement,
        }
    }

    pub(crate) fn input(&self) -> Arc<dyn ExecutionPlan> {
        self.input.clone()
    }
}

impl DisplayAs for OutputRequirementExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "OutputRequirementExec")
    }
}

impl ExecutionPlan for OutputRequirementExec {
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![self.dist_requirement.clone()]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![self.order_requirement.clone()]
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        // Has a single child
        Ok(children[0])
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.remove(0), // has a single child
            self.order_requirement.clone(),
            self.dist_requirement.clone(),
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

impl PhysicalOptimizerRule for OutputRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.mode {
            RuleMode::Add => require_top_ordering(plan),
            RuleMode::Remove => plan.transform_up_old(&|plan| {
                if let Some(sort_req) =
                    plan.as_any().downcast_ref::<OutputRequirementExec>()
                {
                    Ok(Transformed::Yes(sort_req.input()))
                } else {
                    Ok(Transformed::No(plan))
                }
            }),
        }
    }

    fn name(&self) -> &str {
        "OutputRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This functions adds ancillary `OutputRequirementExec` to the the physical plan, so that
/// global requirements are not lost during optimization.
fn require_top_ordering(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let (new_plan, is_changed) = require_top_ordering_helper(plan)?;
    if is_changed {
        Ok(new_plan)
    } else {
        // Add `OutputRequirementExec` to the top, with no specified ordering and distribution requirement.
        Ok(Arc::new(OutputRequirementExec::new(
            new_plan,
            // there is no ordering requirement
            None,
            Distribution::UnspecifiedDistribution,
        )) as _)
    }
}

/// Helper function that adds an ancillary `OutputRequirementExec` to the given plan.
/// First entry in the tuple is resulting plan, second entry indicates whether any
/// `OutputRequirementExec` is added to the plan.
fn require_top_ordering_helper(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    let mut children = plan.children();
    // Global ordering defines desired ordering in the final result.
    if children.len() != 1 {
        Ok((plan, false))
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let req_ordering = sort_exec.output_ordering().unwrap_or(&[]);
        let req_dist = sort_exec.required_input_distribution()[0].clone();
        let reqs = PhysicalSortRequirement::from_sort_exprs(req_ordering);
        Ok((
            Arc::new(OutputRequirementExec::new(plan, Some(reqs), req_dist)) as _,
            true,
        ))
    } else if let Some(spm) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let reqs = PhysicalSortRequirement::from_sort_exprs(spm.expr());
        Ok((
            Arc::new(OutputRequirementExec::new(
                plan,
                Some(reqs),
                Distribution::SinglePartition,
            )) as _,
            true,
        ))
    } else if plan.maintains_input_order()[0]
        && plan.required_input_ordering()[0].is_none()
    {
        // Keep searching for a `SortExec` as long as ordering is maintained,
        // and on-the-way operators do not themselves require an ordering.
        // When an operator requires an ordering, any `SortExec` below can not
        // be responsible for (i.e. the originator of) the global ordering.
        let (new_child, is_changed) =
            require_top_ordering_helper(children.swap_remove(0))?;
        Ok((plan.with_new_children(vec![new_child])?, is_changed))
    } else {
        // Stop searching, there is no global ordering desired for the query.
        Ok((plan, false))
    }
}
