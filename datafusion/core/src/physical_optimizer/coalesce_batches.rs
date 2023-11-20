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

//! CoalesceBatches optimizer that groups batches together rows
//! in bigger batches to avoid overhead with small batches

use crate::config::ConfigOptions;
use crate::physical_optimizer::utils::get_plan_string;
use crate::{
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, filter::FilterExec, joins::HashJoinExec,
        repartition::RepartitionExec, Partitioning,
    },
};
use datafusion_common::tree_node::{DynTreeNode, Transformed, TreeNode, VisitRecursion};
use datafusion_physical_plan::ExecutionPlan;
use itertools::Itertools;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use super::utils::is_recursive_query;

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default)]
pub struct CoalesceBatches {}

impl CoalesceBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

struct CoalesceContext {
    plan: Arc<dyn ExecutionPlan>,
    // keep track of whether we've encountered a RecursiveQuery
    has_recursive_ancestor: bool,
}

impl CoalesceContext {
    /// Only use this method at the root of the plan.
    /// All other contexts should be created using `new_descendent`.
    fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            has_recursive_ancestor: is_recursive_query(&plan),
            plan,
        }
    }

    /// Creates a new context for a descendent of this context.
    /// The descendent will inherit the `has_recursive_ancestor` flag from this context.
    fn new_descendent(&self, descendent_plan: Arc<dyn ExecutionPlan>) -> Self {
        let ancestor = self;
        Self {
            has_recursive_ancestor: ancestor.has_recursive_ancestor
                || is_recursive_query(&descendent_plan),
            plan: descendent_plan,
        }
    }

    /// Computes distribution tracking contexts for every child of the plan.
    fn children(&self) -> Vec<CoalesceContext> {
        self.plan
            .children()
            .into_iter()
            .map(|child| self.new_descendent(child))
            .collect()
    }
}

impl TreeNode for CoalesceContext {
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
            let new_children = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;

            Ok(self.new_descendent(
                self.plan.with_new_arc_children(
                    self.plan.clone(),
                    new_children
                        .into_iter()
                        .map(|CoalesceContext { plan, .. }| plan)
                        .collect(),
                )?,
            ))
        }
    }
}

impl fmt::Display for CoalesceContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let plan_string = get_plan_string(&self.plan);
        write!(f, "plan: {:?}", plan_string)?;
        write!(
            f,
            "has_recursive_ancestor: {:?}",
            self.has_recursive_ancestor
        )?;
        write!(f, "")
    }
}

impl PhysicalOptimizerRule for CoalesceBatches {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }

        let target_batch_size = config.execution.batch_size;
        let ctx = CoalesceContext::new(plan);
        let CoalesceContext { plan, .. } = ctx.transform_up(&|ctx| {
            if ctx.has_recursive_ancestor {
                return Ok(Transformed::No(ctx));
            }
            let plan_any = ctx.plan.as_any();
            // The goal here is to detect operators that could produce small batches and only
            // wrap those ones with a CoalesceBatchesExec operator. An alternate approach here
            // would be to build the coalescing logic directly into the operators
            // See https://github.com/apache/arrow-datafusion/issues/139
            let wrap_in_coalesce = plan_any.downcast_ref::<FilterExec>().is_some()
                || plan_any.downcast_ref::<HashJoinExec>().is_some()
                // Don't need to add CoalesceBatchesExec after a round robin RepartitionExec
                || plan_any
                    .downcast_ref::<RepartitionExec>()
                    .map(|repart_exec| {
                        !matches!(
                            repart_exec.partitioning().clone(),
                            Partitioning::RoundRobinBatch(_)
                        )
                    })
                    .unwrap_or(false);
            if wrap_in_coalesce {
                Ok(Transformed::Yes(ctx.new_descendent(Arc::new(
                    CoalesceBatchesExec::new(ctx.plan.clone(), target_batch_size),
                ))))
            } else {
                Ok(Transformed::No(ctx))
            }
        })?;

        Ok(plan)
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
