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

//! The [`LimitPushdown`] The LimitPushdown optimization rule is designed
//! to improve the performance of query execution by pushing the LIMIT clause down
//!  through the execution plan as far as possible, ideally directly
//! to the [`CoalesceBatchesExec`]. to reduce target_batch_size This means that instead of processing
//! a large amount of data and then applying the limit at the end,
//! the system tries to limit the amount of data being processed throughout the execution of the query.

use std::sync::Arc;

use crate::physical_optimizer::utils::is_global_limit;
use crate::physical_optimizer::PhysicalOptimizerRule;

use crate::physical_plan::limit::GlobalLimitExec;
use crate::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;

use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;

#[allow(missing_docs)]
pub struct LimitPushdown {}

impl LimitPushdown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for LimitPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // if this node is not a global limit, then directly return
        if !is_global_limit(&plan) {
            return Ok(plan);
        }
        // we traverse the treenode to try to push down the limit same logic as project push down
        plan.transform_down(&push_down_limit).data()
    }

    fn name(&self) -> &str {
        "LimitPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
impl LimitPushdown {}
fn new_global_limit_with_input() {}
// try to push down current limit, based on the son
fn push_down_limit(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let input = global_limit.input().as_any();
        if let Some(_) = input.downcast_ref::<CoalescePartitionsExec>() {
            return Ok(Transformed::yes(swap_with_coalesce_partition(global_limit)));
        } else if let Some(coalesce_batches) = input.downcast_ref::<CoalesceBatchesExec>()
        {
            return Ok(Transformed::yes(reset_and_get_new_limit(
                global_limit,
                coalesce_batches,
            )));
        } else {
            return Ok(Transformed::no(plan));
        }
    } else {
        return Ok(Transformed::no(plan));
    }
}
// swap the coalesce_patition exec with current limit
fn swap_with_coalesce_partition(plan: &GlobalLimitExec) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalescePartitionsExec::new(make_with_child(
        plan,
        &plan.input().children()[0],
    )))
}

// create a new node with its child
fn make_with_child(
    plan: &GlobalLimitExec,
    child: &Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(
        child.clone(),
        plan.skip(),
        plan.fetch(),
    ))
}

// reset target size and return a new plan
fn reset_and_get_new_limit(
    plan: &GlobalLimitExec,
    child: &CoalesceBatchesExec,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(
        Arc::new(CoalesceBatchesExec::new(
            child.input().clone(),
            plan.fetch().unwrap_or(child.target_batch_size()),
        )),
        plan.skip(),
        plan.fetch(),
    ))
}
