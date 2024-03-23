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

//! The [`Limit`] rule tries to modify a given plan so that it can
//! accommodate infinite sources and utilize statistical information (if there
//! is any) to obtain more performant plans. To achieve the first goal, it
//! tries to transform a non-runnable query (with the given infinite sources)
//! into a runnable query by replacing pipeline-breaking join operations with
//! pipeline-friendly ones. To achieve the second goal, it selects the proper
//! `PartitionMode` and the build side using the available statistics for hash joins.

use std::sync::Arc;

use crate::datasource::physical_plan::CsvExec;
use crate::physical_optimizer::utils::is_global_limit;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::AggregateExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_optimizer::push_down_limit;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use hashbrown::raw::Global;
use itertools::Itertools;
pub struct LimitPushdown {}

impl LimitPushdown {
    fn new() -> Self {
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

    fn name() -> &str {
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
    let modified =
        if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
            let input = global_limit.input().as_any();
            if let Some(coalesce_partition_exec) =
                input.downcast_ref::<CoalescePartitionsExec>()
            {
                general_swap(plan)
            } else if let Some(coalesce_batch_exec) =
                input.downcast_ref::<CoalesceBatchesExec>()
            {
                general_swap(plan)
            } else {
                None
            }
        } else {
            Ok(Transformed::no(plan))
        };
}
fn general_swap(plan: &GlobalLimitExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {}
