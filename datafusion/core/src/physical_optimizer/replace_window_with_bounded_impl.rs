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

use crate::physical_plan::windows::BoundedWindowAggExec;
use crate::physical_plan::windows::WindowAggExec;
use crate::{
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, filter::FilterExec, joins::HashJoinExec,
        repartition::RepartitionExec, rewrite::TreeNodeRewritable,
    },
};
use datafusion_expr::WindowFrameUnits;
use datafusion_physical_expr::window::WindowExpr;
use std::sync::Arc;

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default)]
pub struct ReplaceWindowWithBoundedImpl {}

impl ReplaceWindowWithBoundedImpl {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl PhysicalOptimizerRule for ReplaceWindowWithBoundedImpl {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        _config: &crate::execution::context::SessionConfig,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(window_agg_exec) = plan.as_any().downcast_ref::<WindowAggExec>() {
                // println!("do analysis for bounded impl");
                let is_contains_groups = window_agg_exec
                    .window_expr()
                    .iter()
                    .any(|elem| is_window_frame_groups(elem));
                let can_run_bounded = window_agg_exec
                    .window_expr()
                    .iter()
                    .all(|elem| elem.can_run_bounded());
                // println!("is_contains_groups: {:?}", is_contains_groups);
                // println!("can_run_bounded: {:?}", can_run_bounded);
                if !is_contains_groups && can_run_bounded {
                    // println!("changing with bounded");
                    return Ok(Some(Arc::new(BoundedWindowAggExec::try_new(
                        window_agg_exec.window_expr().to_vec(),
                        window_agg_exec.input().clone(),
                        window_agg_exec.input().schema(),
                        window_agg_exec.partition_keys.clone(),
                        window_agg_exec.sort_keys.clone(),
                    )?)));
                }
            }
            Ok(None)
        })
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Checks window expression whether it is GROUPS mode
fn is_window_frame_groups(window_expr: &Arc<dyn WindowExpr>) -> bool {
    match window_expr.get_window_frame().units {
        WindowFrameUnits::Groups => true,
        _ => false,
    }
}
