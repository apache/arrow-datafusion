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

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;

/// Optimization rule that eliminate unnecessary [LogicalPlan::Projection].
#[derive(Default)]
pub struct EliminateProjection;

impl EliminateProjection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateProjection {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(projection) => {
                let child_plan = projection.input.as_ref();
                if plan.schema() == child_plan.schema() {
                    Ok(Some(child_plan.clone()))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}
