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

//! Utilizing exact statistics from sources to avoid scanning data
use std::sync::Arc;

use crate::execution::context::ExecutionConfig;
use crate::logical_plan::JoinType;
use crate::physical_plan::cross_join::CrossJoinExec;
use crate::physical_plan::hash_join::HashJoinExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{expressions, ExecutionPlan, PhysicalExpr};

use super::optimizer::PhysicalOptimizerRule;
use super::utils::optimize_children;
use crate::error::Result;

/// BuildProbeOrder reorders the build and probe phase of
/// hash joins. This uses the amount of rows that a datasource has.
/// The rule optimizes the order such that the left (build) side of the join
/// is the smallest.
/// If the information is not available, the order stays the same,
/// so that it could be optimized manually in a query.
pub struct HashBuildProbeOrder {}

impl HashBuildProbeOrder {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn should_swap_join_order(left: &dyn ExecutionPlan, right: &dyn ExecutionPlan) -> bool {
    let left_rows = left.statistics().num_rows;
    let right_rows = right.statistics().num_rows;

    match (left_rows, right_rows) {
        (Some(l), Some(r)) => l > r,
        _ => false,
    }
}

fn supports_swap(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => true,
        JoinType::Semi | JoinType::Anti => false,
    }
}

fn swap_join_type(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Inner => JoinType::Inner,
        JoinType::Full => JoinType::Full,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        _ => unreachable!(),
    }
}

/// When the order of the join is changed by the optimizer,
/// the columns in the output should not be impacted.
/// This helper creates the expressions that will allow to swap
/// back the values from the original left as first columns and
/// those on the right next
fn swap_reverting_projection(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    let right_schema = right.schema();
    let right_cols = right_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(expressions::Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });
    let right_len = right_cols.len();
    let left_schema = left.schema();
    let left_cols = left_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(expressions::Column::new(f.name(), right_len + i))
                as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });

    left_cols.chain(right_cols).collect()
}

impl PhysicalOptimizerRule for HashBuildProbeOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        execution_config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = optimize_children(self, plan, execution_config)?;
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let left = hash_join.left();
            let right = hash_join.right();
            if should_swap_join_order(&**left, &**right)
                && supports_swap(*hash_join.join_type())
            {
                let new_join = HashJoinExec::try_new(
                    Arc::clone(right),
                    Arc::clone(left),
                    hash_join
                        .on()
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect(),
                    &swap_join_type(*hash_join.join_type()),
                    *hash_join.partition_mode(),
                )?;
                let proj = ProjectionExec::try_new(
                    swap_reverting_projection(&**left, &**right),
                    Arc::new(new_join),
                )?;
                return Ok(Arc::new(proj));
            }
        } else if let Some(cross_join) = plan.as_any().downcast_ref::<CrossJoinExec>() {
            let left = cross_join.left();
            let right = cross_join.right();
            if should_swap_join_order(&**left, &**right) {
                let new_join =
                    CrossJoinExec::try_new(Arc::clone(right), Arc::clone(left))?;
                let proj = ProjectionExec::try_new(
                    swap_reverting_projection(&**left, &**right),
                    Arc::new(new_join),
                )?;
                return Ok(Arc::new(proj));
            }
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "hash_build_probe_order"
    }
}
