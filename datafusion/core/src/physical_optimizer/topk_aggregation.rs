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

//! An optimizer rule that detects aggregate operations that could use a limited bucket count

use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::AggregateExec;
use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::ExecutionPlan;
use arrow_schema::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalSortExpr;
use itertools::Itertools;
use std::sync::Arc;

/// An optimizer rule that passes a `limit` hint to aggregations if the whole result is not needed
pub struct TopKAggregation {}

impl TopKAggregation {
    /// Create a new `LimitAggregation`
    pub fn new() -> Self {
        Self {}
    }

    fn transform_agg(
        aggr: &AggregateExec,
        order: &PhysicalSortExpr,
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // ensure the sort direction matches aggregate function
        let (field, desc) = aggr.get_minmax_desc()?;
        if desc != order.options.descending {
            return None;
        }
        let group_key = aggr.group_expr().expr().iter().exactly_one().ok()?;
        let kt = group_key.0.data_type(&aggr.input().schema()).ok()?;
        if !kt.is_primitive() && kt != DataType::Utf8 {
            return None;
        }
        if aggr.filter_expr().iter().any(|e| e.is_some()) {
            return None;
        }

        // ensure the sort is on the same field as the aggregate output
        let col = order.expr.as_any().downcast_ref::<Column>()?;
        if col.name() != field.name() {
            return None;
        }

        // We found what we want: clone, copy the limit down, and return modified node
        let new_aggr = AggregateExec::try_new(
            *aggr.mode(),
            aggr.group_by().clone(),
            aggr.aggr_expr().to_vec(),
            aggr.filter_expr().to_vec(),
            aggr.input().clone(),
            aggr.input_schema(),
        )
        .expect("Unable to copy Aggregate!")
        .with_limit(Some(limit));
        Some(Arc::new(new_aggr))
    }

    fn transform_sort(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let sort = plan.as_any().downcast_ref::<SortExec>()?;

        let children = sort.children();
        let child = children.iter().exactly_one().ok()?;
        let order = sort.output_ordering()?;
        let order = order.iter().exactly_one().ok()?;
        let limit = sort.fetch()?;

        let is_cardinality_preserving = |plan: Arc<dyn ExecutionPlan>| {
            plan.as_any()
                .downcast_ref::<CoalesceBatchesExec>()
                .is_some()
                || plan.as_any().downcast_ref::<RepartitionExec>().is_some()
                || plan.as_any().downcast_ref::<FilterExec>().is_some()
        };

        let mut cardinality_preserved = true;
        let mut closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::No(plan));
            }
            if let Some(aggr) = plan.as_any().downcast_ref::<AggregateExec>() {
                // either we run into an Aggregate and transform it
                match Self::transform_agg(aggr, order, limit) {
                    None => cardinality_preserved = false,
                    Some(plan) => return Ok(Transformed::Yes(plan)),
                }
            } else {
                // or we continue down whitelisted nodes of other types
                if !is_cardinality_preserving(plan.clone()) {
                    cardinality_preserved = false;
                }
            }
            Ok(Transformed::No(plan))
        };
        let child = child.clone().transform_down_mut(&mut closure).ok()?;
        let sort = SortExec::new(sort.expr().to_vec(), child)
            .with_fetch(sort.fetch())
            .with_preserve_partitioning(sort.preserve_partitioning());
        Some(Arc::new(sort))
    }
}

impl Default for TopKAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for TopKAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = if config.optimizer.enable_topk_aggregation {
            plan.transform_down_old(&|plan| {
                Ok(
                    if let Some(plan) = TopKAggregation::transform_sort(plan.clone()) {
                        Transformed::Yes(plan)
                    } else {
                        Transformed::No(plan)
                    },
                )
            })?
        } else {
            plan
        };
        Ok(plan)
    }

    fn name(&self) -> &str {
        "LimitAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// see `aggregate.slt` for tests
