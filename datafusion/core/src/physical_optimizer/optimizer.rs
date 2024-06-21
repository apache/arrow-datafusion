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

//! Physical optimizer traits

use std::sync::Arc;

use super::projection_pushdown::ProjectionPushdown;
use super::update_aggr_exprs::OptimizeAggregateOrder;
use crate::config::ConfigOptions;
use crate::physical_optimizer::aggregate_statistics::AggregateStatistics;
use crate::physical_optimizer::coalesce_batches::CoalesceBatches;
use crate::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use crate::physical_optimizer::enforce_distribution::EnforceDistribution;
use crate::physical_optimizer::enforce_sorting::EnforceSorting;
use crate::physical_optimizer::join_selection::JoinSelection;
use crate::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use crate::physical_optimizer::output_requirements::OutputRequirements;
use crate::physical_optimizer::pipeline_checker::PipelineChecker;
use crate::physical_optimizer::topk_aggregation::TopKAggregation;
use crate::{error::Result, physical_plan::ExecutionPlan};

/// `PhysicalOptimizerRule` transforms one ['ExecutionPlan'] into another which
/// computes the same results, but in a potentially more efficient way.
///
/// Use [`SessionState::add_physical_optimizer_rule`] to register additional
/// `PhysicalOptimizerRule`s.
///
/// [`SessionState::add_physical_optimizer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.add_physical_optimizer_rule
pub trait PhysicalOptimizerRule {
    /// Rewrite `plan` to an optimized form
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// A flag to indicate whether the physical planner should valid the rule will not
    /// change the schema of the plan after the rewriting.
    /// Some of the optimization rules might change the nullable properties of the schema
    /// and should disable the schema check.
    fn schema_check(&self) -> bool;
}

/// A rule-based physical optimizer.
#[derive(Clone)]
pub struct PhysicalOptimizer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl Default for PhysicalOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
            // If there is a output requirement of the query, make sure that
            // this information is not lost across different rules during optimization.
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            // The LimitedDistinctAggregation rule should be applied before the EnforceDistribution rule,
            // as that rule may inject other operations in between the different AggregateExecs.
            // Applying the rule early means only directly-connected AggregateExecs must be examined.
            Arc::new(LimitedDistinctAggregation::new()),
            // The EnforceDistribution rule is for adding essential repartitioning to satisfy distribution
            // requirements. Please make sure that the whole plan tree is determined before this rule.
            // This rule increases parallelism if doing so is beneficial to the physical plan; i.e. at
            // least one of the operators in the plan benefits from increased parallelism.
            Arc::new(EnforceDistribution::new()),
            // The CombinePartialFinalAggregate rule should be applied after the EnforceDistribution rule
            Arc::new(CombinePartialFinalAggregate::new()),
            // The EnforceSorting rule is for adding essential local sorting to satisfy the required
            // ordering. Please make sure that the whole plan tree is determined before this rule.
            // Note that one should always run this rule after running the EnforceDistribution rule
            // as the latter may break local sorting requirements.
            Arc::new(EnforceSorting::new()),
            // Run once after the local sorting requirement is changed
            Arc::new(OptimizeAggregateOrder::new()),
            // TODO: `try_embed_to_hash_join` in the ProjectionPushdown rule would be block by the CoalesceBatches, so add it before CoalesceBatches. Maybe optimize it in the future.
            Arc::new(ProjectionPushdown::new()),
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
            // Remove the ancillary output requirement operator since we are done with the planning
            // phase.
            Arc::new(OutputRequirements::new_remove_mode()),
            // The aggregation limiter will try to find situations where the accumulator count
            // is not tied to the cardinality, i.e. when the output of the aggregation is passed
            // into an `order by max(x) limit y`. In this case it will copy the limit value down
            // to the aggregation, allowing it to use only y number of accumulators.
            Arc::new(TopKAggregation::new()),
            // The ProjectionPushdown rule tries to push projections towards
            // the sources in the execution plan. As a result of this process,
            // a projection can disappear if it reaches the source providers, and
            // sequential projections can merge into one. Even if these two cases
            // are not present, the load of executors such as join or union will be
            // reduced by narrowing their input tables.
            Arc::new(ProjectionPushdown::new()),
            // The PipelineChecker rule will reject non-runnable query plans that use
            // pipeline-breaking operators on infinite input(s). The rule generates a
            // diagnostic error message when this happens. It makes no changes to the
            // given query plan; i.e. it only acts as a final gatekeeping rule.
            Arc::new(PipelineChecker::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}
