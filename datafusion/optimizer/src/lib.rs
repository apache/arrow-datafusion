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

//! # DataFusion Optimizer
//!
//! Contains rules for rewriting [`LogicalPlan`]s
//!
//! 1. [`Analyzer`] applies [`AnalyzerRule`]s to transform `LogicalPlan`s
//! to make the plan valid prior to the rest of the DataFusion optimization
//! process (for example, [`TypeCoercion`]).
//!
//! 2. [`Optimizer`] applies [`OptimizerRule`]s to transform `LogicalPlan`s
//! into equivalent, but more efficient plans.
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan
//! [`TypeCoercion`]: analyzer::type_coercion::TypeCoercion
pub mod analyzer;
pub mod common_subexpr_eliminate;
pub mod decorrelate;
pub mod decorrelate_predicate_subquery;
pub mod eliminate_cross_join;
pub mod eliminate_duplicated_expr;
pub mod eliminate_filter;
pub mod eliminate_group_by_constant;
pub mod eliminate_join;
pub mod eliminate_limit;
pub mod eliminate_nested_union;
pub mod eliminate_one_union;
pub mod eliminate_outer_join;
pub mod extract_equijoin_predicate;
pub mod filter_null_join_keys;
pub mod optimize_projections;
pub mod optimizer;
pub mod propagate_empty_relation;
pub mod push_down_filter;
pub mod push_down_limit;
pub mod replace_distinct_aggregate;
pub mod rewrite_cycle;
pub mod rewrite_disjunctive_predicate;
pub mod scalar_subquery_to_join;
pub mod simplify_expressions;
pub mod single_distinct_to_groupby;
pub mod unwrap_cast_in_comparison;
pub mod utils;

#[cfg(test)]
pub mod test;

pub use analyzer::{Analyzer, AnalyzerRule};
pub use optimizer::{Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule};
pub use utils::optimize_children;

pub(crate) mod join_key_set;
mod plan_signature;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
