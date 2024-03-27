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

//! Query optimizer traits

use std::collections::HashSet;
use std::sync::Arc;

use crate::common_subexpr_eliminate::CommonSubexprEliminate;
use crate::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use crate::eliminate_cross_join::EliminateCrossJoin;
use crate::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use crate::eliminate_filter::EliminateFilter;
use crate::eliminate_join::EliminateJoin;
use crate::eliminate_limit::EliminateLimit;
use crate::eliminate_nested_union::EliminateNestedUnion;
use crate::eliminate_one_union::EliminateOneUnion;
use crate::eliminate_outer_join::EliminateOuterJoin;
use crate::extract_equijoin_predicate::ExtractEquijoinPredicate;
use crate::filter_null_join_keys::FilterNullJoinKeys;
use crate::optimize_projections::OptimizeProjections;
use crate::plan_signature::LogicalPlanSignature;
use crate::propagate_empty_relation::PropagateEmptyRelation;
use crate::push_down_filter::PushDownFilter;
use crate::push_down_limit::PushDownLimit;
use crate::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use crate::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use crate::scalar_subquery_to_join::ScalarSubqueryToJoin;
use crate::simplify_expressions::SimplifyExpressions;
use crate::single_distinct_to_groupby::SingleDistinctToGroupBy;
use crate::unwrap_cast_in_comparison::UnwrapCastInComparison;
use crate::utils::log_plan;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::logical_plan::LogicalPlan;

use chrono::{DateTime, Utc};
use log::{debug, warn};

/// `OptimizerRule` transforms one [`LogicalPlan`] into another which
/// computes the same results, but in a potentially more efficient
/// way. If there are no suitable transformations for the input plan,
/// the optimizer should simply return it unmodified.
///
/// To change the semantics of a `LogicalPlan`, see [`AnalyzerRule`]
///
/// Use [`SessionState::add_optimizer_rule`] to register additional
/// `OptimizerRule`s.
///
/// [`AnalyzerRule`]: crate::analyzer::AnalyzerRule
/// [`SessionState::add_optimizer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html#method.add_optimizer_rule

pub trait OptimizerRule {
    /// Try and rewrite `plan` to an optimized form, returning None if the plan cannot be
    /// optimized by this rule.
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// How should the rule be applied by the optimizer? See comments on [`ApplyOrder`] for details.
    ///
    /// If a rule use default None, it should traverse recursively plan inside itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

/// Options to control the DataFusion Optimizer.
pub trait OptimizerConfig {
    /// Return the time at which the query execution started. This
    /// time is used as the value for now()
    fn query_execution_start_time(&self) -> DateTime<Utc>;

    /// Return alias generator used to generate unique aliases for subqueries
    fn alias_generator(&self) -> Arc<AliasGenerator>;

    fn options(&self) -> &ConfigOptions;
}

/// A standalone [`OptimizerConfig`] that can be used independently
/// of DataFusion's config management
#[derive(Debug)]
pub struct OptimizerContext {
    /// Query execution start time that can be used to rewrite
    /// expressions such as `now()` to use a literal value instead
    query_execution_start_time: DateTime<Utc>,

    /// Alias generator used to generate unique aliases for subqueries
    alias_generator: Arc<AliasGenerator>,

    options: ConfigOptions,
}

impl OptimizerContext {
    /// Create optimizer config
    pub fn new() -> Self {
        let mut options = ConfigOptions::default();
        options.optimizer.filter_null_join_keys = true;

        Self {
            query_execution_start_time: Utc::now(),
            alias_generator: Arc::new(AliasGenerator::new()),
            options,
        }
    }

    /// Specify whether to enable the filter_null_keys rule
    pub fn filter_null_keys(mut self, filter_null_keys: bool) -> Self {
        self.options.optimizer.filter_null_join_keys = filter_null_keys;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_tart_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = query_execution_tart_time;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_skip_failing_rules(mut self, b: bool) -> Self {
        self.options.optimizer.skip_failed_rules = b;
        self
    }

    /// Specify how many times to attempt to optimize the plan
    pub fn with_max_passes(mut self, v: u8) -> Self {
        self.options.optimizer.max_passes = v as usize;
        self
    }
}

impl Default for OptimizerContext {
    /// Create optimizer config
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerConfig for OptimizerContext {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.query_execution_start_time
    }

    fn alias_generator(&self) -> Arc<AliasGenerator> {
        self.alias_generator.clone()
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

/// A rule-based optimizer.
#[derive(Clone)]
pub struct Optimizer {
    /// All optimizer rules to apply
    pub rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

/// If a rule is with `ApplyOrder`, it means the optimizer will derive to handle children instead of
/// recursively handling in rule.
/// We just need handle a subtree pattern itself.
///
/// Notice: **sometime** result after optimize still can be optimized, we need apply again.
///
/// Usage Example: Merge Limit (subtree pattern is: Limit-Limit)
/// ```rust
/// use datafusion_expr::{Limit, LogicalPlan, LogicalPlanBuilder};
/// use datafusion_common::Result;
/// fn merge_limit(parent: &Limit, child: &Limit) -> LogicalPlan {
///     // just for run
///     return parent.input.as_ref().clone();
/// }
/// fn try_optimize(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
///     match plan {
///         LogicalPlan::Limit(limit) => match limit.input.as_ref() {
///             LogicalPlan::Limit(child_limit) => {
///                 // merge limit ...
///                 let optimized_plan = merge_limit(limit, child_limit);
///                 // due to optimized_plan may be optimized again,
///                 // for example: plan is Limit-Limit-Limit
///                 Ok(Some(
///                     try_optimize(&optimized_plan)?
///                         .unwrap_or_else(|| optimized_plan.clone()),
///                 ))
///             }
///             _ => Ok(None),
///         },
///         _ => Ok(None),
///     }
/// }
/// ```
pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(EliminateNestedUnion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(EliminateJoin::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(EliminateDuplicatedExpr::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            // Must be after PropagateEmptyRelation
            Arc::new(EliminateOneUnion::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(OptimizeProjections::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }

    /// Optimizes the logical plan by applying optimizer rules, and
    /// invoking observer function after each call
    pub fn optimize<F>(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        let options = config.options();
        let mut new_plan = plan.clone();

        let start_time = Instant::now();

        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&new_plan));

        let mut i = 0;
        while i < options.optimizer.max_passes {
            log_plan(&format!("Optimizer input (pass {i})"), &new_plan);

            for rule in &self.rules {
                let result =
                    self.optimize_recursively(rule, &new_plan, config)
                        .and_then(|plan| {
                            if let Some(plan) = &plan {
                                assert_schema_is_the_same(rule.name(), plan, &new_plan)?;
                            }
                            Ok(plan)
                        });
                match result {
                    Ok(Some(plan)) => {
                        new_plan = plan;
                        observer(&new_plan, rule.as_ref());
                        log_plan(rule.name(), &new_plan);
                    }
                    Ok(None) => {
                        observer(&new_plan, rule.as_ref());
                        debug!(
                            "Plan unchanged by optimizer rule '{}' (pass {})",
                            rule.name(),
                            i
                        );
                    }
                    Err(e) => {
                        if options.optimizer.skip_failed_rules {
                            // Note to future readers: if you see this warning it signals a
                            // bug in the DataFusion optimizer. Please consider filing a ticket
                            // https://github.com/apache/arrow-datafusion
                            warn!(
                            "Skipping optimizer rule '{}' due to unexpected error: {}",
                            rule.name(),
                            e
                        );
                        } else {
                            return Err(DataFusionError::Context(
                                format!("Optimizer rule '{}' failed", rule.name(),),
                                Box::new(e),
                            ));
                        }
                    }
                }
            }
            log_plan(&format!("Optimized plan (pass {i})"), &new_plan);

            // HashSet::insert returns, whether the value was newly inserted.
            let plan_is_fresh =
                previous_plans.insert(LogicalPlanSignature::new(&new_plan));
            if !plan_is_fresh {
                // plan did not change, so no need to continue trying to optimize
                debug!("optimizer pass {} did not make changes", i);
                break;
            }
            i += 1;
        }
        log_plan("Final optimized plan", &new_plan);
        debug!("Optimizer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }

    fn optimize_node(
        &self,
        rule: &Arc<dyn OptimizerRule + Send + Sync>,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // TODO: future feature: We can do Batch optimize
        rule.try_optimize(plan, config)
    }

    fn optimize_inputs(
        &self,
        rule: &Arc<dyn OptimizerRule + Send + Sync>,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let inputs = plan.inputs();
        let result = inputs
            .iter()
            .map(|sub_plan| self.optimize_recursively(rule, sub_plan, config))
            .collect::<Result<Vec<_>>>()?;
        if result.is_empty() || result.iter().all(|o| o.is_none()) {
            return Ok(None);
        }

        let new_inputs = result
            .into_iter()
            .zip(inputs)
            .map(|(new_plan, old_plan)| match new_plan {
                Some(plan) => plan,
                None => old_plan.clone(),
            })
            .collect();

        let exprs = plan.expressions();
        plan.with_new_exprs(exprs, new_inputs).map(Some)
    }

    /// Use a rule to optimize the whole plan.
    /// If the rule with `ApplyOrder`, we don't need to recursively handle children in rule.
    pub fn optimize_recursively(
        &self,
        rule: &Arc<dyn OptimizerRule + Send + Sync>,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match rule.apply_order() {
            Some(order) => match order {
                ApplyOrder::TopDown => {
                    let optimize_self_opt = self.optimize_node(rule, plan, config)?;
                    let optimize_inputs_opt = match &optimize_self_opt {
                        Some(optimized_plan) => {
                            self.optimize_inputs(rule, optimized_plan, config)?
                        }
                        _ => self.optimize_inputs(rule, plan, config)?,
                    };
                    Ok(optimize_inputs_opt.or(optimize_self_opt))
                }
                ApplyOrder::BottomUp => {
                    let optimize_inputs_opt = self.optimize_inputs(rule, plan, config)?;
                    let optimize_self_opt = match &optimize_inputs_opt {
                        Some(optimized_plan) => {
                            self.optimize_node(rule, optimized_plan, config)?
                        }
                        _ => self.optimize_node(rule, plan, config)?,
                    };
                    Ok(optimize_self_opt.or(optimize_inputs_opt))
                }
            },
            _ => rule.try_optimize(plan, config),
        }
    }
}

/// Returns an error if plans have different schemas.
///
/// It ignores metadata and nullability.
pub(crate) fn assert_schema_is_the_same(
    rule_name: &str,
    prev_plan: &LogicalPlan,
    new_plan: &LogicalPlan,
) -> Result<()> {
    let equivalent = new_plan
        .schema()
        .equivalent_names_and_types(prev_plan.schema());

    if !equivalent {
        let e = DataFusionError::Internal(format!(
            "Failed due to a difference in schemas, original schema: {:?}, new schema: {:?}",
            prev_plan.schema(),
            new_plan.schema()
        ));
        Err(DataFusionError::Context(
            String::from(rule_name),
            Box::new(e),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::ApplyOrder;
    use crate::optimizer::Optimizer;
    use crate::test::test_table_scan;
    use crate::{OptimizerConfig, OptimizerContext, OptimizerRule};

    use datafusion_common::{plan_err, DFSchema, DFSchemaRef, Result};
    use datafusion_expr::logical_plan::EmptyRelation;
    use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder, Projection};

    #[test]
    fn skip_failing_rule() {
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(&plan, &config, &observe).unwrap();
    }

    #[test]
    fn no_skip_failing_rule() {
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let err = opt.optimize(&plan, &config, &observe).unwrap_err();
        assert_eq!(
            "Optimizer rule 'bad rule' failed\ncaused by\n\
            Error during planning: rule failed",
            err.strip_backtrace()
        );
    }

    #[test]
    fn generate_different_schema() {
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let err = opt.optimize(&plan, &config, &observe).unwrap_err();
        assert_eq!(
            "Optimizer rule 'get table_scan rule' failed\ncaused by\nget table_scan rule\ncaused by\n\
            Internal error: Failed due to a difference in schemas, original schema: \
            DFSchema { inner: Schema { fields: \
                [Field { name: \"a\", data_type: UInt32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
                Field { name: \"b\", data_type: UInt32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
                Field { name: \"c\", data_type: UInt32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} }, \
                field_qualifiers: [Some(Bare { table: \"test\" }), Some(Bare { table: \"test\" }), Some(Bare { table: \"test\" })], \
                functional_dependencies: FunctionalDependencies { deps: [] } }, \
            new schema: DFSchema { inner: Schema { \
                fields: [], metadata: {} }, \
                field_qualifiers: [], \
                functional_dependencies: FunctionalDependencies { deps: [] } }.\n\
                This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker",
            err.strip_backtrace()
        );
    }

    #[test]
    fn skip_generate_different_schema() {
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(&plan, &config, &observe).unwrap();
    }

    #[test]
    fn generate_same_schema_different_metadata() -> Result<()> {
        // if the plan creates more metadata than previously (because
        // some wrapping functions are removed, etc) do not error
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);

        let input = Arc::new(test_table_scan()?);
        let input_schema = input.schema().clone();

        let plan = LogicalPlan::Projection(Projection::try_new_with_schema(
            vec![col("a"), col("b"), col("c")],
            input,
            add_metadata_to_fields(input_schema.as_ref()),
        )?);

        // optimizing should be ok, but the schema will have changed  (no metadata)
        assert_ne!(plan.schema().as_ref(), input_schema.as_ref());
        let optimized_plan = opt.optimize(&plan, &config, &observe)?;
        // metadata was removed
        assert_eq!(optimized_plan.schema().as_ref(), input_schema.as_ref());
        Ok(())
    }

    #[test]
    fn optimizer_detects_plan_equal_to_the_initial() -> Result<()> {
        // Run a goofy optimizer, which rotates projection columns
        // [1, 2, 3] -> [2, 3, 1] -> [3, 1, 2] -> [1, 2, 3]

        let opt = Optimizer::with_rules(vec![Arc::new(RotateProjectionRule::new(false))]);
        let config = OptimizerContext::new().with_max_passes(16);

        let initial_plan = LogicalPlanBuilder::empty(false)
            .project([lit(1), lit(2), lit(3)])?
            .project([lit(100)])? // to not trigger changed schema error
            .build()?;

        let mut plans: Vec<LogicalPlan> = Vec::new();
        let final_plan =
            opt.optimize(&initial_plan, &config, |p, _| plans.push(p.clone()))?;

        // initial_plan is not observed, so we have 3 plans
        assert_eq!(3, plans.len());

        // we got again the initial_plan with [1, 2, 3]
        assert_eq!(initial_plan, final_plan);

        Ok(())
    }

    #[test]
    fn optimizer_detects_plan_equal_to_a_non_initial() -> Result<()> {
        // Run a goofy optimizer, which reverses and rotates projection columns
        // [1, 2, 3] -> [3, 2, 1] -> [2, 1, 3] -> [1, 3, 2] -> [3, 2, 1]

        let opt = Optimizer::with_rules(vec![Arc::new(RotateProjectionRule::new(true))]);
        let config = OptimizerContext::new().with_max_passes(16);

        let initial_plan = LogicalPlanBuilder::empty(false)
            .project([lit(1), lit(2), lit(3)])?
            .project([lit(100)])? // to not trigger changed schema error
            .build()?;

        let mut plans: Vec<LogicalPlan> = Vec::new();
        let final_plan =
            opt.optimize(&initial_plan, &config, |p, _| plans.push(p.clone()))?;

        // initial_plan is not observed, so we have 4 plans
        assert_eq!(4, plans.len());

        // we got again the plan with [3, 2, 1]
        assert_eq!(plans[0], final_plan);

        Ok(())
    }

    fn add_metadata_to_fields(schema: &DFSchema) -> DFSchemaRef {
        let new_fields = schema
            .iter()
            .enumerate()
            .map(|(i, (qualifier, field))| {
                let metadata =
                    [("key".into(), format!("value {i}"))].into_iter().collect();

                let new_arrow_field = field.as_ref().clone().with_metadata(metadata);
                (qualifier.cloned(), Arc::new(new_arrow_field))
            })
            .collect::<Vec<_>>();

        let new_metadata = schema.metadata().clone();
        Arc::new(DFSchema::new_with_metadata(new_fields, new_metadata).unwrap())
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    struct BadRule {}

    impl OptimizerRule for BadRule {
        fn try_optimize(
            &self,
            _: &LogicalPlan,
            _: &dyn OptimizerConfig,
        ) -> Result<Option<LogicalPlan>> {
            plan_err!("rule failed")
        }

        fn name(&self) -> &str {
            "bad rule"
        }
    }

    /// Replaces whatever plan with a single table scan
    struct GetTableScanRule {}

    impl OptimizerRule for GetTableScanRule {
        fn try_optimize(
            &self,
            _: &LogicalPlan,
            _: &dyn OptimizerConfig,
        ) -> Result<Option<LogicalPlan>> {
            let table_scan = test_table_scan()?;
            Ok(Some(LogicalPlanBuilder::from(table_scan).build()?))
        }

        fn name(&self) -> &str {
            "get table_scan rule"
        }
    }

    /// A goofy rule doing rotation of columns in all projections.
    ///
    /// Useful to test cycle detection.
    struct RotateProjectionRule {
        // reverse exprs instead of rotating on the first pass
        reverse_on_first_pass: Mutex<bool>,
    }

    impl RotateProjectionRule {
        fn new(reverse_on_first_pass: bool) -> Self {
            Self {
                reverse_on_first_pass: Mutex::new(reverse_on_first_pass),
            }
        }
    }

    impl OptimizerRule for RotateProjectionRule {
        fn try_optimize(
            &self,
            plan: &LogicalPlan,
            _: &dyn OptimizerConfig,
        ) -> Result<Option<LogicalPlan>> {
            let projection = match plan {
                LogicalPlan::Projection(p) if p.expr.len() >= 2 => p,
                _ => return Ok(None),
            };

            let mut exprs = projection.expr.clone();

            let mut reverse = self.reverse_on_first_pass.lock().unwrap();
            if *reverse {
                exprs.reverse();
                *reverse = false;
            } else {
                exprs.rotate_left(1);
            }

            Ok(Some(LogicalPlan::Projection(Projection::try_new(
                exprs,
                projection.input.clone(),
            )?)))
        }

        fn apply_order(&self) -> Option<ApplyOrder> {
            Some(ApplyOrder::TopDown)
        }

        fn name(&self) -> &str {
            "rotate_projection"
        }
    }
}
