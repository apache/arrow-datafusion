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

use crate::analyzer::{Analyzer, AnalyzerConfig, AnalyzerRule};
use crate::optimizer::{assert_schema_is_the_same, Optimizer};
use crate::{OptimizerContext, OptimizerRule};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{assert_contains, internal_err, Result};
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{col, logical_plan::table_scan, LogicalPlan, LogicalPlanBuilder};

use datafusion_common::DataFusionError;
use std::sync::Arc;

pub mod user_defined;

pub fn test_table_scan_fields() -> Vec<Field> {
    vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]
}

/// some tests share a common table with different names
pub fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
    let schema = Schema::new(test_table_scan_fields());
    table_scan(Some(name), &schema, None)?.build()
}

/// some tests share a common table
pub fn test_table_scan() -> Result<LogicalPlan> {
    test_table_scan_with_name("test")
}

/// Scan an empty data source, mainly used in tests
pub fn scan_empty(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    table_scan(name, table_schema, projection)
}

pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
}

pub fn test_subquery_with_name(name: &str) -> Result<Arc<LogicalPlan>> {
    let table_scan = test_table_scan_with_name(name)?;
    Ok(Arc::new(
        LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c")])?
            .build()?,
    ))
}

pub fn scan_tpch_table(table: &str) -> LogicalPlan {
    let schema = Arc::new(get_tpch_table_schema(table));
    table_scan(Some(table), &schema, None)
        .unwrap()
        .build()
        .unwrap()
}

pub fn get_tpch_table_schema(table: &str) -> Schema {
    match table {
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, true),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
        ]),

        _ => unimplemented!("Table: {}", table),
    }
}

struct EmptyRegistryAnalyzerConfig {
    options: ConfigOptions,
}

impl EmptyRegistryAnalyzerConfig {
    fn new() -> Self {
        let options = ConfigOptions::default();
        EmptyRegistryAnalyzerConfig { options }
    }
}

impl FunctionRegistry for EmptyRegistryAnalyzerConfig {
    fn udfs(&self) -> std::collections::HashSet<String> {
        std::collections::HashSet::new()
    }

    fn udf(&self, _name: &str) -> Result<Arc<datafusion_expr::ScalarUDF>> {
        internal_err!("empty registry")
    }

    fn udaf(&self, _name: &str) -> Result<Arc<datafusion_expr::AggregateUDF>> {
        internal_err!("empty registry")
    }

    fn udwf(&self, _name: &str) -> Result<Arc<datafusion_expr::WindowUDF>> {
        internal_err!("empty registry")
    }
}

impl AnalyzerConfig for EmptyRegistryAnalyzerConfig {
    fn function_registry(&self) -> &dyn FunctionRegistry {
        self
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

pub fn assert_analyzed_plan_eq(
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
    plan: &LogicalPlan,
    expected: &str,
) -> Result<()> {
    let analyzer_config = EmptyRegistryAnalyzerConfig::new();
    let analyzed_plan = Analyzer::with_rules(vec![rule]).execute_and_check(
        plan,
        &analyzer_config,
        |_, _| {},
    )?;
    let formatted_plan = format!("{analyzed_plan:?}");
    assert_eq!(formatted_plan, expected);

    Ok(())
}
pub fn assert_analyzed_plan_eq_display_indent(
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
    plan: &LogicalPlan,
    expected: &str,
) -> Result<()> {
    let analyzer_config = EmptyRegistryAnalyzerConfig::new();
    let analyzed_plan = Analyzer::with_rules(vec![rule]).execute_and_check(
        plan,
        &analyzer_config,
        |_, _| {},
    )?;
    let formatted_plan = analyzed_plan.display_indent_schema().to_string();
    assert_eq!(formatted_plan, expected);

    Ok(())
}

pub fn assert_analyzer_check_err(
    rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    plan: &LogicalPlan,
    expected: &str,
) {
    let analyzer_config = EmptyRegistryAnalyzerConfig::new();
    let analyzed_plan =
        Analyzer::with_rules(rules).execute_and_check(plan, &analyzer_config, |_, _| {});
    match analyzed_plan {
        Ok(plan) => assert_eq!(format!("{}", plan.display_indent()), "An error"),
        Err(e) => {
            assert_contains!(e.to_string(), expected);
        }
    }
}
pub fn assert_optimized_plan_eq(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
    plan: &LogicalPlan,
    expected: &str,
) -> Result<()> {
    let optimizer = Optimizer::with_rules(vec![rule.clone()]);
    let optimized_plan = optimizer
        .optimize_recursively(
            optimizer.rules.first().unwrap(),
            plan,
            &OptimizerContext::new(),
        )?
        .unwrap_or_else(|| plan.clone());

    // Ensure schemas always match after an optimization
    assert_schema_is_the_same(rule.name(), plan, &optimized_plan)?;
    let formatted_plan = format!("{optimized_plan:?}");
    assert_eq!(formatted_plan, expected);

    Ok(())
}

pub fn assert_optimized_plan_eq_with_rules(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    plan: &LogicalPlan,
    expected: &str,
) -> Result<()> {
    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}
    let config = &mut OptimizerContext::new()
        .with_max_passes(1)
        .with_skip_failing_rules(false);
    let optimizer = Optimizer::with_rules(rules);
    let optimized_plan = optimizer
        .optimize(plan, config, observe)
        .expect("failed to optimize plan");
    let formatted_plan = format!("{optimized_plan:?}");
    assert_eq!(formatted_plan, expected);
    assert_eq!(plan.schema(), optimized_plan.schema());
    Ok(())
}

pub fn assert_optimized_plan_eq_display_indent(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
    plan: &LogicalPlan,
    expected: &str,
) {
    let optimizer = Optimizer::with_rules(vec![rule]);
    let optimized_plan = optimizer
        .optimize_recursively(
            optimizer.rules.first().unwrap(),
            plan,
            &OptimizerContext::new(),
        )
        .expect("failed to optimize plan")
        .unwrap_or_else(|| plan.clone());
    let formatted_plan = optimized_plan.display_indent_schema().to_string();
    assert_eq!(formatted_plan, expected);
}

pub fn assert_multi_rules_optimized_plan_eq_display_indent(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    plan: &LogicalPlan,
    expected: &str,
) {
    let optimizer = Optimizer::with_rules(rules);
    let mut optimized_plan = plan.clone();
    for rule in &optimizer.rules {
        optimized_plan = optimizer
            .optimize_recursively(rule, &optimized_plan, &OptimizerContext::new())
            .expect("failed to optimize plan")
            .unwrap_or_else(|| optimized_plan.clone());
    }
    let formatted_plan = optimized_plan.display_indent_schema().to_string();
    assert_eq!(formatted_plan, expected);
}

pub fn assert_optimizer_err(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
    plan: &LogicalPlan,
    expected: &str,
) {
    let optimizer = Optimizer::with_rules(vec![rule]);
    let res = optimizer.optimize_recursively(
        optimizer.rules.first().unwrap(),
        plan,
        &OptimizerContext::new(),
    );
    match res {
        Ok(plan) => assert_eq!(format!("{}", plan.unwrap().display_indent()), "An error"),
        Err(ref e) => {
            let actual = format!("{e}");
            if expected.is_empty() || !actual.contains(expected) {
                assert_eq!(actual, expected)
            }
        }
    }
}

pub fn assert_optimization_skipped(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
    plan: &LogicalPlan,
) -> Result<()> {
    let optimizer = Optimizer::with_rules(vec![rule]);
    let new_plan = optimizer
        .optimize_recursively(
            optimizer.rules.first().unwrap(),
            plan,
            &OptimizerContext::new(),
        )?
        .unwrap_or_else(|| plan.clone());
    assert_eq!(
        format!("{}", plan.display_indent()),
        format!("{}", new_plan.display_indent())
    );
    Ok(())
}
