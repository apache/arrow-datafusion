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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{
    AggregateUDF, Expr, LogicalPlan, ScalarUDF, TableProviderFilterPushDown, TableSource,
    WindowUDF,
};
use datafusion_optimizer::{
    Analyzer, AnalyzerRule, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::sync::Arc;

/// This example shows how to use DataFusion's SQL planner to parse SQL text and
/// build `LogicalPlan`s without executing them.
///
/// For example, if you need a SQL planner and optimizer like Apache Calcite,
/// but do not want a Java runtime dependency for some reason, you could use
/// DataFusion as a SQL frontend.
///
/// Normally, users interact with DataFusion via SessionContext. However, using
/// SessionContext requires depending on the full `datafusion` core crate.
///
/// In this example, we demonstrate how to use the lower level APIs directly,
/// which only requires the `datafusion-sql` dependency.
pub fn main() -> Result<()> {
    // First, we parse the SQL string. Note that we use the DataFusion
    // Parser, which wraps the `sqlparser-rs` SQL parser and adds DataFusion
    // specific syntax such as `CREATE EXTERNAL TABLE`
    let dialect = PostgreSqlDialect {};
    let sql = "SELECT name FROM person WHERE age BETWEEN 21 AND 32";
    let statements = Parser::parse_sql(&dialect, sql)?;

    // Now, use DataFusion's SQL planner, called `SqlToRel` to create a
    // `LogicalPlan` from the parsed statement
    //
    // To invoke SqlToRel we must provide it schema and function information
    // via an object that implements the `ContextProvider` trait
    let context_provider = MyContextProvider::default();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let logical_plan = sql_to_rel.sql_statement_to_plan(statements[0].clone())?;

    // Here is the logical plan that was generated:
    assert_eq!(
        logical_plan.display_indent().to_string(),
        "Projection: person.name\
        \n  Filter: person.age BETWEEN Int64(21) AND Int64(32)\
        \n    TableScan: person"
    );

    // The initial LogicalPlan is a mechanical translation from the parsed SQL
    // and often can not run without the Analyzer passes.
    //
    // In this example, `person.age` is actually a different data type (Int8)
    // than the values to which it is compared to which are Int64. Most
    // execution engines, including DataFusion's, will fail if you provide such
    // a plan.
    //
    // To prepare it to run, we must apply type coercion to align types, and
    // check for other semantic errors. In DataFusion this is done by a
    // component called the Analyzer.
    let config = OptimizerContext::default().with_skip_failing_rules(false);
    let analyzed_plan = Analyzer::new().execute_and_check(
        logical_plan,
        config.options(),
        observe_analyzer,
    )?;
    // Note that the Analyzer has added a CAST to the plan to align the types
    assert_eq!(
        analyzed_plan.display_indent().to_string(),
        "Projection: person.name\
        \n  Filter: CAST(person.age AS Int64) BETWEEN Int64(21) AND Int64(32)\
        \n    TableScan: person",
    );

    // As we can see, the Analyzer added a CAST so the types are the same
    // (Int64). However, this plan is not as efficient as it could be, as it
    // will require casting *each row* of the input to UInt64 before comparison
    // to 21 and 32. To optimize this query's performance, it is better to cast
    // the constants once at plan time to UInt8.
    //
    // Query optimization is handled in DataFusion by a component called the
    // Optimizer, which we now invoke
    //
    let optimized_plan =
        Optimizer::new().optimize(analyzed_plan, &config, observe_optimizer)?;

    // Show the fully optimized plan. Note that the optimizer did several things
    // to prepare this plan for execution:
    //
    // 1. Removed casts from person.age as we described above
    // 2. Converted BETWEEN to two single column inequalities (which are typically faster to execute)
    // 3. Pushed the projection of `name` down to the scan (so the scan only returns that column)
    // 4. Pushed the filter into the scan
    // 5. Removed the projection as it was only serving to pass through the name column
    assert_eq!(
        optimized_plan.display_indent().to_string(),
        "TableScan: person projection=[name], full_filters=[person.age >= UInt8(21), person.age <= UInt8(32)]"
    );

    Ok(())
}

// Note that both the optimizer and the analyzer take a callback, called an
// "observer" that is invoked after each pass. We do not do anything with these
// callbacks in this example

fn observe_analyzer(_plan: &LogicalPlan, _rule: &dyn AnalyzerRule) {}
fn observe_optimizer(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

/// Implements the `ContextProvider` trait required to plan SQL
#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if name.table() == "person" {
            Ok(Arc::new(MyTableSource {
                schema: Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::UInt8, false),
                ])),
            }))
        } else {
            plan_err!("Table {} not found", name.table())
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// TableSource is the part of TableProvider needed for creating a LogicalPlan.
struct MyTableSource {
    schema: SchemaRef,
}

impl TableSource for MyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    // For this example, we report to the DataFusion optimizer that
    // this provider can apply filters during the scan
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}
