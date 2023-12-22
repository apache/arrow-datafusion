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
use datafusion::execution::FunctionRegistry;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{internal_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    AggregateUDF, Between, Expr, Filter, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_optimizer::analyzer::{Analyzer, AnalyzerConfig, AnalyzerRule};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerContext, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::sync::Arc;

struct ExamplesAnalyzerConfig<'a> {
    config_options: &'a ConfigOptions,
}

impl<'a> FunctionRegistry for ExamplesAnalyzerConfig<'a> {
    fn udfs(&self) -> std::collections::HashSet<String> {
        std::collections::HashSet::new()
    }

    fn udf(&self, _name: &str) -> Result<Arc<ScalarUDF>> {
        internal_err!("Mock Function Registry")
    }

    fn udaf(&self, _name: &str) -> Result<Arc<AggregateUDF>> {
        internal_err!("Mock Function Registry")
    }

    fn udwf(&self, _name: &str) -> Result<Arc<WindowUDF>> {
        internal_err!("Mock Function Registry")
    }
}

impl<'a> AnalyzerConfig for ExamplesAnalyzerConfig<'a> {
    fn function_registry(&self) -> &dyn FunctionRegistry {
        self
    }

    fn options(&self) -> &ConfigOptions {
        self.config_options
    }
}

pub fn main() -> Result<()> {
    // produce a logical plan using the datafusion-sql crate
    let dialect = PostgreSqlDialect {};
    let sql = "SELECT * FROM person WHERE age BETWEEN 21 AND 32";
    let statements = Parser::parse_sql(&dialect, sql)?;

    // produce a logical plan using the datafusion-sql crate
    let context_provider = MyContextProvider::default();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let logical_plan = sql_to_rel.sql_statement_to_plan(statements[0].clone())?;
    println!(
        "Unoptimized Logical Plan:\n\n{}\n",
        logical_plan.display_indent()
    );

    // run the analyzer with our custom rule
    let config = OptimizerContext::default().with_skip_failing_rules(false);
    let analyzer = Analyzer::with_rules(vec![Arc::new(MyAnalyzerRule {})]);
    let analyzer_config = ExamplesAnalyzerConfig {
        config_options: config.options(),
    };
    let analyzed_plan =
        analyzer.execute_and_check(&logical_plan, &analyzer_config, |_, _| {})?;
    println!(
        "Analyzed Logical Plan:\n\n{}\n",
        analyzed_plan.display_indent()
    );

    // then run the optimizer with our custom rule
    let optimizer = Optimizer::with_rules(vec![Arc::new(MyOptimizerRule {})]);
    let optimized_plan = optimizer.optimize(&analyzed_plan, &config, observe)?;
    println!(
        "Optimized Logical Plan:\n\n{}\n",
        optimized_plan.display_indent()
    );

    Ok(())
}

fn observe(plan: &LogicalPlan, rule: &dyn OptimizerRule) {
    println!(
        "After applying rule '{}':\n{}\n",
        rule.name(),
        plan.display_indent()
    )
}

/// An example analyzer rule that changes Int64 literals to UInt64
struct MyAnalyzerRule {}

impl AnalyzerRule for MyAnalyzerRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &dyn AnalyzerConfig,
    ) -> Result<LogicalPlan> {
        Self::analyze_plan(plan)
    }

    fn name(&self) -> &str {
        "my_analyzer_rule"
    }
}

impl MyAnalyzerRule {
    fn analyze_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform(&|plan| {
            Ok(match plan {
                LogicalPlan::Filter(filter) => {
                    let predicate = Self::analyze_expr(filter.predicate.clone())?;
                    Transformed::Yes(LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        filter.input,
                    )?))
                }
                _ => Transformed::No(plan),
            })
        })
    }

    fn analyze_expr(expr: Expr) -> Result<Expr> {
        expr.transform(&|expr| {
            // closure is invoked for all sub expressions
            Ok(match expr {
                Expr::Literal(ScalarValue::Int64(i)) => {
                    // transform to UInt64
                    Transformed::Yes(Expr::Literal(ScalarValue::UInt64(
                        i.map(|i| i as u64),
                    )))
                }
                _ => Transformed::No(expr),
            })
        })
    }
}

/// An example optimizer rule that rewrite BETWEEN expression to binary compare expressions
struct MyOptimizerRule {}

impl OptimizerRule for MyOptimizerRule {
    fn name(&self) -> &str {
        "my_optimizer_rule"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // recurse down and optimize children first
        let optimized_plan = utils::optimize_children(self, plan, config)?;
        match optimized_plan {
            Some(LogicalPlan::Filter(filter)) => {
                let predicate = my_rewrite(filter.predicate.clone())?;
                Ok(Some(LogicalPlan::Filter(Filter::try_new(
                    predicate,
                    filter.input,
                )?)))
            }
            Some(optimized_plan) => Ok(Some(optimized_plan)),
            None => match plan {
                LogicalPlan::Filter(filter) => {
                    let predicate = my_rewrite(filter.predicate.clone())?;
                    Ok(Some(LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        filter.input.clone(),
                    )?)))
                }
                _ => Ok(None),
            },
        }
    }
}

/// use rewrite_expr to modify the expression tree.
fn my_rewrite(expr: Expr) -> Result<Expr> {
    expr.transform(&|expr| {
        // closure is invoked for all sub expressions
        Ok(match expr {
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                // unbox
                let expr: Expr = *expr;
                let low: Expr = *low;
                let high: Expr = *high;
                if negated {
                    Transformed::Yes(expr.clone().lt(low).or(expr.gt(high)))
                } else {
                    Transformed::Yes(expr.clone().gt_eq(low).and(expr.lt_eq(high)))
                }
            }
            _ => Transformed::No(expr),
        })
    })
}

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
            plan_err!("table not found")
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
}

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
}
