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
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
use datafusion_optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion_optimizer::eliminate_filter::EliminateFilter;
use datafusion_optimizer::eliminate_limit::EliminateLimit;
use datafusion_optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion_optimizer::filter_push_down::FilterPushDown;
use datafusion_optimizer::limit_push_down::LimitPushDown;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::projection_push_down::ProjectionPushDown;
use datafusion_optimizer::reduce_outer_join::ReduceOuterJoin;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion_optimizer::subquery_filter_to_join::SubqueryFilterToJoin;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn inner_join_with_condition() -> Result<()> {
    let sql = "SELECT * FROM test0 t0 JOIN test1 t1 ON t0.c0 = t1.c0 AND t0.c1 < t1.c1";
    let plan = test_sql(sql)?;
    let expected = "\
    Projection: #t0.c0, #t0.c1, #t0.c2, #t0.c3, #t0.c4, #t0.c5, #t1.c0, #t1.c1, #t1.c2, #t1.c3, #t1.c4, #t1.c5\
    \n  Inner Join: #t0.c0 = #t1.c0 Filter: #t0.c1 < #t1.c1\
    \n    Filter: #t0.c0 IS NOT NULL\
    \n      SubqueryAlias: t0\
    \n        TableScan: test0 projection=[c0, c1, c2, c3, c4, c5]\
    \n    Filter: #t1.c0 IS NOT NULL\
    \n      SubqueryAlias: t1\
    \n        TableScan: test1 projection=[c0, c1, c2, c3, c4, c5]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
        // Simplify expressions first to maximize the chance
        // of applying other optimizations
        Arc::new(SimplifyExpressions::new()),
        Arc::new(SubqueryFilterToJoin::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(ProjectionPushDown::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(ReduceOuterJoin::new()),
        Arc::new(FilterPushDown::new()),
        Arc::new(LimitPushDown::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
    ];

    let optimizer = Optimizer::new(rules);

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let schema_provider = MySchemaProvider {};
    let sql_to_rel = SqlToRel::new(&schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    let mut config = OptimizerConfig::new();
    //.with_skip_failing_rules(false); // not available yet
    optimizer.optimize(&plan, &mut config, &observe)
}

struct MySchemaProvider {}

impl ContextProvider for MySchemaProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if table_name.starts_with("test") {
            let schema = Schema::new_with_metadata(
                vec![
                    Field::new("c0", DataType::Int32, true),
                    Field::new("c1", DataType::Float32, true),
                    Field::new("c2", DataType::Utf8, true),
                    Field::new("c3", DataType::Boolean, true),
                    Field::new("c4", DataType::Decimal(10, 2), true),
                    Field::new("c5", DataType::Date32, true),
                ],
                HashMap::new(),
            );

            Ok(Arc::new(MyTableSource {
                schema: Arc::new(schema),
            }))
        } else {
            Err(DataFusionError::Plan("table does not exist".to_string()))
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
}

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

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
