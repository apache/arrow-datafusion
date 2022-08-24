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
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::{
    col, count, count_distinct, AggregateUDF, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
    TableSource,
};
use datafusion_optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion_optimizer::decorrelate_scalar_subquery::DecorrelateScalarSubquery;
use datafusion_optimizer::decorrelate_where_exists::DecorrelateWhereExists;
use datafusion_optimizer::decorrelate_where_in::DecorrelateWhereIn;
use datafusion_optimizer::eliminate_filter::EliminateFilter;
use datafusion_optimizer::eliminate_limit::EliminateLimit;
use datafusion_optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion_optimizer::filter_push_down::FilterPushDown;
use datafusion_optimizer::limit_push_down::LimitPushDown;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::projection_push_down::ProjectionPushDown;
use datafusion_optimizer::reduce_outer_join::ReduceOuterJoin;
use datafusion_optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
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
fn distribute_by() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/3234
    let sql = "SELECT col_int32, col_utf8 FROM test DISTRIBUTE BY (col_utf8)";
    let plan = test_sql(sql)?;
    let expected = "Repartition: DistributeBy(#col_utf8)\
    \n  Projection: #test.col_int32, #test.col_utf8\
    \n    TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn count_distinct_multi_sql() -> Result<()> {
    let sql = "SELECT COUNT(col_int32) AS num, COUNT(DISTINCT col_int32) AS num_distinct FROM test";
    let plan = test_sql(sql)?;
    let expected = "Projection: #COUNT(test.col_int32) AS num, #COUNT(DISTINCT test.col_int32) AS num_distinct\
    \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#test.col_int32), COUNT(DISTINCT #test.col_int32)]]\
    \n    TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn count_distinct_multi_plan_builder() -> Result<()> {
    let schema_provider = MySchemaProvider {};
    let table_name: TableReference = "test".into();
    let table = schema_provider.get_table_provider(table_name)?;
    let table_source = LogicalTableSource::new(table.schema());

    let plan = LogicalPlanBuilder::scan("test", Arc::new(table_source), None)?
        .aggregate(
            vec![col("test.col_int32")],
            vec![
                count(col("test.col_int32")),
                count_distinct(col("test.col_int32")),
            ],
        )?
        .project(vec![col("test.col_int32")])?
        .build()?;

    println!("{}", plan.display_indent());

    let plan = optimize_plan(&plan)?;

    let expected = "Projection: #COUNT(test.col_int32) AS num, #COUNT(DISTINCT test.col_int32) AS num_distinct\
    \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#test.col_int32), COUNT(DISTINCT #test.col_int32)]]\
    \n    TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

fn optimize_plan(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let mut config = OptimizerConfig::new().with_skip_failing_rules(false);
    let optimizer = create_optimizer();
    optimizer.optimize(&plan, &mut config, &observe)
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    let optimizer = create_optimizer();

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let schema_provider = MySchemaProvider {};
    let sql_to_rel = SqlToRel::new(&schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // optimize the logical plan
    let mut config = OptimizerConfig::new().with_skip_failing_rules(false);
    optimizer.optimize(&plan, &mut config, &observe)
}

fn create_optimizer() -> Optimizer {
    let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
        // Simplify expressions first to maximize the chance
        // of applying other optimizations
        Arc::new(SimplifyExpressions::new()),
        Arc::new(DecorrelateWhereExists::new()),
        Arc::new(DecorrelateWhereIn::new()),
        Arc::new(DecorrelateScalarSubquery::new()),
        Arc::new(SubqueryFilterToJoin::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(ProjectionPushDown::new()),
        Arc::new(RewriteDisjunctivePredicate::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(ReduceOuterJoin::new()),
        Arc::new(FilterPushDown::new()),
        Arc::new(LimitPushDown::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
    ];

    let optimizer = Optimizer::new(rules);
    optimizer
}

struct MySchemaProvider {}

impl ContextProvider for MySchemaProvider {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if table_name.starts_with("test") {
            let schema = Schema::new_with_metadata(
                vec![
                    Field::new("col_int32", DataType::Int32, true),
                    Field::new("col_utf8", DataType::Utf8, true),
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
