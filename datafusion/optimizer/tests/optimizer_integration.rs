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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result};
use datafusion_common::logical_type::schema::LogicalSchemaRef;
use datafusion_common::logical_type::TypeRelation;
use datafusion_expr::test::function_stub::sum_udaf;
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{OptimizerConfig, OptimizerContext, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // enable logging so RUST_LOG works
    let _ = env_logger::try_init();
}

#[test]
fn case_when() -> Result<()> {
    let sql = "SELECT CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END AS CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END\
         \n  TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{plan:?}"));

    let sql = "SELECT CASE WHEN col_uint32 > 0 THEN 1 ELSE 0 END FROM test";
    let plan = test_sql(sql)?;
    let expected = "Projection: CASE WHEN test.col_uint32 > UInt32(0) THEN Int64(1) ELSE Int64(0) END AS CASE WHEN test.col_uint32 > Int64(0) THEN Int64(1) ELSE Int64(0) END\
                    \n  TableScan: test projection=[col_uint32]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn subquery_filter_with_cast() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/3760
    let sql = "SELECT col_int32 FROM test \
    WHERE col_int32 > (\
      SELECT avg(col_int32) FROM test \
      WHERE col_utf8 BETWEEN '2002-05-08' \
        AND (cast('2002-05-08' as date) + interval '5 days')\
    )";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_int32\
    \n  Inner Join:  Filter: CAST(test.col_int32 AS Float64) > __scalar_sq_1.avg(test.col_int32)\
    \n    TableScan: test projection=[col_int32]\
    \n    SubqueryAlias: __scalar_sq_1\
    \n      Aggregate: groupBy=[[]], aggr=[[avg(CAST(test.col_int32 AS Float64))]]\
    \n        Projection: test.col_int32\
    \n          Filter: test.col_utf8 >= Utf8(\"2002-05-08\") AND test.col_utf8 <= Utf8(\"2002-05-13\")\
    \n            TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn case_when_aggregate() -> Result<()> {
    let sql = "SELECT col_utf8, sum(CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END) AS n FROM test GROUP BY col_utf8";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_utf8, sum(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END) AS n\
                    \n  Aggregate: groupBy=[[test.col_utf8]], aggr=[[sum(CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END) AS sum(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END)]]\
                    \n    TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn unsigned_target_type() -> Result<()> {
    let sql = "SELECT col_utf8 FROM test WHERE col_uint32 > 0";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_utf8\
                    \n  Filter: test.col_uint32 > UInt32(0)\
                    \n    TableScan: test projection=[col_uint32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn distribute_by() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/3234
    let sql = "SELECT col_int32, col_utf8 FROM test DISTRIBUTE BY (col_utf8)";
    let plan = test_sql(sql)?;
    let expected = "Repartition: DistributeBy(col_utf8)\
    \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn semi_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/2888
    let sql = "SELECT col_utf8 FROM test WHERE EXISTS (\
               SELECT col_utf8 FROM test t2 WHERE test.col_int32 = t2.col_int32 \
               AND test.col_uint32 != t2.col_uint32)";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_utf8\
                    \n  LeftSemi Join: test.col_int32 = __correlated_sq_1.col_int32 Filter: test.col_uint32 != __correlated_sq_1.col_uint32\
                    \n    TableScan: test projection=[col_int32, col_uint32, col_utf8]\
                    \n    SubqueryAlias: __correlated_sq_1\
                    \n      SubqueryAlias: t2\
                    \n        TableScan: test projection=[col_int32, col_uint32]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn anti_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/2888
    let sql = "SELECT col_utf8 FROM test WHERE NOT EXISTS (\
               SELECT col_utf8 FROM test t2 WHERE test.col_int32 = t2.col_int32 \
               AND test.col_uint32 != t2.col_uint32)";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_utf8\
    \n  LeftAnti Join: test.col_int32 = __correlated_sq_1.col_int32 Filter: test.col_uint32 != __correlated_sq_1.col_uint32\
    \n    TableScan: test projection=[col_int32, col_uint32, col_utf8]\
    \n    SubqueryAlias: __correlated_sq_1\
    \n      SubqueryAlias: t2\
    \n        TableScan: test projection=[col_int32, col_uint32]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn where_exists_distinct() -> Result<()> {
    let sql = "SELECT col_int32 FROM test WHERE EXISTS (\
               SELECT DISTINCT col_int32 FROM test t2 WHERE test.col_int32 = t2.col_int32)";
    let plan = test_sql(sql)?;
    let expected = "LeftSemi Join: test.col_int32 = __correlated_sq_1.col_int32\
    \n  TableScan: test projection=[col_int32]\
    \n  SubqueryAlias: __correlated_sq_1\
    \n    Aggregate: groupBy=[[t2.col_int32]], aggr=[[]]\
    \n      SubqueryAlias: t2\
    \n        TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn intersect() -> Result<()> {
    let sql = "SELECT col_int32, col_utf8 FROM test \
    INTERSECT SELECT col_int32, col_utf8 FROM test \
    INTERSECT SELECT col_int32, col_utf8 FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "LeftSemi Join: test.col_int32 = test.col_int32, test.col_utf8 = test.col_utf8\
    \n  Aggregate: groupBy=[[test.col_int32, test.col_utf8]], aggr=[[]]\
    \n    LeftSemi Join: test.col_int32 = test.col_int32, test.col_utf8 = test.col_utf8\
    \n      Aggregate: groupBy=[[test.col_int32, test.col_utf8]], aggr=[[]]\
    \n        TableScan: test projection=[col_int32, col_utf8]\
    \n      TableScan: test projection=[col_int32, col_utf8]\
    \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn between_date32_plus_interval() -> Result<()> {
    let sql = "SELECT count(1) FROM test \
    WHERE col_date32 between '1998-03-18' AND cast('1998-03-18' as date) + INTERVAL '90 days'";
    let plan = test_sql(sql)?;
    let expected =
        "Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]\
        \n  Projection: \
        \n    Filter: test.col_date32 >= Date32(\"1998-03-18\") AND test.col_date32 <= Date32(\"1998-06-16\")\
        \n      TableScan: test projection=[col_date32]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn between_date64_plus_interval() -> Result<()> {
    let sql = "SELECT count(1) FROM test \
    WHERE col_date64 between '1998-03-18T00:00:00' AND cast('1998-03-18' as date) + INTERVAL '90 days'";
    let plan = test_sql(sql)?;
    let expected =
        "Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]\
        \n  Projection: \
        \n    Filter: test.col_date64 >= Date64(\"1998-03-18\") AND test.col_date64 <= Date64(\"1998-06-16\")\
        \n      TableScan: test projection=[col_date64]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn propagate_empty_relation() {
    let sql = "SELECT test.col_int32 FROM test JOIN ( SELECT col_int32 FROM test WHERE false ) AS ta1 ON test.col_int32 = ta1.col_int32;";
    let plan = test_sql(sql).unwrap();
    // when children exist EmptyRelation, it will bottom-up propagate.
    let expected = "EmptyRelation";
    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
fn join_keys_in_subquery_alias() {
    let sql = "SELECT * FROM test AS A, ( SELECT col_int32 as key FROM test ) AS B where A.col_int32 = B.key;";
    let plan = test_sql(sql).unwrap();
    let expected = "Inner Join: a.col_int32 = b.key\
    \n  SubqueryAlias: a\
    \n    Filter: test.col_int32 IS NOT NULL\
    \n      TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64, col_ts_nano_none, col_ts_nano_utc]\
    \n  SubqueryAlias: b\
    \n    Projection: test.col_int32 AS key\
    \n      Filter: test.col_int32 IS NOT NULL\
    \n        TableScan: test projection=[col_int32]";

    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
fn join_keys_in_subquery_alias_1() {
    let sql = "SELECT * FROM test AS A, ( SELECT test.col_int32 AS key FROM test JOIN test AS C on test.col_int32 = C.col_int32 ) AS B where A.col_int32 = B.key;";
    let plan = test_sql(sql).unwrap();
    let expected = "Inner Join: a.col_int32 = b.key\
    \n  SubqueryAlias: a\
    \n    Filter: test.col_int32 IS NOT NULL\
    \n      TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64, col_ts_nano_none, col_ts_nano_utc]\
    \n  SubqueryAlias: b\
    \n    Projection: test.col_int32 AS key\
    \n      Inner Join: test.col_int32 = c.col_int32\
    \n        Filter: test.col_int32 IS NOT NULL\
    \n          TableScan: test projection=[col_int32]\
    \n        SubqueryAlias: c\
    \n          Filter: test.col_int32 IS NOT NULL\
    \n            TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
fn push_down_filter_groupby_expr_contains_alias() {
    let sql = "SELECT * FROM (SELECT (col_int32 + col_uint32) AS c, count(*) FROM test GROUP BY 1) where c > 3";
    let plan = test_sql(sql).unwrap();
    let expected = "Projection: test.col_int32 + test.col_uint32 AS c, count(*)\
    \n  Aggregate: groupBy=[[test.col_int32 + CAST(test.col_uint32 AS Int32)]], aggr=[[count(Int64(1)) AS count(*)]]\
    \n    Filter: test.col_int32 + CAST(test.col_uint32 AS Int32) > Int32(3)\
    \n      TableScan: test projection=[col_int32, col_uint32]";
    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
// issue: https://github.com/apache/datafusion/issues/5334
fn test_same_name_but_not_ambiguous() {
    let sql = "SELECT t1.col_int32 AS col_int32 FROM test t1 intersect SELECT col_int32 FROM test t2";
    let plan = test_sql(sql).unwrap();
    let expected = "LeftSemi Join: t1.col_int32 = t2.col_int32\
    \n  Aggregate: groupBy=[[t1.col_int32]], aggr=[[]]\
    \n    SubqueryAlias: t1\
    \n      TableScan: test projection=[col_int32]\
    \n  SubqueryAlias: t2\
    \n    TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
fn eliminate_nested_filters() {
    let sql = "\
        SELECT col_int32 FROM test \
        WHERE (1=1) AND (col_int32 > 0) \
        AND (1=1) AND (1=0 OR 1=1)";

    let plan = test_sql(sql).unwrap();
    let expected = "\
        Filter: test.col_int32 > Int32(0)\
        \n  TableScan: test projection=[col_int32]";

    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
fn test_propagate_empty_relation_inner_join_and_unions() {
    let sql = "\
        SELECT A.col_int32 FROM test AS A \
        INNER JOIN ( \
          SELECT col_int32 FROM test WHERE 1 = 0 \
        ) AS B ON A.col_int32 = B.col_int32 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE 1 = 1 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE 0 = 0 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE test.col_int32 < 0 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE 1 = 0";

    let plan = test_sql(sql).unwrap();
    let expected = "\
        Union\
        \n  TableScan: test projection=[col_int32]\
        \n  TableScan: test projection=[col_int32]\
        \n  Filter: test.col_int32 < Int32(0)\
        \n    TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{plan:?}"));
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];
    let context_provider = MyContextProvider::default()
        .with_udaf(sum_udaf())
        .with_udaf(count_udaf())
        .with_udaf(avg_udaf());
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    let config = OptimizerContext::new().with_skip_failing_rules(false);
    let analyzer = Analyzer::new();
    let optimizer = Optimizer::new();
    // analyze and optimize the logical plan
    let plan = analyzer.execute_and_check(plan, config.options(), |_, _| {})?;
    optimizer.optimize(plan, &config, observe)
}

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
    udafs: HashMap<String, Arc<AggregateUDF>>,
}

impl MyContextProvider {
    fn with_udaf(mut self, udaf: Arc<AggregateUDF>) -> Self {
        // TODO: change to to_string() if all the function name is converted to lowercase
        self.udafs.insert(udaf.name().to_lowercase(), udaf);
        self
    }
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if table_name.starts_with("test") {
            let schema = Schema::new_with_metadata(
                vec![
                    Field::new("col_int32", DataType::Int32, true),
                    Field::new("col_uint32", DataType::UInt32, true),
                    Field::new("col_utf8", DataType::Utf8, true),
                    Field::new("col_date32", DataType::Date32, true),
                    Field::new("col_date64", DataType::Date64, true),
                    // timestamp with no timezone
                    Field::new(
                        "col_ts_nano_none",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ),
                    // timestamp with UTC timezone
                    Field::new(
                        "col_ts_nano_utc",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                        true,
                    ),
                ],
                HashMap::new(),
            );

            Ok(Arc::new(MyTableSource {
                schema: Arc::new(schema),
            }))
        } else {
            plan_err!("table does not exist")
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.udafs.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<TypeRelation> {
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

struct MyTableSource {
    schema: SchemaRef,
}

impl TableSource for MyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.schema.clone().into())
    }
}
