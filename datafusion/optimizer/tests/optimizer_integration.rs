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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{internal_err, plan_err, DataFusionError, Result};
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion_optimizer::analyzer::{Analyzer, AnalyzerConfig};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{OptimizerConfig, OptimizerContext};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;

use chrono::{DateTime, NaiveDateTime, Utc};

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
    // regression test for https://github.com/apache/arrow-datafusion/issues/3760
    let sql = "SELECT col_int32 FROM test \
    WHERE col_int32 > (\
      SELECT AVG(col_int32) FROM test \
      WHERE col_utf8 BETWEEN '2002-05-08' \
        AND (cast('2002-05-08' as date) + interval '5 days')\
    )";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_int32\
    \n  Inner Join:  Filter: CAST(test.col_int32 AS Float64) > __scalar_sq_1.AVG(test.col_int32)\
    \n    TableScan: test projection=[col_int32]\
    \n    SubqueryAlias: __scalar_sq_1\
    \n      Aggregate: groupBy=[[]], aggr=[[AVG(CAST(test.col_int32 AS Float64))]]\
    \n        Projection: test.col_int32\
    \n          Filter: test.col_utf8 >= Utf8(\"2002-05-08\") AND test.col_utf8 <= Utf8(\"2002-05-13\")\
    \n            TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn case_when_aggregate() -> Result<()> {
    let sql = "SELECT col_utf8, SUM(CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END) AS n FROM test GROUP BY col_utf8";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_utf8, SUM(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END) AS n\
                    \n  Aggregate: groupBy=[[test.col_utf8]], aggr=[[SUM(CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END) AS SUM(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END)]]\
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
    // regression test for https://github.com/apache/arrow-datafusion/issues/3234
    let sql = "SELECT col_int32, col_utf8 FROM test DISTRIBUTE BY (col_utf8)";
    let plan = test_sql(sql)?;
    let expected = "Repartition: DistributeBy(col_utf8)\
    \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn semi_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/2888
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
    // regression test for https://github.com/apache/arrow-datafusion/issues/2888
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
        "Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1))]]\
        \n  Projection: \
        \n    Filter: test.col_date32 >= Date32(\"10303\") AND test.col_date32 <= Date32(\"10393\")\
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
        "Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1))]]\
        \n  Projection: \
        \n    Filter: test.col_date64 >= Date64(\"890179200000\") AND test.col_date64 <= Date64(\"897955200000\")\
        \n      TableScan: test projection=[col_date64]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn concat_literals() -> Result<()> {
    let sql = "SELECT concat(true, col_int32, false, null, 'hello', col_utf8, 12, 3.4) \
        AS col
        FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: concat(Utf8(\"true\"), CAST(test.col_int32 AS Utf8), Utf8(\"falsehello\"), test.col_utf8, Utf8(\"123.4\")) AS col\
        \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn concat_ws_literals() -> Result<()> {
    let sql = "SELECT concat_ws('-', true, col_int32, false, null, 'hello', col_utf8, 12, '', 3.4) \
        AS col
        FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: concat_ws(Utf8(\"-\"), Utf8(\"true\"), CAST(test.col_int32 AS Utf8), Utf8(\"false-hello\"), test.col_utf8, Utf8(\"12--3.4\")) AS col\
        \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn timestamp_nano_ts_none_predicates() -> Result<()> {
    let sql = "SELECT col_int32
        FROM test
        WHERE col_ts_nano_none < (now() - interval '1 hour')";
    let plan = test_sql(sql)?;
    // a scan should have the now()... predicate folded to a single
    // constant and compared to the column without a cast so it can be
    // pushed down / pruned
    let expected =
        "Projection: test.col_int32\
         \n  Filter: test.col_ts_nano_none < TimestampNanosecond(1666612093000000000, None)\
         \n    TableScan: test projection=[col_int32, col_ts_nano_none]";
    assert_eq!(expected, format!("{plan:?}"));
    Ok(())
}

#[test]
fn timestamp_nano_ts_utc_predicates() {
    let sql = "SELECT col_int32
        FROM test
        WHERE col_ts_nano_utc < (now() - interval '1 hour')";
    let plan = test_sql(sql).unwrap();
    // a scan should have the now()... predicate folded to a single
    // constant and compared to the column without a cast so it can be
    // pushed down / pruned
    let expected =
        "Projection: test.col_int32\n  Filter: test.col_ts_nano_utc < TimestampNanosecond(1666612093000000000, Some(\"+00:00\"))\
         \n    TableScan: test projection=[col_int32, col_ts_nano_utc]";
    assert_eq!(expected, format!("{plan:?}"));
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
    let expected = "Projection: test.col_int32 + test.col_uint32 AS c, COUNT(*)\
    \n  Aggregate: groupBy=[[test.col_int32 + CAST(test.col_uint32 AS Int32)]], aggr=[[COUNT(UInt8(1)) AS COUNT(*)]]\
    \n    Filter: test.col_int32 + CAST(test.col_uint32 AS Int32) > Int32(3)\
    \n      TableScan: test projection=[col_int32, col_uint32]";
    assert_eq!(expected, format!("{plan:?}"));
}

#[test]
// issue: https://github.com/apache/arrow-datafusion/issues/5334
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

struct EmptyRegistryAnalyzerConfig<'a> {
    config_options: &'a ConfigOptions,
}

impl<'a> FunctionRegistry for EmptyRegistryAnalyzerConfig<'a> {
    fn udfs(&self) -> std::collections::HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, _name: &str) -> Result<Arc<ScalarUDF>> {
        internal_err!("Mock function registry")
    }

    fn udaf(&self, _name: &str) -> Result<Arc<AggregateUDF>> {
        internal_err!("Mock function registry")
    }

    fn udwf(&self, _name: &str) -> Result<Arc<WindowUDF>> {
        internal_err!("Mock function registry")
    }
}

impl<'a> AnalyzerConfig for EmptyRegistryAnalyzerConfig<'a> {
    fn function_registry(&self) -> &dyn FunctionRegistry {
        self
    }

    fn options(&self) -> &ConfigOptions {
        self.config_options
    }
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = MyContextProvider::default();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // hard code the return value of now()
    let ts = NaiveDateTime::from_timestamp_opt(1666615693, 0).unwrap();
    let now_time = DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc);
    let config = OptimizerContext::new()
        .with_skip_failing_rules(false)
        .with_query_execution_start_time(now_time);
    let analyzer = Analyzer::new();
    let optimizer = Optimizer::new();
    let analyzer_config = EmptyRegistryAnalyzerConfig {
        config_options: config.options(),
    };
    // analyze and optimize the logical plan
    let plan = analyzer.execute_and_check(&plan, &analyzer_config, |_, _| {})?;
    optimizer.optimize(&plan, &config, |_, _| {})
}

#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
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
