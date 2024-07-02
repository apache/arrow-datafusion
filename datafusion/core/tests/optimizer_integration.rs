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

//! Tests for the DataFusion SQL query planner that require functions from the
//! datafusion-functions crate.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_schema::{Fields, SchemaBuilder};
use datafusion_common::config::ConfigOptions;
use datafusion_common::logical_type::schema::LogicalSchemaRef;
use datafusion_common::logical_type::TypeRelation;
use datafusion_common::tree_node::{TransformedResult, TreeNode};
use datafusion_common::{plan_err, DFSchema, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::{Interval, NullableInterval};
use datafusion_expr::{
    col, lit, AggregateUDF, BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator,
    ScalarUDF, TableSource, WindowUDF,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::simplify_expressions::GuaranteeRewriter;
use datafusion_optimizer::{OptimizerConfig, OptimizerContext};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;

use chrono::DateTime;
use datafusion_functions::datetime;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // enable logging so RUST_LOG works
    let _ = env_logger::try_init();
}

#[test]
fn select_arrow_cast() {
    let sql = "SELECT arrow_cast(1234, 'Float64') as f64, arrow_cast('foo', 'LargeUtf8') as large";
    let expected = "Projection: Float64(1234) AS f64, LargeUtf8(\"foo\") AS large\
        \n  EmptyRelation";
    quick_test(sql, expected);
}
#[test]
fn timestamp_nano_ts_none_predicates() -> Result<()> {
    let sql = "SELECT col_int32
        FROM test
        WHERE col_ts_nano_none < (now() - interval '1 hour')";
    // a scan should have the now()... predicate folded to a single
    // constant and compared to the column without a cast so it can be
    // pushed down / pruned
    let expected =
        "Projection: test.col_int32\
         \n  Filter: test.col_ts_nano_none < TimestampNanosecond(1666612093000000000, None)\
         \n    TableScan: test projection=[col_int32, col_ts_nano_none]";
    quick_test(sql, expected);
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
fn concat_literals() -> Result<()> {
    let sql = "SELECT concat(true, col_int32, false, null, 'hello', col_utf8, 12, 3.4) \
        AS col
        FROM test";
    let expected =
        "Projection: concat(Utf8(\"true\"), CAST(test.col_int32 AS Utf8), Utf8(\"falsehello\"), test.col_utf8, Utf8(\"123.4\")) AS col\
        \n  TableScan: test projection=[col_int32, col_utf8]";
    quick_test(sql, expected);
    Ok(())
}

#[test]
fn concat_ws_literals() -> Result<()> {
    let sql = "SELECT concat_ws('-', true, col_int32, false, null, 'hello', col_utf8, 12, '', 3.4) \
        AS col
        FROM test";
    let expected =
        "Projection: concat_ws(Utf8(\"-\"), Utf8(\"true\"), CAST(test.col_int32 AS Utf8), Utf8(\"false-hello\"), test.col_utf8, Utf8(\"12--3.4\")) AS col\
        \n  TableScan: test projection=[col_int32, col_utf8]";
    quick_test(sql, expected);
    Ok(())
}

fn quick_test(sql: &str, expected_plan: &str) {
    let plan = test_sql(sql).unwrap();
    assert_eq!(expected_plan, format!("{:?}", plan));
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = MyContextProvider::default()
        .with_udf(datetime::now())
        .with_udf(datafusion_functions::core::arrow_cast())
        .with_udf(datafusion_functions::string::concat())
        .with_udf(datafusion_functions::string::concat_ws());
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // hard code the return value of now()
    let now_time = DateTime::from_timestamp(1666615693, 0).unwrap();
    let config = OptimizerContext::new()
        .with_skip_failing_rules(false)
        .with_query_execution_start_time(now_time);
    let analyzer = Analyzer::new();
    let optimizer = Optimizer::new();
    // analyze and optimize the logical plan
    let plan = analyzer.execute_and_check(plan, config.options(), |_, _| {})?;
    optimizer.optimize(plan, &config, |_, _| {})
}

#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
    udfs: HashMap<String, Arc<ScalarUDF>>,
}

impl MyContextProvider {
    fn with_udf(mut self, udf: Arc<ScalarUDF>) -> Self {
        self.udfs.insert(udf.name().to_string(), udf);
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

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
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

#[test]
fn test_nested_schema_nullability() {
    let mut builder = SchemaBuilder::new();
    builder.push(Field::new("foo", DataType::Int32, true));
    builder.push(Field::new(
        "parent",
        DataType::Struct(Fields::from(vec![Field::new(
            "child",
            DataType::Int64,
            false,
        )])),
        true,
    ));
    let schema = builder.finish();

    let dfschema = DFSchema::from_field_specific_qualified_schema(
        vec![Some("table_name".into()), None],
        &LogicalSchemaRef::new(schema.into()),
    )
    .unwrap();

    let expr = col("parent").field("child");
    assert!(expr.nullable(&dfschema).unwrap());
}

#[test]
fn test_inequalities_non_null_bounded() {
    let guarantees = vec![
        // x ∈ [1, 3] (not null)
        (
            col("x"),
            NullableInterval::NotNull {
                values: Interval::make(Some(1_i32), Some(3_i32)).unwrap(),
            },
        ),
        // s.y ∈ [1, 3] (not null)
        (
            col("s").field("y"),
            NullableInterval::NotNull {
                values: Interval::make(Some(1_i32), Some(3_i32)).unwrap(),
            },
        ),
    ];

    let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

    // (original_expr, expected_simplification)
    let simplified_cases = &[
        (col("x").lt(lit(0)), false),
        (col("s").field("y").lt(lit(0)), false),
        (col("x").lt_eq(lit(3)), true),
        (col("x").gt(lit(3)), false),
        (col("x").gt(lit(0)), true),
        (col("x").eq(lit(0)), false),
        (col("x").not_eq(lit(0)), true),
        (col("x").between(lit(0), lit(5)), true),
        (col("x").between(lit(5), lit(10)), false),
        (col("x").not_between(lit(0), lit(5)), false),
        (col("x").not_between(lit(5), lit(10)), true),
        (
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("x")),
                op: Operator::IsDistinctFrom,
                right: Box::new(lit(ScalarValue::Null)),
            }),
            true,
        ),
        (
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("x")),
                op: Operator::IsDistinctFrom,
                right: Box::new(lit(5)),
            }),
            true,
        ),
    ];

    validate_simplified_cases(&mut rewriter, simplified_cases);

    let unchanged_cases = &[
        col("x").gt(lit(2)),
        col("x").lt_eq(lit(2)),
        col("x").eq(lit(2)),
        col("x").not_eq(lit(2)),
        col("x").between(lit(3), lit(5)),
        col("x").not_between(lit(3), lit(10)),
    ];

    validate_unchanged_cases(&mut rewriter, unchanged_cases);
}

fn validate_simplified_cases<T>(rewriter: &mut GuaranteeRewriter, cases: &[(Expr, T)])
where
    ScalarValue: From<T>,
    T: Clone,
{
    for (expr, expected_value) in cases {
        let output = expr.clone().rewrite(rewriter).data().unwrap();
        let expected = lit(ScalarValue::from(expected_value.clone()));
        assert_eq!(
            output, expected,
            "{} simplified to {}, but expected {}",
            expr, output, expected
        );
    }
}
fn validate_unchanged_cases(rewriter: &mut GuaranteeRewriter, cases: &[Expr]) {
    for expr in cases {
        let output = expr.clone().rewrite(rewriter).data().unwrap();
        assert_eq!(
            &output, expr,
            "{} was simplified to {}, but expected it to be unchanged",
            expr, output
        );
    }
}
