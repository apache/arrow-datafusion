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

// This file contains an end to end test of parquet pruning. It writes
// data into a parquet file and then
use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::datatypes::TimeUnit;
use arrow::io::parquet::write::{FileWriter, RowGroupIterator};
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, Utf8Array},
    datatypes::{DataType, Field, Schema},
    io::parquet::write::{CompressionOptions, Encoding, Version, WriteOptions},
};
use chrono::{Datelike, Duration};
use datafusion::record_batch::RecordBatch;
use datafusion::{
    arrow_print,
    datasource::TableProvider,
    logical_plan::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder},
    physical_plan::{
        accept, file_format::ParquetExec, metrics::MetricsSet, ExecutionPlan,
        ExecutionPlanVisitor,
    },
    prelude::{ExecutionConfig, ExecutionContext},
    scalar::ScalarValue,
};
use datafusion_common::field_util::SchemaExt;
use tempfile::NamedTempFile;

#[tokio::test]
async fn prune_timestamps_nanos() {
    let output = ContextWithParquet::new(Scenario::Timestamps)
        .await
        .query("SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')")
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_timestamps_micros() {
    let output = ContextWithParquet::new(Scenario::Timestamps)
        .await
        .query(
            "SELECT * FROM t where micros < to_timestamp_micros('2020-01-02 01:01:11Z')",
        )
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_timestamps_millis() {
    let output = ContextWithParquet::new(Scenario::Timestamps)
        .await
        .query(
            "SELECT * FROM t where millis < to_timestamp_millis('2020-01-02 01:01:11Z')",
        )
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_timestamps_seconds() {
    let output = ContextWithParquet::new(Scenario::Timestamps)
        .await
        .query(
            "SELECT * FROM t where seconds < to_timestamp_seconds('2020-01-02 01:01:11Z')",
        )
        .await;
    println!("{}", output.description());
    // This should prune one metrics without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(output.result_rows, 10, "{}", output.description());
}

#[tokio::test]
async fn prune_date32() {
    let output = ContextWithParquet::new(Scenario::Dates)
        .await
        .query("SELECT * FROM t where date32 < cast('2020-01-02' as date)")
        .await;
    println!("{}", output.description());
    // This should prune out groups  without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_date64() {
    // work around for not being able to cast Date32 to Date64 automatically
    let date = "2020-01-02"
        .parse::<chrono::NaiveDate>()
        .unwrap()
        .and_time(chrono::NaiveTime::from_hms(0, 0, 0));
    let date = ScalarValue::Date64(Some(date.timestamp_millis()));

    let output = ContextWithParquet::new(Scenario::Dates)
        .await
        .query_with_expr(col("date64").lt(lit(date)))
        // .query(
        //     "SELECT * FROM t where date64 < caste('2020-01-02' as date)",
        // query results in Plan("'Date64 < Date32' can't be evaluated because there isn't a common type to coerce the types to")
        // )
        .await;

    println!("{}", output.description());
    // This should prune out groups  without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_disabled() {
    let query = "SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')";
    let expected_rows = 10;

    // with pruning
    let output = ContextWithParquet::new(Scenario::Timestamps)
        .await
        .query(query)
        .await;

    // This should prune one without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(1));
    assert_eq!(
        output.result_rows,
        expected_rows,
        "{}",
        output.description()
    );

    // same query, without pruning
    let config = ExecutionConfig::new().with_parquet_pruning(false);

    let output = ContextWithParquet::with_config(Scenario::Timestamps, config)
        .await
        .query(query)
        .await;
    println!("{}", output.description());

    // This should not prune any
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(
        output.result_rows,
        expected_rows,
        "{}",
        output.description()
    );
}

#[tokio::test]
async fn prune_int32_lt() {
    let (expected_errors, expected_row_group_pruned, expected_results) =
        (Some(0), Some(1), 11);

    // resulrt of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where i < 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(output.row_groups_pruned(), expected_row_group_pruned);
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );

    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where -i > -1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(output.row_groups_pruned(), expected_row_group_pruned);
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );
}

#[tokio::test]
async fn prune_int32_eq() {
    // resulrt of sql "SELECT * FROM t where i = 1"
    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where i = 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_int32_scalar_fun_and_eq() {
    // resulrt of sql "SELECT * FROM t where abs(i) = 1 and i = 1"
    // only use "i = 1" to prune
    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where abs(i) = 1  and i = 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(3));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_int32_scalar_fun() {
    // resulrt of sql "SELECT * FROM t where abs(i) = 1" is not supported
    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where abs(i) = 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups with error, because there is not col to
    // prune the row groups.
    assert_eq!(output.predicate_evaluation_errors(), Some(1));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(output.result_rows, 3, "{}", output.description());
}

#[tokio::test]
async fn prune_int32_complex_expr() {
    // resulrt of sql "SELECT * FROM t where i+1 = 1" is not supported
    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where i+1 = 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups with error, because there is not col to
    // prune the row groups.
    assert_eq!(output.predicate_evaluation_errors(), Some(1));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(output.result_rows, 2, "{}", output.description());
}

#[tokio::test]
async fn prune_int32_complex_expr_subtract() {
    // resulrt of sql "SELECT * FROM t where 1-i > 1" is not supported
    let output = ContextWithParquet::new(Scenario::Int32)
        .await
        .query("SELECT * FROM t where 1-i > 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups with error, because there is not col to
    // prune the row groups.
    assert_eq!(output.predicate_evaluation_errors(), Some(1));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(output.result_rows, 9, "{}", output.description());
}

#[tokio::test]
async fn prune_f64_lt() {
    let (expected_errors, expected_row_group_pruned, expected_results) =
        (Some(0), Some(1), 11);

    // resulrt of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    let output = ContextWithParquet::new(Scenario::Float64)
        .await
        .query("SELECT * FROM t where f < 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(output.row_groups_pruned(), expected_row_group_pruned);
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );

    let output = ContextWithParquet::new(Scenario::Float64)
        .await
        .query("SELECT * FROM t where -f > -1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(output.row_groups_pruned(), expected_row_group_pruned);
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );
}

#[tokio::test]
async fn prune_f64_scalar_fun_and_gt() {
    // resulrt of sql "SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1"
    // only use "f >= 0" to prune
    let output = ContextWithParquet::new(Scenario::Float64)
        .await
        .query("SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1")
        .await;

    println!("{}", output.description());
    // This should prune out groups without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_groups_pruned(), Some(2));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_f64_scalar_fun() {
    // resulrt of sql "SELECT * FROM t where abs(f-1) <= 0.000001" is not supported
    let output = ContextWithParquet::new(Scenario::Float64)
        .await
        .query("SELECT * FROM t where abs(f-1) <= 0.000001")
        .await;

    println!("{}", output.description());
    // This should prune out groups with error, because there is not col to
    // prune the row groups.
    assert_eq!(output.predicate_evaluation_errors(), Some(1));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
async fn prune_f64_complex_expr() {
    // resulrt of sql "SELECT * FROM t where f+1 > 1.1"" is not supported
    let output = ContextWithParquet::new(Scenario::Float64)
        .await
        .query("SELECT * FROM t where f+1 > 1.1")
        .await;

    println!("{}", output.description());
    // This should prune out groups with error, because there is not col to
    // prune the row groups.
    assert_eq!(output.predicate_evaluation_errors(), Some(1));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(output.result_rows, 9, "{}", output.description());
}

#[tokio::test]
async fn prune_f64_complex_expr_subtract() {
    // resulrt of sql "SELECT * FROM t where 1-f > 1" is not supported
    let output = ContextWithParquet::new(Scenario::Float64)
        .await
        .query("SELECT * FROM t where 1-f > 1")
        .await;

    println!("{}", output.description());
    // This should prune out groups with error, because there is not col to
    // prune the row groups.
    assert_eq!(output.predicate_evaluation_errors(), Some(1));
    assert_eq!(output.row_groups_pruned(), Some(0));
    assert_eq!(output.result_rows, 9, "{}", output.description());
}

// ----------------------
// Begin test fixture
// ----------------------

/// What data to use
enum Scenario {
    Timestamps,
    Dates,
    Int32,
    Float64,
}

/// Test fixture that has an execution context that has an external
/// table "t" registered, pointing at a parquet file made with
/// `make_test_file`
struct ContextWithParquet {
    #[allow(dead_code)]
    /// temp file parquet data is written to. The file is cleaned up
    /// when dropped
    file: NamedTempFile,
    provider: Arc<dyn TableProvider>,
    ctx: ExecutionContext,
}

/// The output of running one of the test cases
struct TestOutput {
    /// The input string
    sql: String,
    /// Execution metrics for the Parquet Scan
    parquet_metrics: MetricsSet,
    /// number of rows in results
    result_rows: usize,
    /// the contents of the input, as a string
    pretty_input: String,
    /// the raw results, as a string
    pretty_results: String,
}

impl TestOutput {
    /// retrieve the value of the named metric, if any
    fn metric_value(&self, metric_name: &str) -> Option<usize> {
        self.parquet_metrics
            .sum(|metric| metric.value().name() == metric_name)
            .map(|v| v.as_usize())
    }

    /// The number of times the pruning predicate evaluation errors
    fn predicate_evaluation_errors(&self) -> Option<usize> {
        self.metric_value("predicate_evaluation_errors")
    }

    /// The number of times the pruning predicate evaluation errors
    fn row_groups_pruned(&self) -> Option<usize> {
        self.metric_value("row_groups_pruned")
    }

    fn description(&self) -> String {
        format!(
            "Input:\n{}\nQuery:\n{}\nOutput:\n{}\nMetrics:\n{}",
            self.pretty_input, self.sql, self.pretty_results, self.parquet_metrics,
        )
    }
}

/// Creates an execution context that has an external table "t"
/// registered pointing at a parquet file made with `make_test_file`
/// and the appropriate scenario
impl ContextWithParquet {
    async fn new(scenario: Scenario) -> Self {
        Self::with_config(scenario, ExecutionConfig::new()).await
    }

    async fn with_config(scenario: Scenario, config: ExecutionConfig) -> Self {
        let file = make_test_file(scenario).await;
        let parquet_path = file.path().to_string_lossy();

        // now, setup a the file as a data source and run a query against it
        let mut ctx = ExecutionContext::with_config(config);

        ctx.register_parquet("t", &parquet_path).await.unwrap();
        let provider = ctx.deregister_table("t").unwrap().unwrap();
        ctx.register_table("t", provider.clone()).unwrap();

        Self {
            file,
            provider,
            ctx,
        }
    }

    /// runs a query like "SELECT * from t WHERE <expr> and returns
    /// the number of output rows and normalized execution metrics
    async fn query_with_expr(&mut self, expr: Expr) -> TestOutput {
        let sql = format!("EXPR only: {:?}", expr);
        let logical_plan = LogicalPlanBuilder::scan("t", self.provider.clone(), None)
            .unwrap()
            .filter(expr)
            .unwrap()
            .build()
            .unwrap();
        self.run_test(logical_plan, sql).await
    }

    /// Runs the specified SQL query and returns the number of output
    /// rows and normalized execution metrics
    async fn query(&mut self, sql: &str) -> TestOutput {
        println!("Planning sql {}", sql);
        let logical_plan = self.ctx.sql(sql).await.expect("planning").to_logical_plan();
        self.run_test(logical_plan, sql).await
    }

    /// runs the logical plan
    async fn run_test(
        &mut self,
        logical_plan: LogicalPlan,
        sql: impl Into<String>,
    ) -> TestOutput {
        let input = self
            .ctx
            .sql("SELECT * from t")
            .await
            .expect("planning")
            .collect()
            .await
            .expect("getting input");
        let pretty_input = arrow_print::write(&input);

        let logical_plan = self.ctx.optimize(&logical_plan).expect("optimizing plan");
        let physical_plan = self
            .ctx
            .create_physical_plan(&logical_plan)
            .await
            .expect("creating physical plan");

        let runtime = self.ctx.state.lock().runtime_env.clone();
        let results = datafusion::physical_plan::collect(physical_plan.clone(), runtime)
            .await
            .expect("Running");

        // find the parquet metrics
        struct MetricsFinder {
            metrics: Option<MetricsSet>,
        }
        impl ExecutionPlanVisitor for MetricsFinder {
            type Error = std::convert::Infallible;
            fn pre_visit(
                &mut self,
                plan: &dyn ExecutionPlan,
            ) -> Result<bool, Self::Error> {
                if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
                    self.metrics = plan.metrics();
                }
                // stop searching once we have found the metrics
                Ok(self.metrics.is_none())
            }
        }
        let mut finder = MetricsFinder { metrics: None };
        accept(physical_plan.as_ref(), &mut finder).unwrap();
        let parquet_metrics = finder.metrics.unwrap();

        let result_rows = results.iter().map(|b| b.num_rows()).sum();

        let pretty_results = arrow_print::write(&results);

        let sql = sql.into();
        TestOutput {
            sql,
            parquet_metrics,
            result_rows,
            pretty_input,
            pretty_results,
        }
    }
}

/// Create a test parquet file with varioud data types
async fn make_test_file(scenario: Scenario) -> NamedTempFile {
    let output_file = tempfile::Builder::new()
        .prefix("parquet_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let batches = match scenario {
        Scenario::Timestamps => {
            vec![
                make_timestamp_batch(Duration::seconds(0)),
                make_timestamp_batch(Duration::seconds(10)),
                make_timestamp_batch(Duration::minutes(10)),
                make_timestamp_batch(Duration::days(10)),
            ]
        }
        Scenario::Dates => {
            vec![
                make_date_batch(Duration::days(0)),
                make_date_batch(Duration::days(10)),
                make_date_batch(Duration::days(300)),
                make_date_batch(Duration::days(3600)),
            ]
        }
        Scenario::Int32 => {
            vec![
                make_int32_batch(-5, 0),
                make_int32_batch(-4, 1),
                make_int32_batch(0, 5),
                make_int32_batch(5, 10),
            ]
        }
        Scenario::Float64 => {
            vec![
                make_f64_batch(vec![-5.0, -4.0, -3.0, -2.0, -1.0]),
                make_f64_batch(vec![-4.0, -3.0, -2.0, -1.0, 0.0]),
                make_f64_batch(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
                make_f64_batch(vec![5.0, 6.0, 7.0, 8.0, 9.0]),
            ]
        }
    };

    let schema = batches[0].schema();

    let options = WriteOptions {
        compression: CompressionOptions::Uncompressed,
        write_statistics: true,
        version: Version::V1,
    };
    let encodings: Vec<Vec<Encoding>> = schema
        .fields()
        .iter()
        .map(|field| {
            if let DataType::Dictionary(_, _, _) = field.data_type() {
                vec![Encoding::RleDictionary]
            } else {
                vec![Encoding::Plain]
            }
        })
        .collect();
    let row_groups = RowGroupIterator::try_new(
        batches.iter().map(|batch| Ok(batch.into())),
        schema,
        options,
        encodings,
    );

    let mut file = output_file.as_file();

    let mut writer =
        FileWriter::try_new(&mut file, schema.as_ref().clone(), options).unwrap();
    for rg in row_groups.unwrap() {
        let group = rg.unwrap();
        writer.write(group).unwrap();
    }
    writer.end(None).unwrap();

    output_file
}

/// Return record batch with a few rows of data for all of the supported timestamp types
/// values with the specified offset
///
/// Columns are named:
/// "nanos" --> TimestampNanosecondArray
/// "micros" --> TimestampMicrosecondArray
/// "millis" --> TimestampMillisecondArray
/// "seconds" --> TimestampSecondArray
/// "names" --> StringArray
fn make_timestamp_batch(offset: Duration) -> RecordBatch {
    let ts_strings = vec![
        Some("2020-01-01T01:01:01.0000000000001"),
        Some("2020-01-01T01:02:01.0000000000001"),
        Some("2020-01-01T02:01:01.0000000000001"),
        None,
        Some("2020-01-02T01:01:01.0000000000001"),
    ];

    let offset_nanos = offset.num_nanoseconds().expect("non overflow nanos");

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                offset_nanos
                    + t.parse::<chrono::NaiveDateTime>()
                        .unwrap()
                        .timestamp_nanos()
            })
        })
        .collect::<Vec<_>>();

    let ts_micros = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000))
        .collect::<Vec<_>>();

    let ts_millis = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000))
        .collect::<Vec<_>>();

    let ts_seconds = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000000))
        .collect::<Vec<_>>();

    let names = ts_nanos
        .iter()
        .enumerate()
        .map(|(i, _)| format!("Row {} + {}", i, offset))
        .collect::<Vec<_>>();

    let arr_nanos = PrimitiveArray::<i64>::from(ts_nanos)
        .to(DataType::Timestamp(TimeUnit::Nanosecond, None));
    let arr_micros = PrimitiveArray::<i64>::from(ts_micros)
        .to(DataType::Timestamp(TimeUnit::Microsecond, None));
    let arr_millis = PrimitiveArray::<i64>::from(ts_millis)
        .to(DataType::Timestamp(TimeUnit::Millisecond, None));
    let arr_seconds = PrimitiveArray::<i64>::from(ts_seconds)
        .to(DataType::Timestamp(TimeUnit::Second, None));

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = Utf8Array::<i32>::from_slice(names);

    let schema = Schema::new(vec![
        Field::new("nanos", arr_nanos.data_type().clone(), true),
        Field::new("micros", arr_micros.data_type().clone(), true),
        Field::new("millis", arr_millis.data_type().clone(), true),
        Field::new("seconds", arr_seconds.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_nanos),
            Arc::new(arr_micros),
            Arc::new(arr_millis),
            Arc::new(arr_seconds),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}

/// Return record batch with i32 sequence
///
/// Columns are named
/// "i" -> Int32Array
fn make_int32_batch(start: i32, end: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let v: Vec<i32> = (start..end).collect();
    let array = Arc::new(Int32Array::from_values(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with f64 vector
///
/// Columns are named
/// "f" -> Float64Array
fn make_f64_batch(v: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let array = Arc::new(Float64Array::from_values(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with a few rows of data for all of the supported date
/// types with the specified offset (in days)
///
/// Columns are named:
/// "date32" --> Date32Array
/// "date64" --> Date64Array
/// "names" --> StringArray
fn make_date_batch(offset: Duration) -> RecordBatch {
    let date_strings = vec![
        Some("2020-01-01"),
        Some("2020-01-02"),
        Some("2020-01-03"),
        None,
        Some("2020-01-04"),
    ];

    let names = date_strings
        .iter()
        .enumerate()
        .map(|(i, val)| format!("Row {} + {}: {:?}", i, offset, val))
        .collect::<Vec<_>>();

    // Copied from `cast.rs` cast kernel due to lack of temporal kernels
    // https://github.com/apache/arrow-rs/issues/527
    const EPOCH_DAYS_FROM_CE: i32 = 719_163;

    let date_seconds = date_strings
        .iter()
        .map(|t| {
            t.map(|t| {
                let t = t.parse::<chrono::NaiveDate>().unwrap();
                let t = t + offset;
                t.num_days_from_ce() - EPOCH_DAYS_FROM_CE
            })
        })
        .collect::<Vec<_>>();

    let date_millis = date_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                let t = t
                    .parse::<chrono::NaiveDate>()
                    .unwrap()
                    .and_time(chrono::NaiveTime::from_hms(0, 0, 0));
                let t = t + offset;
                t.timestamp_millis()
            })
        })
        .collect::<Vec<_>>();

    let arr_date32 = Int32Array::from(date_seconds).to(DataType::Date32);
    let arr_date64 = Int64Array::from(date_millis).to(DataType::Date64);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = Utf8Array::<i32>::from_slice(names);

    let schema = Schema::new(vec![
        Field::new("date32", arr_date32.data_type().clone(), true),
        Field::new("date64", arr_date64.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_date32),
            Arc::new(arr_date64),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}
