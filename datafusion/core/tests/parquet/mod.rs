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

//! Parquet integration tests
use crate::parquet::utils::MetricsFinder;
use arrow::array::Decimal128Array;
use arrow::datatypes::i256;
use arrow::{
    array::{
        make_array, Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array,
        Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Float16Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeStringArray, StringArray, StructArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field, Int32Type, Int8Type, Schema, TimeUnit},
    record_batch::RecordBatch,
    util::pretty::pretty_format_batches,
};
use chrono::{Datelike, Duration, TimeDelta};
use datafusion::{
    datasource::{provider_as_source, TableProvider},
    physical_plan::metrics::MetricsSet,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use half::f16;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::sync::Arc;
use tempfile::NamedTempFile;

mod arrow_statistics;
mod custom_reader;
// Don't run on windows as tempfiles don't seem to work the same
#[cfg(not(target_os = "windows"))]
mod external_access_plan;
mod file_statistics;
#[cfg(not(target_family = "windows"))]
mod filter_pushdown;
mod page_pruning;
mod row_group_pruning;
mod schema;
mod schema_coercion;
mod utils;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}

// ----------------------
// Begin test fixture
// ----------------------

/// What data to use
#[derive(Debug, Clone, Copy)]
enum Scenario {
    Boolean,
    Timestamps,
    Dates,
    Int,
    Int32Range,
    UInt,
    UInt32Range,
    Time32Second,
    Time32Millisecond,
    Time64Nanosecond,
    Time64Microsecond,
    /// 7 Rows, for each i8, i16, i32, i64, u8, u16, u32, u64, f32, f64
    /// -MIN, -100, -1, 0, 1, 100, MAX
    NumericLimits,
    Float16,
    Float32,
    Float64,
    Decimal,
    Decimal256,
    DecimalBloomFilterInt32,
    DecimalBloomFilterInt64,
    DecimalLargePrecision,
    DecimalLargePrecisionBloomFilter,
    /// StringArray, BinaryArray, FixedSizeBinaryArray
    ByteArray,
    /// DictionaryArray
    Dictionary,
    PeriodsInColumnNames,
    WithNullValues,
    WithNullValuesPageLevel,
    StructArray,
    UTF8,
}

enum Unit {
    // pass max row per row_group in parquet writer
    RowGroup(usize),
    // pass max row per page in parquet writer
    Page(usize),
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
    ctx: SessionContext,
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

    /// The number of row_groups matched by bloom filter
    fn row_groups_matched_bloom_filter(&self) -> Option<usize> {
        self.metric_value("row_groups_matched_bloom_filter")
    }

    /// The number of row_groups pruned by bloom filter
    fn row_groups_pruned_bloom_filter(&self) -> Option<usize> {
        self.metric_value("row_groups_pruned_bloom_filter")
    }

    /// The number of row_groups matched by statistics
    fn row_groups_matched_statistics(&self) -> Option<usize> {
        self.metric_value("row_groups_matched_statistics")
    }

    /// The number of row_groups pruned by statistics
    fn row_groups_pruned_statistics(&self) -> Option<usize> {
        self.metric_value("row_groups_pruned_statistics")
    }

    /// The number of row_groups matched by bloom filter or statistics
    fn row_groups_matched(&self) -> Option<usize> {
        self.row_groups_matched_bloom_filter()
            .zip(self.row_groups_matched_statistics())
            .map(|(a, b)| a + b)
    }

    /// The number of row_groups pruned
    fn row_groups_pruned(&self) -> Option<usize> {
        self.row_groups_pruned_bloom_filter()
            .zip(self.row_groups_pruned_statistics())
            .map(|(a, b)| a + b)
    }

    /// The number of row pages pruned
    fn row_pages_pruned(&self) -> Option<usize> {
        self.metric_value("page_index_rows_filtered")
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
    async fn new(scenario: Scenario, unit: Unit) -> Self {
        Self::with_config(scenario, unit, SessionConfig::new()).await
    }

    async fn with_config(
        scenario: Scenario,
        unit: Unit,
        mut config: SessionConfig,
    ) -> Self {
        let file = match unit {
            Unit::RowGroup(row_per_group) => {
                config = config.with_parquet_bloom_filter_pruning(true);
                make_test_file_rg(scenario, row_per_group).await
            }
            Unit::Page(row_per_page) => {
                config = config.with_parquet_page_index_pruning(true);
                make_test_file_page(scenario, row_per_page).await
            }
        };
        let parquet_path = file.path().to_string_lossy();

        // now, setup a the file as a data source and run a query against it
        let ctx = SessionContext::new_with_config(config);

        ctx.register_parquet("t", &parquet_path, ParquetReadOptions::default())
            .await
            .unwrap();
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
        let sql = format!("EXPR only: {expr:?}");
        let logical_plan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(self.provider.clone()),
            None,
        )
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
        println!("Planning sql {sql}");
        let logical_plan = self
            .ctx
            .sql(sql)
            .await
            .expect("planning")
            .into_unoptimized_plan();
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
        let pretty_input = pretty_format_batches(&input).unwrap().to_string();

        let state = self.ctx.state();
        let logical_plan = state.optimize(&logical_plan).expect("optimizing plan");

        let physical_plan = state
            .create_physical_plan(&logical_plan)
            .await
            .expect("creating physical plan");

        let task_ctx = state.task_ctx();
        let results = datafusion::physical_plan::collect(physical_plan.clone(), task_ctx)
            .await
            .expect("Running");

        // find the parquet metrics
        let parquet_metrics =
            MetricsFinder::find_metrics(physical_plan.as_ref()).unwrap();

        let result_rows = results.iter().map(|b| b.num_rows()).sum();

        let pretty_results = pretty_format_batches(&results).unwrap().to_string();

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

fn make_boolean_batch(v: Vec<Option<bool>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "bool",
        DataType::Boolean,
        true,
    )]));
    let array = Arc::new(BooleanArray::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with a few rows of data for all of the supported timestamp types
/// values with the specified offset
///
/// Columns are named:
/// "nanos" --> TimestampNanosecondArray
/// "nanos_timezoned" --> TimestampNanosecondArray with timezone
/// "micros" --> TimestampMicrosecondArray
/// "micros_timezoned" --> TimestampMicrosecondArray with timezone
/// "millis" --> TimestampMillisecondArray
/// "millis_timezoned" --> TimestampMillisecondArray with timezone
/// "seconds" --> TimestampSecondArray
/// "seconds_timezoned" --> TimestampSecondArray with timezone
/// "names" --> StringArray
fn make_timestamp_batch(offset: Duration) -> RecordBatch {
    let ts_strings = vec![
        Some("2020-01-01T01:01:01.0000000000001"),
        Some("2020-01-01T01:02:01.0000000000001"),
        Some("2020-01-01T02:01:01.0000000000001"),
        None,
        Some("2020-01-02T01:01:01.0000000000001"),
    ];

    let tz_string = "Pacific/Efate";

    let offset_nanos = offset.num_nanoseconds().expect("non overflow nanos");

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                offset_nanos
                    + t.parse::<chrono::NaiveDateTime>()
                        .unwrap()
                        .and_utc()
                        .timestamp_nanos_opt()
                        .unwrap()
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
        .map(|(i, _)| format!("Row {i} + {offset}"))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from(ts_nanos.clone());
    let arr_nanos_timezoned =
        TimestampNanosecondArray::from(ts_nanos).with_timezone(tz_string);
    let arr_micros = TimestampMicrosecondArray::from(ts_micros.clone());
    let arr_micros_timezoned =
        TimestampMicrosecondArray::from(ts_micros).with_timezone(tz_string);
    let arr_millis = TimestampMillisecondArray::from(ts_millis.clone());
    let arr_millis_timezoned =
        TimestampMillisecondArray::from(ts_millis).with_timezone(tz_string);
    let arr_seconds = TimestampSecondArray::from(ts_seconds.clone());
    let arr_seconds_timezoned =
        TimestampSecondArray::from(ts_seconds).with_timezone(tz_string);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("nanos", arr_nanos.data_type().clone(), true),
        Field::new(
            "nanos_timezoned",
            arr_nanos_timezoned.data_type().clone(),
            true,
        ),
        Field::new("micros", arr_micros.data_type().clone(), true),
        Field::new(
            "micros_timezoned",
            arr_micros_timezoned.data_type().clone(),
            true,
        ),
        Field::new("millis", arr_millis.data_type().clone(), true),
        Field::new(
            "millis_timezoned",
            arr_millis_timezoned.data_type().clone(),
            true,
        ),
        Field::new("seconds", arr_seconds.data_type().clone(), true),
        Field::new(
            "seconds_timezoned",
            arr_seconds_timezoned.data_type().clone(),
            true,
        ),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_nanos),
            Arc::new(arr_nanos_timezoned),
            Arc::new(arr_micros),
            Arc::new(arr_micros_timezoned),
            Arc::new(arr_millis),
            Arc::new(arr_millis_timezoned),
            Arc::new(arr_seconds),
            Arc::new(arr_seconds_timezoned),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}

/// Return record batch with i8, i16, i32, and i64 sequences
///
/// Columns are named
/// "i8" -> Int8Array
/// "i16" -> Int16Array
/// "i32" -> Int32Array
/// "i64" -> Int64Array
fn make_int_batches(start: i8, end: i8) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
    ]));
    let v8: Vec<i8> = (start..end).collect();
    let v16: Vec<i16> = (start as _..end as _).collect();
    let v32: Vec<i32> = (start as _..end as _).collect();
    let v64: Vec<i64> = (start as _..end as _).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int8Array::from(v8)) as ArrayRef,
            Arc::new(Int16Array::from(v16)) as ArrayRef,
            Arc::new(Int32Array::from(v32)) as ArrayRef,
            Arc::new(Int64Array::from(v64)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Return record batch with Time32Second, Time32Millisecond sequences
fn make_time32_batches(scenario: Scenario, v: Vec<i32>) -> RecordBatch {
    match scenario {
        Scenario::Time32Second => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "second",
                DataType::Time32(TimeUnit::Second),
                true,
            )]));
            let array = Arc::new(Time32SecondArray::from(v)) as ArrayRef;
            RecordBatch::try_new(schema, vec![array]).unwrap()
        }
        Scenario::Time32Millisecond => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "millisecond",
                DataType::Time32(TimeUnit::Millisecond),
                true,
            )]));
            let array = Arc::new(Time32MillisecondArray::from(v)) as ArrayRef;
            RecordBatch::try_new(schema, vec![array]).unwrap()
        }
        _ => panic!("Unsupported scenario for Time32"),
    }
}

/// Return record batch with Time64Microsecond, Time64Nanosecond sequences
fn make_time64_batches(scenario: Scenario, v: Vec<i64>) -> RecordBatch {
    match scenario {
        Scenario::Time64Microsecond => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "microsecond",
                DataType::Time64(TimeUnit::Microsecond),
                true,
            )]));
            let array = Arc::new(Time64MicrosecondArray::from(v)) as ArrayRef;
            RecordBatch::try_new(schema, vec![array]).unwrap()
        }
        Scenario::Time64Nanosecond => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "nanosecond",
                DataType::Time64(TimeUnit::Nanosecond),
                true,
            )]));
            let array = Arc::new(Time64NanosecondArray::from(v)) as ArrayRef;
            RecordBatch::try_new(schema, vec![array]).unwrap()
        }
        _ => panic!("Unsupported scenario for Time64"),
    }
}
/// Return record batch with u8, u16, u32, and u64 sequences
///
/// Columns are named
/// "u8" -> UInt8Array
/// "u16" -> UInt16Array
/// "u32" -> UInt32Array
/// "u64" -> UInt64Array
fn make_uint_batches(start: u8, end: u8) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
    ]));
    let v8: Vec<u8> = (start..end).collect();
    let v16: Vec<u16> = (start as _..end as _).collect();
    let v32: Vec<u32> = (start as _..end as _).collect();
    let v64: Vec<u64> = (start as _..end as _).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt8Array::from(v8)) as ArrayRef,
            Arc::new(UInt16Array::from(v16)) as ArrayRef,
            Arc::new(UInt32Array::from(v32)) as ArrayRef,
            Arc::new(UInt64Array::from(v64)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn make_int32_range(start: i32, end: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let v = vec![start, end];
    let array = Arc::new(Int32Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

fn make_uint32_range(start: u32, end: u32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt32, true)]));
    let v = vec![start, end];
    let array = Arc::new(UInt32Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with f64 vector
///
/// Columns are named
/// "f" -> Float64Array
fn make_f64_batch(v: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let array = Arc::new(Float64Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

fn make_f32_batch(v: Vec<f32>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float32, true)]));
    let array = Arc::new(Float32Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

fn make_f16_batch(v: Vec<f16>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float16, true)]));
    let array = Arc::new(Float16Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with decimal vector
///
/// Columns are named
/// "decimal_col" -> DecimalArray
fn make_decimal_batch(v: Vec<i128>, precision: u8, scale: i8) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_col",
        DataType::Decimal128(precision, scale),
        true,
    )]));
    let array = Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    ) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with decimal256 vector
///
/// Columns are named
/// "decimal256_col" -> Decimal256Array
fn make_decimal256_batch(v: Vec<i256>, precision: u8, scale: i8) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal256_col",
        DataType::Decimal256(precision, scale),
        true,
    )]));
    let array = Arc::new(
        Decimal256Array::from(v)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    ) as ArrayRef;
    RecordBatch::try_new(schema, vec![array]).unwrap()
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
        .map(|(i, val)| format!("Row {i} + {offset}: {val:?}"))
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
                    .and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                let t = t + offset;
                t.and_utc().timestamp_millis()
            })
        })
        .collect::<Vec<_>>();

    let arr_date32 = Date32Array::from(date_seconds);
    let arr_date64 = Date64Array::from(date_millis);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

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

/// returns a batch with two columns (note "service.name" is the name
/// of the column. It is *not* a table named service.name
///
/// name | service.name
fn make_bytearray_batch(
    name: &str,
    string_values: Vec<&str>,
    binary_values: Vec<&[u8]>,
    fixedsize_values: Vec<&[u8; 3]>,
    // i64 offset.
    large_binary_values: Vec<&[u8]>,
) -> RecordBatch {
    let num_rows = string_values.len();
    let name: StringArray = std::iter::repeat(Some(name)).take(num_rows).collect();
    let service_string: StringArray = string_values.iter().map(Some).collect();
    let service_binary: BinaryArray = binary_values.iter().map(Some).collect();
    let service_fixedsize: FixedSizeBinaryArray = fixedsize_values
        .iter()
        .map(|value| Some(value.as_slice()))
        .collect::<Vec<_>>()
        .into();
    let service_large_binary: LargeBinaryArray =
        large_binary_values.iter().map(Some).collect();

    let schema = Schema::new(vec![
        Field::new("name", name.data_type().clone(), true),
        // note the column name has a period in it!
        Field::new("service_string", service_string.data_type().clone(), true),
        Field::new("service_binary", service_binary.data_type().clone(), true),
        Field::new(
            "service_fixedsize",
            service_fixedsize.data_type().clone(),
            true,
        ),
        Field::new(
            "service_large_binary",
            service_large_binary.data_type().clone(),
            true,
        ),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name),
            Arc::new(service_string),
            Arc::new(service_binary),
            Arc::new(service_fixedsize),
            Arc::new(service_large_binary),
        ],
    )
    .unwrap()
}

/// returns a batch with two columns (note "service.name" is the name
/// of the column. It is *not* a table named service.name
///
/// name | service.name
fn make_names_batch(name: &str, service_name_values: Vec<&str>) -> RecordBatch {
    let num_rows = service_name_values.len();
    let name: StringArray = std::iter::repeat(Some(name)).take(num_rows).collect();
    let service_name: StringArray = service_name_values.iter().map(Some).collect();

    let schema = Schema::new(vec![
        Field::new("name", name.data_type().clone(), true),
        // note the column name has a period in it!
        Field::new("service.name", service_name.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(schema, vec![Arc::new(name), Arc::new(service_name)]).unwrap()
}

/// Return record batch with i8, i16, i32, and i64 sequences with Null values
/// here 5 rows in page when using Unit::Page
fn make_int_batches_with_null(
    null_values: usize,
    no_null_values_start: usize,
    no_null_values_end: usize,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
    ]));

    let v8: Vec<i8> = (no_null_values_start as _..no_null_values_end as _).collect();
    let v16: Vec<i16> = (no_null_values_start as _..no_null_values_end as _).collect();
    let v32: Vec<i32> = (no_null_values_start as _..no_null_values_end as _).collect();
    let v64: Vec<i64> = (no_null_values_start as _..no_null_values_end as _).collect();

    RecordBatch::try_new(
        schema,
        vec![
            make_array(
                Int8Array::from_iter(
                    v8.into_iter()
                        .map(Some)
                        .chain(std::iter::repeat(None).take(null_values)),
                )
                .to_data(),
            ),
            make_array(
                Int16Array::from_iter(
                    v16.into_iter()
                        .map(Some)
                        .chain(std::iter::repeat(None).take(null_values)),
                )
                .to_data(),
            ),
            make_array(
                Int32Array::from_iter(
                    v32.into_iter()
                        .map(Some)
                        .chain(std::iter::repeat(None).take(null_values)),
                )
                .to_data(),
            ),
            make_array(
                Int64Array::from_iter(
                    v64.into_iter()
                        .map(Some)
                        .chain(std::iter::repeat(None).take(null_values)),
                )
                .to_data(),
            ),
        ],
    )
    .unwrap()
}

fn make_numeric_limit_batch() -> RecordBatch {
    let i8 = Int8Array::from(vec![i8::MIN, 100, -1, 0, 1, -100, i8::MAX]);
    let i16 = Int16Array::from(vec![i16::MIN, 100, -1, 0, 1, -100, i16::MAX]);
    let i32 = Int32Array::from(vec![i32::MIN, 100, -1, 0, 1, -100, i32::MAX]);
    let i64 = Int64Array::from(vec![i64::MIN, 100, -1, 0, 1, -100, i64::MAX]);
    let u8 = UInt8Array::from(vec![u8::MIN, 100, 1, 0, 1, 100, u8::MAX]);
    let u16 = UInt16Array::from(vec![u16::MIN, 100, 1, 0, 1, 100, u16::MAX]);
    let u32 = UInt32Array::from(vec![u32::MIN, 100, 1, 0, 1, 100, u32::MAX]);
    let u64 = UInt64Array::from(vec![u64::MIN, 100, 1, 0, 1, 100, u64::MAX]);
    let f32 = Float32Array::from(vec![f32::MIN, 100.0, -1.0, 0.0, 1.0, -100.0, f32::MAX]);
    let f64 = Float64Array::from(vec![f64::MIN, 100.0, -1.0, 0.0, 1.0, -100.0, f64::MAX]);
    let f32_nan =
        Float32Array::from(vec![f32::NAN, 100.0, -1.0, 0.0, 1.0, -100.0, f32::NAN]);
    let f64_nan =
        Float64Array::from(vec![f64::NAN, 100.0, -1.0, 0.0, 1.0, -100.0, f64::NAN]);

    RecordBatch::try_from_iter(vec![
        ("i8", Arc::new(i8) as _),
        ("i16", Arc::new(i16) as _),
        ("i32", Arc::new(i32) as _),
        ("i64", Arc::new(i64) as _),
        ("u8", Arc::new(u8) as _),
        ("u16", Arc::new(u16) as _),
        ("u32", Arc::new(u32) as _),
        ("u64", Arc::new(u64) as _),
        ("f32", Arc::new(f32) as _),
        ("f64", Arc::new(f64) as _),
        ("f32_nan", Arc::new(f32_nan) as _),
        ("f64_nan", Arc::new(f64_nan) as _),
    ])
    .unwrap()
}

fn make_utf8_batch(value: Vec<Option<&str>>) -> RecordBatch {
    let utf8 = StringArray::from(value.clone());
    let large_utf8 = LargeStringArray::from(value);
    RecordBatch::try_from_iter(vec![
        ("utf8", Arc::new(utf8) as _),
        ("large_utf8", Arc::new(large_utf8) as _),
    ])
    .unwrap()
}

fn make_dict_batch() -> RecordBatch {
    let values = [
        Some("abc"),
        Some("def"),
        None,
        Some("def"),
        Some("abc"),
        Some("fffff"),
        Some("aaa"),
    ];
    let dict_i8_array = DictionaryArray::<Int8Type>::from_iter(values.iter().cloned());
    let dict_i32_array = DictionaryArray::<Int32Type>::from_iter(values.iter().cloned());

    // Dictionary array of integers
    let int64_values = Int64Array::from(vec![0, -100, 100]);
    let keys = Int8Array::from_iter([
        Some(0),
        Some(1),
        None,
        Some(0),
        Some(0),
        Some(2),
        Some(0),
    ]);
    let dict_i8_int_array =
        DictionaryArray::<Int8Type>::try_new(keys, Arc::new(int64_values)).unwrap();

    RecordBatch::try_from_iter(vec![
        ("string_dict_i8", Arc::new(dict_i8_array) as _),
        ("string_dict_i32", Arc::new(dict_i32_array) as _),
        ("int_dict_i8", Arc::new(dict_i8_int_array) as _),
    ])
    .unwrap()
}

fn create_data_batch(scenario: Scenario) -> Vec<RecordBatch> {
    match scenario {
        Scenario::Boolean => {
            vec![
                make_boolean_batch(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(false),
                    None,
                ]),
                make_boolean_batch(vec![
                    Some(false),
                    Some(false),
                    Some(false),
                    Some(false),
                    Some(false),
                ]),
            ]
        }
        Scenario::Timestamps => {
            vec![
                make_timestamp_batch(TimeDelta::try_seconds(0).unwrap()),
                make_timestamp_batch(TimeDelta::try_seconds(10).unwrap()),
                make_timestamp_batch(TimeDelta::try_minutes(10).unwrap()),
                make_timestamp_batch(TimeDelta::try_days(10).unwrap()),
            ]
        }
        Scenario::Dates => {
            vec![
                make_date_batch(TimeDelta::try_days(0).unwrap()),
                make_date_batch(TimeDelta::try_days(10).unwrap()),
                make_date_batch(TimeDelta::try_days(300).unwrap()),
                make_date_batch(TimeDelta::try_days(3600).unwrap()),
            ]
        }
        Scenario::Int => {
            vec![
                make_int_batches(-5, 0),
                make_int_batches(-4, 1),
                make_int_batches(0, 5),
                make_int_batches(5, 10),
            ]
        }
        Scenario::Int32Range => {
            vec![make_int32_range(0, 10), make_int32_range(200000, 300000)]
        }
        Scenario::UInt => {
            vec![
                make_uint_batches(0, 5),
                make_uint_batches(1, 6),
                make_uint_batches(5, 10),
                make_uint_batches(250, 255),
            ]
        }
        Scenario::UInt32Range => {
            vec![make_uint32_range(0, 10), make_uint32_range(200000, 300000)]
        }
        Scenario::NumericLimits => {
            vec![make_numeric_limit_batch()]
        }
        Scenario::Float16 => {
            vec![
                make_f16_batch(
                    vec![-5.0, -4.0, -3.0, -2.0, -1.0]
                        .into_iter()
                        .map(f16::from_f32)
                        .collect(),
                ),
                make_f16_batch(
                    vec![-4.0, -3.0, -2.0, -1.0, 0.0]
                        .into_iter()
                        .map(f16::from_f32)
                        .collect(),
                ),
                make_f16_batch(
                    vec![0.0, 1.0, 2.0, 3.0, 4.0]
                        .into_iter()
                        .map(f16::from_f32)
                        .collect(),
                ),
                make_f16_batch(
                    vec![5.0, 6.0, 7.0, 8.0, 9.0]
                        .into_iter()
                        .map(f16::from_f32)
                        .collect(),
                ),
            ]
        }
        Scenario::Float32 => {
            vec![
                make_f32_batch(vec![-5.0, -4.0, -3.0, -2.0, -1.0]),
                make_f32_batch(vec![-4.0, -3.0, -2.0, -1.0, 0.0]),
                make_f32_batch(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
                make_f32_batch(vec![5.0, 6.0, 7.0, 8.0, 9.0]),
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
        Scenario::Decimal => {
            // decimal record batch
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![-500, 100, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![2000, 3000, 3000, 4000, 6000], 9, 2),
            ]
        }
        Scenario::Decimal256 => {
            // decimal256 record batch
            vec![
                make_decimal256_batch(
                    vec![
                        i256::from(100),
                        i256::from(200),
                        i256::from(300),
                        i256::from(400),
                        i256::from(600),
                    ],
                    9,
                    2,
                ),
                make_decimal256_batch(
                    vec![
                        i256::from(-500),
                        i256::from(100),
                        i256::from(300),
                        i256::from(400),
                        i256::from(600),
                    ],
                    9,
                    2,
                ),
                make_decimal256_batch(
                    vec![
                        i256::from(2000),
                        i256::from(3000),
                        i256::from(3000),
                        i256::from(4000),
                        i256::from(6000),
                    ],
                    9,
                    2,
                ),
            ]
        }
        Scenario::DecimalBloomFilterInt32 => {
            // decimal record batch
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 500], 6, 2),
                make_decimal_batch(vec![100, 200, 300, 400, 600], 6, 2),
                make_decimal_batch(vec![100, 200, 300, 400, 600], 6, 2),
            ]
        }
        Scenario::DecimalBloomFilterInt64 => {
            // decimal record batch
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 500], 9, 2),
                make_decimal_batch(vec![100, 200, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![100, 200, 300, 400, 600], 9, 2),
            ]
        }
        Scenario::DecimalLargePrecision => {
            // decimal record batch with large precision,
            // and the data will stored as FIXED_LENGTH_BYTE_ARRAY
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 600], 38, 2),
                make_decimal_batch(vec![-500, 100, 300, 400, 600], 38, 2),
                make_decimal_batch(vec![2000, 3000, 3000, 4000, 6000], 38, 2),
            ]
        }
        Scenario::DecimalLargePrecisionBloomFilter => {
            // decimal record batch with large precision,
            // and the data will stored as FIXED_LENGTH_BYTE_ARRAY
            vec![
                make_decimal_batch(vec![100000, 200000, 300000, 400000, 500000], 38, 5),
                make_decimal_batch(vec![-100000, 200000, 300000, 400000, 600000], 38, 5),
                make_decimal_batch(vec![100000, 200000, 300000, 400000, 600000], 38, 5),
            ]
        }
        Scenario::ByteArray => {
            // frontends first, then backends. All in order, except frontends 4 and 7
            // are swapped to cause a statistics false positive on the 'fixed size' column.
            vec![
                make_bytearray_batch(
                    "all frontends",
                    vec![
                        "frontend one",
                        "frontend two",
                        "frontend three",
                        "frontend seven",
                        "frontend five",
                    ],
                    vec![
                        b"frontend one",
                        b"frontend two",
                        b"frontend three",
                        b"frontend seven",
                        b"frontend five",
                    ],
                    vec![b"fe1", b"fe2", b"fe3", b"fe7", b"fe5"],
                    vec![
                        b"frontend one",
                        b"frontend two",
                        b"frontend three",
                        b"frontend seven",
                        b"frontend five",
                    ],
                ),
                make_bytearray_batch(
                    "mixed",
                    vec![
                        "frontend six",
                        "frontend four",
                        "backend one",
                        "backend two",
                        "backend three",
                    ],
                    vec![
                        b"frontend six",
                        b"frontend four",
                        b"backend one",
                        b"backend two",
                        b"backend three",
                    ],
                    vec![b"fe6", b"fe4", b"be1", b"be2", b"be3"],
                    vec![
                        b"frontend six",
                        b"frontend four",
                        b"backend one",
                        b"backend two",
                        b"backend three",
                    ],
                ),
                make_bytearray_batch(
                    "all backends",
                    vec![
                        "backend four",
                        "backend five",
                        "backend six",
                        "backend seven",
                        "backend eight",
                    ],
                    vec![
                        b"backend four",
                        b"backend five",
                        b"backend six",
                        b"backend seven",
                        b"backend eight",
                    ],
                    vec![b"be4", b"be5", b"be6", b"be7", b"be8"],
                    vec![
                        b"backend four",
                        b"backend five",
                        b"backend six",
                        b"backend seven",
                        b"backend eight",
                    ],
                ),
            ]
        }
        Scenario::Dictionary => {
            vec![make_dict_batch()]
        }
        Scenario::PeriodsInColumnNames => {
            vec![
                // all frontend
                make_names_batch(
                    "HTTP GET / DISPATCH",
                    vec!["frontend", "frontend", "frontend", "frontend", "frontend"],
                ),
                // both frontend and backend
                make_names_batch(
                    "HTTP PUT / DISPATCH",
                    vec!["frontend", "frontend", "backend", "backend", "backend"],
                ),
                // all backend
                make_names_batch(
                    "HTTP GET / DISPATCH",
                    vec!["backend", "backend", "backend", "backend", "backend"],
                ),
            ]
        }
        Scenario::WithNullValues => {
            vec![
                make_int_batches_with_null(5, 0, 0),
                make_int_batches(1, 6),
                make_int_batches_with_null(5, 0, 0),
            ]
        }
        Scenario::WithNullValuesPageLevel => {
            vec![
                make_int_batches_with_null(5, 1, 6),
                make_int_batches(1, 11),
                make_int_batches_with_null(1, 1, 10),
                make_int_batches_with_null(5, 1, 6),
            ]
        }
        Scenario::StructArray => {
            let struct_array_data = struct_array(vec![
                (Some(1), Some(6.0), Some(12.0)),
                (Some(2), Some(8.5), None),
                (None, Some(8.5), Some(14.0)),
            ]);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "struct",
                struct_array_data.data_type().clone(),
                true,
            )]));
            vec![RecordBatch::try_new(schema, vec![struct_array_data]).unwrap()]
        }
        Scenario::Time32Second => {
            vec![
                make_time32_batches(
                    Scenario::Time32Second,
                    vec![18506, 18507, 18508, 18509],
                ),
                make_time32_batches(
                    Scenario::Time32Second,
                    vec![18510, 18511, 18512, 18513],
                ),
                make_time32_batches(
                    Scenario::Time32Second,
                    vec![18514, 18515, 18516, 18517],
                ),
                make_time32_batches(
                    Scenario::Time32Second,
                    vec![18518, 18519, 18520, 18521],
                ),
            ]
        }
        Scenario::Time32Millisecond => {
            vec![
                make_time32_batches(
                    Scenario::Time32Millisecond,
                    vec![3600000, 3600001, 3600002, 3600003],
                ),
                make_time32_batches(
                    Scenario::Time32Millisecond,
                    vec![3600004, 3600005, 3600006, 3600007],
                ),
                make_time32_batches(
                    Scenario::Time32Millisecond,
                    vec![3600008, 3600009, 3600010, 3600011],
                ),
                make_time32_batches(
                    Scenario::Time32Millisecond,
                    vec![3600012, 3600013, 3600014, 3600015],
                ),
            ]
        }
        Scenario::Time64Microsecond => {
            vec![
                make_time64_batches(
                    Scenario::Time64Microsecond,
                    vec![1234567890123, 1234567890124, 1234567890125, 1234567890126],
                ),
                make_time64_batches(
                    Scenario::Time64Microsecond,
                    vec![1234567890127, 1234567890128, 1234567890129, 1234567890130],
                ),
                make_time64_batches(
                    Scenario::Time64Microsecond,
                    vec![1234567890131, 1234567890132, 1234567890133, 1234567890134],
                ),
                make_time64_batches(
                    Scenario::Time64Microsecond,
                    vec![1234567890135, 1234567890136, 1234567890137, 1234567890138],
                ),
            ]
        }
        Scenario::Time64Nanosecond => {
            vec![
                make_time64_batches(
                    Scenario::Time64Nanosecond,
                    vec![
                        987654321012345,
                        987654321012346,
                        987654321012347,
                        987654321012348,
                    ],
                ),
                make_time64_batches(
                    Scenario::Time64Nanosecond,
                    vec![
                        987654321012349,
                        987654321012350,
                        987654321012351,
                        987654321012352,
                    ],
                ),
                make_time64_batches(
                    Scenario::Time64Nanosecond,
                    vec![
                        987654321012353,
                        987654321012354,
                        987654321012355,
                        987654321012356,
                    ],
                ),
                make_time64_batches(
                    Scenario::Time64Nanosecond,
                    vec![
                        987654321012357,
                        987654321012358,
                        987654321012359,
                        987654321012360,
                    ],
                ),
            ]
        }
        Scenario::UTF8 => {
            vec![
                make_utf8_batch(vec![Some("a"), Some("b"), Some("c"), Some("d"), None]),
                make_utf8_batch(vec![
                    Some("e"),
                    Some("f"),
                    Some("g"),
                    Some("h"),
                    Some("i"),
                ]),
            ]
        }
    }
}

/// Create a test parquet file with various data types
async fn make_test_file_rg(scenario: Scenario, row_per_group: usize) -> NamedTempFile {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquet_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(row_per_group)
        .set_bloom_filter_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();

    let batches = create_data_batch(scenario);
    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema, Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();

    output_file
}

async fn make_test_file_page(scenario: Scenario, row_per_page: usize) -> NamedTempFile {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquet_page_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    // set row count to row_per_page, should get same result as rowGroup
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(row_per_page)
        .set_write_batch_size(row_per_page)
        .build();

    let batches = create_data_batch(scenario);

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema, Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();
    output_file
}

// returns a struct array with columns "int32_col", "float32_col" and "float64_col" with the specified values
fn struct_array(input: Vec<(Option<i32>, Option<f32>, Option<f64>)>) -> ArrayRef {
    let int_32: Int32Array = input.iter().map(|(i, _, _)| i).collect();
    let float_32: Float32Array = input.iter().map(|(_, f, _)| f).collect();
    let float_64: Float64Array = input.iter().map(|(_, _, f)| f).collect();

    let nullable = true;
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("int32_col", DataType::Int32, nullable)),
            Arc::new(int_32) as ArrayRef,
        ),
        (
            Arc::new(Field::new("float32_col", DataType::Float32, nullable)),
            Arc::new(float_32) as ArrayRef,
        ),
        (
            Arc::new(Field::new("float64_col", DataType::Float64, nullable)),
            Arc::new(float_64) as ArrayRef,
        ),
    ]);
    Arc::new(struct_array)
}
