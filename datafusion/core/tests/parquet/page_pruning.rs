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

use crate::parquet::Unit::Page;
use crate::parquet::{ContextWithParquet, Scenario};

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, Statistics, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{col, lit, Expr};
use datafusion_physical_expr::create_physical_expr;

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectMeta;

async fn get_parquet_exec(state: &SessionState, filter: Expr) -> ParquetExec {
    let object_store_url = ObjectStoreUrl::local_filesystem();
    let store = state.runtime_env().object_store(&object_store_url).unwrap();

    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{testdata}/alltypes_tiny_pages.parquet");

    let location = Path::from_filesystem_path(filename.as_str()).unwrap();
    let metadata = std::fs::metadata(filename).expect("Local file metadata");
    let meta = ObjectMeta {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len() as usize,
        e_tag: None,
        version: None,
    };

    let schema = ParquetFormat::default()
        .infer_schema(state, &store, &[meta.clone()])
        .await
        .unwrap();

    let partitioned_file = PartitionedFile {
        object_meta: meta,
        partition_values: vec![],
        range: None,
        statistics: None,
        extensions: None,
    };

    let df_schema = schema.clone().to_dfschema().unwrap();
    let execution_props = ExecutionProps::new();
    let predicate = create_physical_expr(&filter, &df_schema, &execution_props).unwrap();

    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            object_store_url,
            file_groups: vec![vec![partitioned_file]],
            file_schema: schema.clone(),
            statistics: Statistics::new_unknown(&schema),
            // file has 10 cols so index 12 should be month
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        },
        Some(predicate),
        None,
        Default::default(),
    );
    parquet_exec.with_enable_page_index(true)
}

#[tokio::test]
async fn page_index_filter_one_col() {
    let session_ctx = SessionContext::new();
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();

    // 1.create filter month == 1;
    let filter = col("month").eq(lit(1_i32));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    // `month = 1` from the page index should create below RowSelection
    //  vec.push(RowSelector::select(312));
    //  vec.push(RowSelector::skip(3330));
    //  vec.push(RowSelector::select(339));
    //  vec.push(RowSelector::skip(3319));
    // total 651 row
    assert_eq!(batch.num_rows(), 651);

    // 2. create filter month == 1 or month == 2;
    let filter = col("month").eq(lit(1_i32)).or(col("month").eq(lit(2_i32)));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    // `month = 1` or `month = 2` from the page index should create below RowSelection
    //  vec.push(RowSelector::select(312));
    //  vec.push(RowSelector::skip(900));
    //  vec.push(RowSelector::select(312));
    //  vec.push(RowSelector::skip(2118));
    //  vec.push(RowSelector::select(339));
    //  vec.push(RowSelector::skip(873));
    //  vec.push(RowSelector::select(318));
    //  vec.push(RowSelector::skip(2128));
    assert_eq!(batch.num_rows(), 1281);

    // 3. create filter month == 1 and month == 12;
    let filter = col("month")
        .eq(lit(1_i32))
        .and(col("month").eq(lit(12_i32)));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await;

    assert!(batch.is_none());

    // 4.create filter 0 < month < 2 ;
    let filter = col("month").gt(lit(0_i32)).and(col("month").lt(lit(2_i32)));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    // should same with `month = 1`
    assert_eq!(batch.num_rows(), 651);

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();

    // 5.create filter date_string_col == 1;
    let filter = col("date_string_col").eq(lit("01/01/09"));
    let parquet_exec = get_parquet_exec(&state, filter).await;
    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();
    let batch = results.next().await.unwrap().unwrap();

    // there should only two pages match the filter
    //                                  min                                        max
    // page-20                        0  01/01/09                                  01/02/09
    // page-21                        0  01/01/09                                  01/01/09
    // each 7 rows
    assert_eq!(batch.num_rows(), 14);
}

#[tokio::test]
async fn page_index_filter_multi_col() {
    let session_ctx = SessionContext::new();
    let state = session_ctx.state();
    let task_ctx = session_ctx.task_ctx();

    // create filter month == 1 and year = 2009;
    let filter = col("month").eq(lit(1_i32)).and(col("year").eq(lit(2009)));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();

    //  `year = 2009` from the page index should create below RowSelection
    //  vec.push(RowSelector::select(3663));
    //  vec.push(RowSelector::skip(3642));
    //  combine with `month = 1` total 333 row
    assert_eq!(batch.num_rows(), 333);

    // create filter (year = 2009 or id = 1) and month = 1;
    // this should only use `month = 1` to evaluate the page index.
    let filter = col("month")
        .eq(lit(1_i32))
        .and(col("year").eq(lit(2009)).or(col("id").eq(lit(1))));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 651);

    // create filter (year = 2009 or id = 1)
    // this filter use two columns will not push down
    let filter = col("year").eq(lit(2009)).or(col("id").eq(lit(1)));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 7300);

    // create filter (year = 2009 and id = 1) or (year = 2010)
    // this filter use two columns will not push down
    // todo but after use CNF rewrite it could rewrite to (year = 2009 or  year = 2010) and (id = 1 or year = 2010)
    // which could push (year = 2009 or year = 2010) down.
    let filter = col("year")
        .eq(lit(2009))
        .and(col("id").eq(lit(1)))
        .or(col("year").eq(lit(2010)));

    let parquet_exec = get_parquet_exec(&state, filter).await;

    let mut results = parquet_exec.execute(0, task_ctx.clone()).unwrap();

    let batch = results.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 7300);
}

async fn test_prune(
    case_data_type: Scenario,
    sql: &str,
    expected_errors: Option<usize>,
    expected_row_pages_pruned: Option<usize>,
    expected_results: usize,
) {
    let output: crate::parquet::TestOutput =
        ContextWithParquet::new(case_data_type, Page)
            .await
            .query(sql)
            .await;

    println!("{}", output.description());
    assert_eq!(output.predicate_evaluation_errors(), expected_errors);
    assert_eq!(output.row_pages_pruned(), expected_row_pages_pruned);
    assert_eq!(
        output.result_rows,
        expected_results,
        "{}",
        output.description()
    );
}

#[tokio::test]
//                       null count  min                                       max
// page-0                         1  2020-01-01T01:01:01.000000000             2020-01-02T01:01:01.000000000
// page-1                         1  2020-01-01T01:01:11.000000000             2020-01-02T01:01:11.000000000
// page-2                         1  2020-01-01T01:11:01.000000000             2020-01-02T01:11:01.000000000
// page-3                         1  2020-01-11T01:01:01.000000000             2020-01-12T01:01:01.000000000
async fn prune_timestamps_nanos() {
    test_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where nanos < to_timestamp('2020-01-02 01:01:11Z')",
        Some(0),
        Some(5),
        10,
    )
    .await;
}

#[tokio::test]
//                         null count  min                                       max
// page-0                         1  2020-01-01T01:01:01.000000                2020-01-02T01:01:01.000000
// page-1                         1  2020-01-01T01:01:11.000000                2020-01-02T01:01:11.000000
// page-2                         1  2020-01-01T01:11:01.000000                2020-01-02T01:11:01.000000
// page-3                         1  2020-01-11T01:01:01.000000                2020-01-12T01:01:01.000000
async fn prune_timestamps_micros() {
    test_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where micros < to_timestamp_micros('2020-01-02 01:01:11Z')",
        Some(0),
        Some(5),
        10,
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         1  2020-01-01T01:01:01.000                   2020-01-02T01:01:01.000
// page-1                         1  2020-01-01T01:01:11.000                   2020-01-02T01:01:11.000
// page-2                         1  2020-01-01T01:11:01.000                   2020-01-02T01:11:01.000
// page-3                         1  2020-01-11T01:01:01.000                   2020-01-12T01:01:01.000
async fn prune_timestamps_millis() {
    test_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where millis < to_timestamp_millis('2020-01-02 01:01:11Z')",
        Some(0),
        Some(5),
        10,
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         1  1577840461                                1577926861
// page-1                         1  1577840471                                1577926871
// page-2                         1  1577841061                                1577927461
// page-3                         1  1578704461                                1578790861

async fn prune_timestamps_seconds() {
    test_prune(
        Scenario::Timestamps,
        "SELECT * FROM t where seconds < to_timestamp_seconds('2020-01-02 01:01:11Z')",
        Some(0),
        Some(5),
        10,
    )
    .await;
}

#[tokio::test]
//                       null count  min                                       max
// page-0                         1  2020-01-01                                2020-01-04
// page-1                         1  2020-01-11                                2020-01-14
// page-2                         1  2020-10-27                                2020-10-30
// page-3                         1  2029-11-09                                2029-11-12
async fn prune_date32() {
    test_prune(
        Scenario::Dates,
        "SELECT * FROM t where date32 < cast('2020-01-02' as date)",
        Some(0),
        Some(15),
        1,
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         1  2020-01-01                                2020-01-04
// page-1                         1  2020-01-11                                2020-01-14
// page-2                         1  2020-10-27                                2020-10-30
// page-3                         1  2029-11-09                                2029-11-12
async fn prune_date64() {
    // work around for not being able to cast Date32 to Date64 automatically
    let date = "2020-01-02"
        .parse::<chrono::NaiveDate>()
        .unwrap()
        .and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let date = ScalarValue::Date64(Some(date.and_utc().timestamp_millis()));

    let output = ContextWithParquet::new(Scenario::Dates, Page)
        .await
        .query_with_expr(col("date64").lt(lit(date)))
        .await;

    println!("{}", output.description());
    // This should prune out groups  without error
    assert_eq!(output.predicate_evaluation_errors(), Some(0));
    assert_eq!(output.row_pages_pruned(), Some(15));
    assert_eq!(output.result_rows, 1, "{}", output.description());
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         0  -5                                        -1
// page-1                         0  -4                                        0
// page-2                         0  0                                         4
// page-3                         0  5                                         9
async fn prune_int32_lt() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where i < 1",
        Some(0),
        Some(5),
        11,
    )
    .await;
    // result of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where -i > -1",
        Some(0),
        Some(5),
        11,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_gt() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where i > 8",
        Some(0),
        Some(15),
        1,
    )
    .await;

    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where -i < -8",
        Some(0),
        Some(15),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where i = 1",
        Some(0),
        Some(15),
        1,
    )
    .await;
}
#[tokio::test]
async fn prune_int32_scalar_fun_and_eq() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where abs(i) = 1  and i = 1",
        Some(0),
        Some(15),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_scalar_fun() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where abs(i) = 1",
        Some(0),
        Some(0),
        3,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_complex_expr() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where i+1 = 1",
        Some(0),
        Some(0),
        2,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_complex_expr_subtract() {
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where 1-i > 1",
        Some(0),
        Some(0),
        9,
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         0  -5.0                                      -1.0
// page-1                         0  -4.0                                      0.0
// page-2                         0  0.0                                       4.0
// page-3                         0  5.0                                       9.0
async fn prune_f64_lt() {
    test_prune(
        Scenario::Float64,
        "SELECT * FROM t where f < 1",
        Some(0),
        Some(5),
        11,
    )
    .await;
    test_prune(
        Scenario::Float64,
        "SELECT * FROM t where -f > -1",
        Some(0),
        Some(5),
        11,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_scalar_fun_and_gt() {
    // result of sql "SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1"
    // only use "f >= 0" to prune
    test_prune(
        Scenario::Float64,
        "SELECT * FROM t where abs(f - 1) <= 0.000001  and f >= 0.1",
        Some(0),
        Some(10),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_scalar_fun() {
    // result of sql "SELECT * FROM t where abs(f-1) <= 0.000001" is not supported
    test_prune(
        Scenario::Float64,
        "SELECT * FROM t where abs(f-1) <= 0.000001",
        Some(0),
        Some(0),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_complex_expr() {
    // result of sql "SELECT * FROM t where f+1 > 1.1"" is not supported
    test_prune(
        Scenario::Float64,
        "SELECT * FROM t where f+1 > 1.1",
        Some(0),
        Some(0),
        9,
    )
    .await;
}

#[tokio::test]
async fn prune_f64_complex_expr_subtract() {
    // result of sql "SELECT * FROM t where 1-f > 1" is not supported
    test_prune(
        Scenario::Float64,
        "SELECT * FROM t where 1-f > 1",
        Some(0),
        Some(0),
        9,
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         0  -5                                        -1
// page-1                         0  -4                                        0
// page-2                         0  0                                         4
// page-3                         0  5                                         9
async fn prune_int32_eq_in_list() {
    // result of sql "SELECT * FROM t where in (1)"
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where i in (1)",
        Some(0),
        Some(15),
        1,
    )
    .await;
}

#[tokio::test]
async fn prune_int32_eq_in_list_negated() {
    // result of sql "SELECT * FROM t where not in (1)" prune nothing
    test_prune(
        Scenario::Int32,
        "SELECT * FROM t where i not in (1)",
        Some(0),
        Some(0),
        19,
    )
    .await;
}

#[tokio::test]
async fn prune_decimal_lt() {
    // The data type of decimal_col is decimal(9,2)
    // There are three pages each 5 rows:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col < 4",
        Some(0),
        Some(5),
        6,
    )
    .await;
    // compare with the casted decimal value
    test_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))",
        Some(0),
        Some(5),
        8,
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col < 4",
        Some(0),
        Some(5),
        6,
    )
    .await;
    // compare with the casted decimal value
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col < cast(4.55 as decimal(20,2))",
        Some(0),
        Some(5),
        8,
    )
    .await;
}

#[tokio::test]
async fn prune_decimal_eq() {
    // The data type of decimal_col is decimal(9,2)
    // There are three pages:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col = 4",
        Some(0),
        Some(5),
        2,
    )
    .await;
    test_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col = 4.00",
        Some(0),
        Some(5),
        2,
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col = 4",
        Some(0),
        Some(5),
        2,
    )
    .await;
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col = 4.00",
        Some(0),
        Some(5),
        2,
    )
    .await;
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col = 30.00",
        Some(0),
        Some(10),
        2,
    )
    .await;
}

#[tokio::test]
async fn prune_decimal_in_list() {
    // The data type of decimal_col is decimal(9,2)
    // There are three pages:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col in (4,3,2,123456789123)",
        Some(0),
        Some(5),
        5,
    )
    .await;
    test_prune(
        Scenario::Decimal,
        "SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)",
        Some(0),
        Some(5),
        6,
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col in (4,3,2,123456789123)",
        Some(0),
        Some(5),
        5,
    )
    .await;
    test_prune(
        Scenario::DecimalLargePrecision,
        "SELECT * FROM t where decimal_col in (4.00,3.00,11.2345,1)",
        Some(0),
        Some(5),
        6,
    )
    .await;
}

#[tokio::test]
async fn without_pushdown_filter() {
    let mut context = ContextWithParquet::new(Scenario::Timestamps, Page).await;

    let output1 = context.query("SELECT * FROM t").await;

    let mut context = ContextWithParquet::new(Scenario::Timestamps, Page).await;

    let output2 = context
        .query("SELECT * FROM t where nanos < to_timestamp('2023-01-02 01:01:11Z')")
        .await;

    let bytes_scanned_without_filter = cast_count_metric(
        output1
            .parquet_metrics
            .sum_by_name("bytes_scanned")
            .unwrap(),
    )
    .unwrap();
    let bytes_scanned_with_filter = cast_count_metric(
        output2
            .parquet_metrics
            .sum_by_name("bytes_scanned")
            .unwrap(),
    )
    .unwrap();

    // Without filter will not read pageIndex.
    assert!(bytes_scanned_with_filter > bytes_scanned_without_filter);
}

fn cast_count_metric(metric: MetricValue) -> Option<usize> {
    match metric {
        MetricValue::Count { count, .. } => Some(count.value()),
        _ => None,
    }
}
