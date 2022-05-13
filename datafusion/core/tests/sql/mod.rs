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

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::{
    array::*, datatypes::*, record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use chrono::prelude::*;
use chrono::Duration;

use datafusion::assert_batches_eq;
use datafusion::assert_batches_sorted_eq;
use datafusion::assert_contains;
use datafusion::assert_not_contains;
use datafusion::datasource::TableProvider;
use datafusion::from_slice::FromSlice;
use datafusion::logical_plan::plan::{Aggregate, Projection};
use datafusion::logical_plan::LogicalPlan;
use datafusion::logical_plan::TableScan;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanVisitor;
use datafusion::prelude::*;
use datafusion::test_util;
use datafusion::{datasource::MemTable, physical_plan::collect};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ColumnarValue,
};
use datafusion::{execution::context::SessionContext, physical_plan::displayable};
use datafusion_expr::Volatility;
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

/// A macro to assert that some particular line contains two substrings
///
/// Usage: `assert_metrics!(actual, operator_name, metrics)`
///
macro_rules! assert_metrics {
    ($ACTUAL: expr, $OPERATOR_NAME: expr, $METRICS: expr) => {
        let found = $ACTUAL
            .lines()
            .any(|line| line.contains($OPERATOR_NAME) && line.contains($METRICS));
        assert!(
            found,
            "Can not find a line with both '{}' and '{}' in\n\n{}",
            $OPERATOR_NAME, $METRICS, $ACTUAL
        );
    };
}

macro_rules! test_expression {
    ($SQL:expr, $EXPECTED:expr) => {
        let ctx = SessionContext::new();
        let sql = format!("SELECT {}", $SQL);
        let actual = execute(&ctx, sql.as_str()).await;
        assert_eq!(actual[0][0], $EXPECTED);
    };
}

pub mod aggregates;
#[cfg(feature = "avro")]
pub mod avro;
pub mod create_drop;
pub mod errors;
pub mod explain_analyze;
pub mod expr;
pub mod functions;
pub mod group_by;
pub mod intersection;
pub mod joins;
pub mod json;
pub mod limit;
pub mod order;
pub mod parquet;
pub mod predicates;
pub mod projection;
pub mod references;
pub mod select;
pub mod timestamp;
pub mod udf;
pub mod union;
pub mod wildcard;
pub mod window;

pub mod decimal;
mod explain;
mod idenfifers;
pub mod information_schema;
mod partitioned_csv;
#[cfg(feature = "unicode_expressions")]
pub mod unicode;

fn assert_float_eq<T>(expected: &[Vec<T>], received: &[Vec<String>])
where
    T: AsRef<str>,
{
    expected
        .iter()
        .flatten()
        .zip(received.iter().flatten())
        .for_each(|(l, r)| {
            let (l, r) = (
                l.as_ref().parse::<f64>().unwrap(),
                r.as_str().parse::<f64>().unwrap(),
            );
            assert!((l - r).abs() <= 2.0 * f64::EPSILON);
        });
}

#[allow(clippy::unnecessary_wraps)]
fn create_ctx() -> Result<SessionContext> {
    let mut ctx = SessionContext::new();

    // register a custom UDF
    ctx.register_udf(create_udf(
        "custom_sqrt",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(custom_sqrt),
    ));

    Ok(ctx)
}

fn custom_sqrt(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arg = &args[0];
    if let ColumnarValue::Array(v) = arg {
        let input = v
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast failed");

        let array: Float64Array = input.iter().map(|v| v.map(|x| x.sqrt())).collect();
        Ok(ColumnarValue::Array(Arc::new(array)))
    } else {
        unimplemented!()
    }
}

fn create_case_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Utf8, true)]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
        ]))],
    )?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    ctx.register_table("t1", Arc::new(table))?;
    Ok(ctx)
}

fn create_join_context(column_left: &str, column_right: &str) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from_slice(&[11, 22, 33, 44])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from_slice(&[11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

fn create_join_context_qualified() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::UInt32, true),
        Field::new("c", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from_slice(&[1, 2, 3, 4])),
            Arc::new(UInt32Array::from_slice(&[10, 20, 30, 40])),
            Arc::new(UInt32Array::from_slice(&[50, 60, 70, 80])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::UInt32, true),
        Field::new("c", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from_slice(&[1, 2, 9, 4])),
            Arc::new(UInt32Array::from_slice(&[100, 200, 300, 400])),
            Arc::new(UInt32Array::from_slice(&[500, 600, 700, 800])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

/// the table column_left has more rows than the table column_right
fn create_join_context_unbalanced(
    column_left: &str,
    column_right: &str,
) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from_slice(&[11, 22, 33, 44, 77])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from_slice(&[11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

// Create memory tables with nulls
fn create_join_context_with_nulls() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("t1_id", DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 33, 44, 77, 88, 99])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
                None,
                None,
            ])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("t2_id", DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 44, 55, 99])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                None,
                Some("x"),
                Some("w"),
                Some("u"),
            ])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

fn get_tpch_table_schema(table: &str) -> Schema {
    match table {
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

async fn register_tpch_csv(ctx: &SessionContext, table: &str) -> Result<()> {
    let schema = get_tpch_table_schema(table);

    ctx.register_csv(
        table,
        format!("tests/tpch-csv/{}.csv", table).as_str(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_aggregate_csv_by_sql(ctx: &SessionContext) {
    let testdata = datafusion::test_util::arrow_test_data();

    // TODO: The following c9 should be migrated to UInt32 and c10 should be UInt64 once
    // unsigned is supported.
    let df = ctx
        .sql(&format!(
            "
    CREATE EXTERNAL TABLE aggregate_test_100 (
        c1  VARCHAR NOT NULL,
        c2  INT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT NOT NULL,
        c5  INT NOT NULL,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  BIGINT NOT NULL,
        c10 VARCHAR NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION '{}/csv/aggregate_test_100.csv'
    ",
            testdata
        ))
        .await
        .expect("Creating dataframe for CREATE EXTERNAL TABLE");

    // Mimic the CLI and execute the resulting plan -- even though it
    // is effectively a no-op (returns zero rows)
    let results = df.collect().await.expect("Executing CREATE EXTERNAL TABLE");
    assert!(
        results.is_empty(),
        "Expected no rows from executing CREATE EXTERNAL TABLE"
    );
}

/// Create table "t1" with two boolean columns "a" and "b"
async fn register_boolean(ctx: &SessionContext) -> Result<()> {
    let a: BooleanArray = [
        Some(true),
        Some(true),
        Some(true),
        None,
        None,
        None,
        Some(false),
        Some(false),
        Some(false),
    ]
    .iter()
    .collect();
    let b: BooleanArray = [
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
    ]
    .iter()
    .collect();

    let data =
        RecordBatch::try_from_iter([("a", Arc::new(a) as _), ("b", Arc::new(b) as _)])?;
    let table = MemTable::try_new(data.schema(), vec![vec![data]])?;
    ctx.register_table("t1", Arc::new(table))?;
    Ok(())
}

async fn register_aggregate_simple_csv(ctx: &SessionContext) -> Result<()> {
    // It's not possible to use aggregate_test_100, not enought similar values to test grouping on floats
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Float32, false),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    ctx.register_csv(
        "aggregate_simple",
        "tests/aggregate_simple.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_aggregate_csv(ctx: &SessionContext) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    let schema = test_util::aggr_test_schema();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

/// Execute SQL and return results as a RecordBatch
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Execute query and return results as a Vec of RecordBatches
async fn execute_to_batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(sql)
        .map_err(|e| format!("{:?} at {}", e, msg))
        .unwrap();
    let logical_schema = plan.schema();

    let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, msg))
        .unwrap();
    let optimized_logical_schema = plan.schema();

    println!("plan {:?}", plan);

    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx
        .create_physical_plan(&plan)
        .await
        .map_err(|e| format!("{:?} at {}", e, msg))
        .unwrap();

    println!("plan {:?}", plan);

    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx)
        .await
        .map_err(|e| format!("{:?} at {}", e, msg))
        .unwrap();

    assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
    results
}

/// Execute query and return result set as 2-d table of Vecs
/// `result[row][column]`
async fn execute(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    result_vec(&execute_to_batches(ctx, sql).await)
}

/// Execute SQL and return results
async fn execute_with_partition(
    sql: &str,
    partition_count: usize,
) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let ctx = create_ctx_with_partition(&tmp_dir, partition_count).await?;
    plan_and_collect(&ctx, sql).await
}

/// Generate a partitioned CSV file and register it with an execution context
async fn create_ctx_with_partition(
    tmp_dir: &TempDir,
    partition_count: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(8));

    let schema = populate_csv_partitions(tmp_dir, partition_count, ".csv")?;

    // register csv file with the execution context
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    Ok(ctx)
}

/// Generate CSV partitions within the supplied directory
fn populate_csv_partitions(
    tmp_dir: &TempDir,
    partition_count: usize,
    file_extension: &str,
) -> Result<SchemaRef> {
    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{}.{}", partition, file_extension);
        let file_path = tmp_dir.path().join(&filename);
        let mut file = File::create(file_path)?;

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes())?;
        }
    }

    Ok(schema)
}

/// Return a new table which provide this decimal column
pub fn table_with_decimal() -> Arc<dyn TableProvider> {
    let batch_decimal = make_decimal();
    let schema = batch_decimal.schema();
    let partitions = vec![vec![batch_decimal]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

fn make_decimal() -> RecordBatch {
    let mut decimal_builder = DecimalBuilder::new(20, 10, 3);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128).unwrap();
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128).unwrap();
    }
    let array = decimal_builder.finish();
    let schema = Schema::new(vec![Field::new("c1", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

/// Return a RecordBatch with a single Int32 array with values (0..sz)
pub fn make_partition(sz: i32) -> RecordBatch {
    let seq_start = 0;
    let seq_end = sz;
    let values = (seq_start..seq_end).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from(values));
    let arr = arr as ArrayRef;

    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Specialised String representation
fn col_str(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    // Special case ListArray as there is no pretty print support for it yet
    if let DataType::FixedSizeList(_, n) = column.data_type() {
        let array = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .value(row_index);

        let mut r = Vec::with_capacity(*n as usize);
        for i in 0..*n {
            r.push(col_str(&array, i as usize));
        }
        return format!("[{}]", r.join(","));
    }

    array_value_to_string(column, row_index)
        .ok()
        .unwrap_or_else(|| "???".to_string())
}

/// Converts the results into a 2d array of strings, `result[row][column]`
/// Special cases nulls to NULL for testing
fn result_vec(results: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let row_vec = batch
                .columns()
                .iter()
                .map(|column| col_str(column, row_index))
                .collect();
            result.push(row_vec);
        }
    }
    result
}

async fn register_decimal_csv_table_by_sql(ctx: &SessionContext) {
    let df = ctx
        .sql(
            "CREATE EXTERNAL TABLE decimal_simple (
            c1  DECIMAL(10,6) NOT NULL,
            c2  DOUBLE NOT NULL,
            c3  BIGINT NOT NULL,
            c4  BOOLEAN NOT NULL,
            c5  DECIMAL(12,7) NOT NULL
            )
            STORED AS CSV
            WITH HEADER ROW
            LOCATION 'tests/decimal_data.csv'",
        )
        .await
        .expect("Creating dataframe for CREATE EXTERNAL TABLE with decimal data type");

    let results = df.collect().await.expect("Executing CREATE EXTERNAL TABLE");
    assert!(
        results.is_empty(),
        "Expected no rows from executing CREATE EXTERNAL TABLE"
    );
}

async fn register_alltypes_parquet(ctx: &SessionContext) {
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{}/alltypes_plain.parquet", testdata),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
}

fn make_timestamp_table<A>() -> Result<Arc<MemTable>>
where
    A: ArrowTimestampType,
{
    make_timestamp_tz_table::<A>(None)
}

fn make_timestamp_tz_table<A>(tz: Option<String>) -> Result<Arc<MemTable>>
where
    A: ArrowTimestampType,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(A::get_time_unit(), tz.clone()),
            false,
        ),
        Field::new("value", DataType::Int32, true),
    ]));

    let divisor = match A::get_time_unit() {
        TimeUnit::Nanosecond => 1,
        TimeUnit::Microsecond => 1000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Second => 1_000_000_000,
    };

    let timestamps = vec![
        1599572549190855000i64 / divisor, // 2020-09-08T13:42:29.190855+00:00
        1599568949190855000 / divisor,    // 2020-09-08T12:42:29.190855+00:00
        1599565349190855000 / divisor,    // 2020-09-08T11:42:29.190855+00:00
    ]; // 2020-09-08T11:42:29.190855+00:00

    let array = PrimitiveArray::<A>::from_vec(timestamps, tz);

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(array),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    Ok(Arc::new(table))
}

fn make_timestamp_nano_table() -> Result<Arc<MemTable>> {
    make_timestamp_table::<TimestampNanosecondType>()
}

/// Return a new table provider that has a single Int32 column with
/// values between `seq_start` and `seq_end`
pub fn table_with_sequence(
    seq_start: i32,
    seq_end: i32,
) -> Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from((seq_start..=seq_end).collect::<Vec<_>>()));
    let partitions = vec![vec![RecordBatch::try_new(
        schema.clone(),
        vec![arr as ArrayRef],
    )?]];
    Ok(Arc::new(MemTable::try_new(schema, partitions)?))
}

// Normalizes parts of an explain plan that vary from run to run (such as path)
fn normalize_for_explain(s: &str) -> String {
    // Convert things like /Users/alamb/Software/arrow/testing/data/csv/aggregate_test_100.csv
    // to ARROW_TEST_DATA/csv/aggregate_test_100.csv
    let data_path = datafusion::test_util::arrow_test_data();
    let s = s.replace(&data_path, "ARROW_TEST_DATA");

    // convert things like partitioning=RoundRobinBatch(16)
    // to partitioning=RoundRobinBatch(NUM_CORES)
    let needle = format!("RoundRobinBatch({})", num_cpus::get());
    s.replace(&needle, "RoundRobinBatch(NUM_CORES)")
}

/// Applies normalize_for_explain to every line
fn normalize_vec_for_explain(v: Vec<Vec<String>>) -> Vec<Vec<String>> {
    v.into_iter()
        .map(|l| {
            l.into_iter()
                .map(|s| normalize_for_explain(&s))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

/// Return a new table provider containing all of the supported timestamp types
pub fn table_with_timestamps() -> Arc<dyn TableProvider> {
    let batch = make_timestamps();
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

/// Return  record batch with all of the supported timestamp types
/// values
///
/// Columns are named:
/// "nanos" --> TimestampNanosecondArray
/// "micros" --> TimestampMicrosecondArray
/// "millis" --> TimestampMillisecondArray
/// "secs" --> TimestampSecondArray
/// "names" --> StringArray
pub fn make_timestamps() -> RecordBatch {
    let ts_strings = vec![
        Some("2018-11-13T17:11:10.011375885995"),
        Some("2011-12-13T11:13:10.12345"),
        None,
        Some("2021-1-1T05:11:10.432"),
    ];

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                t.parse::<chrono::NaiveDateTime>()
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

    let ts_secs = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000000))
        .collect::<Vec<_>>();

    let names = ts_nanos
        .iter()
        .enumerate()
        .map(|(i, _)| format!("Row {}", i))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from_opt_vec(ts_nanos, None);
    let arr_micros = TimestampMicrosecondArray::from_opt_vec(ts_micros, None);
    let arr_millis = TimestampMillisecondArray::from_opt_vec(ts_millis, None);
    let arr_secs = TimestampSecondArray::from_opt_vec(ts_secs, None);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("nanos", arr_nanos.data_type().clone(), true),
        Field::new("micros", arr_micros.data_type().clone(), true),
        Field::new("millis", arr_millis.data_type().clone(), true),
        Field::new("secs", arr_secs.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_nanos),
            Arc::new(arr_micros),
            Arc::new(arr_millis),
            Arc::new(arr_secs),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn nyc() -> Result<()> {
    // schema for nyxtaxi csv files
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Utf8, true),
        Field::new("trip_distance", DataType::Float64, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]);

    let ctx = SessionContext::new();
    ctx.register_csv(
        "tripdata",
        "file.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    let logical_plan = ctx.create_logical_plan(
        "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
         FROM tripdata GROUP BY passenger_count",
    )?;

    let optimized_plan = ctx.optimize(&logical_plan)?;

    match &optimized_plan {
        LogicalPlan::Projection(Projection { input, .. }) => match input.as_ref() {
            LogicalPlan::Aggregate(Aggregate { input, .. }) => match input.as_ref() {
                LogicalPlan::TableScan(TableScan {
                    ref projected_schema,
                    ..
                }) => {
                    assert_eq!(2, projected_schema.fields().len());
                    assert_eq!(projected_schema.field(0).name(), "passenger_count");
                    assert_eq!(projected_schema.field(1).name(), "fare_amount");
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    Ok(())
}
