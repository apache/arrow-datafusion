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

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Fields, SchemaBuilder, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::file_format::FileWriterSaveMode;
use std::fs;
use std::ops::{Div, Mul};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use datafusion::common::cast::{
    as_date32_array, as_decimal128_array, as_float64_array, as_int32_array,
    as_int64_array, as_string_array,
};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Cast;
use datafusion::prelude::*;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::MemTable,
    error::{DataFusionError, Result},
};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

/// The `.tbl` file contains a trailing column
pub fn get_tbl_tpch_table_schema(table: &str) -> Schema {
    let mut schema = SchemaBuilder::from(get_tpch_table_schema(table).fields);
    schema.push(Field::new("__placeholder", DataType::Utf8, false));
    schema.finish()
}

/// Get the schema for the benchmarks derived from TPC-H
pub fn get_tpch_table_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
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
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
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

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

/// Get the expected schema for the results of a query
pub fn get_answer_schema(n: usize) -> Schema {
    match n {
        1 => Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8, true),
            Field::new("l_linestatus", DataType::Utf8, true),
            Field::new("sum_qty", DataType::Decimal128(15, 2), true),
            Field::new("sum_base_price", DataType::Decimal128(15, 2), true),
            Field::new("sum_disc_price", DataType::Decimal128(15, 2), true),
            Field::new("sum_charge", DataType::Decimal128(15, 2), true),
            Field::new("avg_qty", DataType::Decimal128(15, 2), true),
            Field::new("avg_price", DataType::Decimal128(15, 2), true),
            Field::new("avg_disc", DataType::Decimal128(15, 2), true),
            Field::new("count_order", DataType::Int64, true),
        ]),

        2 => Schema::new(vec![
            Field::new("s_acctbal", DataType::Decimal128(15, 2), true),
            Field::new("s_name", DataType::Utf8, true),
            Field::new("n_name", DataType::Utf8, true),
            Field::new("p_partkey", DataType::Int64, true),
            Field::new("p_mfgr", DataType::Utf8, true),
            Field::new("s_address", DataType::Utf8, true),
            Field::new("s_phone", DataType::Utf8, true),
            Field::new("s_comment", DataType::Utf8, true),
        ]),

        3 => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
            Field::new("o_orderdate", DataType::Date32, true),
            Field::new("o_shippriority", DataType::Int32, true),
        ]),

        4 => Schema::new(vec![
            Field::new("o_orderpriority", DataType::Utf8, true),
            Field::new("order_count", DataType::Int64, true),
        ]),

        5 => Schema::new(vec![
            Field::new("n_name", DataType::Utf8, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
        ]),

        6 => Schema::new(vec![Field::new(
            "revenue",
            DataType::Decimal128(15, 2),
            true,
        )]),

        7 => Schema::new(vec![
            Field::new("supp_nation", DataType::Utf8, true),
            Field::new("cust_nation", DataType::Utf8, true),
            Field::new("l_year", DataType::Float64, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
        ]),

        8 => Schema::new(vec![
            Field::new("o_year", DataType::Float64, true),
            Field::new("mkt_share", DataType::Decimal128(15, 2), true),
        ]),

        9 => Schema::new(vec![
            Field::new("nation", DataType::Utf8, true),
            Field::new("o_year", DataType::Float64, true),
            Field::new("sum_profit", DataType::Decimal128(15, 2), true),
        ]),

        10 => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, true),
            Field::new("c_name", DataType::Utf8, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), true),
            Field::new("n_name", DataType::Utf8, true),
            Field::new("c_address", DataType::Utf8, true),
            Field::new("c_phone", DataType::Utf8, true),
            Field::new("c_comment", DataType::Utf8, true),
        ]),

        11 => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, true),
            Field::new("value", DataType::Decimal128(15, 2), true),
        ]),

        12 => Schema::new(vec![
            Field::new("l_shipmode", DataType::Utf8, true),
            Field::new("high_line_count", DataType::Int64, true),
            Field::new("low_line_count", DataType::Int64, true),
        ]),

        13 => Schema::new(vec![
            Field::new("c_count", DataType::Int64, true),
            Field::new("custdist", DataType::Int64, true),
        ]),

        14 => Schema::new(vec![Field::new("promo_revenue", DataType::Float64, true)]),

        15 => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, true),
            Field::new("s_name", DataType::Utf8, true),
            Field::new("s_address", DataType::Utf8, true),
            Field::new("s_phone", DataType::Utf8, true),
            Field::new("total_revenue", DataType::Decimal128(15, 2), true),
        ]),

        16 => Schema::new(vec![
            Field::new("p_brand", DataType::Utf8, true),
            Field::new("p_type", DataType::Utf8, true),
            Field::new("p_size", DataType::Int32, true),
            Field::new("supplier_cnt", DataType::Int64, true),
        ]),

        17 => Schema::new(vec![Field::new("avg_yearly", DataType::Float64, true)]),

        18 => Schema::new(vec![
            Field::new("c_name", DataType::Utf8, true),
            Field::new("c_custkey", DataType::Int64, true),
            Field::new("o_orderkey", DataType::Int64, true),
            Field::new("o_orderdate", DataType::Date32, true),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), true),
            Field::new("sum_l_quantity", DataType::Decimal128(15, 2), true),
        ]),

        19 => Schema::new(vec![Field::new(
            "revenue",
            DataType::Decimal128(15, 2),
            true,
        )]),

        20 => Schema::new(vec![
            Field::new("s_name", DataType::Utf8, true),
            Field::new("s_address", DataType::Utf8, true),
        ]),

        21 => Schema::new(vec![
            Field::new("s_name", DataType::Utf8, true),
            Field::new("numwait", DataType::Int64, true),
        ]),

        22 => Schema::new(vec![
            Field::new("cntrycode", DataType::Utf8, true),
            Field::new("numcust", DataType::Int64, true),
            Field::new("totacctbal", DataType::Decimal128(15, 2), true),
        ]),

        _ => unimplemented!(),
    }
}

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: usize) -> Result<Vec<String>> {
    if query > 0 && query < 23 {
        let possibilities = vec![
            format!("queries/q{query}.sql"),
            format!("benchmarks/queries/q{query}.sql"),
        ];
        let mut errors = vec![];
        for filename in possibilities {
            match fs::read_to_string(&filename) {
                Ok(contents) => {
                    return Ok(contents
                        .split(';')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect());
                }
                Err(e) => errors.push(format!("{filename}: {e}")),
            };
        }
        Err(DataFusionError::Plan(format!(
            "invalid query. Could not find query: {errors:?}"
        )))
    } else {
        Err(DataFusionError::Plan(
            "invalid query. Expected value between 1 and 22".to_owned(),
        ))
    }
}

/// Conver tbl (csv) file to parquet
pub async fn convert_tbl(
    input_path: &str,
    output_path: &str,
    file_format: &str,
    partitions: usize,
    batch_size: usize,
    compression: Compression,
) -> Result<()> {
    let output_root_path = Path::new(output_path);
    for table in TPCH_TABLES {
        let start = Instant::now();
        let schema = get_tbl_tpch_table_schema(table);

        let input_path = format!("{input_path}/{table}.tbl");
        let options = CsvReadOptions::new()
            .schema(&schema)
            .has_header(false)
            .delimiter(b'|')
            .file_extension(".tbl");

        let config = SessionConfig::new().with_batch_size(batch_size);
        let ctx = SessionContext::with_config(config);

        // build plan to read the TBL file
        let mut csv = ctx.read_csv(&input_path, options).await?;

        // Select all apart from the padding column
        let selection = csv
            .schema()
            .fields()
            .iter()
            .take(schema.fields.len() - 1)
            .map(|d| Expr::Column(d.qualified_column()))
            .collect();

        csv = csv.select(selection)?;
        // optionally, repartition the file
        if partitions > 1 {
            csv = csv.repartition(Partitioning::RoundRobinBatch(partitions))?
        }

        // create the physical plan
        let csv = csv.create_physical_plan().await?;

        let output_path = output_root_path.join(table);
        let output_path = output_path.to_str().unwrap().to_owned();

        println!(
            "Converting '{}' to {} files in directory '{}'",
            &input_path, &file_format, &output_path
        );
        match file_format {
            "csv" => ctx.write_csv(csv, output_path).await?,
            "parquet" => {
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .build();
                ctx.write_parquet(
                    csv,
                    output_path,
                    Some(props),
                    FileWriterSaveMode::Overwrite,
                )
                .await?
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Invalid output format: {other}"
                )));
            }
        }
        println!("Conversion completed in {} ms", start.elapsed().as_millis());
    }

    Ok(())
}

/// Converts the results into a 2d array of strings, `result[row][column]`
/// Special cases nulls to NULL for testing
pub fn result_vec(results: &[RecordBatch]) -> Vec<Vec<ScalarValue>> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let row_vec = batch
                .columns()
                .iter()
                .map(|column| col_to_scalar(column, row_index))
                .collect();
            result.push(row_vec);
        }
    }
    result
}

/// convert expected schema to all utf8 so columns can be read as strings to be parsed separately
/// this is due to the fact that the csv parser cannot handle leading/trailing spaces
pub fn string_schema(schema: Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                Field::new(
                    Field::name(field),
                    DataType::Utf8,
                    Field::is_nullable(field),
                )
            })
            .collect::<Vec<Field>>(),
    )
}

fn col_to_scalar(column: &ArrayRef, row_index: usize) -> ScalarValue {
    if column.is_null(row_index) {
        return ScalarValue::Null;
    }
    match column.data_type() {
        DataType::Int32 => {
            let array = as_int32_array(column).unwrap();
            ScalarValue::Int32(Some(array.value(row_index)))
        }
        DataType::Int64 => {
            let array = as_int64_array(column).unwrap();
            ScalarValue::Int64(Some(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = as_float64_array(column).unwrap();
            ScalarValue::Float64(Some(array.value(row_index)))
        }
        DataType::Decimal128(p, s) => {
            let array = as_decimal128_array(column).unwrap();
            ScalarValue::Decimal128(Some(array.value(row_index)), *p, *s)
        }
        DataType::Date32 => {
            let array = as_date32_array(column).unwrap();
            ScalarValue::Date32(Some(array.value(row_index)))
        }
        DataType::Utf8 => {
            let array = as_string_array(column).unwrap();
            ScalarValue::Utf8(Some(array.value(row_index).to_string()))
        }
        other => panic!("unexpected data type in benchmark: {other}"),
    }
}

pub async fn transform_actual_result(
    result: Vec<RecordBatch>,
    n: usize,
) -> Result<Vec<RecordBatch>> {
    // to compare the recorded answers to the answers we got back from running the query,
    // we need to round the decimal columns and trim the Utf8 columns
    // we also need to rewrite the batches to use a compatible schema
    let ctx = SessionContext::new();
    let fields: Fields = result[0]
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let simple_name = match f.name().find('.') {
                Some(i) => f.name()[i + 1..].to_string(),
                _ => f.name().to_string(),
            };
            f.as_ref().clone().with_name(simple_name)
        })
        .collect();
    let result_schema = SchemaRef::new(Schema::new(fields));
    let result = result
        .iter()
        .map(|b| {
            RecordBatch::try_new(result_schema.clone(), b.columns().to_vec())
                .map_err(|e| e.into())
        })
        .collect::<Result<Vec<_>>>()?;
    let table = Arc::new(MemTable::try_new(result_schema.clone(), vec![result])?);
    let mut df = ctx.read_table(table)?
        .select(
            result_schema
                .fields
                .iter()
                .map(|field| {
                    match field.data_type() {
                        DataType::Decimal128(_, _) => {
                            // if decimal, then round it to 2 decimal places like the answers
                            // round() doesn't support the second argument for decimal places to round to
                            // this can be simplified to remove the mul and div when
                            // https://github.com/apache/arrow-datafusion/issues/2420 is completed
                            // cast it back to an over-sized Decimal with 2 precision when done rounding
                            let round = Box::new(Expr::ScalarFunction {
                                fun: datafusion::logical_expr::BuiltinScalarFunction::Round,
                                args: vec![col(Field::name(field)).mul(lit(100))],
                            }.div(lit(100)));
                            Expr::Cast(Cast::new(
                                    round,
                                    DataType::Decimal128(15, 2),
                                )).alias(field.name())
                        }
                        DataType::Utf8 => {
                            // if string, then trim it like the answers got trimmed
                            trim(col(Field::name(field))).alias(field.name())
                        }
                        _ => {
                            col(field.name())
                        }
                    }
                }).collect()
        )?;
    if let Some(x) = QUERY_LIMIT[n - 1] {
        df = df.limit(0, Some(x))?;
    }

    let df = df.collect().await?;
    Ok(df)
}

pub const QUERY_LIMIT: [Option<usize>; 22] = [
    None,
    Some(100),
    Some(10),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(20),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(100),
    None,
    None,
    Some(100),
    None,
];
