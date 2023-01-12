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

use datafusion::config::ConfigOptions;
use datafusion::datasource::TableProvider;
use datafusion::from_slice::FromSlice;
use datafusion::logical_expr::{Aggregate, LogicalPlan, Projection, TableScan};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanVisitor;
use datafusion::prelude::*;
use datafusion::test_util;
use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
use datafusion::{datasource::MemTable, physical_plan::collect};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ColumnarValue,
};
use datafusion::{execution::context::SessionContext, physical_plan::displayable};
use datafusion_common::cast::as_float64_array;
use datafusion_common::{assert_contains, assert_not_contains};
use datafusion_expr::Volatility;
use object_store::path::Path;
use std::fs::File;
use std::io::Write;
use std::ops::Sub;
use std::path::PathBuf;
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
pub mod cast;
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
pub mod math;
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
pub mod explain;
pub mod idenfifers;
pub mod information_schema;
pub mod parquet_schema;
pub mod partitioned_csv;
pub mod set_variable;
pub mod subqueries;
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
            if l.is_nan() || r.is_nan() {
                assert!(l.is_nan() && r.is_nan());
            } else if (l - r).abs() > 2.0 * f64::EPSILON {
                panic!("{l} != {r}")
            }
        });
}

fn create_ctx() -> SessionContext {
    let ctx = SessionContext::new();

    // register a custom UDF
    ctx.register_udf(create_udf(
        "custom_sqrt",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(custom_sqrt),
    ));

    ctx
}

fn custom_sqrt(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arg = &args[0];
    if let ColumnarValue::Array(v) = arg {
        let input = as_float64_array(v).expect("cast failed");
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
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
        ]))],
    )?;
    ctx.register_batch("t1", data)?;
    Ok(ctx)
}

fn create_join_context(
    column_left: &str,
    column_right: &str,
    repartition_joins: bool,
) -> Result<SessionContext> {
    let ctx = SessionContext::with_config(
        SessionConfig::new()
            .with_repartition_joins(repartition_joins)
            .with_target_partitions(2)
            .with_batch_size(4096),
    );

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from_slice([11, 22, 33, 44])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
            Arc::new(UInt32Array::from_slice([1, 2, 3, 4])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
        Field::new("t2_int", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from_slice([11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
            Arc::new(UInt32Array::from_slice([3, 1, 3, 3])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_left_semi_anti_join_context_with_null_ids(
    column_left: &str,
    column_right: &str,
    repartition_joins: bool,
) -> Result<SessionContext> {
    let ctx = SessionContext::with_config(
        SessionConfig::new()
            .with_repartition_joins(repartition_joins)
            .with_target_partitions(2)
            .with_batch_size(4096),
    );

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from(vec![
                Some(11),
                Some(11),
                Some(22),
                Some(33),
                Some(44),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
            Arc::new(UInt32Array::from_slice([1, 1, 2, 3, 4, 0])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
        Field::new("t2_int", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from(vec![
                Some(11),
                Some(11),
                Some(22),
                Some(44),
                Some(55),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
                Some("v"),
            ])),
            Arc::new(UInt32Array::from_slice([3, 3, 1, 3, 3, 0])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_right_semi_anti_join_context_with_null_ids(
    column_left: &str,
    column_right: &str,
    repartition_joins: bool,
) -> Result<SessionContext> {
    let ctx = SessionContext::with_config(
        SessionConfig::new()
            .with_repartition_joins(repartition_joins)
            .with_target_partitions(2)
            .with_batch_size(4096),
    );

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from(vec![
                Some(11),
                Some(22),
                Some(33),
                Some(44),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
            Arc::new(UInt32Array::from_slice([1, 2, 3, 4, 0])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    // t2 data size is smaller than t1
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from(vec![Some(11), Some(11), None])),
            Arc::new(StringArray::from(vec![Some("a"), Some("x"), None])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_join_context_qualified(
    left_name: &str,
    right_name: &str,
) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::UInt32, true),
        Field::new("c", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from_slice([1, 2, 3, 4])),
            Arc::new(UInt32Array::from_slice([10, 20, 30, 40])),
            Arc::new(UInt32Array::from_slice([50, 60, 70, 80])),
        ],
    )?;
    ctx.register_batch(left_name, t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::UInt32, true),
        Field::new("c", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from_slice([1, 2, 9, 4])),
            Arc::new(UInt32Array::from_slice([100, 200, 300, 400])),
            Arc::new(UInt32Array::from_slice([500, 600, 700, 800])),
        ],
    )?;
    ctx.register_batch(right_name, t2_data)?;

    Ok(ctx)
}

fn create_hashjoin_datatype_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let t1_schema = Schema::new(vec![
        Field::new("c1", DataType::Date32, true),
        Field::new("c2", DataType::Date64, true),
        Field::new("c3", DataType::Decimal128(5, 2), true),
        Field::new(
            "c4",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let dict1: DictionaryArray<Int32Type> =
        vec!["abc", "def", "ghi", "jkl"].into_iter().collect();
    let t1_data = RecordBatch::try_new(
        Arc::new(t1_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), Some(2), None, Some(3)])),
            Arc::new(Date64Array::from(vec![
                Some(86400000),
                Some(172800000),
                Some(259200000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from_iter_values([123, 45600, 78900, -12312])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
            Arc::new(dict1),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Schema::new(vec![
        Field::new("c1", DataType::Date32, true),
        Field::new("c2", DataType::Date64, true),
        Field::new("c3", DataType::Decimal128(10, 2), true),
        Field::new(
            "c4",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let dict2: DictionaryArray<Int32Type> =
        vec!["abc", "abcdefg", "qwerty", ""].into_iter().collect();
    let t2_data = RecordBatch::try_new(
        Arc::new(t2_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), None, None, Some(3)])),
            Arc::new(Date64Array::from(vec![
                Some(86400000),
                None,
                Some(259200000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from_iter_values([-12312, 10000000, 0, 78900])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(dict2),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

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
        t1_schema,
        vec![
            Arc::new(UInt32Array::from_slice([11, 22, 33, 44, 77])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from_slice([11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

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
        t1_schema,
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
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("t2_id", DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
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
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_sort_merge_join_context(
    column_left: &str,
    column_right: &str,
) -> Result<SessionContext> {
    let mut config = ConfigOptions::new();
    config.optimizer.prefer_hash_join = false;

    let ctx = SessionContext::with_config(config.into());

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema,
        vec![
            Arc::new(UInt32Array::from_slice([11, 22, 33, 44])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
            Arc::new(UInt32Array::from_slice([1, 2, 3, 4])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
        Field::new("t2_int", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema,
        vec![
            Arc::new(UInt32Array::from_slice([11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
            Arc::new(UInt32Array::from_slice([3, 1, 3, 3])),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_sort_merge_join_datatype_context() -> Result<SessionContext> {
    let mut config = ConfigOptions::new();
    config.optimizer.prefer_hash_join = false;
    config.execution.target_partitions = 2;
    config.execution.batch_size = 4096;

    let ctx = SessionContext::with_config(config.into());

    let t1_schema = Schema::new(vec![
        Field::new("c1", DataType::Date32, true),
        Field::new("c2", DataType::Date64, true),
        Field::new("c3", DataType::Decimal128(5, 2), true),
        Field::new(
            "c4",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let dict1: DictionaryArray<Int32Type> =
        vec!["abc", "def", "ghi", "jkl"].into_iter().collect();
    let t1_data = RecordBatch::try_new(
        Arc::new(t1_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), Some(2), None, Some(3)])),
            Arc::new(Date64Array::from(vec![
                Some(86400000),
                Some(172800000),
                Some(259200000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from_iter_values([123, 45600, 78900, -12312])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
            Arc::new(dict1),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let t2_schema = Schema::new(vec![
        Field::new("c1", DataType::Date32, true),
        Field::new("c2", DataType::Date64, true),
        Field::new("c3", DataType::Decimal128(10, 2), true),
        Field::new(
            "c4",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let dict2: DictionaryArray<Int32Type> =
        vec!["abc", "abcdefg", "qwerty", ""].into_iter().collect();
    let t2_data = RecordBatch::try_new(
        Arc::new(t2_schema),
        vec![
            Arc::new(Date32Array::from(vec![Some(1), None, None, Some(3)])),
            Arc::new(Date64Array::from(vec![
                Some(86400000),
                None,
                Some(259200000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from_iter_values([-12312, 10000000, 0, 78900])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(dict2),
        ],
    )?;
    ctx.register_batch("t2", t2_data)?;

    Ok(ctx)
}

fn create_muti_table_join_context() -> Result<SessionContext> {
    let ctx = SessionContext::with_config(
        SessionConfig::new()
            .with_repartition_joins(false)
            .with_target_partitions(2)
            .with_batch_size(4096),
    );

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("t1_id", DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
        Field::new("t1_int", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::new_empty(t1_schema);
    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("t2_id", DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
        Field::new("t2_int", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::new_empty(t2_schema);
    let t3_schema = Arc::new(Schema::new(vec![
        Field::new("t3_id", DataType::UInt32, true),
        Field::new("t3_name", DataType::Utf8, true),
        Field::new("t3_int", DataType::UInt32, true),
    ]));
    let t3_data = RecordBatch::new_empty(t3_schema);

    ctx.register_batch("t1", t1_data)?;
    ctx.register_batch("t2", t2_data)?;
    ctx.register_batch("t3", t3_data)?;

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

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!("Table: {}", table),
    }
}

async fn register_tpch_csv(ctx: &SessionContext, table: &str) -> Result<()> {
    let schema = get_tpch_table_schema(table);

    ctx.register_csv(
        table,
        format!("tests/tpch-csv/{table}.csv").as_str(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_tpch_csv_data(
    ctx: &SessionContext,
    table_name: &str,
    data: &str,
) -> Result<()> {
    let schema = Arc::new(get_tpch_table_schema(table_name));

    let mut reader = ::csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(data.as_bytes());
    let records: Vec<_> = reader.records().map(|it| it.unwrap()).collect();

    let mut cols: Vec<Box<dyn ArrayBuilder>> = vec![];
    for field in schema.fields().iter() {
        match field.data_type() {
            DataType::Utf8 => cols.push(Box::new(StringBuilder::new())),
            DataType::Date32 => {
                cols.push(Box::new(Date32Builder::with_capacity(records.len())))
            }
            DataType::Int32 => {
                cols.push(Box::new(Int32Builder::with_capacity(records.len())))
            }
            DataType::Int64 => {
                cols.push(Box::new(Int64Builder::with_capacity(records.len())))
            }
            DataType::Decimal128(_, _) => {
                cols.push(Box::new(Decimal128Builder::with_capacity(records.len())))
            }
            _ => {
                let msg = format!("Not implemented: {}", field.data_type());
                Err(DataFusionError::Plan(msg))?
            }
        }
    }

    for record in records.iter() {
        for (idx, val) in record.iter().enumerate() {
            let col = cols.get_mut(idx).unwrap();
            let field = schema.field(idx);
            match field.data_type() {
                DataType::Utf8 => {
                    let sb = col.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                    sb.append_value(val);
                }
                DataType::Date32 => {
                    let sb = col.as_any_mut().downcast_mut::<Date32Builder>().unwrap();
                    let dt = NaiveDate::parse_from_str(val.trim(), "%Y-%m-%d").unwrap();
                    let dt = dt
                        .sub(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32;
                    sb.append_value(dt);
                }
                DataType::Int32 => {
                    let sb = col.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
                    sb.append_value(val.trim().parse().unwrap());
                }
                DataType::Int64 => {
                    let sb = col.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                    sb.append_value(val.trim().parse().unwrap());
                }
                DataType::Decimal128(_, _) => {
                    let sb = col
                        .as_any_mut()
                        .downcast_mut::<Decimal128Builder>()
                        .unwrap();
                    let val = val.trim().replace('.', "");
                    let value_i128 = val.parse::<i128>().unwrap();
                    sb.append_value(value_i128);
                }
                _ => Err(DataFusionError::Plan(format!(
                    "Not implemented: {}",
                    field.data_type()
                )))?,
            }
        }
    }
    let cols: Vec<ArrayRef> = cols
        .iter_mut()
        .zip(schema.fields())
        .map(|(it, field)| match field.data_type() {
            DataType::Decimal128(p, s) => Arc::new(
                it.as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .unwrap()
                    .finish()
                    .with_precision_and_scale(*p, *s)
                    .unwrap(),
            ),
            _ => it.finish(),
        })
        .collect();

    let batch = RecordBatch::try_new(Arc::clone(&schema), cols)?;

    let _ = ctx.register_batch(table_name, batch).unwrap();

    Ok(())
}

async fn register_aggregate_csv_by_sql(ctx: &SessionContext) {
    let testdata = datafusion::test_util::arrow_test_data();

    let df = ctx
        .sql(&format!(
            "
    CREATE EXTERNAL TABLE aggregate_test_100 (
        c1  VARCHAR NOT NULL,
        c2  TINYINT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT NOT NULL,
        c5  INTEGER NOT NULL,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  INT UNSIGNED NOT NULL,
        c10 BIGINT UNSIGNED NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION '{testdata}/csv/aggregate_test_100.csv'
    "
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
    ctx.register_batch("t1", data)?;
    Ok(())
}

async fn register_aggregate_simple_csv(ctx: &SessionContext) -> Result<()> {
    // It's not possible to use aggregate_test_100 as it doesn't have enough similar values to test grouping on floats.
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Float32, false),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    ctx.register_csv(
        "aggregate_simple",
        "tests/data/aggregate_simple.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_aggregate_null_cases_csv(ctx: &SessionContext) -> Result<()> {
    // It's not possible to use aggregate_test_100, not enought similar values to test grouping on floats
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::Int64, false),
    ]));

    ctx.register_csv(
        "null_cases",
        "tests/data/null_cases.csv",
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
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

/// Execute SQL and return results as a RecordBatch
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Execute query and return results as a Vec of RecordBatches or an error
async fn try_execute_to_batches(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let dataframe = ctx.sql(sql).await?;
    let logical_schema = dataframe.schema().clone();
    let (state, plan) = dataframe.into_parts();

    let optimized = state.optimize(&plan)?;
    assert_eq!(&logical_schema, optimized.schema().as_ref());
    DataFrame::new(state, optimized).collect().await
}

/// Execute query and return results as a Vec of RecordBatches
async fn execute_to_batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await.unwrap();

    // We are not really interested in the direct output of optimized_logical_plan
    // since the physical plan construction already optimizes the given logical plan
    // and we want to avoid double-optimization as a consequence. So we just construct
    // it here to make sure that it doesn't fail at this step and get the optimized
    // schema (to assert later that the logical and optimized schemas are the same).
    let optimized = df.clone().into_optimized_plan().unwrap();
    assert_eq!(df.logical_plan().schema(), optimized.schema());

    df.collect().await.unwrap()
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
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = tmp_dir.path().join(filename);
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
    let mut decimal_builder = Decimal128Builder::with_capacity(20);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128);
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128);
    }
    let array = decimal_builder
        .finish()
        .with_precision_and_scale(10, 3)
        .unwrap();
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
            LOCATION 'tests/data/decimal_data.csv'",
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
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
}

fn make_timestamp_table<A>() -> Result<Arc<MemTable>>
where
    A: ArrowTimestampType<Native = i64>,
{
    make_timestamp_tz_table::<A>(None)
}

fn make_timestamp_tz_table<A>(tz: Option<String>) -> Result<Arc<MemTable>>
where
    A: ArrowTimestampType<Native = i64>,
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

    let array = PrimitiveArray::<A>::from_iter_values(timestamps).with_timezone_opt(tz);

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

pub struct ExplainNormalizer {
    replacements: Vec<(String, String)>,
}

impl ExplainNormalizer {
    fn new() -> Self {
        let mut replacements = vec![];

        let mut push_path = |path: PathBuf, key: &str| {
            // Push path as is
            replacements.push((path.to_string_lossy().to_string(), key.to_string()));

            // Push URL representation of path
            let path = Path::from_filesystem_path(path).unwrap();
            replacements.push((path.to_string(), key.to_string()));
        };

        push_path(test_util::arrow_test_data().into(), "ARROW_TEST_DATA");
        push_path(std::env::current_dir().unwrap(), "WORKING_DIR");

        // convert things like partitioning=RoundRobinBatch(16)
        // to partitioning=RoundRobinBatch(NUM_CORES)
        let needle = format!("RoundRobinBatch({})", num_cpus::get());
        replacements.push((needle, "RoundRobinBatch(NUM_CORES)".to_string()));

        Self { replacements }
    }

    fn normalize(&self, s: impl Into<String>) -> String {
        let mut s = s.into();
        for (from, to) in &self.replacements {
            s = s.replace(from, to);
        }
        s
    }
}

/// Applies normalize_for_explain to every line
fn normalize_vec_for_explain(v: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let normalizer = ExplainNormalizer::new();
    v.into_iter()
        .map(|l| {
            l.into_iter()
                .map(|s| normalizer.normalize(s))
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
        .map(|(i, _)| format!("Row {i}"))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from(ts_nanos);
    let arr_micros = TimestampMicrosecondArray::from(ts_micros);
    let arr_millis = TimestampMillisecondArray::from(ts_millis);
    let arr_secs = TimestampSecondArray::from(ts_secs);

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

/// Return a new table provider containing all of the supported timestamp types
pub fn table_with_times() -> Arc<dyn TableProvider> {
    let batch = make_times();
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

/// Return  record batch with all of the supported time types
/// values
///
/// Columns are named:
/// "nanos" --> Time64NanosecondArray
/// "micros" --> Time64MicrosecondArray
/// "millis" --> Time32MillisecondArray
/// "secs" --> Time32SecondArray
/// "names" --> StringArray
pub fn make_times() -> RecordBatch {
    let ts_strings = vec![
        Some("18:06:30.243620451"),
        Some("20:08:28.161121654"),
        Some("19:11:04.156423842"),
        Some("21:06:28.247821084"),
    ];

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                let integer_sec = t
                    .parse::<chrono::NaiveTime>()
                    .unwrap()
                    .num_seconds_from_midnight() as i64;
                let extra_nano =
                    t.parse::<chrono::NaiveTime>().unwrap().nanosecond() as i64;
                // Total time in nanoseconds given by integer number of seconds multiplied by 10^9
                // plus number of nanoseconds corresponding to the extra fraction of second
                integer_sec * 1_000_000_000 + extra_nano
            })
        })
        .collect::<Vec<_>>();

    let ts_micros = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000))
        .collect::<Vec<_>>();

    let ts_millis = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| { ts_nanos / 1000000 } as i32))
        .collect::<Vec<_>>();

    let ts_secs = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| { ts_nanos / 1000000000 } as i32))
        .collect::<Vec<_>>();

    let names = ts_nanos
        .iter()
        .enumerate()
        .map(|(i, _)| format!("Row {i}"))
        .collect::<Vec<_>>();

    let arr_nanos = Time64NanosecondArray::from(ts_nanos);
    let arr_micros = Time64MicrosecondArray::from(ts_micros);
    let arr_millis = Time32MillisecondArray::from(ts_millis);
    let arr_secs = Time32SecondArray::from(ts_secs);

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
        "file:///file.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    let dataframe = ctx
        .sql(
            "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
         FROM tripdata GROUP BY passenger_count",
        )
        .await?;
    let optimized_plan = dataframe.into_optimized_plan().unwrap();

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
