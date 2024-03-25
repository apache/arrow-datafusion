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

use arrow::array::Date32Array;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::sync::Arc;

/// This example demonstrates how to use the to_char function via sql
///
/// This function accepts date, time, timestamp and duration values
/// in the first argument and string values for the second
#[tokio::main]
async fn main() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", DataType::Date32, false),
        Field::new("patterns", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![18506, 18507, 18508, 18509])),
            Arc::new(StringArray::from(vec![
                "%Y-%m-%d", "%Y:%m:%d", "%Y%m%d", "%d-%m-%Y",
            ])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let _ = ctx.table("t").await?;

    // use to_char function to convert col 'values' to timestamp type using
    // patterns stored in col 'patterns'
    let result = ctx
        .sql("SELECT to_char(values, patterns) from t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------+",
            "| to_char(t.values,t.patterns) |",
            "+------------------------------+",
            "| 2020-09-01                   |",
            "| 2020:09:02                   |",
            "| 20200903                     |",
            "| 04-09-2020                   |",
            "+------------------------------+",
        ],
        &result
    );

    // the date_format alias for the to_char function can be used as well
    let result = ctx
        .sql("SELECT date_format(values, patterns) from t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------+",
            "| to_char(t.values,t.patterns) |",
            "+------------------------------+",
            "| 2020-09-01                   |",
            "| 2020:09:02                   |",
            "| 20200903                     |",
            "| 04-09-2020                   |",
            "+------------------------------+",
        ],
        &result
    );

    // use to_char function to convert col 'values' with a fixed format
    let result = ctx
        .sql("SELECT to_char(values, '%m-%d-%Y') FROM t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------------+",
            "| to_char(t.values,Utf8(\"%m-%d-%Y\")) |",
            "+------------------------------------+",
            "| 09-01-2020                         |",
            "| 09-02-2020                         |",
            "| 09-03-2020                         |",
            "| 09-04-2020                         |",
            "+------------------------------------+",
        ],
        &result
    );

    // if you want to just use the default format cast to a string
    let result = ctx
        .sql("SELECT arrow_cast(values, 'Utf8') from t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-----------------------------------+",
            "| arrow_cast(t.values,Utf8(\"Utf8\")) |",
            "+-----------------------------------+",
            "| 2020-09-01                        |",
            "| 2020-09-02                        |",
            "| 2020-09-03                        |",
            "| 2020-09-04                        |",
            "+-----------------------------------+",
        ],
        &result
    );

    // use can use literals as well (note the use of timestamp here)
    let result = ctx
        .sql("SELECT to_char(arrow_cast(TIMESTAMP '2023-08-03 14:38:50Z', 'Timestamp(Second, None)'), '%d-%m-%Y %H:%M:%S')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-------------------------------------------------------------------------------------------------------------+",
            "| to_char(arrow_cast(Utf8(\"2023-08-03 14:38:50Z\"),Utf8(\"Timestamp(Second, None)\")),Utf8(\"%d-%m-%Y %H:%M:%S\")) |",
            "+-------------------------------------------------------------------------------------------------------------+",
            "| 03-08-2023 14:38:50                                                                                         |",
            "+-------------------------------------------------------------------------------------------------------------+",
        ],
        &result
    );

    // durations are supported though the output format is limited to two formats
    // 'pretty' and 'ISO8601'
    let result = ctx
        .sql("SELECT to_char(arrow_cast(123456, 'Duration(Second)'), 'pretty')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----------------------------------------------------------------------------+",
            "| to_char(arrow_cast(Int64(123456),Utf8(\"Duration(Second)\")),Utf8(\"pretty\")) |",
            "+----------------------------------------------------------------------------+",
            "| 1 days 10 hours 17 mins 36 secs                                            |",
            "+----------------------------------------------------------------------------+",
        ],
        &result
    );

    // durations are supported though the output format is limited to two formats
    // 'pretty' and 'ISO8601'
    let result = ctx
        .sql("SELECT to_char(arrow_cast(123456, 'Duration(Second)'), 'iso8601')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-----------------------------------------------------------------------------+",
            "| to_char(arrow_cast(Int64(123456),Utf8(\"Duration(Second)\")),Utf8(\"iso8601\")) |",
            "+-----------------------------------------------------------------------------+",
            "| PT123456S                                                                   |",
            "+-----------------------------------------------------------------------------+",
        ],
        &result
    );

    // output format is null

    let result = ctx
        .sql("SELECT to_char(arrow_cast(123456, 'Duration(Second)'), null) as result")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+--------+",
            "| result |",
            "+--------+",
            "|        |",
            "+--------+",
        ],
        &result
    );

    Ok(())
}
