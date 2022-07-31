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

use datafusion::datasource::file_format::parquet::{
    ParquetFormat, ParquetFormatOptions, DEFAULT_PARQUET_EXTENSION,
};
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::sync::Arc;

/// This example demonstrates executing a simple query against an Arrow data source (a directory
/// with multiple Parquet files) and fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    // Configure listing options
    let file_format =
        ParquetFormat::new(ParquetFormatOptions::new().with_enable_pruning(true));
    let listing_options = ListingOptions {
        file_extension: DEFAULT_PARQUET_EXTENSION.to_owned(),
        format: Arc::new(file_format),
        table_partition_cols: vec![],
        collect_stat: true,
        target_partitions: 1,
    };

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_listing_table(
        "my_table",
        &format!("file://{}", testdata),
        listing_options,
        None,
    )
    .await
    .unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain \
        WHERE id > 1 AND tinyint_col < double_col",
        )
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
