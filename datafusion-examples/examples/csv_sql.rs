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

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
    let ctx = SessionContext::new_with_config(cfg);

    let testdata = datafusion::test_util::arrow_test_data();
    let path = &format!("{testdata}/csv/aggregate_test_100.csv");
    // register csv file with the execution context
    ctx.register_csv("aggregate_test_100", path, CsvReadOptions::new())
        .await?;

    // execute the query
    let df = ctx
        .sql(
            r#"SELECT c1, MIN(c12), MAX(c12)
        FROM aggregate_test_100
        WHERE c11 > 0.1 AND c11 < 0.9
        GROUP BY c1"#,
        )
        .await?;

    // print the results
    df.show().await?;

    ctx.enable_url_table()?;
    // query the file by the path dynamically.
    let df = ctx
        .sql(
            format!(
                r#"SELECT c1, MIN(c12), MAX(c12)
        FROM '{}'
        WHERE c11 > 0.1 AND c11 < 0.9
        GROUP BY c1"#,
                path
            )
            .as_str(),
        )
        .await?;

    // print the results
    df.show().await?;

    // query compressed CSV with specific options
    let csv_options = CsvReadOptions::default()
        .has_header(true)
        .file_compression_type(FileCompressionType::GZIP)
        .file_extension("csv.gz");
    let df = ctx
        .read_csv(
            &format!("{testdata}/csv/aggregate_test_100.csv.gz"),
            csv_options,
        )
        .await?;
    let df = df
        .filter(col("c1").eq(lit("a")))?
        .select_columns(&["c2", "c3"])?;

    df.show().await?;

    Ok(())
}
