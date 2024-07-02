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

use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use url::Url;

/// This example demonstrates querying data in an S3 bucket.
///
/// The following environment variables must be defined:
///
/// - AWS_ACCESS_KEY_ID
/// - AWS_SECRET_ACCESS_KEY
///
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // the region must be set to the region where the bucket exists until the following
    // issue is resolved
    // https://github.com/apache/arrow-rs/issues/2795
    let region = "us-east-1";
    let bucket_name = "nyc-tlc";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(region)
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .build()?;

    let path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&path).unwrap();
    ctx.register_object_store(&s3_url, Arc::new(s3));

    // cannot query the parquet files from this bucket because the path contains a whitespace
    // and we don't support that yet
    // https://github.com/apache/arrow-rs/issues/2799
    let path = format!("s3://{bucket_name}/csv_backup/yellow_tripdata_2022-02.csv");
    ctx.register_csv("trips", &path, CsvReadOptions::default())
        .await?;

    // execute the query
    let df = ctx.sql("SELECT * FROM trips LIMIT 10").await?;

    // print the results
    df.show().await?;

    // dynamic query by the file path
    ctx.enable_url_table()?;
    let df = ctx
        .sql(format!(r#"SELECT * FROM '{}' LIMIT 10"#, &path).as_str())
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
