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

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg;
use datafusion::prelude::*;
use datafusion::test_util::arrow_test_data;
use datafusion_common::ScalarValue;

/// This example demonstrates how to use the DataFrame API to create a subquery.
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_test_data("t1", &ctx).await?;
    register_aggregate_test_data("t2", &ctx).await?;

    where_scalar_subquery(&ctx).await?;

    where_in_subquery(&ctx).await?;

    where_exist_subquery(&ctx).await?;

    Ok(())
}

//select c1,c2 from t1 where (select avg(t2.c2) from t2 where t1.c1 = t2.c1)>0 limit 3;
async fn where_scalar_subquery(ctx: &SessionContext) -> Result<()> {
    ctx.table("t1")
        .await?
        .filter(
            scalar_subquery(Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(out_ref_col(DataType::Utf8, "t1.c1").eq(col("t2.c1")))?
                    .aggregate(vec![], vec![avg(col("t2.c2"))])?
                    .select(vec![avg(col("t2.c2"))])?
                    .into_unoptimized_plan(),
            ))
            .gt(lit(0u8)),
        )?
        .select(vec![col("t1.c1"), col("t1.c2")])?
        .limit(0, Some(3))?
        .show()
        .await?;
    Ok(())
}

//SELECT t1.c1, t1.c2 FROM t1 WHERE t1.c2 in (select max(t2.c2) from t2 where t2.c1 > 0 ) limit 3;
async fn where_in_subquery(ctx: &SessionContext) -> Result<()> {
    ctx.table("t1")
        .await?
        .filter(in_subquery(
            col("t1.c2"),
            Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(col("t2.c1").gt(lit(ScalarValue::UInt8(Some(0)))))?
                    .aggregate(vec![], vec![max(col("t2.c2"))])?
                    .select(vec![max(col("t2.c2"))])?
                    .into_unoptimized_plan(),
            ),
        ))?
        .select(vec![col("t1.c1"), col("t1.c2")])?
        .limit(0, Some(3))?
        .show()
        .await?;
    Ok(())
}

//SELECT t1.c1, t1.c2 FROM t1 WHERE EXISTS (select t2.c2 from t2 where t1.c1 = t2.c1) limit 3;
async fn where_exist_subquery(ctx: &SessionContext) -> Result<()> {
    ctx.table("t1")
        .await?
        .filter(exists(Arc::new(
            ctx.table("t2")
                .await?
                .filter(out_ref_col(DataType::Utf8, "t1.c1").eq(col("t2.c1")))?
                .select(vec![col("t2.c2")])?
                .into_unoptimized_plan(),
        )))?
        .select(vec![col("t1.c1"), col("t1.c2")])?
        .limit(0, Some(3))?
        .show()
        .await?;
    Ok(())
}

pub async fn register_aggregate_test_data(
    name: &str,
    ctx: &SessionContext,
) -> Result<()> {
    let testdata = arrow_test_data();
    ctx.register_csv(
        name,
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await?;
    Ok(())
}
