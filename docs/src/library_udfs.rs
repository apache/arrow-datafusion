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

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::{create_udf, SessionContext};
use std::sync::Arc;
use tokio;

//begin:add_one
fn add_one(args: &[ArrayRef]) -> Result<ArrayRef> {
    let i64s = as_int64_array(&args[0])?;

    let new_array = i64s
        .iter()
        .map(|array_elem| array_elem.map(|value| value + 1))
        .collect::<Int64Array>();

    Ok(Arc::new(new_array))
}
//end:add_one

#[test]
fn call_add_one() -> Result<()> {
    //begin:call_add_one
    let input = vec![Some(1), None, Some(3)];
    let input = Arc::new(Int64Array::from(input)) as ArrayRef;

    let result = add_one(&[input])?;
    let result = result
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("result is Int64Array");

    assert_eq!(result, &Int64Array::from(vec![Some(2), None, Some(4)]));
    //end:call_add_one

    Ok(())
}

#[test]
fn register_udf() -> Result<()> {
    //begin:create_udf
    let udf = create_udf(
        "add_one",
        vec![DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        make_scalar_function(add_one),
    );
    //end:create_udf
    //begin:register_udf
    let ctx = SessionContext::new();
    ctx.register_udf(udf);
    //end:register_udf
    Ok(())
}

#[tokio::test]
async fn call_udf() -> Result<()> {
    let udf = create_udf(
        "add_one",
        vec![DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        make_scalar_function(add_one),
    );
    let ctx = SessionContext::new();
    //begin:call_udf
    let sql = "SELECT add_one(1)";
    let df = ctx.sql(&sql).await?;
    //end:call_udf
    Ok(())
}
