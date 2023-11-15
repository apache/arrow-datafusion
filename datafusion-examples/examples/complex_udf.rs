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

use datafusion::{
    arrow::{
        array::{Float32Array, Float64Array},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    logical_expr::Volatility,
};

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_expr::function::{ConstantArg, ReturnTypeFactory};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Signature,
};
use std::sync::Arc;

// create local execution context with an in-memory table
fn create_context() -> Result<SessionContext> {
    use datafusion::arrow::datatypes::{Field, Schema};
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float32Array::from(vec![2.1, 3.1, 4.1, 5.1, 6.1])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    const UDF_NAME: &str = "take";

    let ctx = create_context()?;

    // Syntax:
    //     `take(float32_expr, float64_expr, index)`
    // If index eq 0, return float32_expr, which DataType is DataType::Float32;
    // If index eq 1, return float64_expr, which DataType is DataType::Float64;
    // Else return Err.
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let take_idx = match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) if v < &2 => *v as usize,
            _ => unreachable!(),
        };
        match &args[take_idx] {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(array.clone())),
            ColumnarValue::Scalar(_) => unimplemented!(),
        }
    });

    // Implement a ReturnTypeFactory.
    struct ReturnType;

    impl ReturnTypeFactory for ReturnType {
        fn infer(
            &self,
            input_types: &[DataType],
            constant_args: &[ConstantArg],
        ) -> Result<Arc<DataType>> {
            assert_eq!(constant_args.len(), 1);
            let (idx, val) = &constant_args[0];
            assert_eq!(idx, &2);

            let take_idx = match val {
                ScalarValue::Int64(Some(v)) if v < &2 => *v as usize,
                _ => unreachable!(),
            };

            Ok(Arc::new(input_types[take_idx].clone()))
        }
    }

    let signature = Signature::exact(
        vec![DataType::Float32, DataType::Float64, DataType::Int64],
        Volatility::Immutable,
    );

    let udf = ScalarUDF {
        name: UDF_NAME.to_string(),
        signature,
        return_type: Arc::new(ReturnType {}),
        fun,
    };

    ctx.register_udf(udf);

    // SELECT take(a, b, 0) AS take0, take(a, b, 1) AS take1 FROM t;
    let df = ctx.table("t").await?;
    let take = df.registry().udf(UDF_NAME)?;
    let expr0 = take
        .call(vec![col("a"), col("b"), lit(0_i64)])
        .alias("take0");
    let expr1 = take
        .call(vec![col("a"), col("b"), lit(1_i64)])
        .alias("take1");

    let df = df.select(vec![expr0, expr1])?;
    let schema = df.schema();

    // Check output schema
    assert_eq!(schema.field(0).data_type(), &DataType::Float32);
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);

    df.show().await?;

    Ok(())
}
