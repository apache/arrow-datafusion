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

use super::*;

#[tokio::test]
async fn sqrt_f32_vs_f64() -> Result<()> {
    let ctx = create_ctx();
    register_aggregate_csv(&ctx).await?;
    // sqrt(f32)'s plan passes
    let sql = "SELECT avg(sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let sql = "SELECT avg(CAST(sqrt(c11) AS double)) FROM aggregate_test_100";
    let expected = execute(&ctx, sql).await;

    assert_eq!(actual, expected);
    let sql = "SELECT avg(sqrt(CAST(c11 AS double))) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT CAST(c12 AS float) FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------------------+",
        "| aggregate_test_100.c12 |",
        "+------------------------+",
        "| 0.39144436             |",
        "| 0.3887028              |",
        "+------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast_literal() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT c12, CAST(1 AS float) FROM aggregate_test_100 WHERE c12 > CAST(0 AS float) LIMIT 2";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+--------------------+----------+",
        "| c12                | Int64(1) |",
        "+--------------------+----------+",
        "| 0.9294097332465232 | 1.0      |",
        "| 0.3114712539863804 | 1.0      |",
        "+--------------------+----------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_concat() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_slice(["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT concat(c1, '-hi-', cast(c2 as varchar)) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------+",
        "| concat(test.c1,Utf8(\"-hi-\"),test.c2) |",
        "+--------------------------------------+",
        "| -hi-0                                |",
        "| a-hi-1                               |",
        "| aa-hi-                               |",
        "| aaa-hi-3                             |",
        "+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_array() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_slice(["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT make_array(c1, cast(c2 as varchar)) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| makearray(test.c1,test.c2) |",
        "+----------------------------+",
        "| [, 0]                      |",
        "| [a, 1]                     |",
        "| [aa, ]                     |",
        "| [aaa, 3]                   |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_array_scalar() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT make_array(1, 2, 3);";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------------+",
        "| makearray(Int64(1),Int64(2),Int64(3)) |",
        "+---------------------------------------+",
        "| [1, 2, 3]                             |",
        "+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_static_empty_value() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT COALESCE('', 'test')";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------+",
        "| coalesce(Utf8(\"\"),Utf8(\"test\")) |",
        "+---------------------------------+",
        "|                                 |",
        "+---------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_static_value_with_null() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT COALESCE(NULL, 'test')";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------+",
        "| coalesce(NULL,Utf8(\"test\")) |",
        "+-----------------------------+",
        "| test                        |",
        "+-----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_result() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), None, Some(1), None, None])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(0),
                Some(1),
                None,
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT COALESCE(c1, c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------+",
        "| coalesce(test.c1,test.c2) |",
        "+---------------------------+",
        "| 0                         |",
        "| 1                         |",
        "| 1                         |",
        "| 1                         |",
        "|                           |",
        "+---------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_result_with_default_value() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), None, Some(1), None, None])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(0),
                Some(1),
                None,
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT COALESCE(c1, c2, '-1') FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------+",
        "| coalesce(test.c1,test.c2,Utf8(\"-1\")) |",
        "+--------------------------------------+",
        "| 0                                    |",
        "| 1                                    |",
        "| 1                                    |",
        "| 1                                    |",
        "| -1                                   |",
        "+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_sum_with_default_value() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(1), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(2), None, None])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT SUM(COALESCE(c1, c2, 0)) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------------------+",
        "| SUM(coalesce(test.c1,test.c2,Int64(0))) |",
        "+-----------------------------------------+",
        "| 4                                       |",
        "+-----------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn coalesce_mul_with_default_value() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(1), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(2), None, None])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT COALESCE(c1 * c2, 0) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------+",
        "| coalesce(test.c1 * test.c2,Int64(0)) |",
        "+--------------------------------------+",
        "| 2                                    |",
        "| 0                                    |",
        "| 0                                    |",
        "| 0                                    |",
        "+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn case_sensitive_identifiers_functions() {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let expected = vec![
        "+-----------+",
        "| sqrt(t.i) |",
        "+-----------+",
        "| 1.0       |",
        "+-----------+",
    ];

    let results = plan_and_collect(&ctx, "SELECT sqrt(i) FROM t")
        .await
        .unwrap();

    assert_batches_sorted_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT SQRT(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);

    // Using double quotes allows specifying the function name with capitalization
    let err = plan_and_collect(&ctx, "SELECT \"SQRT\"(i) FROM t")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Invalid function 'SQRT'"
    );

    let results = plan_and_collect(&ctx, "SELECT \"sqrt\"(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn case_builtin_math_expression() {
    let ctx = SessionContext::new();

    let type_values = vec![
        (
            DataType::Int8,
            Arc::new(Int8Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Int16,
            Arc::new(Int16Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Int32,
            Arc::new(Int32Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Int64,
            Arc::new(Int64Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt8,
            Arc::new(UInt8Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt16,
            Arc::new(UInt16Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt32,
            Arc::new(UInt32Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from_slice([1])) as ArrayRef,
        ),
        (
            DataType::Float32,
            Arc::new(Float32Array::from_slice([1.0_f32])) as ArrayRef,
        ),
        (
            DataType::Float64,
            Arc::new(Float64Array::from_slice([1.0_f64])) as ArrayRef,
        ),
    ];

    for (data_type, array) in type_values.iter() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", data_type.clone(), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
        ctx.deregister_table("t").unwrap();
        ctx.register_batch("t", batch).unwrap();
        let expected = vec![
            "+-----------+",
            "| sqrt(t.v) |",
            "+-----------+",
            "| 1.0       |",
            "+-----------+",
        ];
        let results = plan_and_collect(&ctx, "SELECT sqrt(v) FROM t")
            .await
            .unwrap();

        assert_batches_sorted_eq!(expected, &results);
    }
}

#[tokio::test]
async fn test_power() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int16, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int16Array::from(vec![
                Some(2),
                Some(5),
                Some(0),
                Some(-14),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(5),
                Some(0),
                Some(-14),
                None,
            ])),
            Arc::new(Float32Array::from(vec![
                Some(1.0),
                Some(2.5),
                Some(0.0),
                Some(-14.5),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(2.5),
                Some(0.0),
                Some(-14.5),
                None,
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = r"SELECT power(i32, exp_i) as power_i32,
                 power(i64, exp_f) as power_i64,
                 pow(f32, exp_i) as power_f32,
                 power(f64, exp_f) as power_f64,
                 pow(2, 3) as power_int_scalar,
                 power(2.5, 3.0) as power_float_scalar
          FROM (select test.*, 3 as exp_i, 3.0 as exp_f from test) a";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+-----------+-----------+-----------+------------------+--------------------+",
        "| power_i32 | power_i64 | power_f32 | power_f64 | power_int_scalar | power_float_scalar |",
        "+-----------+-----------+-----------+-----------+------------------+--------------------+",
        "| 8         | 8.0       | 1.0       | 1.0       | 8                | 15.625             |",
        "| 125       | 125.0     | 15.625    | 15.625    | 8                | 15.625             |",
        "| 0         | 0.0       | 0.0       | 0.0       | 8                | 15.625             |",
        "| -2744     | -2744.0   | -3048.625 | -3048.625 | 8                | 15.625             |",
        "|           |           |           |           | 8                | 15.625             |",
        "+-----------+-----------+-----------+-----------+------------------+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    //dbg!(actual[0].schema().fields());
    assert_eq!(
        actual[0]
            .schema()
            .field_with_name("power_i32")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Int64
    );
    assert_eq!(
        actual[0]
            .schema()
            .field_with_name("power_i64")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Float64
    );
    assert_eq!(
        actual[0]
            .schema()
            .field_with_name("power_f32")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Float64
    );
    assert_eq!(
        actual[0]
            .schema()
            .field_with_name("power_f64")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Float64
    );
    assert_eq!(
        actual[0]
            .schema()
            .field_with_name("power_int_scalar")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Int64
    );
    assert_eq!(
        actual[0]
            .schema()
            .field_with_name("power_float_scalar")
            .unwrap()
            .data_type()
            .to_owned(),
        DataType::Float64
    );

    Ok(())
}

#[tokio::test]
async fn if_static_value() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT IF(true, 1, 2)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------------+",
        "| if(Boolean(true),Int64(1),Int64(2)) |",
        "+-------------------------------------+",
        "| 1                                   |",
        "+-------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT IF(false, 1, 2)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------+",
        "| if(Boolean(false),Int64(1),Int64(2)) |",
        "+--------------------------------------+",
        "| 2                                    |",
        "+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT IF(1 = 2, 1 + 5, 2 + 4)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------------------------------------------+",
        "| if(Int64(1) = Int64(2),Int64(1) + Int64(5),Int64(2) + Int64(4)) |",
        "+-----------------------------------------------------------------+",
        "| 6                                                               |",
        "+-----------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT IF(null, 1, 2)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| if(NULL,Int64(1),Int64(2)) |",
        "+----------------------------+",
        "| 2                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn if_normal() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(2), Some(1), None])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT IF(c1 > c2, c1, c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------------+",
        "| if(test.c1 > test.c2,test.c1,test.c2) |",
        "+---------------------------------------+",
        "| 2                                     |",
        "| 2                                     |",
        "| 3                                     |",
        "|                                       |",
        "+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int64, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None])),
            Arc::new(Int64Array::from(vec![
                Some(2i64),
                Some(5i64),
                Some(1i64),
                None,
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT IF(c1 > c2, c1, c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------------+",
        "| if(test.c1 > test.c2,test.c1,test.c2) |",
        "+---------------------------------------+",
        "| 2                                     |",
        "| 5                                     |",
        "| 3                                     |",
        "|                                       |",
        "+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
