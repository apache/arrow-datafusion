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
async fn csv_query_group_by_string_min_max() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+-----------------------------+",
        "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) |",
        "+----+-----------------------------+-----------------------------+",
        "| a  | 0.02182578039211991         | 0.9800193410444061          |",
        "| b  | 0.04893135681998029         | 0.9185813970744787          |",
        "| c  | 0.0494924465469434          | 0.991517828651004           |",
        "| d  | 0.061029375346466685        | 0.9748360509016578          |",
        "| e  | 0.01479305307777301         | 0.9965400387585364          |",
        "+----+-----------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_group_on_null() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            Some(3),
            None,
            Some(1),
            Some(3),
        ]))],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT COUNT(*), c1 FROM test GROUP BY c1";

    let actual = execute_to_batches(&ctx, sql).await;

    // Note that the results also
    // include a row for NULL (c1=NULL, count = 1)
    let expected = vec![
        "+-----------------+----+",
        "| COUNT(UInt8(1)) | c1 |",
        "+-----------------+----+",
        "| 1               |    |",
        "| 1               | 0  |",
        "| 1               | 1  |",
        "| 2               | 3  |",
        "+-----------------+----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_group_on_null_multi_col() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Utf8, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(0),
                Some(3),
                None,
                None,
                Some(3),
                Some(0),
                None,
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![
                None,
                None,
                Some("foo"),
                None,
                Some("bar"),
                Some("foo"),
                None,
                Some("bar"),
                Some("foo"),
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT COUNT(*), c1, c2 FROM test GROUP BY c1, c2";

    let actual = execute_to_batches(&ctx, sql).await;

    // Note that the results also include values for null
    // include a row for NULL (c1=NULL, count = 1)
    let expected = vec![
        "+-----------------+----+-----+",
        "| COUNT(UInt8(1)) | c1 | c2  |",
        "+-----------------+----+-----+",
        "| 1               |    |     |",
        "| 2               |    | bar |",
        "| 3               | 0  |     |",
        "| 3               | 3  | foo |",
        "+-----------------+----+-----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    // Also run query with group columns reversed (results should be the same)
    let sql = "SELECT COUNT(*), c1, c2 FROM test GROUP BY c2, c1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_group_by_date() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("cnt", DataType::Int32, false),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![
                Some(100),
                Some(100),
                Some(100),
                Some(101),
                Some(101),
                Some(101),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(3),
                Some(3),
                Some(3),
            ])),
        ],
    )?;

    ctx.register_batch("dates", data)?;
    let sql = "SELECT SUM(cnt) FROM dates GROUP BY date";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| SUM(dates.cnt) |",
        "+----------------+",
        "| 6              |",
        "| 9              |",
        "+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_group_by_time32second() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time32(TimeUnit::Second), false),
        Field::new("cnt", DataType::Int32, false),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Time32SecondArray::from(vec![
                Some(5_000),
                Some(5_000),
                Some(5_500),
                Some(5_500),
                Some(5_900),
                Some(5_900),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(1),
                Some(3),
            ])),
        ],
    )?;

    ctx.register_batch("times", data)?;
    let sql = "SELECT SUM(cnt) FROM times GROUP BY time";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| SUM(times.cnt) |",
        "+----------------+",
        "| 2              |",
        "| 3              |",
        "| 4              |",
        "+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_group_by_time32millisecond() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time32(TimeUnit::Millisecond), false),
        Field::new("cnt", DataType::Int32, false),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Time32MillisecondArray::from(vec![
                Some(5_000_000),
                Some(5_000_000),
                Some(5_500_000),
                Some(5_500_000),
                Some(5_900_000),
                Some(5_900_000),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(1),
                Some(3),
            ])),
        ],
    )?;

    ctx.register_batch("times", data)?;
    let sql = "SELECT SUM(cnt) FROM times GROUP BY time";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| SUM(times.cnt) |",
        "+----------------+",
        "| 2              |",
        "| 3              |",
        "| 4              |",
        "+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_group_by_time64microsecond() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new("cnt", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(5_000_000_000),
                Some(5_000_000_000),
                Some(5_500_000_000),
                Some(5_500_000_000),
                Some(5_900_000_000),
                Some(5_900_000_000),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(1),
                Some(3),
            ])),
        ],
    )?;

    ctx.register_batch("times", data)?;
    let sql = "SELECT SUM(cnt) FROM times GROUP BY time";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| SUM(times.cnt) |",
        "+----------------+",
        "| 2              |",
        "| 3              |",
        "| 4              |",
        "+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_group_by_time64nanosecond() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time64(TimeUnit::Nanosecond), false),
        Field::new("cnt", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Time64NanosecondArray::from(vec![
                Some(5_000_000_000_000),
                Some(5_000_000_000_000),
                Some(5_500_000_000_000),
                Some(5_500_000_000_000),
                Some(5_900_000_000_000),
                Some(5_900_000_000_000),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(1),
                Some(3),
            ])),
        ],
    )?;

    ctx.register_batch("times", data)?;
    let sql = "SELECT SUM(cnt) FROM times GROUP BY time";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| SUM(times.cnt) |",
        "+----------------+",
        "| 2              |",
        "| 3              |",
        "| 4              |",
        "+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_date_trunc() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c2", DataType::UInt64, false),
        Field::new(
            "t1",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    // generate a partitioned file
    for partition in 0..4 {
        let filename = format!("partition-{}.{}", partition, "csv");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;

        // generate some data
        for i in 0..10 {
            let data = format!("{},2020-12-{}T00:00:00.000Z\n", i, i + 10);
            file.write_all(data.as_bytes())?;
        }
    }

    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let results = plan_and_collect(
        &ctx,
        "SELECT date_trunc('week', t1) as week, SUM(c2) FROM test GROUP BY date_trunc('week', t1)",
    ).await?;

    let expected = vec![
        "+---------------------+--------------+",
        "| week                | SUM(test.c2) |",
        "+---------------------+--------------+",
        "| 2020-12-07T00:00:00 | 24           |",
        "| 2020-12-14T00:00:00 | 156          |",
        "+---------------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn group_by_largeutf8() {
    let ctx = SessionContext::new();

    // input data looks like:
    // A, 1
    // B, 2
    // A, 2
    // A, 4
    // C, 1
    // A, 1

    let str_array: LargeStringArray = vec!["A", "B", "A", "A", "C", "A"]
        .into_iter()
        .map(Some)
        .collect();
    let str_array = Arc::new(str_array);

    let val_array: Int64Array = vec![1, 2, 2, 4, 1, 1].into();
    let val_array = Arc::new(val_array);

    let schema = Arc::new(Schema::new(vec![
        Field::new("str", str_array.data_type().clone(), false),
        Field::new("val", val_array.data_type().clone(), false),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![str_array, val_array]).unwrap();

    ctx.register_batch("t", batch).unwrap();

    let results = plan_and_collect(&ctx, "SELECT str, count(val) FROM t GROUP BY str")
        .await
        .expect("ran plan correctly");

    let expected = vec![
        "+-----+--------------+",
        "| str | COUNT(t.val) |",
        "+-----+--------------+",
        "| A   | 4            |",
        "| B   | 1            |",
        "| C   | 1            |",
        "+-----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn group_by_dictionary() {
    async fn run_test_case<K: ArrowDictionaryKeyType>() {
        let ctx = SessionContext::new();

        // input data looks like:
        // A, 1
        // B, 2
        // A, 2
        // A, 4
        // C, 1
        // A, 1

        let dict_array: DictionaryArray<K> =
            vec!["A", "B", "A", "A", "C", "A"].into_iter().collect();
        let dict_array = Arc::new(dict_array);

        let val_array: Int64Array = vec![1, 2, 2, 4, 1, 1].into();
        let val_array = Arc::new(val_array);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_array.data_type().clone(), false),
            Field::new("val", val_array.data_type().clone(), false),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![dict_array, val_array]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let results =
            plan_and_collect(&ctx, "SELECT dict, count(val) FROM t GROUP BY dict")
                .await
                .expect("ran plan correctly");

        let expected = vec![
            "+------+--------------+",
            "| dict | COUNT(t.val) |",
            "+------+--------------+",
            "| A    | 4            |",
            "| B    | 1            |",
            "| C    | 1            |",
            "+------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        // Now, use dict as an aggregate
        let results =
            plan_and_collect(&ctx, "SELECT val, count(dict) FROM t GROUP BY val")
                .await
                .expect("ran plan correctly");

        let expected = vec![
            "+-----+---------------+",
            "| val | COUNT(t.dict) |",
            "+-----+---------------+",
            "| 1   | 3             |",
            "| 2   | 2             |",
            "| 4   | 1             |",
            "+-----+---------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        // Now, use dict as an aggregate
        let results = plan_and_collect(
            &ctx,
            "SELECT val, count(distinct dict) FROM t GROUP BY val",
        )
        .await
        .expect("ran plan correctly");

        let expected = vec![
            "+-------+------------------------+",
            "| t.val | COUNT(DISTINCT t.dict) |",
            "+-------+------------------------+",
            "| 1     | 2                      |",
            "| 2     | 2                      |",
            "| 4     | 1                      |",
            "+-------+------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
    }

    run_test_case::<Int8Type>().await;
    run_test_case::<Int16Type>().await;
    run_test_case::<Int32Type>().await;
    run_test_case::<Int64Type>().await;
    run_test_case::<UInt8Type>().await;
    run_test_case::<UInt16Type>().await;
    run_test_case::<UInt32Type>().await;
    run_test_case::<UInt64Type>().await;
}
