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
async fn csv_query_group_by_int_min_max() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c2, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+-----------------------------+",
        "| c2 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) |",
        "+----+-----------------------------+-----------------------------+",
        "| 1  | 0.05636955101974106         | 0.9965400387585364          |",
        "| 2  | 0.16301110515739792         | 0.991517828651004           |",
        "| 3  | 0.047343434291126085        | 0.9293883502480845          |",
        "| 4  | 0.02182578039211991         | 0.9237877978193884          |",
        "| 5  | 0.01479305307777301         | 0.9723580396501548          |",
        "+----+-----------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_float32() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql =
        "SELECT COUNT(*) as cnt, c1 FROM aggregate_simple GROUP BY c1 ORDER BY cnt DESC";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-----+---------+",
        "| cnt | c1      |",
        "+-----+---------+",
        "| 5   | 0.00005 |",
        "| 4   | 0.00004 |",
        "| 3   | 0.00003 |",
        "| 2   | 0.00002 |",
        "| 1   | 0.00001 |",
        "+-----+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_float64() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql =
        "SELECT COUNT(*) as cnt, c2 FROM aggregate_simple GROUP BY c2 ORDER BY cnt DESC";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-----+---------+",
        "| cnt | c2      |",
        "+-----+---------+",
        "| 5   | 5.0e-12 |",
        "| 4   | 4.0e-12 |",
        "| 3   | 3.0e-12 |",
        "| 2   | 2.0e-12 |",
        "| 1   | 1.0e-12 |",
        "+-----+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_boolean() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql =
        "SELECT COUNT(*) as cnt, c3 FROM aggregate_simple GROUP BY c3 ORDER BY cnt DESC";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-----+-------+",
        "| cnt | c3    |",
        "+-----+-------+",
        "| 9   | true  |",
        "| 6   | false |",
        "+-----+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_boolean2() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql =
        "SELECT COUNT(*), c3 FROM aggregate_simple GROUP BY c3 ORDER BY COUNT(*) DESC";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-----------------+-------+",
        "| COUNT(UInt8(1)) | c3    |",
        "+-----------------+-------+",
        "| 9               | true  |",
        "| 6               | false |",
        "+-----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_two_columns() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c2, MIN(c3) FROM aggregate_test_100 GROUP BY c1, c2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+----------------------------+",
        "| c1 | c2 | MIN(aggregate_test_100.c3) |",
        "+----+----+----------------------------+",
        "| a  | 1  | -85                        |",
        "| a  | 2  | -48                        |",
        "| a  | 3  | -72                        |",
        "| a  | 4  | -101                       |",
        "| a  | 5  | -101                       |",
        "| b  | 1  | 12                         |",
        "| b  | 2  | -60                        |",
        "| b  | 3  | -101                       |",
        "| b  | 4  | -117                       |",
        "| b  | 5  | -82                        |",
        "| c  | 1  | -24                        |",
        "| c  | 2  | -117                       |",
        "| c  | 3  | -2                         |",
        "| c  | 4  | -90                        |",
        "| c  | 5  | -94                        |",
        "| d  | 1  | -99                        |",
        "| d  | 2  | 93                         |",
        "| d  | 3  | -76                        |",
        "| d  | 4  | 5                          |",
        "| d  | 5  | -59                        |",
        "| e  | 1  | 36                         |",
        "| e  | 2  | -61                        |",
        "| e  | 3  | -95                        |",
        "| e  | 4  | -56                        |",
        "| e  | 5  | -86                        |",
        "+----+----+----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_and_having() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, MIN(c3) AS m FROM aggregate_test_100 GROUP BY c1 HAVING m < -100 AND MAX(c3) > 70";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+------+",
        "| c1 | m    |",
        "+----+------+",
        "| a  | -101 |",
        "| c  | -117 |",
        "+----+------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_and_having_and_where() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, MIN(c3) AS m
               FROM aggregate_test_100
               WHERE c1 IN ('a', 'b')
               GROUP BY c1
               HAVING m < -100 AND MAX(c3) > 70";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+------+",
        "| c1 | m    |",
        "+----+------+",
        "| a  | -101 |",
        "+----+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_substr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    // there is an input column "c1" as well a projection expression aliased as "c1"
    let sql = "SELECT substr(c1, 1, 1) c1 \
        FROM aggregate_test_100 \
        GROUP BY substr(c1, 1, 1) \
        ";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| c1 |",
        "+----+",
        "| a  |",
        "| b  |",
        "| c  |",
        "| d  |",
        "| e  |",
        "+----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_avg() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+",
        "| c1 | AVG(aggregate_test_100.c12) |",
        "+----+-----------------------------+",
        "| a  | 0.48754517466109415         |",
        "| b  | 0.41040709263815384         |",
        "| c  | 0.6600456536439784          |",
        "| d  | 0.48855379387549824         |",
        "| e  | 0.48600669271341534         |",
        "+----+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_with_aliases() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 AS c12, avg(c12) AS c1 FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+---------------------+",
        "| c12 | c1                  |",
        "+-----+---------------------+",
        "| a   | 0.48754517466109415 |",
        "| b   | 0.41040709263815384 |",
        "| c   | 0.6600456536439784  |",
        "| d   | 0.48855379387549824 |",
        "| e   | 0.48600669271341534 |",
        "+-----+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_int_count() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, count(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-------------------------------+",
        "| c1 | COUNT(aggregate_test_100.c12) |",
        "+----+-------------------------------+",
        "| a  | 21                            |",
        "| b  | 19                            |",
        "| c  | 21                            |",
        "| d  | 18                            |",
        "| e  | 21                            |",
        "+----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_with_aliased_aggregate() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, count(c12) AS count FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-------+",
        "| c1 | count |",
        "+----+-------+",
        "| a  | 21    |",
        "| b  | 19    |",
        "| c  | 21    |",
        "| d  | 18    |",
        "| e  | 21    |",
        "+----+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

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

#[tokio::test]
async fn csv_query_group_by_order_by_substr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT substr(c1, 1, 1), avg(c12) \
        FROM aggregate_test_100 \
        GROUP BY substr(c1, 1, 1) \
        ORDER BY substr(c1, 1, 1)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------------------------+-----------------------------+",
        "| substr(aggregate_test_100.c1,Int64(1),Int64(1)) | AVG(aggregate_test_100.c12) |",
        "+-------------------------------------------------+-----------------------------+",
        "| a                                               | 0.48754517466109415         |",
        "| b                                               | 0.41040709263815384         |",
        "| c                                               | 0.6600456536439784          |",
        "| d                                               | 0.48855379387549824         |",
        "| e                                               | 0.48600669271341534         |",
        "+-------------------------------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_order_by_substr_aliased_projection() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT substr(c1, 1, 1) as name, avg(c12) as average \
        FROM aggregate_test_100 \
        GROUP BY substr(c1, 1, 1) \
        ORDER BY substr(c1, 1, 1)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------+---------------------+",
        "| name | average             |",
        "+------+---------------------+",
        "| a    | 0.48754517466109415 |",
        "| b    | 0.41040709263815384 |",
        "| c    | 0.6600456536439784  |",
        "| d    | 0.48855379387549824 |",
        "| e    | 0.48600669271341534 |",
        "+------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_order_by_avg_group_by_substr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT substr(c1, 1, 1) as name, avg(c12) as average \
        FROM aggregate_test_100 \
        GROUP BY substr(c1, 1, 1) \
        ORDER BY avg(c12)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------+---------------------+",
        "| name | average             |",
        "+------+---------------------+",
        "| b    | 0.41040709263815384 |",
        "| e    | 0.48600669271341534 |",
        "| a    | 0.48754517466109415 |",
        "| d    | 0.48855379387549824 |",
        "| c    | 0.6600456536439784  |",
        "+------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_with_dup_group_set() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12) FROM aggregate_test_100 GROUP BY GROUPING SETS((c1),(c1),())";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Projection: aggregate_test_100.c1, AVG(aggregate_test_100.c12) [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N]",
        "  Aggregate: groupBy=[[GROUPING SETS ((aggregate_test_100.c1, UInt32(1) AS #grouping_set_id), (aggregate_test_100.c1, UInt32(2) AS #grouping_set_id), (UInt32(3) AS #grouping_set_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, #grouping_set_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "    TableScan: aggregate_test_100 projection=[c1, c12] [c1:Utf8, c12:Float64]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+",
        "| c1 | AVG(aggregate_test_100.c12) |",
        "+----+-----------------------------+",
        "|    | 0.5089725099127211          |",
        "| a  | 0.48754517466109415         |",
        "| b  | 0.41040709263815384         |",
        "| c  | 0.6600456536439784          |",
        "| d  | 0.48855379387549824         |",
        "| e  | 0.48600669271341534         |",
        "| a  | 0.48754517466109415         |",
        "| b  | 0.41040709263815384         |",
        "| c  | 0.6600456536439784          |",
        "| d  | 0.48855379387549824         |",
        "| e  | 0.48600669271341534         |",
        "+----+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_with_grouping_id_func() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c2, c3, GROUPING_ID(c1, c2, c3), avg(c12) FROM \
    (select c1, c2, '0' as c3, c12 from aggregate_test_100)
    GROUP BY GROUPING SETS((c1, c2), (c1, c3), (c3),())";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Projection: aggregate_test_100.c1, aggregate_test_100.c2, c3, #grouping_id AS GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2,c3), AVG(aggregate_test_100.c12) [c1:Utf8, c2:UInt32, c3:Utf8, GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2,c3):UInt32;N, AVG(aggregate_test_100.c12):Float64;N]",
        "  Aggregate: groupBy=[[GROUPING SETS ((aggregate_test_100.c1, aggregate_test_100.c2, UInt32(1) AS #grouping_id), (aggregate_test_100.c1, c3, UInt32(2) AS #grouping_id), (c3, UInt32(6) AS #grouping_id), (UInt32(7) AS #grouping_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, c2:UInt32, c3:Utf8, #grouping_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "    Projection: aggregate_test_100.c1, aggregate_test_100.c2, Utf8(\"0\") AS c3, aggregate_test_100.c12 [c1:Utf8, c2:UInt32, c3:Utf8, c12:Float64]",
        "      TableScan: aggregate_test_100 projection=[c1, c2, c12] [c1:Utf8, c2:UInt32, c12:Float64]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+----+-------------------------------------------------------------+-----------------------------+",
        "| c1 | c2 | c3 | GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2,c3) | AVG(aggregate_test_100.c12) |",
        "+----+----+----+-------------------------------------------------------------+-----------------------------+",
        "|    |    |    | 7                                                           | 0.5089725099127211          |",
        "|    |    | 0  | 6                                                           | 0.5089725099127211          |",
        "| a  |    | 0  | 2                                                           | 0.48754517466109415         |",
        "| a  | 1  |    | 1                                                           | 0.4693685626367209          |",
        "| a  | 2  |    | 1                                                           | 0.5945188963859894          |",
        "| a  | 3  |    | 1                                                           | 0.5996111195922015          |",
        "| a  | 4  |    | 1                                                           | 0.3653038379118398          |",
        "| a  | 5  |    | 1                                                           | 0.3497223654469457          |",
        "| b  |    | 0  | 2                                                           | 0.41040709263815384         |",
        "| b  | 1  |    | 1                                                           | 0.16148594845154118         |",
        "| b  | 2  |    | 1                                                           | 0.5857678873564655          |",
        "| b  | 3  |    | 1                                                           | 0.42804338065410286         |",
        "| b  | 4  |    | 1                                                           | 0.33400957036260354         |",
        "| b  | 5  |    | 1                                                           | 0.4888141504446429          |",
        "| c  |    | 0  | 2                                                           | 0.6600456536439784          |",
        "| c  | 1  |    | 1                                                           | 0.6430620563927849          |",
        "| c  | 2  |    | 1                                                           | 0.7736013221256991          |",
        "| c  | 3  |    | 1                                                           | 0.421733279717472           |",
        "| c  | 4  |    | 1                                                           | 0.6827805579021969          |",
        "| c  | 5  |    | 1                                                           | 0.7277229477969185          |",
        "| d  |    | 0  | 2                                                           | 0.48855379387549824         |",
        "| d  | 1  |    | 1                                                           | 0.49931809179640024         |",
        "| d  | 2  |    | 1                                                           | 0.5181987328311988          |",
        "| d  | 3  |    | 1                                                           | 0.586369575965718           |",
        "| d  | 4  |    | 1                                                           | 0.49575895804943215         |",
        "| d  | 5  |    | 1                                                           | 0.2488799233225611          |",
        "| e  |    | 0  | 2                                                           | 0.48600669271341534         |",
        "| e  | 1  |    | 1                                                           | 0.780297346359783           |",
        "| e  | 2  |    | 1                                                           | 0.660795726704708           |",
        "| e  | 3  |    | 1                                                           | 0.5165824734324667          |",
        "| e  | 4  |    | 1                                                           | 0.2720288398836001          |",
        "| e  | 5  |    | 1                                                           | 0.29536905073188496         |",
        "+----+----+----+-------------------------------------------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_with_multi_grouping_funcs() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c2, GROUPING(C1), GROUPING(C2), GROUPING_ID(c1, c2), avg(c12) FROM aggregate_test_100 GROUP BY CUBE(c1, c2)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Projection: aggregate_test_100.c1, aggregate_test_100.c2, CAST((#grouping_id AS #grouping_id AS #grouping_id AS #grouping_id >> UInt32(1)) & UInt32(1) AS UInt8) AS GROUPING(aggregate_test_100.c1), CAST(#grouping_id AS #grouping_id AS #grouping_id AS #grouping_id & UInt32(1) AS UInt8) AS GROUPING(aggregate_test_100.c2), #grouping_id AS #grouping_id AS #grouping_id AS #grouping_id AS GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2), AVG(aggregate_test_100.c12) [c1:Utf8, c2:UInt32, GROUPING(aggregate_test_100.c1):UInt8;N, GROUPING(aggregate_test_100.c2):UInt8;N, GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2):UInt32;N, AVG(aggregate_test_100.c12):Float64;N]",
        "  Aggregate: groupBy=[[GROUPING SETS ((UInt32(3) AS #grouping_id), (aggregate_test_100.c1, UInt32(1) AS #grouping_id), (aggregate_test_100.c2, UInt32(2) AS #grouping_id), (aggregate_test_100.c1, aggregate_test_100.c2, UInt32(0) AS #grouping_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, c2:UInt32, #grouping_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "    TableScan: aggregate_test_100 projection=[c1, c2, c12] [c1:Utf8, c2:UInt32, c12:Float64]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+---------------------------------+---------------------------------+----------------------------------------------------------+-----------------------------+",
        "| c1 | c2 | GROUPING(aggregate_test_100.c1) | GROUPING(aggregate_test_100.c2) | GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2) | AVG(aggregate_test_100.c12) |",
        "+----+----+---------------------------------+---------------------------------+----------------------------------------------------------+-----------------------------+",
        "|    |    | 1                               | 1                               | 3                                                        | 0.5089725099127211          |",
        "|    | 1  | 1                               | 0                               | 2                                                        | 0.5108939802619781          |",
        "|    | 2  | 1                               | 0                               | 2                                                        | 0.6545641966127662          |",
        "|    | 3  | 1                               | 0                               | 2                                                        | 0.5245329062820169          |",
        "|    | 4  | 1                               | 0                               | 2                                                        | 0.40234192123489837         |",
        "|    | 5  | 1                               | 0                               | 2                                                        | 0.4312272637333415          |",
        "| a  |    | 0                               | 1                               | 1                                                        | 0.48754517466109415         |",
        "| a  | 1  | 0                               | 0                               | 0                                                        | 0.4693685626367209          |",
        "| a  | 2  | 0                               | 0                               | 0                                                        | 0.5945188963859894          |",
        "| a  | 3  | 0                               | 0                               | 0                                                        | 0.5996111195922015          |",
        "| a  | 4  | 0                               | 0                               | 0                                                        | 0.3653038379118398          |",
        "| a  | 5  | 0                               | 0                               | 0                                                        | 0.3497223654469457          |",
        "| b  |    | 0                               | 1                               | 1                                                        | 0.41040709263815384         |",
        "| b  | 1  | 0                               | 0                               | 0                                                        | 0.16148594845154118         |",
        "| b  | 2  | 0                               | 0                               | 0                                                        | 0.5857678873564655          |",
        "| b  | 3  | 0                               | 0                               | 0                                                        | 0.42804338065410286         |",
        "| b  | 4  | 0                               | 0                               | 0                                                        | 0.33400957036260354         |",
        "| b  | 5  | 0                               | 0                               | 0                                                        | 0.4888141504446429          |",
        "| c  |    | 0                               | 1                               | 1                                                        | 0.6600456536439784          |",
        "| c  | 1  | 0                               | 0                               | 0                                                        | 0.6430620563927849          |",
        "| c  | 2  | 0                               | 0                               | 0                                                        | 0.7736013221256991          |",
        "| c  | 3  | 0                               | 0                               | 0                                                        | 0.421733279717472           |",
        "| c  | 4  | 0                               | 0                               | 0                                                        | 0.6827805579021969          |",
        "| c  | 5  | 0                               | 0                               | 0                                                        | 0.7277229477969185          |",
        "| d  |    | 0                               | 1                               | 1                                                        | 0.48855379387549824         |",
        "| d  | 1  | 0                               | 0                               | 0                                                        | 0.49931809179640024         |",
        "| d  | 2  | 0                               | 0                               | 0                                                        | 0.5181987328311988          |",
        "| d  | 3  | 0                               | 0                               | 0                                                        | 0.586369575965718           |",
        "| d  | 4  | 0                               | 0                               | 0                                                        | 0.49575895804943215         |",
        "| d  | 5  | 0                               | 0                               | 0                                                        | 0.2488799233225611          |",
        "| e  |    | 0                               | 1                               | 1                                                        | 0.48600669271341534         |",
        "| e  | 1  | 0                               | 0                               | 0                                                        | 0.780297346359783           |",
        "| e  | 2  | 0                               | 0                               | 0                                                        | 0.660795726704708           |",
        "| e  | 3  | 0                               | 0                               | 0                                                        | 0.5165824734324667          |",
        "| e  | 4  | 0                               | 0                               | 0                                                        | 0.2720288398836001          |",
        "| e  | 5  | 0                               | 0                               | 0                                                        | 0.29536905073188496         |",
        "+----+----+---------------------------------+---------------------------------+----------------------------------------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn group_by_with_dup_group_set_and_grouping_func() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12), GROUPING(C1) FROM aggregate_test_100 GROUP BY GROUPING SETS((c1),(c1),())";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Projection: aggregate_test_100.c1, AVG(aggregate_test_100.c12), CAST(#grouping_id & UInt32(1) AS UInt8) AS GROUPING(aggregate_test_100.c1) [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N, GROUPING(aggregate_test_100.c1):UInt8;N]",
        "  Aggregate: groupBy=[[GROUPING SETS ((aggregate_test_100.c1, UInt32(0) AS #grouping_id, UInt32(1) AS #grouping_set_id), (aggregate_test_100.c1, UInt32(0) AS #grouping_id, UInt32(2) AS #grouping_set_id), (UInt32(1) AS #grouping_id, UInt32(3) AS #grouping_set_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, #grouping_id:UInt32, #grouping_set_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "    TableScan: aggregate_test_100 projection=[c1, c12] [c1:Utf8, c12:Float64]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+---------------------------------+",
        "| c1 | AVG(aggregate_test_100.c12) | GROUPING(aggregate_test_100.c1) |",
        "+----+-----------------------------+---------------------------------+",
        "|    | 0.5089725099127211          | 1                               |",
        "| a  | 0.48754517466109415         | 0                               |",
        "| a  | 0.48754517466109415         | 0                               |",
        "| b  | 0.41040709263815384         | 0                               |",
        "| b  | 0.41040709263815384         | 0                               |",
        "| c  | 0.6600456536439784          | 0                               |",
        "| c  | 0.6600456536439784          | 0                               |",
        "| d  | 0.48855379387549824         | 0                               |",
        "| d  | 0.48855379387549824         | 0                               |",
        "| e  | 0.48600669271341534         | 0                               |",
        "| e  | 0.48600669271341534         | 0                               |",
        "+----+-----------------------------+---------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_with_grouping_func_and_having() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12), GROUPING(C1) FROM aggregate_test_100 \
    GROUP BY GROUPING SETS((c1),(c1),()) HAVING GROUPING(C1) = 1 and avg(c12) > 0";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Projection: aggregate_test_100.c1, AVG(aggregate_test_100.c12), CAST(#grouping_id & UInt32(1) AS UInt8) AS GROUPING(aggregate_test_100.c1) [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N, GROUPING(aggregate_test_100.c1):UInt8;N]",
        "  Filter: (#grouping_id & UInt32(1)) = UInt32(1) AND AVG(aggregate_test_100.c12) > Float64(0) [c1:Utf8, #grouping_id:UInt32, #grouping_set_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "    Aggregate: groupBy=[[GROUPING SETS ((aggregate_test_100.c1, UInt32(0) AS #grouping_id, UInt32(1) AS #grouping_set_id), (aggregate_test_100.c1, UInt32(0) AS #grouping_id, UInt32(2) AS #grouping_set_id), (UInt32(1) AS #grouping_id, UInt32(3) AS #grouping_set_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, #grouping_id:UInt32, #grouping_set_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "      TableScan: aggregate_test_100 projection=[c1, c12] [c1:Utf8, c12:Float64]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+---------------------------------+",
        "| c1 | AVG(aggregate_test_100.c12) | GROUPING(aggregate_test_100.c1) |",
        "+----+-----------------------------+---------------------------------+",
        "|    | 0.5089725099127211          | 1                               |",
        "+----+-----------------------------+---------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_with_grouping_func_as_expr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12), GROUPING(C1) + GROUPING(C2) as grouping_lvl FROM aggregate_test_100 \
                    GROUP BY GROUPING SETS((c1),(c1),(c1, c2))\
                    ORDER BY CASE WHEN grouping_lvl = 0 THEN 0 ELSE 1 END";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Sort: CASE WHEN grouping_lvl = UInt8(0) THEN Int64(0) ELSE Int64(1) END AS CASE WHEN grouping_lvl = Int64(0) THEN Int64(0) ELSE Int64(1) END ASC NULLS LAST [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N, grouping_lvl:UInt8;N]",
        "  Projection: aggregate_test_100.c1, AVG(aggregate_test_100.c12), CAST((#grouping_id >> UInt32(1)) & UInt32(1) AS UInt8) + CAST(#grouping_id & UInt32(1) AS UInt8) AS grouping_lvl [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N, grouping_lvl:UInt8;N]",
        "    Aggregate: groupBy=[[GROUPING SETS ((aggregate_test_100.c1, UInt32(1) AS #grouping_id, UInt32(1) AS #grouping_set_id), (aggregate_test_100.c1, UInt32(1) AS #grouping_id, UInt32(2) AS #grouping_set_id), (aggregate_test_100.c1, aggregate_test_100.c2, UInt32(0) AS #grouping_id, UInt32(3) AS #grouping_set_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, c2:UInt32, #grouping_id:UInt32, #grouping_set_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "      TableScan: aggregate_test_100 projection=[c1, c2, c12] [c1:Utf8, c2:UInt32, c12:Float64]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+--------------+",
        "| c1 | AVG(aggregate_test_100.c12) | grouping_lvl |",
        "+----+-----------------------------+--------------+",
        "| a  | 0.3497223654469457          | 0            |",
        "| a  | 0.3653038379118398          | 0            |",
        "| a  | 0.4693685626367209          | 0            |",
        "| a  | 0.48754517466109415         | 1            |",
        "| a  | 0.48754517466109415         | 1            |",
        "| a  | 0.5945188963859894          | 0            |",
        "| a  | 0.5996111195922015          | 0            |",
        "| b  | 0.16148594845154118         | 0            |",
        "| b  | 0.33400957036260354         | 0            |",
        "| b  | 0.41040709263815384         | 1            |",
        "| b  | 0.41040709263815384         | 1            |",
        "| b  | 0.42804338065410286         | 0            |",
        "| b  | 0.4888141504446429          | 0            |",
        "| b  | 0.5857678873564655          | 0            |",
        "| c  | 0.421733279717472           | 0            |",
        "| c  | 0.6430620563927849          | 0            |",
        "| c  | 0.6600456536439784          | 1            |",
        "| c  | 0.6600456536439784          | 1            |",
        "| c  | 0.6827805579021969          | 0            |",
        "| c  | 0.7277229477969185          | 0            |",
        "| c  | 0.7736013221256991          | 0            |",
        "| d  | 0.2488799233225611          | 0            |",
        "| d  | 0.48855379387549824         | 1            |",
        "| d  | 0.48855379387549824         | 1            |",
        "| d  | 0.49575895804943215         | 0            |",
        "| d  | 0.49931809179640024         | 0            |",
        "| d  | 0.5181987328311988          | 0            |",
        "| d  | 0.586369575965718           | 0            |",
        "| e  | 0.2720288398836001          | 0            |",
        "| e  | 0.29536905073188496         | 0            |",
        "| e  | 0.48600669271341534         | 1            |",
        "| e  | 0.48600669271341534         | 1            |",
        "| e  | 0.5165824734324667          | 0            |",
        "| e  | 0.660795726704708           | 0            |",
        "| e  | 0.780297346359783           | 0            |",
        "+----+-----------------------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_with_grouping_func_and_order_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12), GROUPING_ID(C1, c2) FROM aggregate_test_100 \
    GROUP BY CUBE(c1,c2) ORDER BY GROUPING_ID(C1, c2) DESC, avg(c12) ASC";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Sort: GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2) DESC NULLS FIRST, AVG(aggregate_test_100.c12) ASC NULLS LAST [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N, GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2):UInt32;N]",
        "  Projection: aggregate_test_100.c1, AVG(aggregate_test_100.c12), #grouping_id AS GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2) [c1:Utf8, AVG(aggregate_test_100.c12):Float64;N, GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2):UInt32;N]",
        "    Aggregate: groupBy=[[GROUPING SETS ((UInt32(3) AS #grouping_id), (aggregate_test_100.c1, UInt32(1) AS #grouping_id), (aggregate_test_100.c2, UInt32(2) AS #grouping_id), (aggregate_test_100.c1, aggregate_test_100.c2, UInt32(0) AS #grouping_id))]], aggr=[[AVG(aggregate_test_100.c12)]] [c1:Utf8, c2:UInt32, #grouping_id:UInt32, AVG(aggregate_test_100.c12):Float64;N]",
        "      TableScan: aggregate_test_100 projection=[c1, c2, c12] [c1:Utf8, c2:UInt32, c12:Float64]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+----------------------------------------------------------+",
        "| c1 | AVG(aggregate_test_100.c12) | GROUPING_ID(aggregate_test_100.c1,aggregate_test_100.c2) |",
        "+----+-----------------------------+----------------------------------------------------------+",
        "|    | 0.5089725099127211          | 3                                                        |",
        "|    | 0.40234192123489837         | 2                                                        |",
        "|    | 0.4312272637333415          | 2                                                        |",
        "|    | 0.5108939802619781          | 2                                                        |",
        "|    | 0.5245329062820169          | 2                                                        |",
        "|    | 0.6545641966127662          | 2                                                        |",
        "| b  | 0.41040709263815384         | 1                                                        |",
        "| e  | 0.48600669271341534         | 1                                                        |",
        "| a  | 0.48754517466109415         | 1                                                        |",
        "| d  | 0.48855379387549824         | 1                                                        |",
        "| c  | 0.6600456536439784          | 1                                                        |",
        "| b  | 0.16148594845154118         | 0                                                        |",
        "| d  | 0.2488799233225611          | 0                                                        |",
        "| e  | 0.2720288398836001          | 0                                                        |",
        "| e  | 0.29536905073188496         | 0                                                        |",
        "| b  | 0.33400957036260354         | 0                                                        |",
        "| a  | 0.3497223654469457          | 0                                                        |",
        "| a  | 0.3653038379118398          | 0                                                        |",
        "| c  | 0.421733279717472           | 0                                                        |",
        "| b  | 0.42804338065410286         | 0                                                        |",
        "| a  | 0.4693685626367209          | 0                                                        |",
        "| b  | 0.4888141504446429          | 0                                                        |",
        "| d  | 0.49575895804943215         | 0                                                        |",
        "| d  | 0.49931809179640024         | 0                                                        |",
        "| e  | 0.5165824734324667          | 0                                                        |",
        "| d  | 0.5181987328311988          | 0                                                        |",
        "| b  | 0.5857678873564655          | 0                                                        |",
        "| d  | 0.586369575965718           | 0                                                        |",
        "| a  | 0.5945188963859894          | 0                                                        |",
        "| a  | 0.5996111195922015          | 0                                                        |",
        "| c  | 0.6430620563927849          | 0                                                        |",
        "| e  | 0.660795726704708           | 0                                                        |",
        "| c  | 0.6827805579021969          | 0                                                        |",
        "| c  | 0.7277229477969185          | 0                                                        |",
        "| c  | 0.7736013221256991          | 0                                                        |",
        "| e  | 0.780297346359783           | 0                                                        |",
        "+----+-----------------------------+----------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_rollup_with_count_wildcard_and_order_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, c2, c3, COUNT(*) \
        FROM aggregate_test_100 \
        WHERE c1 IN ('a', 'b', NULL) \
        GROUP BY c1, ROLLUP (c2, c3) \
        ORDER BY c1, c2, c3";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.clone().into_optimized_plan()?;
    let expected = vec![
        "Sort: aggregate_test_100.c1 ASC NULLS LAST, aggregate_test_100.c2 ASC NULLS LAST, aggregate_test_100.c3 ASC NULLS LAST [c1:Utf8, c2:UInt32, c3:Int8, COUNT(UInt8(1)):Int64;N]",
        "  Aggregate: groupBy=[[GROUPING SETS ((aggregate_test_100.c1), (aggregate_test_100.c1, aggregate_test_100.c2), (aggregate_test_100.c1, aggregate_test_100.c2, aggregate_test_100.c3))]], aggr=[[COUNT(UInt8(1))]] [c1:Utf8, c2:UInt32, c3:Int8, COUNT(UInt8(1)):Int64;N]",
        "    Filter: aggregate_test_100.c1 = Utf8(NULL) OR aggregate_test_100.c1 = Utf8(\"b\") OR aggregate_test_100.c1 = Utf8(\"a\") [c1:Utf8, c2:UInt32, c3:Int8]",
        "      TableScan: aggregate_test_100 projection=[c1, c2, c3], partial_filters=[aggregate_test_100.c1 = Utf8(NULL) OR aggregate_test_100.c1 = Utf8(\"b\") OR aggregate_test_100.c1 = Utf8(\"a\")] [c1:Utf8, c2:UInt32, c3:Int8]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+------+-----------------+",
        "| c1 | c2 | c3   | COUNT(UInt8(1)) |",
        "+----+----+------+-----------------+",
        "| a  | 1  | -85  | 1               |",
        "| a  | 1  | -56  | 1               |",
        "| a  | 1  | -25  | 1               |",
        "| a  | 1  | -5   | 1               |",
        "| a  | 1  | 83   | 1               |",
        "| a  | 1  |      | 5               |",
        "| a  | 2  | -48  | 1               |",
        "| a  | 2  | -43  | 1               |",
        "| a  | 2  | 45   | 1               |",
        "| a  | 2  |      | 3               |",
        "| a  | 3  | -72  | 1               |",
        "| a  | 3  | -12  | 1               |",
        "| a  | 3  | 13   | 2               |",
        "| a  | 3  | 14   | 1               |",
        "| a  | 3  | 17   | 1               |",
        "| a  | 3  |      | 6               |",
        "| a  | 4  | -101 | 1               |",
        "| a  | 4  | -54  | 1               |",
        "| a  | 4  | -38  | 1               |",
        "| a  | 4  | 65   | 1               |",
        "| a  | 4  |      | 4               |",
        "| a  | 5  | -101 | 1               |",
        "| a  | 5  | -31  | 1               |",
        "| a  | 5  | 36   | 1               |",
        "| a  | 5  |      | 3               |",
        "| a  |    |      | 21              |",
        "| b  | 1  | 12   | 1               |",
        "| b  | 1  | 29   | 1               |",
        "| b  | 1  | 54   | 1               |",
        "| b  | 1  |      | 3               |",
        "| b  | 2  | -60  | 1               |",
        "| b  | 2  | 31   | 1               |",
        "| b  | 2  | 63   | 1               |",
        "| b  | 2  | 68   | 1               |",
        "| b  | 2  |      | 4               |",
        "| b  | 3  | -101 | 1               |",
        "| b  | 3  | 17   | 1               |",
        "| b  | 3  |      | 2               |",
        "| b  | 4  | -117 | 1               |",
        "| b  | 4  | -111 | 1               |",
        "| b  | 4  | -59  | 1               |",
        "| b  | 4  | 17   | 1               |",
        "| b  | 4  | 47   | 1               |",
        "| b  | 4  |      | 5               |",
        "| b  | 5  | -82  | 1               |",
        "| b  | 5  | -44  | 1               |",
        "| b  | 5  | -5   | 1               |",
        "| b  | 5  | 62   | 1               |",
        "| b  | 5  | 68   | 1               |",
        "| b  | 5  |      | 5               |",
        "| b  |    |      | 19              |",
        "+----+----+------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn invalid_grouping_func() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12), GROUPING(c3) FROM aggregate_test_100 GROUP BY GROUPING SETS((c1),(c2),())";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let err = dataframe.into_optimized_plan().err().unwrap();
    assert_eq!(
        "Plan(\"Column of GROUPING(aggregate_test_100.c3) can't be found in GROUP BY columns [aggregate_test_100.c1, aggregate_test_100.c2]\")",
        &format!("{err:?}")
    );

    Ok(())
}

#[tokio::test]
async fn invalid_grouping_id_func() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1, avg(c12), GROUPING_ID(c1) FROM aggregate_test_100 GROUP BY GROUPING SETS((c1),(c2),())";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let err = dataframe.into_optimized_plan().err().unwrap();
    assert_eq!(
        "Plan(\"Columns of GROUPING_ID([aggregate_test_100.c1]) does not match GROUP BY columns [aggregate_test_100.c1, aggregate_test_100.c2]\")",
        &format!("{err:?}")
    );

    Ok(())
}

#[tokio::test]
async fn invalid_grouping_id_func2() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;

    // The column ordering of the GROUPING_ID() matters
    let sql = "SELECT c1, avg(c12), GROUPING_ID(c2, c1) FROM aggregate_test_100 GROUP BY CUBE(c1,c2)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let err = dataframe.into_optimized_plan().err().unwrap();
    assert_eq!(
        "Plan(\"Columns of GROUPING_ID([aggregate_test_100.c2, aggregate_test_100.c1]) does not match GROUP BY columns [aggregate_test_100.c1, aggregate_test_100.c2]\")",
        &format!("{err:?}")
    );

    Ok(())
}
