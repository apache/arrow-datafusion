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
use datafusion::scalar::ScalarValue;
use datafusion::test_util::scan_empty;
use datafusion_common::cast::as_float64_array;

#[tokio::test]
async fn csv_query_array_agg_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT array_agg(distinct c2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;

    // The results for this query should be something like the following:
    //    +------------------------------------------+
    //    | ARRAYAGG(DISTINCT aggregate_test_100.c2) |
    //    +------------------------------------------+
    //    | [4, 2, 3, 5, 1]                          |
    //    +------------------------------------------+
    // Since ARRAY_AGG(DISTINCT) ordering is nondeterministic, check the schema and contents.
    assert_eq!(
        *actual[0].schema(),
        Schema::new(vec![Field::new(
            "ARRAYAGG(DISTINCT aggregate_test_100.c2)",
            DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
            false
        ),])
    );

    // We should have 1 row containing a list
    let column = actual[0].column(0);
    assert_eq!(column.len(), 1);

    if let ScalarValue::List(Some(mut v), _) = ScalarValue::try_from_array(column, 0)? {
        // workaround lack of Ord of ScalarValue
        let cmp = |a: &ScalarValue, b: &ScalarValue| {
            a.partial_cmp(b).expect("Can compare ScalarValues")
        };
        v.sort_by(cmp);
        assert_eq!(
            *v,
            vec![
                ScalarValue::UInt32(Some(1)),
                ScalarValue::UInt32(Some(2)),
                ScalarValue::UInt32(Some(3)),
                ScalarValue::UInt32(Some(4)),
                ScalarValue::UInt32(Some(5))
            ]
        );
    } else {
        unreachable!();
    }

    Ok(())
}

#[tokio::test]
async fn aggregate() -> Result<()> {
    let results = execute_with_partition("SELECT SUM(c1), SUM(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| SUM(test.c1) | SUM(test.c2) |",
        "+--------------+--------------+",
        "| 60           | 220          |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_empty() -> Result<()> {
    // The predicate on this query purposely generates no results
    let results =
        execute_with_partition("SELECT SUM(c1), SUM(c2) FROM test where c1 > 100000", 4)
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| SUM(test.c1) | SUM(test.c2) |",
        "+--------------+--------------+",
        "|              |              |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_avg() -> Result<()> {
    let results = execute_with_partition("SELECT AVG(c1), AVG(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| AVG(test.c1) | AVG(test.c2) |",
        "+--------------+--------------+",
        "| 1.5          | 5.5          |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_max() -> Result<()> {
    let results = execute_with_partition("SELECT MAX(c1), MAX(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| MAX(test.c1) | MAX(test.c2) |",
        "+--------------+--------------+",
        "| 3            | 10           |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_min() -> Result<()> {
    let results = execute_with_partition("SELECT MIN(c1), MIN(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+--------------+",
        "| MIN(test.c1) | MIN(test.c2) |",
        "+--------------+--------------+",
        "| 0            | 1            |",
        "+--------------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, SUM(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | SUM(test.c2) |",
        "+----+--------------+",
        "| 0  | 55           |",
        "| 1  | 55           |",
        "| 2  | 55           |",
        "| 3  | 55           |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_avg() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, AVG(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | AVG(test.c2) |",
        "+----+--------------+",
        "| 0  | 5.5          |",
        "| 1  | 5.5          |",
        "| 2  | 5.5          |",
        "| 3  | 5.5          |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_empty() -> Result<()> {
    let results = execute_with_partition(
        "SELECT c1, AVG(c2) FROM test WHERE c1 = 123 GROUP BY c1",
        4,
    )
    .await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | AVG(test.c2) |",
        "+----+--------------+",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_max() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, MAX(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | MAX(test.c2) |",
        "+----+--------------+",
        "| 0  | 10           |",
        "| 1  | 10           |",
        "| 2  | 10           |",
        "| 3  | 10           |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_grouped_min() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, MIN(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+--------------+",
        "| c1 | MIN(test.c2) |",
        "+----+--------------+",
        "| 0  | 1            |",
        "| 1  | 1            |",
        "| 2  | 1            |",
        "| 3  | 1            |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_min_max_w_custom_window_frames() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT
        MIN(c12) OVER (ORDER BY C12 RANGE BETWEEN 0.3 PRECEDING AND 0.2 FOLLOWING) as min1,
        MAX(c12) OVER (ORDER BY C11 RANGE BETWEEN 0.1 PRECEDING AND 0.2 FOLLOWING) as max1
        FROM aggregate_test_100
        ORDER BY C9
        LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+--------------------+",
        "| min1                | max1               |",
        "+---------------------+--------------------+",
        "| 0.01479305307777301 | 0.9965400387585364 |",
        "| 0.01479305307777301 | 0.9800193410444061 |",
        "| 0.01479305307777301 | 0.9706712283358269 |",
        "| 0.2667177795079635  | 0.9965400387585364 |",
        "| 0.3600766362333053  | 0.9706712283358269 |",
        "+---------------------+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn aggregate_min_max_w_custom_window_frames_unbounded_start() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql =
        "SELECT
        MIN(c12) OVER (ORDER BY C12 RANGE BETWEEN UNBOUNDED PRECEDING AND 0.2 FOLLOWING) as min1,
        MAX(c12) OVER (ORDER BY C11 RANGE BETWEEN UNBOUNDED PRECEDING AND 0.2 FOLLOWING) as max1
        FROM aggregate_test_100
        ORDER BY C9
        LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------+--------------------+",
        "| min1                | max1               |",
        "+---------------------+--------------------+",
        "| 0.01479305307777301 | 0.9965400387585364 |",
        "| 0.01479305307777301 | 0.9800193410444061 |",
        "| 0.01479305307777301 | 0.9800193410444061 |",
        "| 0.01479305307777301 | 0.9965400387585364 |",
        "| 0.01479305307777301 | 0.9800193410444061 |",
        "+---------------------+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn aggregate_avg_add() -> Result<()> {
    let results = execute_with_partition(
        "SELECT AVG(c1), AVG(c1) + 1, AVG(c1) + 2, 1 + AVG(c1) FROM test",
        4,
    )
    .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+--------------+-------------------------+-------------------------+-------------------------+",
        "| AVG(test.c1) | AVG(test.c1) + Int64(1) | AVG(test.c1) + Int64(2) | Int64(1) + AVG(test.c1) |",
        "+--------------+-------------------------+-------------------------+-------------------------+",
        "| 1.5          | 2.5                     | 3.5                     | 2.5                     |",
        "+--------------+-------------------------+-------------------------+-------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn case_sensitive_identifiers_aggregates() {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let expected = vec![
        "+----------+",
        "| MAX(t.i) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];

    let results = plan_and_collect(&ctx, "SELECT max(i) FROM t")
        .await
        .unwrap();

    assert_batches_sorted_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT MAX(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);

    // Using double quotes allows specifying the function name with capitalization
    let err = plan_and_collect(&ctx, "SELECT \"MAX\"(i) FROM t")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Invalid function 'MAX'"
    );

    let results = plan_and_collect(&ctx, "SELECT \"max\"(i) FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn count_basic() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(c1), COUNT(c2) FROM test", 1).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----------------+----------------+",
        "| COUNT(test.c1) | COUNT(test.c2) |",
        "+----------------+----------------+",
        "| 10             | 10             |",
        "+----------------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_partitioned() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(c1), COUNT(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----------------+----------------+",
        "| COUNT(test.c1) | COUNT(test.c2) |",
        "+----------------+----------------+",
        "| 40             | 40             |",
        "+----------------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_aggregated() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, COUNT(c2) FROM test GROUP BY c1", 4).await?;

    let expected = vec![
        "+----+----------------+",
        "| c1 | COUNT(test.c2) |",
        "+----+----------------+",
        "| 0  | 10             |",
        "| 1  | 10             |",
        "| 2  | 10             |",
        "| 3  | 10             |",
        "+----+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_aggregated_cube() -> Result<()> {
    let results = execute_with_partition(
        "SELECT c1, c2, COUNT(c3) FROM test GROUP BY CUBE (c1, c2) ORDER BY c1, c2",
        4,
    )
    .await?;

    let expected = vec![
        "+----+----+----------------+",
        "| c1 | c2 | COUNT(test.c3) |",
        "+----+----+----------------+",
        "|    |    | 40             |",
        "|    | 1  | 4              |",
        "|    | 10 | 4              |",
        "|    | 2  | 4              |",
        "|    | 3  | 4              |",
        "|    | 4  | 4              |",
        "|    | 5  | 4              |",
        "|    | 6  | 4              |",
        "|    | 7  | 4              |",
        "|    | 8  | 4              |",
        "|    | 9  | 4              |",
        "| 0  |    | 10             |",
        "| 0  | 1  | 1              |",
        "| 0  | 10 | 1              |",
        "| 0  | 2  | 1              |",
        "| 0  | 3  | 1              |",
        "| 0  | 4  | 1              |",
        "| 0  | 5  | 1              |",
        "| 0  | 6  | 1              |",
        "| 0  | 7  | 1              |",
        "| 0  | 8  | 1              |",
        "| 0  | 9  | 1              |",
        "| 1  |    | 10             |",
        "| 1  | 1  | 1              |",
        "| 1  | 10 | 1              |",
        "| 1  | 2  | 1              |",
        "| 1  | 3  | 1              |",
        "| 1  | 4  | 1              |",
        "| 1  | 5  | 1              |",
        "| 1  | 6  | 1              |",
        "| 1  | 7  | 1              |",
        "| 1  | 8  | 1              |",
        "| 1  | 9  | 1              |",
        "| 2  |    | 10             |",
        "| 2  | 1  | 1              |",
        "| 2  | 10 | 1              |",
        "| 2  | 2  | 1              |",
        "| 2  | 3  | 1              |",
        "| 2  | 4  | 1              |",
        "| 2  | 5  | 1              |",
        "| 2  | 6  | 1              |",
        "| 2  | 7  | 1              |",
        "| 2  | 8  | 1              |",
        "| 2  | 9  | 1              |",
        "| 3  |    | 10             |",
        "| 3  | 1  | 1              |",
        "| 3  | 10 | 1              |",
        "| 3  | 2  | 1              |",
        "| 3  | 3  | 1              |",
        "| 3  | 4  | 1              |",
        "| 3  | 5  | 1              |",
        "| 3  | 6  | 1              |",
        "| 3  | 7  | 1              |",
        "| 3  | 8  | 1              |",
        "| 3  | 9  | 1              |",
        "+----+----+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn count_multi_expr() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(0),
                None,
                None,
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT count(c1, c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------------------+",
        "| COUNT(test.c1,test.c2) |",
        "+------------------------+",
        "| 2                      |",
        "+------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn simple_avg() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let result = plan_and_collect(&ctx, "SELECT AVG(a) FROM t").await?;

    let batch = &result[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let values = as_float64_array(batch.column(0)).expect("failed to cast version");
    assert_eq!(values.len(), 1);
    // avg(1,2,3,4,5) = 3.0
    assert_eq!(values.value(0), 3.0_f64);
    Ok(())
}

#[tokio::test]
async fn simple_mean() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
    )?;
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from_slice([4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let result = plan_and_collect(&ctx, "SELECT MEAN(a) FROM t").await?;

    let batch = &result[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(1, batch.num_rows());

    let values = as_float64_array(batch.column(0)).expect("failed to cast version");
    assert_eq!(values.len(), 1);
    // mean(1,2,3,4,5) = 3.0
    assert_eq!(values.value(0), 3.0_f64);
    Ok(())
}

async fn run_count_distinct_integers_aggregated_scenario(
    partitions: Vec<Vec<(&str, u64)>>,
) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_group", DataType::Utf8, false),
        Field::new("c_int8", DataType::Int8, false),
        Field::new("c_int16", DataType::Int16, false),
        Field::new("c_int32", DataType::Int32, false),
        Field::new("c_int64", DataType::Int64, false),
        Field::new("c_uint8", DataType::UInt8, false),
        Field::new("c_uint16", DataType::UInt16, false),
        Field::new("c_uint32", DataType::UInt32, false),
        Field::new("c_uint64", DataType::UInt64, false),
    ]));

    for (i, partition) in partitions.iter().enumerate() {
        let filename = format!("partition-{i}.csv");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;
        for row in partition {
            let row_str = format!(
                "{},{}\n",
                row.0,
                // Populate values for each of the integer fields in the
                // schema.
                (0..8)
                    .map(|_| { row.1.to_string() })
                    .collect::<Vec<_>>()
                    .join(","),
            );
            file.write_all(row_str.as_bytes())?;
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
        "
          SELECT
            c_group,
            COUNT(c_uint64),
            COUNT(DISTINCT c_int8),
            COUNT(DISTINCT c_int16),
            COUNT(DISTINCT c_int32),
            COUNT(DISTINCT c_int64),
            COUNT(DISTINCT c_uint8),
            COUNT(DISTINCT c_uint16),
            COUNT(DISTINCT c_uint32),
            COUNT(DISTINCT c_uint64)
          FROM test
          GROUP BY c_group
        ",
    )
    .await?;

    Ok(results)
}

#[tokio::test]
async fn count_distinct_integers_aggregated_single_partition() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![
            ("a", 1),
            ("a", 1),
            ("a", 2),
            ("b", 9),
            ("c", 9),
            ("c", 10),
            ("c", 9),
        ],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    let expected = vec![
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| c_group | COUNT(test.c_uint64) | COUNT(DISTINCT test.c_int8) | COUNT(DISTINCT test.c_int16) | COUNT(DISTINCT test.c_int32) | COUNT(DISTINCT test.c_int64) | COUNT(DISTINCT test.c_uint8) | COUNT(DISTINCT test.c_uint16) | COUNT(DISTINCT test.c_uint32) | COUNT(DISTINCT test.c_uint64) |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| a       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |",
        "| b       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |",
        "| c       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn count_distinct_integers_aggregated_multiple_partitions() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![("a", 1), ("a", 1), ("a", 2), ("b", 9), ("c", 9)],
        vec![("a", 1), ("a", 3), ("b", 8), ("b", 9), ("b", 10), ("b", 11)],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    let expected = vec![
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| c_group | COUNT(test.c_uint64) | COUNT(DISTINCT test.c_int8) | COUNT(DISTINCT test.c_int16) | COUNT(DISTINCT test.c_int32) | COUNT(DISTINCT test.c_int64) | COUNT(DISTINCT test.c_uint8) | COUNT(DISTINCT test.c_uint16) | COUNT(DISTINCT test.c_uint32) | COUNT(DISTINCT test.c_uint64) |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        "| a       | 5                    | 3                           | 3                            | 3                            | 3                            | 3                            | 3                             | 3                             | 3                             |",
        "| b       | 5                    | 4                           | 4                            | 4                            | 4                            | 4                            | 4                             | 4                             | 4                             |",
        "| c       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |",
        "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn count_distinct_multiple_exprs() -> Result<()> {
    let results =
        execute_with_partition("SELECT COUNT(distinct c1, c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+---------------------------------+",
        "| COUNT(DISTINCT test.c1,test.c2) |",
        "+---------------------------------+",
        "| 40                              |",
        "+---------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2),
                None,
                Some(0),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(0),
                None,
                None,
                Some(1),
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT count(distinct c1, c2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------------------------------+",
        "| COUNT(DISTINCT test.c1,test.c2) |",
        "+---------------------------------+",
        "| 2                               |",
        "+---------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn count_distinct_multiple_exprs_with_groups() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Utf8, true),
        Field::new("c3", DataType::Utf8, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(1),
                Some(2),
                None,
                Some(0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                None,
                None,
                Some("a"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
                Some("b"),
            ])),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;
    let sql = "SELECT c3, count(distinct c1, c2) FROM test group by c3";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+----+---------------------------------+",
        "| c3 | COUNT(DISTINCT test.c1,test.c2) |",
        "+----+---------------------------------+",
        "| a  | 2                               |",
        "| b  | 1                               |",
        "+----+---------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn aggregate_with_alias() -> Result<()> {
    let ctx = SessionContext::new();
    let state = ctx.state();

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
    ]));

    let plan = scan_empty(None, schema.as_ref(), None)?
        .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
        .project(vec![col("c1"), sum(col("c2")).alias("total_salary")])?
        .build()?;

    let plan = state.optimize(&plan)?;
    let physical_plan = state.create_physical_plan(&Arc::new(plan)).await?;
    assert_eq!("c1", physical_plan.schema().field(0).name().as_str());
    assert_eq!(
        "total_salary",
        physical_plan.schema().field(1).name().as_str()
    );
    Ok(())
}

#[tokio::test]
async fn test_accumulator_row_accumulator() -> Result<()> {
    let config = SessionConfig::new();
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT c1, c2, MIN(c13) as min1, MIN(c9) as min2, MAX(c13) as max1, MAX(c9) as max2, AVG(c9) as avg1, MIN(c13) as min3, COUNT(C9) as cnt1, 0.5*SUM(c9-c8) as sum1
    FROM aggregate_test_100
    GROUP BY c1, c2
    ORDER BY c1, c2
    LIMIT 5";

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+",
        "| c1 | c2 | min1                           | min2      | max1                           | max2       | avg1               | min3                           | cnt1 | sum1         |",
        "+----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+",
        "| a  | 1  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB | 774637006 | waIGbOGl1PM6gnzZ4uuZt4E2yDWRHs | 4015442341 | 2437927011.0       | 0keZ5G8BffGwgF2RwQD59TFzMStxCB | 5    | 6094771121.5 |",
        "| a  | 2  | b3b9esRhTzFEawbs6XhpKnD9ojutHB | 145294611 | ukyD7b0Efj7tNlFSRmzZ0IqkEzg2a8 | 3717551163 | 2267588664.0       | b3b9esRhTzFEawbs6XhpKnD9ojutHB | 3    | 3401364777.0 |",
        "| a  | 3  | Amn2K87Db5Es3dFQO9cw9cvpAM6h35 | 431948861 | oLZ21P2JEDooxV1pU31cIxQHEeeoLu | 3998790955 | 2225685115.1666665 | Amn2K87Db5Es3dFQO9cw9cvpAM6h35 | 6    | 6676994872.5 |",
        "| a  | 4  | KJFcmTVjdkCMv94wYCtfHMFhzyRsmH | 466439833 | ydkwycaISlYSlEq3TlkS2m15I2pcp8 | 2502326480 | 1655431654.0       | KJFcmTVjdkCMv94wYCtfHMFhzyRsmH | 4    | 3310812222.5 |",
        "| a  | 5  | MeSTAXq8gVxVjbEjgkvU9YLte0X9uE | 141047417 | QJYm7YRA3YetcBHI5wkMZeLXVmfuNy | 2496054700 | 1216992989.6666667 | MeSTAXq8gVxVjbEjgkvU9YLte0X9uE | 3    | 1825431770.0 |",
        "+----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}
