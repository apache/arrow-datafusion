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
use datafusion::{datasource::empty::EmptyTable, from_slice::FromSlice};
use datafusion_common::ScalarValue;
use tempfile::TempDir;

#[tokio::test]
async fn query_get_indexed_field() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new_list(
        "some_list",
        Field::new("item", DataType::Int64, true),
        false,
    )]));
    let builder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
    let mut lb = ListBuilder::new(builder);
    for int_vec in vec![vec![0, 1, 2], vec![4, 5, 6], vec![7, 8, 9]] {
        let builder = lb.values();
        for int in int_vec {
            builder.append_value(int);
        }
        lb.append(true);
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;

    ctx.register_batch("ints", data)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_list[1] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| i0 |",
        "+----+",
        "| 0  |",
        "| 4  |",
        "| 7  |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_nested_get_indexed_field() -> Result<()> {
    let ctx = SessionContext::new();
    let nested_dt = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    // Nested schema of { "some_list": [[i64]] }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_list",
        DataType::List(Arc::new(Field::new("item", nested_dt.clone(), true))),
        false,
    )]));

    let builder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
    let nested_lb = ListBuilder::new(builder);
    let mut lb = ListBuilder::new(nested_lb);
    for int_vec_vec in vec![
        vec![vec![0, 1], vec![2, 3], vec![3, 4]],
        vec![vec![5, 6], vec![7, 8], vec![9, 10]],
        vec![vec![11, 12], vec![13, 14], vec![15, 16]],
    ] {
        let nested_builder = lb.values();
        for int_vec in int_vec_vec {
            let builder = nested_builder.values();
            for int in int_vec {
                builder.append_value(int);
            }
            nested_builder.append(true);
        }
        lb.append(true);
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;

    ctx.register_batch("ints", data)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_list[1] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| i0       |",
        "+----------+",
        "| [0, 1]   |",
        "| [5, 6]   |",
        "| [11, 12] |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    let sql = "SELECT some_list[1][1] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| i0 |",
        "+----+",
        "| 0  |",
        "| 5  |",
        "| 11 |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_nested_get_indexed_field_on_struct() -> Result<()> {
    let ctx = SessionContext::new();
    let nested_dt = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    // Nested schema of { "some_struct": { "bar": [i64] } }
    let struct_fields = vec![Field::new("bar", nested_dt.clone(), true)];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_struct",
        DataType::Struct(struct_fields.clone().into()),
        false,
    )]));

    let builder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
    let nested_lb = ListBuilder::new(builder);
    let mut sb = StructBuilder::new(struct_fields, vec![Box::new(nested_lb)]);
    for int_vec in vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]] {
        let lb = sb.field_builder::<ListBuilder<Int64Builder>>(0).unwrap();
        for int in int_vec {
            lb.values().append_value(int);
        }
        lb.append(true);
        sb.append(true);
    }
    let s = sb.finish();
    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(s)])?;

    ctx.register_batch("structs", data)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_struct['bar'] as l0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| l0             |",
        "+----------------+",
        "| [0, 1, 2, 3]   |",
        "| [4, 5, 6, 7]   |",
        "| [8, 9, 10, 11] |",
        "+----------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Access to field of struct by CompoundIdentifier
    let sql = "SELECT some_struct.bar as l0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| l0             |",
        "+----------------+",
        "| [0, 1, 2, 3]   |",
        "| [4, 5, 6, 7]   |",
        "| [8, 9, 10, 11] |",
        "+----------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT some_struct['bar'][1] as i0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| i0 |",
        "+----+",
        "| 0  |",
        "| 4  |",
        "| 8  |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "dictionary_expressions")]
async fn query_on_string_dictionary() -> Result<()> {
    // Test to ensure DataFusion can operate on dictionary types
    // Use StringDictionary (32 bit indexes = keys)
    let d1: DictionaryArray<Int32Type> =
        vec![Some("one"), None, Some("three")].into_iter().collect();

    let d2: DictionaryArray<Int32Type> = vec![Some("blarg"), None, Some("three")]
        .into_iter()
        .collect();

    let d3: StringArray = vec![Some("XYZ"), None, Some("three")].into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
    ])
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    // Basic SELECT
    let sql = "SELECT d1 FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| one   |",
        "|       |",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // basic filtering
    let sql = "SELECT d1 FROM test WHERE d1 IS NOT NULL";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| one   |",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // comparison with constant
    let sql = "SELECT d1 FROM test WHERE d1 = 'three'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // comparison with another dictionary column
    let sql = "SELECT d1 FROM test WHERE d1 = d2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // order comparison with another dictionary column
    let sql = "SELECT d1 FROM test WHERE d1 <= d2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // comparison with a non dictionary column
    let sql = "SELECT d1 FROM test WHERE d1 = d3";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // filtering with constant
    let sql = "SELECT d1 FROM test WHERE d1 = 'three'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Expression evaluation
    let sql = "SELECT concat(d1, '-foo') FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------------------------+",
        "| concat(test.d1,Utf8(\"-foo\")) |",
        "+------------------------------+",
        "| one-foo                      |",
        "| -foo                         |",
        "| three-foo                    |",
        "+------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Expression evaluation with two dictionaries
    let sql = "SELECT concat(d1, d2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| concat(test.d1,test.d2) |",
        "+-------------------------+",
        "| oneblarg                |",
        "|                         |",
        "| threethree              |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation
    let sql = "SELECT COUNT(d1) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| COUNT(test.d1) |",
        "+----------------+",
        "| 2              |",
        "+----------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation min
    let sql = "SELECT MIN(d1) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| MIN(test.d1) |",
        "+--------------+",
        "| one          |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation max
    let sql = "SELECT MAX(d1) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| MAX(test.d1) |",
        "+--------------+",
        "| three        |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // grouping
    let sql = "SELECT d1, COUNT(*) FROM test group by d1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+-----------------+",
        "| d1    | COUNT(UInt8(1)) |",
        "+-------+-----------------+",
        "| one   | 1               |",
        "|       | 1               |",
        "| three | 1               |",
        "+-------+-----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    // window functions
    let sql = "SELECT d1, row_number() OVER (partition by d1) as rn1 FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+-----+",
        "| d1    | rn1 |",
        "+-------+-----+",
        "|       | 1   |",
        "| one   | 1   |",
        "| three | 1   |",
        "+-------+-----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn sort_on_window_null_string() -> Result<()> {
    let d1: DictionaryArray<Int32Type> =
        vec![Some("one"), None, Some("three")].into_iter().collect();
    let d2: StringArray = vec![Some("ONE"), None, Some("THREE")].into_iter().collect();
    let d3: LargeStringArray =
        vec![Some("One"), None, Some("Three")].into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
    ])
    .unwrap();

    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("test", batch)?;

    let sql =
        "SELECT d1, row_number() OVER (partition by d1) as rn1 FROM test order by d1 asc";

    let actual = execute_to_batches(&ctx, sql).await;
    // NULLS LAST
    let expected = vec![
        "+-------+-----+",
        "| d1    | rn1 |",
        "+-------+-----+",
        "| one   | 1   |",
        "| three | 1   |",
        "|       | 1   |",
        "+-------+-----+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql =
        "SELECT d2, row_number() OVER (partition by d2) as rn1 FROM test ORDER BY d2 asc";
    let actual = execute_to_batches(&ctx, sql).await;
    // NULLS LAST
    let expected = vec![
        "+-------+-----+",
        "| d2    | rn1 |",
        "+-------+-----+",
        "| ONE   | 1   |",
        "| THREE | 1   |",
        "|       | 1   |",
        "+-------+-----+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql =
        "SELECT d2, row_number() OVER (partition by d2 order by d2 desc) as rn1 FROM test ORDER BY d2 desc";

    let actual = execute_to_batches(&ctx, sql).await;
    // NULLS FIRST
    let expected = vec![
        "+-------+-----+",
        "| d2    | rn1 |",
        "+-------+-----+",
        "|       | 1   |",
        "| THREE | 1   |",
        "| ONE   | 1   |",
        "+-------+-----+",
    ];
    assert_batches_eq!(expected, &actual);

    // FIXME sort on LargeUtf8 String has bug.
    // let sql =
    //     "SELECT d3, row_number() OVER (partition by d3) as rn1 FROM test";
    // let actual = execute_to_batches(&ctx, sql).await;
    // let expected = vec![
    //     "+-------+-----+",
    //     "| d3    | rn1 |",
    //     "+-------+-----+",
    //     "|       | 1   |",
    //     "| One   | 1   |",
    //     "| Three | 1   |",
    //     "+-------+-----+",
    // ];
    // assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn filter_with_time32second() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time32(TimeUnit::Second), false),
        Field::new("value", DataType::Int64, false),
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
            Arc::new(Int64Array::from(vec![
                Some(2505),
                Some(2436),
                Some(2384),
                Some(1815),
                Some(2330),
                Some(2065),
            ])),
        ],
    )?;

    ctx.register_batch("temporal", data)?;
    let sql = "SELECT value FROM temporal WHERE time = '01:23:20'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| value |",
        "+-------+",
        "| 2436  |",
        "| 2505  |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn filter_with_time32millisecond() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time32(TimeUnit::Millisecond), false),
        Field::new("value", DataType::Int64, false),
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
            Arc::new(Int64Array::from(vec![
                Some(2505),
                Some(2436),
                Some(2384),
                Some(1815),
                Some(2330),
                Some(2065),
            ])),
        ],
    )?;

    ctx.register_batch("temporal", data)?;
    let sql = "SELECT value FROM temporal WHERE time = '01:23:20'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| value |",
        "+-------+",
        "| 2436  |",
        "| 2505  |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn filter_with_time64microsecond() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new("value", DataType::Int64, false),
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
                Some(2505),
                Some(2436),
                Some(2384),
                Some(1815),
                Some(2330),
                Some(2065),
            ])),
        ],
    )?;

    ctx.register_batch("temporal", data)?;
    let sql = "SELECT value FROM temporal WHERE time = '01:23:20'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| value |",
        "+-------+",
        "| 2436  |",
        "| 2505  |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn filter_with_time64nanosecond() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Time64(TimeUnit::Nanosecond), false),
        Field::new("value", DataType::Int64, false),
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
                Some(2505),
                Some(2436),
                Some(2384),
                Some(1815),
                Some(2330),
                Some(2065),
            ])),
        ],
    )?;

    ctx.register_batch("temporal", data)?;
    let sql = "SELECT value FROM temporal WHERE time = '01:23:20'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| value |",
        "+-------+",
        "| 2436  |",
        "| 2505  |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cte_with_alias() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("a", DataType::Int16, false),
    ]);
    let empty_table = Arc::new(EmptyTable::new(Arc::new(schema)));
    ctx.register_table("t1", empty_table)?;
    let sql = "WITH \
        v1 AS (SELECT * FROM t1), \
        v2 AS (SELECT v1.id AS id, v1a.id AS id_a, v1b.id AS id_b \
        FROM v1, v1 v1a, v1 v1b \
        WHERE v1a.id = v1.id - 1 \
        AND v1b.id = v1.id + 1) \
        SELECT * FROM v2";
    let actual = execute_to_batches(&ctx, sql).await;
    // the purpose of this test is just to make sure the query produces a valid plan
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cte() -> Result<()> {
    // Test for SELECT <expression> without FROM.
    // Should evaluate expressions in project position.
    let ctx = SessionContext::new();

    // simple with
    let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| Int64(1) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    // with + union
    let sql =
        "WITH t AS (SELECT 1 AS a), u AS (SELECT 2 AS a) SELECT * FROM t UNION ALL SELECT * FROM u";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| a |",
        "+---+",
        "| 1 |",
        "| 2 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);

    // with + join
    let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT 1 AS id2, 5 as x) SELECT x FROM t JOIN u ON (id1 = id2)";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| x |",
        "+---+",
        "| 5 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);

    // backward reference
    let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT * FROM t) SELECT * from u";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+-----+",
        "| id1 |",
        "+-----+",
        "| 1   |",
        "+-----+"
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_select_nested() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT o1, o2, c3
               FROM (
                 SELECT c1 AS o1, c2 + 1 AS o2, c3
                 FROM (
                   SELECT c1, c2, c3, c4
                   FROM aggregate_test_100
                   WHERE c1 = 'a' AND c2 >= 4
                   ORDER BY c2 ASC, c3 ASC
                 ) AS a
               ) AS b";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+------+",
        "| o1 | o2 | c3   |",
        "+----+----+------+",
        "| a  | 5  | -101 |",
        "| a  | 5  | -54  |",
        "| a  | 5  | -38  |",
        "| a  | 5  | 65   |",
        "| a  | 6  | -101 |",
        "| a  | 6  | -31  |",
        "| a  | 6  | 36   |",
        "+----+----+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_select_nested_without_aliases() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT o1, o2, c3
               FROM (
                 SELECT c1 AS o1, c2 + 1 AS o2, c3
                 FROM (
                   SELECT c1, c2, c3, c4
                   FROM aggregate_test_100
                   WHERE c1 = 'a' AND c2 >= 4
                   ORDER BY c2 ASC, c3 ASC
                 )
               )";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+------+",
        "| o1 | o2 | c3   |",
        "+----+----+------+",
        "| a  | 5  | -101 |",
        "| a  | 5  | -54  |",
        "| a  | 5  | -38  |",
        "| a  | 5  | 65   |",
        "| a  | 6  | -101 |",
        "| a  | 6  | -31  |",
        "| a  | 6  | 36   |",
        "+----+----+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_join_unaliased_subqueries() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT o1, o2, c3, p1, p2, p3 FROM \
        (SELECT c1 AS o1, c2 + 1 AS o2, c3 FROM aggregate_test_100), \
        (SELECT c1 AS p1, c2 - 1 AS p2, c3 AS p3 FROM aggregate_test_100) LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+----+----+----+-----+",
        "| o1 | o2 | c3 | p1 | p2 | p3  |",
        "+----+----+----+----+----+-----+",
        "| c  | 3  | 1  | c  | 1  | 1   |",
        "| c  | 3  | 1  | d  | 4  | -40 |",
        "| c  | 3  | 1  | b  | 0  | 29  |",
        "| c  | 3  | 1  | a  | 0  | -85 |",
        "| c  | 3  | 1  | b  | 4  | -82 |",
        "+----+----+----+----+----+-----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

// Test prepare statement from sql to final result
// This test is equivalent with the test parallel_query_with_filter below but using prepare statement
#[tokio::test]
async fn test_prepare_statement() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

    // sql to statement then to prepare logical plan with parameters
    // c1 defined as UINT32, c2 defined as UInt64 but the params are Int32 and Float64
    let dataframe =
        ctx.sql("PREPARE my_plan(INT, DOUBLE) AS SELECT c1, c2 FROM test WHERE c1 > $2 AND c1 < $1").await?;

    // prepare logical plan to logical plan without parameters
    let param_values = vec![ScalarValue::Int32(Some(3)), ScalarValue::Float64(Some(0.0))];
    let dataframe = dataframe.with_param_values(param_values)?;
    let results = dataframe.collect().await?;

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 1  | 1  |",
        "| 1  | 10 |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 2  | 1  |",
        "| 2  | 10 |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn parallel_query_with_filter() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

    let dataframe = ctx
        .sql("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")
        .await?;
    let results = dataframe.collect().await.unwrap();
    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 1  | 1  |",
        "| 1  | 10 |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 2  | 1  |",
        "| 2  | 10 |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn query_with_filter_string_type_coercion() {
    let large_string_array = LargeStringArray::from(vec!["1", "2", "3", "4", "5"]);
    let schema =
        Schema::new(vec![Field::new("large_string", DataType::LargeUtf8, false)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(large_string_array)])
            .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    let sql = "select * from t where large_string = '1'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| large_string |",
        "+--------------+",
        "| 1            |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from t where large_string != '1'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| large_string |",
        "+--------------+",
        "| 2            |",
        "| 3            |",
        "| 4            |",
        "| 5            |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn query_empty_table() {
    let ctx = SessionContext::new();
    let empty_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
    ctx.register_table("test_tbl", empty_table).unwrap();
    let sql = "SELECT * FROM test_tbl";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("Query empty table");
    let expected = vec!["++", "++"];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn boolean_literal() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, c3 FROM test WHERE c1 > 2 AND c3 = true", 4)
            .await?;

    let expected = vec![
        "+----+------+",
        "| c1 | c3   |",
        "+----+------+",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "+----+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unprojected_filter() {
    let config = SessionConfig::new();
    let ctx = SessionContext::with_config(config);
    let df = ctx.read_table(table_with_sequence(1, 3).unwrap()).unwrap();

    let df = df
        .filter(col("i").gt(lit(2)))
        .unwrap()
        .select(vec![col("i") + col("i")])
        .unwrap();

    let plan = df.clone().into_optimized_plan().unwrap();
    println!("{}", plan.display_indent());

    let results = df.collect().await.unwrap();

    let expected = vec![
        "+-----------------------+",
        "| ?table?.i + ?table?.i |",
        "+-----------------------+",
        "| 6                     |",
        "+-----------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn case_sensitive_in_default_dialect() {
    let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![Field::new("INT32", DataType::Int32, false)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int32_array)]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    {
        let sql = "select \"int32\" from t";
        ctx.sql(sql).await.unwrap_err();
    }

    {
        let sql = "select \"INT32\" from t";
        let actual = execute_to_batches(&ctx, sql).await;

        let expected = vec![
            "+-------+",
            "| INT32 |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "| 3     |",
            "| 4     |",
            "| 5     |",
            "+-------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
}
