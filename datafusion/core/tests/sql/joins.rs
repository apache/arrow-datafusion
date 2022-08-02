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
use datafusion::from_slice::FromSlice;

#[tokio::test]
async fn equijoin() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    let ctx = create_join_context_qualified("t1", "t2")?;
    let equivalent_sql = [
        "SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.a ORDER BY t1.a",
        "SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t2.a = t1.a ORDER BY t1.a",
    ];
    let expected = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| 1 | 100 |",
        "| 2 | 200 |",
        "| 4 | 400 |",
        "+---+-----+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_multiple_condition_ordering() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t1_name <> t2_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t2_name <> t1_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id AND t1_name <> t2_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id AND t2_name <> t1_name ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_and_other_condition() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t2_name >= 'y' ORDER BY t1_id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_left_and_condition_from_right() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t2_name >= 'y' ORDER BY t1_id";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       |         |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn equijoin_right_and_condition_from_left() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id AND t1_id >= 22 ORDER BY t2_name";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "|       |         | w       |",
        "| 44    | d       | x       |",
        "| 22    | b       | y       |",
        "|       |         | z       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_left_and_condition_from_left() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t1_id >= 44 ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       |         |",
        "| 22    | b       |         |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn equijoin_left_and_condition_from_both() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_int, t2_int FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t1_int >= t2_int ORDER BY t1_id";
    let expected = vec![
        "+-------+--------+--------+",
        "| t1_id | t1_int | t2_int |",
        "+-------+--------+--------+",
        "| 11    | 1      |        |",
        "| 22    | 2      | 1      |",
        "| 33    | 3      |        |",
        "| 44    | 4      | 3      |",
        "+-------+--------+--------+",
    ];
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn equijoin_right_and_condition_from_right() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id AND t2_id >= 22 ORDER BY t2_name";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "|       |         | w       |",
        "| 44    | d       | x       |",
        "| 22    | b       | y       |",
        "|       |         | z       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_right_and_condition_from_both() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_int, t2_int, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id AND t2_int <= t1_int ORDER BY t2_id";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------+--------+-------+",
        "| t1_int | t2_int | t2_id |",
        "+--------+--------+-------+",
        "|        | 3      | 11    |",
        "| 2      | 1      | 22    |",
        "| 4      | 3      | 44    |",
        "|        | 3      | 55    |",
        "+--------+--------+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn left_join_unbalanced() -> Result<()> {
    // the t1_id is larger than t2_id so the hash_build_probe_order optimizer should kick in
    let ctx = create_join_context_unbalanced("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "| 77    | e       |         |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn left_join_null_filter() -> Result<()> {
    // Since t2 is the non-preserved side of the join, we cannot push down a NULL filter.
    // Note that this is only true because IS NULL does not remove nulls. For filters that
    // remove nulls, we can rewrite the join as an inner join and then push down the filter.
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_name IS NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 22    | 22    |         |",
        "| 33    |       |         |",
        "| 77    |       |         |",
        "| 88    |       |         |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join_null_filter_on_join_column() -> Result<()> {
    // Again, since t2 is the non-preserved side of the join, we cannot push down a NULL filter.
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_id IS NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 33    |       |         |",
        "| 77    |       |         |",
        "| 88    |       |         |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join_not_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_name IS NOT NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 11    | 11    | z       |",
        "| 44    | 44    | x       |",
        "| 99    | 99    | u       |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn left_join_not_null_filter_on_join_column() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t2_id, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id WHERE t2_id IS NOT NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+-------+---------+",
        "| t1_id | t2_id | t2_name |",
        "+-------+-------+---------+",
        "| 11    | 11    | z       |",
        "| 22    | 22    |         |",
        "| 44    | 44    | x       |",
        "| 99    | 99    | u       |",
        "+-------+-------+---------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn self_join_non_equijoin() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql =
        "SELECT x.t1_id, y.t1_id FROM t1 x JOIN t1 y ON x.t1_id = 11 AND y.t1_id = 44";
    let expected = vec![
        "+-------+-------+",
        "| t1_id | t1_id |",
        "+-------+-------+",
        "| 11    | 44    |",
        "+-------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_name IS NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "|       |         | 55    |",
        "| 99    |         | 99    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_null_filter_on_join_column() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_id IS NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "|       |         | 55    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_not_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_name IS NOT NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 11    | a       | 11    |",
        "| 22    | b       | 22    |",
        "| 44    | d       | 44    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join_not_null_filter_on_join_column() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 RIGHT JOIN t2 ON t1_id = t2_id WHERE t1_id IS NOT NULL ORDER BY t2_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 11    | a       | 11    |",
        "| 22    | b       | 22    |",
        "| 44    | d       | 44    |",
        "| 99    |         | 99    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn full_join_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id WHERE t1_name IS NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 88    |         |       |",
        "| 99    |         | 99    |",
        "|       |         | 55    |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn full_join_not_null_filter() -> Result<()> {
    let ctx = create_join_context_with_nulls()?;
    let sql = "SELECT t1_id, t1_name, t2_id FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id WHERE t1_name IS NOT NULL ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+-------+",
        "| t1_id | t1_name | t2_id |",
        "+-------+---------+-------+",
        "| 11    | a       | 11    |",
        "| 22    | b       | 22    |",
        "| 33    | c       |       |",
        "| 44    | d       | 44    |",
        "| 77    | e       |       |",
        "+-------+---------+-------+",
    ];

    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn right_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t2_id = t1_id ORDER BY t1_id"
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "|       |         | w       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn full_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "|       |         | w       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL OUTER JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn left_join_using() -> Result<()> {
    let ctx = create_join_context("id", "id")?;
    let sql = "SELECT id, t1_name, t2_name FROM t1 LEFT JOIN t2 USING (id) ORDER BY id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+---------+---------+",
        "| id | t1_name | t2_name |",
        "+----+---------+---------+",
        "| 11 | a       | z       |",
        "| 22 | b       | y       |",
        "| 33 | c       |         |",
        "| 44 | d       | x       |",
        "+----+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax_with_filter() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql = "SELECT t1_id, t1_name, t2_name \
        FROM t1, t2 \
        WHERE t1_id > 0 \
        AND t1_id = t2_id \
        AND t2_id < 99 \
        ORDER BY t1_id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax_reversed() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t2_id = t1_id ORDER BY t1_id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cross_join() {
    let ctx = create_join_context("t1_id", "t2_id").unwrap();

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1, t2 ORDER BY t1_id";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4, actual.len());

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE 1=1 ORDER BY t1_id";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4, actual.len());

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN t2";

    let actual = execute(&ctx, sql).await;
    assert_eq!(4 * 4, actual.len());

    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 11    | a       | y       |",
        "| 11    | a       | x       |",
        "| 11    | a       | w       |",
        "| 22    | b       | z       |",
        "| 22    | b       | y       |",
        "| 22    | b       | x       |",
        "| 22    | b       | w       |",
        "| 33    | c       | z       |",
        "| 33    | c       | y       |",
        "| 33    | c       | x       |",
        "| 33    | c       | w       |",
        "| 44    | d       | z       |",
        "| 44    | d       | y       |",
        "| 44    | d       | x       |",
        "| 44    | d       | w       |",
        "+-------+---------+---------+",
    ];

    assert_batches_eq!(expected, &actual);

    // Two partitions (from UNION) on the left
    let sql = "SELECT * FROM (SELECT t1_id, t1_name FROM t1 UNION ALL SELECT t1_id, t1_name FROM t1) AS t1 CROSS JOIN t2";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4 * 2, actual.len());

    // Two partitions (from UNION) on the right
    let sql = "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN (SELECT t2_name FROM t2 UNION ALL SELECT t2_name FROM t2) AS t2";
    let actual = execute(&ctx, sql).await;

    assert_eq!(4 * 4 * 2, actual.len());
}

#[tokio::test]
async fn cross_join_unbalanced() {
    // the t1_id is larger than t2_id so the hash_build_probe_order optimizer should kick in
    let ctx = create_join_context_unbalanced("t1_id", "t2_id").unwrap();

    // the order of the values is not determinisitic, so we need to sort to check the values
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN t2 ORDER BY t1_id, t1_name, t2_name";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | w       |",
        "| 11    | a       | x       |",
        "| 11    | a       | y       |",
        "| 11    | a       | z       |",
        "| 22    | b       | w       |",
        "| 22    | b       | x       |",
        "| 22    | b       | y       |",
        "| 22    | b       | z       |",
        "| 33    | c       | w       |",
        "| 33    | c       | x       |",
        "| 33    | c       | y       |",
        "| 33    | c       | z       |",
        "| 44    | d       | w       |",
        "| 44    | d       | x       |",
        "| 44    | d       | y       |",
        "| 44    | d       | z       |",
        "| 77    | e       | w       |",
        "| 77    | e       | x       |",
        "| 77    | e       | y       |",
        "| 77    | e       | z       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_join_timestamp() -> Result<()> {
    let ctx = SessionContext::new();

    // register time table
    let timestamp_schema = Arc::new(Schema::new(vec![Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let timestamp_data = RecordBatch::try_new(
        timestamp_schema.clone(),
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            131964190213133,
            131964190213134,
            131964190213135,
        ]))],
    )?;
    let timestamp_table =
        MemTable::try_new(timestamp_schema, vec![vec![timestamp_data]])?;
    ctx.register_table("timestamp", Arc::new(timestamp_table))?;

    let sql = "SELECT * \
                     FROM timestamp as a \
                     JOIN (SELECT * FROM timestamp) as b \
                     ON a.time = b.time \
                     ORDER BY a.time";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------------------------+-------------------------------+",
        "| time                          | time                          |",
        "+-------------------------------+-------------------------------+",
        "| 1970-01-02 12:39:24.190213133 | 1970-01-02 12:39:24.190213133 |",
        "| 1970-01-02 12:39:24.190213134 | 1970-01-02 12:39:24.190213134 |",
        "| 1970-01-02 12:39:24.190213135 | 1970-01-02 12:39:24.190213135 |",
        "+-------------------------------+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_join_float32() -> Result<()> {
    let ctx = SessionContext::new();

    // register population table
    let population_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("population", DataType::Float32, true),
    ]));
    let population_data = RecordBatch::try_new(
        population_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            Arc::new(Float32Array::from_slice(&[838.698, 1778.934, 626.443])),
        ],
    )?;
    let population_table =
        MemTable::try_new(population_schema, vec![vec![population_data]])?;
    ctx.register_table("population", Arc::new(population_table))?;

    let sql = "SELECT * \
                     FROM population as a \
                     JOIN (SELECT * FROM population) as b \
                     ON a.population = b.population \
                     ORDER BY a.population";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------+------------+------+------------+",
        "| city | population | city | population |",
        "+------+------------+------+------------+",
        "| c    | 626.443    | c    | 626.443    |",
        "| a    | 838.698    | a    | 838.698    |",
        "| b    | 1778.934   | b    | 1778.934   |",
        "+------+------------+------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_join_float64() -> Result<()> {
    let ctx = SessionContext::new();

    // register population table
    let population_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("population", DataType::Float64, true),
    ]));
    let population_data = RecordBatch::try_new(
        population_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            Arc::new(Float64Array::from_slice(&[838.698, 1778.934, 626.443])),
        ],
    )?;
    let population_table =
        MemTable::try_new(population_schema, vec![vec![population_data]])?;
    ctx.register_table("population", Arc::new(population_table))?;

    let sql = "SELECT * \
                     FROM population as a \
                     JOIN (SELECT * FROM population) as b \
                     ON a.population = b.population \
                     ORDER BY a.population";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------+------------+------+------------+",
        "| city | population | city | population |",
        "+------+------------+------+------------+",
        "| c    | 626.443    | c    | 626.443    |",
        "| a    | 838.698    | a    | 838.698    |",
        "| b    | 1778.934   | b    | 1778.934   |",
        "+------+------------+------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

// TODO Tests to prove correct implementation of INNER JOIN's with qualified names.
//  https://issues.apache.org/jira/projects/ARROW/issues/ARROW-11432.
#[tokio::test]
#[ignore]
async fn inner_join_qualified_names() -> Result<()> {
    // Setup the statements that test qualified names function correctly.
    let equivalent_sql = [
        "SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c
            FROM t1
            INNER JOIN t2 ON t1.a = t2.a
            ORDER BY t1.a",
        "SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c
            FROM t1
            INNER JOIN t2 ON t2.a = t1.a
            ORDER BY t1.a",
    ];

    let expected = vec![
        "+---+----+----+---+-----+-----+",
        "| a | b  | c  | a | b   | c   |",
        "+---+----+----+---+-----+-----+",
        "| 1 | 10 | 50 | 1 | 100 | 500 |",
        "| 2 | 20 | 60 | 2 | 200 | 600 |",
        "| 4 | 40 | 80 | 4 | 400 | 800 |",
        "+---+----+----+---+-----+-----+",
    ];

    for sql in equivalent_sql.iter() {
        let ctx = create_join_context_qualified("t1", "t2")?;
        let actual = execute_to_batches(&ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn issue_3002() -> Result<()> {
    // repro case for https://github.com/apache/arrow-datafusion/issues/3002
    let sql = "select a.a, b.b from a join b on a.a = b.b";
    let expected = vec!["++", "++"];
    let ctx = create_join_context_qualified("a", "b")?;
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn inner_join_nulls() {
    let sql = "SELECT * FROM (SELECT null AS id1) t1
            INNER JOIN (SELECT null AS id2) t2 ON id1 = id2";

    #[rustfmt::skip]
    let expected = vec![
        "++",
        "++",
    ];

    let ctx = create_join_context_qualified("t1", "t2").unwrap();
    let actual = execute_to_batches(&ctx, sql).await;

    // left and right shouldn't match anything
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn join_tables_with_duplicated_column_name_not_in_on_constraint() -> Result<()> {
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from_slice(&[1, 2, 3])) as _),
        (
            "country",
            Arc::new(StringArray::from_slice(&["Germany", "Sweden", "Japan"])) as _,
        ),
    ])
    .unwrap();
    let countries = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5, 6, 7])) as _,
        ),
        (
            "city",
            Arc::new(StringArray::from_slice(&[
                "Hamburg",
                "Stockholm",
                "Osaka",
                "Berlin",
                "Göteborg",
                "Tokyo",
                "Kyoto",
            ])) as _,
        ),
        (
            "country_id",
            Arc::new(Int32Array::from_slice(&[1, 2, 3, 1, 2, 3, 3])) as _,
        ),
    ])
    .unwrap();
    let cities = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    ctx.register_table("countries", Arc::new(countries))?;
    ctx.register_table("cities", Arc::new(cities))?;

    // city.id is not in the on constraint, but the output result will contain both city.id and
    // country.id
    let sql = "SELECT t1.id, t2.id, t1.city, t2.country FROM cities AS t1 JOIN countries AS t2 ON t1.country_id = t2.id ORDER BY t1.id";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+-----------+---------+",
        "| id | id | city      | country |",
        "+----+----+-----------+---------+",
        "| 1  | 1  | Hamburg   | Germany |",
        "| 2  | 2  | Stockholm | Sweden  |",
        "| 3  | 3  | Osaka     | Japan   |",
        "| 4  | 1  | Berlin    | Germany |",
        "| 5  | 2  | Göteborg  | Sweden  |",
        "| 6  | 3  | Tokyo     | Japan   |",
        "| 7  | 3  | Kyoto     | Japan   |",
        "+----+----+-----------+---------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn join_timestamp() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let expected = vec![
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
        "| nanos                         | micros                     | millis                  | secs                | name  | nanos                         | micros                     | millis                  | secs                | name  |",
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
        "| 2011-12-13 11:13:10.123450    | 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123 | 2011-12-13 11:13:10 | Row 1 | 2011-12-13 11:13:10.123450    | 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123 | 2011-12-13 11:13:10 | Row 1 |",
        "| 2018-11-13 17:11:10.011375885 | 2018-11-13 17:11:10.011375 | 2018-11-13 17:11:10.011 | 2018-11-13 17:11:10 | Row 0 | 2018-11-13 17:11:10.011375885 | 2018-11-13 17:11:10.011375 | 2018-11-13 17:11:10.011 | 2018-11-13 17:11:10 | Row 0 |",
        "| 2021-01-01 05:11:10.432       | 2021-01-01 05:11:10.432    | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10 | Row 3 | 2021-01-01 05:11:10.432       | 2021-01-01 05:11:10.432    | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10 | Row 3 |",
        "+-------------------------------+----------------------------+-------------------------+---------------------+-------+-------------------------------+----------------------------+-------------------------+---------------------+-------+",
    ];

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.nanos = t2.nanos",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.micros = t2.micros",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    let results = execute_to_batches(
        &ctx,
        "SELECT * FROM t as t1  \
         JOIN (SELECT * FROM t) as t2 \
         ON t1.millis = t2.millis",
    )
    .await;

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn left_join_should_not_panic_with_empty_side() -> Result<()> {
    let ctx = SessionContext::new();

    let t1_schema = Schema::new(vec![
        Field::new("t1_id", DataType::Int64, true),
        Field::new("t1_value", DataType::Utf8, false),
    ]);
    let t1_data = RecordBatch::try_new(
        Arc::new(t1_schema),
        vec![
            Arc::new(Int64Array::from_slice(&[5247, 3821, 6321, 8821, 7748])),
            Arc::new(StringArray::from_slice(&["a", "b", "c", "d", "e"])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_data.schema(), vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Schema::new(vec![
        Field::new("t2_id", DataType::Int64, true),
        Field::new("t2_value", DataType::Boolean, true),
    ]);
    let t2_data = RecordBatch::try_new(
        Arc::new(t2_schema),
        vec![
            Arc::new(Int64Array::from_slice(&[358, 2820, 3804, 7748])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                None,
            ])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_data.schema(), vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    let expected_left_join = vec![
        "+-------+----------+-------+----------+",
        "| t1_id | t1_value | t2_id | t2_value |",
        "+-------+----------+-------+----------+",
        "| 5247  | a        |       |          |",
        "| 3821  | b        |       |          |",
        "| 6321  | c        |       |          |",
        "| 8821  | d        |       |          |",
        "| 7748  | e        | 7748  |          |",
        "+-------+----------+-------+----------+",
    ];

    let results_left_join =
        execute_to_batches(&ctx, "SELECT * FROM t1 LEFT JOIN t2 ON t1_id = t2_id").await;
    assert_batches_sorted_eq!(expected_left_join, &results_left_join);

    let expected_right_join = vec![
        "+-------+----------+-------+----------+",
        "| t2_id | t2_value | t1_id | t1_value |",
        "+-------+----------+-------+----------+",
        "|       |          | 3821  | b        |",
        "|       |          | 5247  | a        |",
        "|       |          | 6321  | c        |",
        "|       |          | 8821  | d        |",
        "| 7748  |          | 7748  | e        |",
        "+-------+----------+-------+----------+",
    ];

    let result_right_join =
        execute_to_batches(&ctx, "SELECT * FROM t2 RIGHT JOIN t1 ON t1_id = t2_id").await;
    assert_batches_sorted_eq!(expected_right_join, &result_right_join);

    Ok(())
}

#[tokio::test]
async fn left_join_using_2() -> Result<()> {
    let results = execute_with_partition(
        "SELECT t1.c1, t2.c2 FROM test t1 JOIN test t2 USING (c2) ORDER BY t2.c2",
        1,
    )
    .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 0  | 1  |",
        "| 0  | 2  |",
        "| 0  | 3  |",
        "| 0  | 4  |",
        "| 0  | 5  |",
        "| 0  | 6  |",
        "| 0  | 7  |",
        "| 0  | 8  |",
        "| 0  | 9  |",
        "| 0  | 10 |",
        "+----+----+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn left_join_using_join_key_projection() -> Result<()> {
    let results = execute_with_partition(
        "SELECT t1.c1, t1.c2, t2.c2 FROM test t1 JOIN test t2 USING (c2) ORDER BY t2.c2",
        1,
    )
    .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+----+",
        "| c1 | c2 | c2 |",
        "+----+----+----+",
        "| 0  | 1  | 1  |",
        "| 0  | 2  | 2  |",
        "| 0  | 3  | 3  |",
        "| 0  | 4  | 4  |",
        "| 0  | 5  | 5  |",
        "| 0  | 6  | 6  |",
        "| 0  | 7  | 7  |",
        "| 0  | 8  | 8  |",
        "| 0  | 9  | 9  |",
        "| 0  | 10 | 10 |",
        "+----+----+----+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn left_join_2() -> Result<()> {
    let results = execute_with_partition(
        "SELECT t1.c1, t1.c2, t2.c2 FROM test t1 JOIN test t2 ON t1.c2 = t2.c2 ORDER BY t1.c2",
        1,
    )
        .await?;
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+----+",
        "| c1 | c2 | c2 |",
        "+----+----+----+",
        "| 0  | 1  | 1  |",
        "| 0  | 2  | 2  |",
        "| 0  | 3  | 3  |",
        "| 0  | 4  | 4  |",
        "| 0  | 5  | 5  |",
        "| 0  | 6  | 6  |",
        "| 0  | 7  | 7  |",
        "| 0  | 8  | 8  |",
        "| 0  | 9  | 9  |",
        "| 0  | 10 | 10 |",
        "+----+----+----+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn join_partitioned() -> Result<()> {
    // self join on partition id (workaround for duplicate column name)
    let results = execute_with_partition(
        "SELECT 1 FROM test JOIN (SELECT c1 AS id1 FROM test) AS a ON c1=id1",
        4,
    )
    .await?;

    assert_eq!(
        results.iter().map(|b| b.num_rows()).sum::<usize>(),
        4 * 10 * 10
    );

    Ok(())
}

#[tokio::test]
async fn hash_join_with_date32() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // inner join on hash supported data type (Date32)
    let sql = "select * from t1 join t2 on t1.c1 = t2.c1";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.c1, #t1.c2, #t1.c3, #t1.c4, #t2.c1, #t2.c2, #t2.c3, #t2.c4 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    Inner Join: #t1.c1 = #t2.c1 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "      TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "      TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    let expected = vec![
        "+------------+------------+---------+-----+------------+------------+---------+-----+",
        "| c1         | c2         | c3      | c4  | c1         | c2         | c3      | c4  |",
        "+------------+------------+---------+-----+------------+------------+---------+-----+",
        "| 1970-01-02 | 1970-01-02 | 1.23    | abc | 1970-01-02 | 1970-01-02 | -123.12 | abc |",
        "| 1970-01-04 |            | -123.12 | jkl | 1970-01-04 |            | 789.00  |     |",
        "+------------+------------+---------+-----+------------+------------+---------+-----+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn hash_join_with_date64() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // left join on hash supported data type (Date64)
    let sql = "select * from t1 left join t2 on t1.c2 = t2.c2";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.c1, #t1.c2, #t1.c3, #t1.c4, #t2.c1, #t2.c2, #t2.c3, #t2.c4 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    Left Join: #t1.c2 = #t2.c2 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "      TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "      TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    let expected = vec![
        "+------------+------------+---------+-----+------------+------------+---------+--------+",
        "| c1         | c2         | c3      | c4  | c1         | c2         | c3      | c4     |",
        "+------------+------------+---------+-----+------------+------------+---------+--------+",
        "|            | 1970-01-04 | 789.00  | ghi |            | 1970-01-04 | 0.00    | qwerty |",
        "| 1970-01-02 | 1970-01-02 | 1.23    | abc | 1970-01-02 | 1970-01-02 | -123.12 | abc    |",
        "| 1970-01-03 | 1970-01-03 | 456.00  | def |            |            |         |        |",
        "| 1970-01-04 |            | -123.12 | jkl |            |            |         |        |",
        "+------------+------------+---------+-----+------------+------------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn hash_join_with_decimal() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // right join on hash supported data type (Decimal)
    let sql = "select * from t1 right join t2 on t1.c3 = t2.c3";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
    "Explain [plan_type:Utf8, plan:Utf8]",
    "  Projection: #t1.c1, #t1.c2, #t1.c3, #t1.c4, #t2.c1, #t2.c2, #t2.c3, #t2.c4 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    "    Right Join: #t1.c3 = #t2.c3 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    "      TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
    "      TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    let expected = vec![
    "+------------+------------+---------+-----+------------+------------+-----------+---------+",
    "| c1         | c2         | c3      | c4  | c1         | c2         | c3        | c4      |",
    "+------------+------------+---------+-----+------------+------------+-----------+---------+",
    "|            |            |         |     |            |            | 100000.00 | abcdefg |",
    "|            |            |         |     |            | 1970-01-04 | 0.00      | qwerty  |",
    "|            | 1970-01-04 | 789.00  | ghi | 1970-01-04 |            | 789.00    |         |",
    "| 1970-01-04 |            | -123.12 | jkl | 1970-01-02 | 1970-01-02 | -123.12   | abc     |",
    "+------------+------------+---------+-----+------------+------------+-----------+---------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn hash_join_with_dictionary() -> Result<()> {
    let ctx = create_hashjoin_datatype_context()?;

    // inner join on hash supported data type (Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)))
    let sql = "select * from t1 join t2 on t1.c4 = t2.c4";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.c1, #t1.c2, #t1.c3, #t1.c4, #t2.c1, #t2.c2, #t2.c3, #t2.c4 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "    Inner Join: #t1.c4 = #t2.c4 [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N, c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "      TableScan: t1 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(5, 2);N, c4:Dictionary(Int32, Utf8);N]",
        "      TableScan: t2 projection=[c1, c2, c3, c4] [c1:Date32;N, c2:Date64;N, c3:Decimal128(10, 2);N, c4:Dictionary(Int32, Utf8);N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    let expected = vec![
        "+------------+------------+------+-----+------------+------------+---------+-----+",
        "| c1         | c2         | c3   | c4  | c1         | c2         | c3      | c4  |",
        "+------------+------------+------+-----+------------+------------+---------+-----+",
        "| 1970-01-02 | 1970-01-02 | 1.23 | abc | 1970-01-02 | 1970-01-02 | -123.12 | abc |",
        "+------------+------------+------+-----+------------+------------+---------+-----+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_left_join_1() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to inner join
    let sql = "select * from t1 left join t2 on t1.t1_id = t2.t2_id where t2.t2_id < 100";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Inner Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Filter: #t1.t1_id < Int64(100) [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "      Filter: #t2.t2_id < Int64(100) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_left_join_2() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to inner join
    let sql = "select * from t1 left join t2 on t1.t1_id = t2.t2_id where t2.t2_int < 10 or (t1.t1_int > 2 and t2.t2_name != 'w')";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Filter: #t2.t2_int < Int64(10) OR #t1.t1_int > Int64(2) AND #t2.t2_name != Utf8(\"w\") [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Inner Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_left_join_3() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce subquery to inner join
    let sql = "select * from (select t1.* from t1 left join t2 on t1.t1_id = t2.t2_id where t2.t2_int < 3) t3 left join t2 on t3.t1_int = t2.t2_int where t3.t1_id < 100";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t3.t1_id, #t3.t1_name, #t3.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Left Join: #t3.t1_int = #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Projection: #t3.t1_id, #t3.t1_name, #t3.t1_int, alias=t3 [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, alias=t3 [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "          Inner Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "            Filter: #t1.t1_id < Int64(100) [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "              TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "            Filter: #t2.t2_int < Int64(3) AND #t2.t2_id < Int64(100) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "              TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 22    | b       | 2      |       |         |        |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_right_join_1() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to inner join
    let sql = "select * from t1 right join t2 on t1.t1_id = t2.t2_id where t1.t1_int is not null";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Inner Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Filter: #t1.t1_int IS NOT NULL [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "      TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_right_join_2() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to inner join
    let sql = "select * from t1 right join t2 on t1.t1_id = t2.t2_id where not(t1.t1_int = t2.t2_int)";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Filter: #t1.t1_int != #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Inner Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_full_join_to_right_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to right join
    let sql = "select * from t1 full join t2 on t1.t1_id = t2.t2_id where t2.t2_name is not null";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Right Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "      Filter: #t2.t2_name IS NOT NULL [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "|       |         |        | 55    | w       | 3      |",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn reduce_full_join_to_left_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to left join
    let sql =
        "select * from t1 full join t2 on t1.t1_id = t2.t2_id where t1.t1_name != 'b'";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Left Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Filter: #t1.t1_name != Utf8(\"b\") [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "      TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 33    | c       | 3      |       |         |        |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn reduce_full_join_to_inner_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;

    // reduce to inner join
    let sql = "select * from t1 full join t2 on t1.t1_id = t2.t2_id where t1.t1_name != 'b' and t2.t2_name = 'x'";
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #t1.t1_id, #t1.t1_name, #t1.t1_int, #t2.t2_id, #t2.t2_name, #t2.t2_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Inner Join: #t1.t1_id = #t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Filter: #t1.t1_name != Utf8(\"b\") [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "      Filter: #t2.t2_name = Utf8(\"x\") [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t2 projection=[t2_id, t2_name, t2_int] [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
