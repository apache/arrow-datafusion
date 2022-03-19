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

#[tokio::test]
async fn csv_query_avg_multi_batch() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
    Ok(())
}

#[tokio::test]
async fn csv_query_avg() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.5089725099127211"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_covariance_1() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT covar_pop(c2, c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["-0.07916932235380847"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_covariance_2() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT covar(c2, c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["-0.07996901247859442"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_correlation() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT corr(c2, c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["-0.19064544190576607"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_1() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT var_pop(c2) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.8675"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_2() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT var_pop(c6) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["26156334342021890000000000000000000000"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_3() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT var_pop(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.09234223721582163"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_4() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT var(c2) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.8863636363636365"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_variance_5() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT var_samp(c2) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.8863636363636365"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_1() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT stddev_pop(c2) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["1.3665650368716449"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_2() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT stddev_pop(c6) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["5114326382039172000"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_3() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT stddev_pop(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.30387865541334363"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_4() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT stddev(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.3054095399405338"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_5() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT stddev_samp(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.3054095399405338"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_stddev_6() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "select stddev(sq.column1) from (values (1.1), (2.0), (3.0)) as sq";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.9504384952922168"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_median_1() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT approx_median(c2) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["3"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_median_2() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT approx_median(c6) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["1146409980542786560"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_median_3() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT approx_median(c12) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0.5550065410522981"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_external_table_count() {
    let mut ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_external_table_sum() {
    let mut ctx = SessionContext::new();
    // cast smallint and int to bigint to avoid overflow during calculation
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql =
        "SELECT SUM(CAST(c7 AS BIGINT)), SUM(CAST(c8 AS BIGINT)) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------------------+-------------------------------------------+",
        "| SUM(CAST(aggregate_test_100.c7 AS Int64)) | SUM(CAST(aggregate_test_100.c8 AS Int64)) |",
        "+-------------------------------------------+-------------------------------------------+",
        "| 13060                                     | 3017641                                   |",
        "+-------------------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT count(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_distinct() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT count(distinct c2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+---------------------------------------+",
        "| COUNT(DISTINCT aggregate_test_100.c2) |",
        "+---------------------------------------+",
        "| 5                                     |",
        "+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_distinct_expr() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT count(distinct c2 % 2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------------+",
        "| COUNT(DISTINCT aggregate_test_100.c2 % Int64(2)) |",
        "+--------------------------------------------------+",
        "| 2                                                |",
        "+--------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_star() {
    let mut ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(*) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count_one() {
    let mut ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_approx_count() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT approx_distinct(c9) count_c9, approx_distinct(cast(c9 as varchar)) count_c9_str FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+--------------+",
        "| count_c9 | count_c9_str |",
        "+----------+--------------+",
        "| 100      | 99           |",
        "+----------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

// This test executes the APPROX_PERCENTILE_CONT aggregation against the test
// data, asserting the estimated quantiles are ±5% their actual values.
//
// Actual quantiles calculated with:
//
// ```r
// read_csv("./testing/data/csv/aggregate_test_100.csv") |>
//     select_if(is.numeric) |>
//     summarise_all(~ quantile(., c(0.1, 0.5, 0.9)))
// ```
//
// Giving:
//
// ```text
//      c2    c3      c4           c5       c6    c7     c8          c9     c10   c11    c12
//   <dbl> <dbl>   <dbl>        <dbl>    <dbl> <dbl>  <dbl>       <dbl>   <dbl> <dbl>  <dbl>
// 1     1 -95.3 -22925. -1882606710  -7.25e18  18.9  2671.  472608672. 1.83e18 0.109 0.0714
// 2     3  15.5   4599    377164262   1.13e18 134.  30634  2365817608. 9.30e18 0.491 0.551
// 3     5 102.   25334.  1991374996.  7.37e18 231   57518. 3776538487. 1.61e19 0.834 0.946
// ```
//
// Column `c12` is omitted due to a large relative error (~10%) due to the small
// float values.
#[tokio::test]
async fn csv_query_approx_percentile_cont() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;

    // Generate an assertion that the estimated $percentile value for $column is
    // within 5% of the $actual percentile value.
    macro_rules! percentile_test {
        ($ctx:ident, column=$column:literal, percentile=$percentile:literal, actual=$actual:literal) => {
            let sql = format!("SELECT (ABS(1 - CAST(approx_percentile_cont({}, {}) AS DOUBLE) / {}) < 0.05) AS q FROM aggregate_test_100", $column, $percentile, $actual);
            let actual = execute_to_batches(&ctx, &sql).await;
            //
            //   "+------+",
            //   "| q    |",
            //   "+------+",
            //   "| true |",
            //   "+------+",
            //
            let want = ["+------+", "| q    |", "+------+", "| true |", "+------+"];
            assert_batches_eq!(want, &actual);
        };
    }

    percentile_test!(ctx, column = "c2", percentile = 0.1, actual = 1.0);
    percentile_test!(ctx, column = "c2", percentile = 0.5, actual = 3.0);
    percentile_test!(ctx, column = "c2", percentile = 0.9, actual = 5.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c3", percentile = 0.1, actual = -95.3);
    percentile_test!(ctx, column = "c3", percentile = 0.5, actual = 15.5);
    percentile_test!(ctx, column = "c3", percentile = 0.9, actual = 102.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c4", percentile = 0.1, actual = -22925.0);
    percentile_test!(ctx, column = "c4", percentile = 0.5, actual = 4599.0);
    percentile_test!(ctx, column = "c4", percentile = 0.9, actual = 25334.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c5", percentile = 0.1, actual = -1882606710.0);
    percentile_test!(ctx, column = "c5", percentile = 0.5, actual = 377164262.0);
    percentile_test!(ctx, column = "c5", percentile = 0.9, actual = 1991374996.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c6", percentile = 0.1, actual = -7.25e18);
    percentile_test!(ctx, column = "c6", percentile = 0.5, actual = 1.13e18);
    percentile_test!(ctx, column = "c6", percentile = 0.9, actual = 7.37e18);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c7", percentile = 0.1, actual = 18.9);
    percentile_test!(ctx, column = "c7", percentile = 0.5, actual = 134.0);
    percentile_test!(ctx, column = "c7", percentile = 0.9, actual = 231.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c8", percentile = 0.1, actual = 2671.0);
    percentile_test!(ctx, column = "c8", percentile = 0.5, actual = 30634.0);
    percentile_test!(ctx, column = "c8", percentile = 0.9, actual = 57518.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c9", percentile = 0.1, actual = 472608672.0);
    percentile_test!(ctx, column = "c9", percentile = 0.5, actual = 2365817608.0);
    percentile_test!(ctx, column = "c9", percentile = 0.9, actual = 3776538487.0);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c10", percentile = 0.1, actual = 1.83e18);
    percentile_test!(ctx, column = "c10", percentile = 0.5, actual = 9.30e18);
    percentile_test!(ctx, column = "c10", percentile = 0.9, actual = 1.61e19);
    ////////////////////////////////////
    percentile_test!(ctx, column = "c11", percentile = 0.1, actual = 0.109);
    percentile_test!(ctx, column = "c11", percentile = 0.5, actual = 0.491);
    percentile_test!(ctx, column = "c11", percentile = 0.9, actual = 0.834);

    Ok(())
}

#[tokio::test]
async fn csv_query_sum_crossjoin() {
    let mut ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT a.c1, b.c1, SUM(a.c2) FROM aggregate_test_100 as a CROSS JOIN aggregate_test_100 as b GROUP BY a.c1, b.c1 ORDER BY a.c1, b.c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+-----------+",
        "| c1 | c1 | SUM(a.c2) |",
        "+----+----+-----------+",
        "| a  | a  | 1260      |",
        "| a  | b  | 1140      |",
        "| a  | c  | 1260      |",
        "| a  | d  | 1080      |",
        "| a  | e  | 1260      |",
        "| b  | a  | 1302      |",
        "| b  | b  | 1178      |",
        "| b  | c  | 1302      |",
        "| b  | d  | 1116      |",
        "| b  | e  | 1302      |",
        "| c  | a  | 1176      |",
        "| c  | b  | 1064      |",
        "| c  | c  | 1176      |",
        "| c  | d  | 1008      |",
        "| c  | e  | 1176      |",
        "| d  | a  | 924       |",
        "| d  | b  | 836       |",
        "| d  | c  | 924       |",
        "| d  | d  | 792       |",
        "| d  | e  | 924       |",
        "| e  | a  | 1323      |",
        "| e  | b  | 1197      |",
        "| e  | c  | 1323      |",
        "| e  | d  | 1134      |",
        "| e  | e  | 1323      |",
        "+----+----+-----------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn query_count_without_from() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT count(1 + 1)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| COUNT(Int64(1) + Int64(1)) |",
        "+----------------------------+",
        "| 1                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 2) test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------------------------------------------------------------+",
        "| ARRAYAGG(test.c13)                                               |",
        "+------------------------------------------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm, 0keZ5G8BffGwgF2RwQD59TFzMStxCB] |",
        "+------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_empty() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 LIMIT 0) test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------+",
        "| ARRAYAGG(test.c13) |",
        "+--------------------+",
        "| []                 |",
        "+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_one() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 1) test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| ARRAYAGG(test.c13)               |",
        "+----------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm] |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_distinct() -> Result<()> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx).await?;
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
async fn aggregate_timestamps_sum() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = plan_and_collect(
        &mut ctx,
        "SELECT sum(nanos), sum(micros), sum(millis), sum(secs) FROM t",
    )
    .await
    .unwrap_err();

    assert_eq!(results.to_string(), "Error during planning: The function Sum does not support inputs of type Timestamp(Nanosecond, None).");

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_count() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT count(nanos), count(micros), count(millis), count(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+----------------+-----------------+-----------------+---------------+",
        "| COUNT(t.nanos) | COUNT(t.micros) | COUNT(t.millis) | COUNT(t.secs) |",
        "+----------------+-----------------+-----------------+---------------+",
        "| 3              | 3               | 3               | 3             |",
        "+----------------+-----------------+-----------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_min() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT min(nanos), min(micros), min(millis), min(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+----------------------------+----------------------------+-------------------------+---------------------+",
        "| MIN(t.nanos)               | MIN(t.micros)              | MIN(t.millis)           | MIN(t.secs)         |",
        "+----------------------------+----------------------------+-------------------------+---------------------+",
        "| 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123450 | 2011-12-13 11:13:10.123 | 2011-12-13 11:13:10 |",
        "+----------------------------+----------------------------+-------------------------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_max() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = execute_to_batches(
        &ctx,
        "SELECT max(nanos), max(micros), max(millis), max(secs) FROM t",
    )
    .await;

    let expected = vec![
        "+-------------------------+-------------------------+-------------------------+---------------------+",
        "| MAX(t.nanos)            | MAX(t.micros)           | MAX(t.millis)           | MAX(t.secs)         |",
        "+-------------------------+-------------------------+-------------------------+---------------------+",
        "| 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10.432 | 2021-01-01 05:11:10 |",
        "+-------------------------+-------------------------+-------------------------+---------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn aggregate_timestamps_avg() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_table("t", table_with_timestamps()).unwrap();

    let results = plan_and_collect(
        &mut ctx,
        "SELECT avg(nanos), avg(micros), avg(millis), avg(secs) FROM t",
    )
    .await
    .unwrap_err();

    assert_eq!(results.to_string(), "Error during planning: The function Avg does not support inputs of type Timestamp(Nanosecond, None).");
    Ok(())
}

#[tokio::test]
async fn csv_query_bitmap_i8() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT bitmap_distinct(c2) FROM aggregate_test_100";
    let results = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------+",
        "| BITMAPCOUNTDISTINCT(aggregate_test_100.c2) |",
        "+--------------------------------------------+",
        "| 5                                          |",
        "+--------------------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn csv_query_bitmap_u8() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT bitmap_distinct(c3) FROM aggregate_test_100";
    let results = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------+",
        "| BITMAPCOUNTDISTINCT(aggregate_test_100.c3) |",
        "+--------------------------------------------+",
        "| 80                                         |",
        "+--------------------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn csv_query_bitmap_u32() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT bitmap_distinct(c5) FROM aggregate_test_100";
    let results = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------+",
        "| BITMAPCOUNTDISTINCT(aggregate_test_100.c5) |",
        "+--------------------------------------------+",
        "| 100                                        |",
        "+--------------------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn csv_query_bitmap_group() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql1 = "SELECT bitmap_distinct(c5) as a FROM aggregate_test_100 group by c1 order by a limit 5 ";
    let result1 = execute_to_batches(&mut ctx, sql1).await;

    let sql2 = "SELECT count(distinct c5) as a FROM aggregate_test_100 group by c1 order by a limit 5 ";
    let result2 = execute_to_batches(&mut ctx, sql2).await;

    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&result1)
            .unwrap()
            .to_string(),
        arrow::util::pretty::pretty_format_batches(&result2)
            .unwrap()
            .to_string()
    );
    Ok(())
}
