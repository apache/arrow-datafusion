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

// Include tests in dataframe_functions
mod dataframe_functions;
mod describe;

use arrow::datatypes::{DataType, Field, Float32Type, Int32Type, Schema, UInt64Type};
use arrow::util::pretty::pretty_format_batches;
use arrow::{
    array::{
        ArrayRef, FixedSizeListArray, FixedSizeListBuilder, Int32Array, Int32Builder,
        LargeListArray, ListArray, ListBuilder, StringArray, StringBuilder,
        StructBuilder, UInt32Array, UInt32Builder,
    },
    record_batch::RecordBatch,
};
use arrow_array::Float32Array;
use arrow_schema::ArrowError;
use datafusion_functions_aggregate::count::count_udaf;
use object_store::local::LocalFileSystem;
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::prelude::JoinType;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};
use datafusion::test_util::{parquet_test_data, populate_csv_partitions};
use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
use datafusion_common::{assert_contains, DataFusionError, ScalarValue, UnnestOptions};
use datafusion_common::logical_type::TypeRelation;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::expr::{GroupingSet, Sort};
use datafusion_expr::var_provider::{VarProvider, VarType};
use datafusion_expr::{
    array_agg, cast, col, exists, expr, in_subquery, lit, max, out_ref_col, placeholder,
    scalar_subquery, when, wildcard, Expr, ExprSchemable, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_functions_aggregate::expr_fn::{avg, count, sum};

#[tokio::test]
async fn test_count_wildcard_on_sort() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select b,count(*) from t1 group by b order by count(*)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    let df_results = ctx
        .table("t1")
        .await?
        .aggregate(vec![col("b")], vec![count(wildcard())])?
        .sort(vec![count(wildcard()).sort(true, false)])?
        .explain(false, false)?
        .collect()
        .await?;
    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );
    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_in() -> Result<()> {
    let ctx = create_join_context()?;
    let sql_results = ctx
        .sql("SELECT a,b FROM t1 WHERE a in (SELECT count(*) FROM t2)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    // In the same SessionContext, AliasGenerator will increase subquery_alias id by 1
    // https://github.com/apache/datafusion/blame/cf45eb9020092943b96653d70fafb143cc362e19/datafusion/optimizer/src/alias.rs#L40-L43
    // for compare difference betwwen sql and df logical plan, we need to create a new SessionContext here
    let ctx = create_join_context()?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(in_subquery(
            col("a"),
            Arc::new(
                ctx.table("t2")
                    .await?
                    .aggregate(vec![], vec![count(wildcard())])?
                    .select(vec![count(wildcard())])?
                    .into_unoptimized_plan(),
                // Usually, into_optimized_plan() should be used here, but due to
                // https://github.com/apache/datafusion/issues/5771,
                // subqueries in SQL cannot be optimized, resulting in differences in logical_plan. Therefore, into_unoptimized_plan() is temporarily used here.
            ),
        ))?
        .select(vec![col("a"), col("b")])?
        .explain(false, false)?
        .collect()
        .await?;

    // make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_exist() -> Result<()> {
    let ctx = create_join_context()?;
    let sql_results = ctx
        .sql("SELECT a, b FROM t1 WHERE EXISTS (SELECT count(*) FROM t2)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(exists(Arc::new(
            ctx.table("t2")
                .await?
                .aggregate(vec![], vec![count(wildcard())])?
                .select(vec![count(wildcard())])?
                .into_unoptimized_plan(),
            // Usually, into_optimized_plan() should be used here, but due to
            // https://github.com/apache/datafusion/issues/5771,
            // subqueries in SQL cannot be optimized, resulting in differences in logical_plan. Therefore, into_unoptimized_plan() is temporarily used here.
        )))?
        .select(vec![col("a"), col("b")])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_window() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select count(*) OVER(ORDER BY a DESC RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING)  from t1")
        .await?
        .explain(false, false)?
        .collect()
        .await?;
    let df_results = ctx
        .table("t1")
        .await?
        .select(vec![Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(count_udaf()),
            vec![wildcard()],
            vec![],
            vec![Expr::Sort(Sort::new(Box::new(col("a")), false, true))],
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(ScalarValue::UInt32(Some(6))),
                WindowFrameBound::Following(ScalarValue::UInt32(Some(2))),
            ),
            None,
        ))])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&df_results)?.to_string(),
        pretty_format_batches(&sql_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_aggregate() -> Result<()> {
    let ctx = create_join_context()?;
    register_alltypes_tiny_pages_parquet(&ctx).await?;

    let sql_results = ctx
        .sql("select count(*) from t1")
        .await?
        .select(vec![col("count(*)")])?
        .explain(false, false)?
        .collect()
        .await?;

    // add `.select(vec![count(wildcard())])?` to make sure we can analyze all node instead of just top node.
    let df_results = ctx
        .table("t1")
        .await?
        .aggregate(vec![], vec![count(wildcard())])?
        .select(vec![count(wildcard())])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}
#[tokio::test]
async fn test_count_wildcard_on_where_scalar_subquery() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select a,b from t1 where (select count(*) from t2 where t1.a = t2.a)>0;")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    // In the same SessionContext, AliasGenerator will increase subquery_alias id by 1
    // https://github.com/apache/datafusion/blame/cf45eb9020092943b96653d70fafb143cc362e19/datafusion/optimizer/src/alias.rs#L40-L43
    // for compare difference between sql and df logical plan, we need to create a new SessionContext here
    let ctx = create_join_context()?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(
            scalar_subquery(Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(out_ref_col(DataType::UInt32, "t1.a").eq(col("t2.a")))?
                    .aggregate(vec![], vec![count(wildcard())])?
                    .select(vec![col(count(wildcard()).to_string())])?
                    .into_unoptimized_plan(),
            ))
            .gt(lit(ScalarValue::UInt8(Some(0)))),
        )?
        .select(vec![col("t1.a"), col("t1.b")])?
        .explain(false, false)?
        .collect()
        .await?;

    //make sure sql plan same with df plan
    assert_eq!(
        pretty_format_batches(&sql_results)?.to_string(),
        pretty_format_batches(&df_results)?.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn join() -> Result<()> {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("aa", batch1)?;

    let df1 = ctx.table("aa").await?;

    ctx.register_batch("aaa", batch2)?;

    let df2 = ctx.table("aaa").await?;

    let a = df1.join(df2, JoinType::Inner, &["a"], &["a"], None)?;

    let batches = a.collect().await?;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);

    Ok(())
}

#[tokio::test]
async fn sort_on_unprojected_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .sort(vec![Expr::Sort(Sort::new(Box::new(col("b")), false, true))])
        .unwrap();
    let results = df.collect().await.unwrap();

    #[rustfmt::skip]
        let expected = ["+-----+",
        "| a   |",
        "+-----+",
        "| 100 |",
        "| 10  |",
        "| 10  |",
        "| 1   |",
        "+-----+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sort_on_distinct_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![Expr::Sort(Sort::new(Box::new(col("a")), false, true))])
        .unwrap();
    let results = df.collect().await.unwrap();

    #[rustfmt::skip]
        let expected = ["+-----+",
        "| a   |",
        "+-----+",
        "| 100 |",
        "| 10  |",
        "| 1   |",
        "+-----+"];
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn sort_on_distinct_unprojected_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5])),
        ],
    )?;

    // Cannot sort on a column after distinct that would add a new column
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let err = ctx
        .table("t")
        .await?
        .select(vec![col("a")])?
        .distinct()?
        .sort(vec![Expr::Sort(Sort::new(Box::new(col("b")), false, true))])
        .unwrap_err();
    assert_eq!(err.strip_backtrace(), "Error during planning: For SELECT DISTINCT, ORDER BY expressions b must appear in select list");
    Ok(())
}

#[tokio::test]
async fn sort_on_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .sort(vec![col("b").sort(true, true)])
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.strip_backtrace(), expected);
    Ok(())
}

#[tokio::test]
async fn group_by_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .aggregate(vec![col("b")], vec![max(col("a"))])
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.strip_backtrace(), expected);
    Ok(())
}

#[tokio::test]
async fn filter_on_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .filter(col("b").eq(lit(1)))
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.strip_backtrace(), expected);
    Ok(())
}

#[tokio::test]
async fn select_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .select(vec![col("b")])
        .unwrap_err();

    let expected = "Schema error: Ambiguous reference to unqualified field b";
    assert_eq!(err.strip_backtrace(), expected);
    Ok(())
}

#[tokio::test]
async fn filter_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![(col("a").eq(lit(10))).alias("a")])
        .unwrap()
        .filter(col("a"))
        .unwrap();
    let results = df.collect().await.unwrap();

    #[rustfmt::skip]
        let expected = ["+------+",
        "| a    |",
        "+------+",
        "| true |",
        "| true |",
        "+------+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx
        .table("t")
        .await?
        .select(vec![col("a").alias("a")])?
        .select(vec![(col("a").eq(lit(10))).alias("a")])?
        .select(vec![col("a")])?;

    let results = df.collect().await?;

    #[rustfmt::skip]
        let expected = ["+-------+",
        "| a     |",
        "+-------+",
        "| false |",
        "| true  |",
        "| true  |",
        "| false |",
        "+-------+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_grouping_sets() -> Result<()> {
    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("a")],
        vec![col("b")],
        vec![col("a"), col("b")],
    ]));

    let df = create_test_table("test")
        .await?
        .aggregate(vec![grouping_set_expr], vec![count(col("a"))])?
        .sort(vec![
            Expr::Sort(Sort::new(Box::new(col("a")), false, true)),
            Expr::Sort(Sort::new(Box::new(col("b")), false, true)),
        ])?;

    let results = df.collect().await?;

    let expected = vec![
        "+-----------+-----+---------------+",
        "| a         | b   | count(test.a) |",
        "+-----------+-----+---------------+",
        "|           | 100 | 1             |",
        "|           | 10  | 2             |",
        "|           | 1   | 1             |",
        "| abcDEF    |     | 1             |",
        "| abcDEF    | 1   | 1             |",
        "| abc123    |     | 1             |",
        "| abc123    | 10  | 1             |",
        "| CBAdef    |     | 1             |",
        "| CBAdef    | 10  | 1             |",
        "| 123AbcDef |     | 1             |",
        "| 123AbcDef | 100 | 1             |",
        "+-----------+-----+---------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_grouping_sets_count() -> Result<()> {
    let ctx = SessionContext::new();

    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("c1")],
        vec![col("c2")],
    ]));

    let df = aggregates_table(&ctx)
        .await?
        .aggregate(vec![grouping_set_expr], vec![count(lit(1))])?
        .sort(vec![
            Expr::Sort(Sort::new(Box::new(col("c1")), false, true)),
            Expr::Sort(Sort::new(Box::new(col("c2")), false, true)),
        ])?;

    let results = df.collect().await?;

    let expected = vec![
        "+----+----+-----------------+",
        "| c1 | c2 | count(Int32(1)) |",
        "+----+----+-----------------+",
        "|    | 5  | 14              |",
        "|    | 4  | 23              |",
        "|    | 3  | 19              |",
        "|    | 2  | 22              |",
        "|    | 1  | 22              |",
        "| e  |    | 21              |",
        "| d  |    | 18              |",
        "| c  |    | 21              |",
        "| b  |    | 19              |",
        "| a  |    | 21              |",
        "+----+----+-----------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_grouping_set_array_agg_with_overflow() -> Result<()> {
    let ctx = SessionContext::new();

    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("c1")],
        vec![col("c2")],
        vec![col("c1"), col("c2")],
    ]));

    let df = aggregates_table(&ctx)
        .await?
        .aggregate(
            vec![grouping_set_expr],
            vec![
                sum(col("c3")).alias("sum_c3"),
                avg(col("c3")).alias("avg_c3"),
            ],
        )?
        .sort(vec![
            Expr::Sort(Sort::new(Box::new(col("c1")), false, true)),
            Expr::Sort(Sort::new(Box::new(col("c2")), false, true)),
        ])?;

    let results = df.collect().await?;

    let expected = vec![
        "+----+----+--------+---------------------+",
        "| c1 | c2 | sum_c3 | avg_c3              |",
        "+----+----+--------+---------------------+",
        "|    | 5  | -194   | -13.857142857142858 |",
        "|    | 4  | 29     | 1.2608695652173914  |",
        "|    | 3  | 395    | 20.789473684210527  |",
        "|    | 2  | 184    | 8.363636363636363   |",
        "|    | 1  | 367    | 16.681818181818183  |",
        "| e  |    | 847    | 40.333333333333336  |",
        "| e  | 5  | -22    | -11.0               |",
        "| e  | 4  | 261    | 37.285714285714285  |",
        "| e  | 3  | 192    | 48.0                |",
        "| e  | 2  | 189    | 37.8                |",
        "| e  | 1  | 227    | 75.66666666666667   |",
        "| d  |    | 458    | 25.444444444444443  |",
        "| d  | 5  | -99    | -49.5               |",
        "| d  | 4  | 162    | 54.0                |",
        "| d  | 3  | 124    | 41.333333333333336  |",
        "| d  | 2  | 328    | 109.33333333333333  |",
        "| d  | 1  | -57    | -8.142857142857142  |",
        "| c  |    | -28    | -1.3333333333333333 |",
        "| c  | 5  | 24     | 12.0                |",
        "| c  | 4  | -43    | -10.75              |",
        "| c  | 3  | 190    | 47.5                |",
        "| c  | 2  | -389   | -55.57142857142857  |",
        "| c  | 1  | 190    | 47.5                |",
        "| b  |    | -111   | -5.842105263157895  |",
        "| b  | 5  | -1     | -0.2                |",
        "| b  | 4  | -223   | -44.6               |",
        "| b  | 3  | -84    | -42.0               |",
        "| b  | 2  | 102    | 25.5                |",
        "| b  | 1  | 95     | 31.666666666666668  |",
        "| a  |    | -385   | -18.333333333333332 |",
        "| a  | 5  | -96    | -32.0               |",
        "| a  | 4  | -128   | -32.0               |",
        "| a  | 3  | -27    | -4.5                |",
        "| a  | 2  | -46    | -15.333333333333334 |",
        "| a  | 1  | -88    | -17.6               |",
        "+----+----+--------+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn join_with_alias_filter() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;
    let t1_schema = t1.schema().clone();
    let t2_schema = t2.schema().clone();

    // filter: t1.a + CAST(Int64(1), UInt32) = t2.a + CAST(Int64(2), UInt32) as t1.a + 1 = t2.a + 2
    let filter = Expr::eq(
        col("t1.a") + lit(3i64).cast_to(&DataType::UInt32.into(), &t1_schema)?,
        col("t2.a") + lit(1i32).cast_to(&DataType::UInt32.into(), &t2_schema)?,
    )
    .alias("t1.b + 1 = t2.a + 2");

    let df = t1
        .join(t2, JoinType::Inner, &[], &[], Some(filter))?
        .select(vec![
            col("t1.a"),
            col("t2.a"),
            col("t1.b"),
            col("t1.c"),
            col("t2.b"),
            col("t2.c"),
        ])?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.a, t2.a, t1.b, t1.c, t2.b, t2.c [a:UInt32, a:UInt32, b:Utf8, c:Int32, b:Utf8, c:Int32]",
        "  Inner Join: t1.a + UInt32(3) = t2.a + UInt32(1) [a:UInt32, b:Utf8, c:Int32, a:UInt32, b:Utf8, c:Int32]",
        "    TableScan: t1 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
        "    TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected: Vec<&str> = vec![
        "+----+----+---+----+---+---+",
        "| a  | a  | b | c  | b | c |",
        "+----+----+---+----+---+---+",
        "| 11 | 13 | c | 30 | c | 3 |",
        "| 1  | 3  | a | 10 | a | 1 |",
        "+----+----+---+----+---+---+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn right_semi_with_alias_filter() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;

    // t1.a = t2.a and t1.c > 1 and t2.c > 1
    let filter = col("t1.a")
        .eq(col("t2.a"))
        .and(col("t1.c").gt(lit(1u32)))
        .and(col("t2.c").gt(lit(1u32)));

    let df = t1
        .join(t2, JoinType::RightSemi, &[], &[], Some(filter))?
        .select(vec![col("t2.a"), col("t2.b"), col("t2.c")])?;
    let optimized_plan = df.clone().into_optimized_plan()?;
    let expected = vec![
        "RightSemi Join: t1.a = t2.a [a:UInt32, b:Utf8, c:Int32]",
        "  Projection: t1.a [a:UInt32]",
        "    Filter: t1.c > Int32(1) [a:UInt32, c:Int32]",
        "      TableScan: t1 projection=[a, c] [a:UInt32, c:Int32]",
        "  Filter: t2.c > Int32(1) [a:UInt32, b:Utf8, c:Int32]",
        "    TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected: Vec<&str> = vec![
        "+-----+---+---+",
        "| a   | b | c |",
        "+-----+---+---+",
        "| 10  | b | 2 |",
        "| 100 | d | 4 |",
        "+-----+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn right_anti_filter_push_down() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;

    // t1.a = t2.a and t1.c > 1 and t2.c > 1
    let filter = col("t1.a")
        .eq(col("t2.a"))
        .and(col("t1.c").gt(lit(1u32)))
        .and(col("t2.c").gt(lit(1u32)));

    let df = t1
        .join(t2, JoinType::RightAnti, &[], &[], Some(filter))?
        .select(vec![col("t2.a"), col("t2.b"), col("t2.c")])?;
    let optimized_plan = df.clone().into_optimized_plan()?;
    let expected = vec![
        "RightAnti Join: t1.a = t2.a Filter: t2.c > Int32(1) [a:UInt32, b:Utf8, c:Int32]",
        "  Projection: t1.a [a:UInt32]",
        "    Filter: t1.c > Int32(1) [a:UInt32, c:Int32]",
        "      TableScan: t1 projection=[a, c] [a:UInt32, c:Int32]",
        "  TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected: Vec<&str> = vec![
        "+----+---+---+",
        "| a  | b | c |",
        "+----+---+---+",
        "| 13 | c | 3 |",
        "| 3  | a | 1 |",
        "+----+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn unnest_columns() -> Result<()> {
    const NUM_ROWS: usize = 4;
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.collect().await?;
    let expected = ["+----------+------------------------------------------------+--------------------+",
        "| shape_id | points                                         | tags               |",
        "+----------+------------------------------------------------+--------------------+",
        "| 1        | [{x: -3, y: -4}, {x: -3, y: 6}, {x: 2, y: -2}] | [tag1]             |",
        "| 2        |                                                | [tag1, tag2]       |",
        "| 3        | [{x: -9, y: 2}, {x: -10, y: -4}]               |                    |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | [tag1, tag2, tag3] |",
        "+----------+------------------------------------------------+--------------------+"];
    assert_batches_sorted_eq!(expected, &results);

    // Unnest tags
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.unnest_columns(&["tags"])?.collect().await?;
    let expected = [
        "+----------+------------------------------------------------+------+",
        "| shape_id | points                                         | tags |",
        "+----------+------------------------------------------------+------+",
        "| 1        | [{x: -3, y: -4}, {x: -3, y: 6}, {x: 2, y: -2}] | tag1 |",
        "| 2        |                                                | tag1 |",
        "| 2        |                                                | tag2 |",
        "| 3        | [{x: -9, y: 2}, {x: -10, y: -4}]               |      |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | tag1 |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | tag2 |",
        "| 4        | [{x: -3, y: 5}, {x: 2, y: -1}]                 | tag3 |",
        "+----------+------------------------------------------------+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Test aggregate results for tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df.unnest_columns(&["tags"])?.count().await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    // Unnest points
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.unnest_columns(&["points"])?.collect().await?;
    let expected = [
        "+----------+-----------------+--------------------+",
        "| shape_id | points          | tags               |",
        "+----------+-----------------+--------------------+",
        "| 1        | {x: -3, y: -4}  | [tag1]             |",
        "| 1        | {x: -3, y: 6}   | [tag1]             |",
        "| 1        | {x: 2, y: -2}   | [tag1]             |",
        "| 2        |                 | [tag1, tag2]       |",
        "| 3        | {x: -10, y: -4} |                    |",
        "| 3        | {x: -9, y: 2}   |                    |",
        "| 4        | {x: -3, y: 5}   | [tag1, tag2, tag3] |",
        "| 4        | {x: 2, y: -1}   | [tag1, tag2, tag3] |",
        "+----------+-----------------+--------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Test aggregate results for points.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df.unnest_columns(&["points"])?.count().await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    // Unnest both points and tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_columns(&["points"])?
        .unnest_columns(&["tags"])?
        .collect()
        .await?;
    let expected = vec![
        "+----------+-----------------+------+",
        "| shape_id | points          | tags |",
        "+----------+-----------------+------+",
        "| 1        | {x: -3, y: -4}  | tag1 |",
        "| 1        | {x: -3, y: 6}   | tag1 |",
        "| 1        | {x: 2, y: -2}   | tag1 |",
        "| 2        |                 | tag1 |",
        "| 2        |                 | tag2 |",
        "| 3        | {x: -10, y: -4} |      |",
        "| 3        | {x: -9, y: 2}   |      |",
        "| 4        | {x: -3, y: 5}   | tag1 |",
        "| 4        | {x: -3, y: 5}   | tag2 |",
        "| 4        | {x: -3, y: 5}   | tag3 |",
        "| 4        | {x: 2, y: -1}   | tag1 |",
        "| 4        | {x: 2, y: -1}   | tag2 |",
        "| 4        | {x: 2, y: -1}   | tag3 |",
        "+----------+-----------------+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Test aggregate results for points and tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df
        .unnest_columns(&["points"])?
        .unnest_columns(&["tags"])?
        .count()
        .await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    Ok(())
}

#[tokio::test]
async fn unnest_column_nulls() -> Result<()> {
    let df = table_with_lists_and_nulls().await?;
    let results = df.clone().collect().await?;
    let expected = [
        "+--------+----+",
        "| list   | id |",
        "+--------+----+",
        "| [1, 2] | A  |",
        "|        | B  |",
        "| []     | C  |",
        "| [3]    | D  |",
        "+--------+----+",
    ];
    assert_batches_eq!(expected, &results);

    // Unnest, preserving nulls (row with B is preserved)
    let options = UnnestOptions::new().with_preserve_nulls(true);

    let results = df
        .clone()
        .unnest_columns_with_options(&["list"], options)?
        .collect()
        .await?;
    let expected = [
        "+------+----+",
        "| list | id |",
        "+------+----+",
        "| 1    | A  |",
        "| 2    | A  |",
        "|      | B  |",
        "| 3    | D  |",
        "+------+----+",
    ];
    assert_batches_eq!(expected, &results);

    let options = UnnestOptions::new().with_preserve_nulls(false);
    let results = df
        .unnest_columns_with_options(&["list"], options)?
        .collect()
        .await?;
    let expected = [
        "+------+----+",
        "| list | id |",
        "+------+----+",
        "| 1    | A  |",
        "| 2    | A  |",
        "| 3    | D  |",
        "+------+----+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_fixed_list() -> Result<()> {
    let batch = get_fixed_list_batch()?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    let expected = [
        "+----------+----------------+",
        "| shape_id | tags           |",
        "+----------+----------------+",
        "| 1        |                |",
        "| 2        | [tag21, tag22] |",
        "| 3        | [tag31, tag32] |",
        "| 4        |                |",
        "| 5        | [tag51, tag52] |",
        "| 6        | [tag61, tag62] |",
        "+----------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    let options = UnnestOptions::new().with_preserve_nulls(true);

    let results = df
        .unnest_columns_with_options(&["tags"], options)?
        .collect()
        .await?;
    let expected = vec![
        "+----------+-------+",
        "| shape_id | tags  |",
        "+----------+-------+",
        "| 1        |       |",
        "| 2        | tag21 |",
        "| 2        | tag22 |",
        "| 3        | tag31 |",
        "| 3        | tag32 |",
        "| 4        |       |",
        "| 5        | tag51 |",
        "| 5        | tag52 |",
        "| 6        | tag61 |",
        "| 6        | tag62 |",
        "+----------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_fixed_list_drop_nulls() -> Result<()> {
    let batch = get_fixed_list_batch()?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    let expected = [
        "+----------+----------------+",
        "| shape_id | tags           |",
        "+----------+----------------+",
        "| 1        |                |",
        "| 2        | [tag21, tag22] |",
        "| 3        | [tag31, tag32] |",
        "| 4        |                |",
        "| 5        | [tag51, tag52] |",
        "| 6        | [tag61, tag62] |",
        "+----------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    let options = UnnestOptions::new().with_preserve_nulls(false);

    let results = df
        .unnest_columns_with_options(&["tags"], options)?
        .collect()
        .await?;
    let expected = [
        "+----------+-------+",
        "| shape_id | tags  |",
        "+----------+-------+",
        "| 2        | tag21 |",
        "| 2        | tag22 |",
        "| 3        | tag31 |",
        "| 3        | tag32 |",
        "| 5        | tag51 |",
        "| 5        | tag52 |",
        "| 6        | tag61 |",
        "| 6        | tag62 |",
        "+----------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_fixed_list_nonull() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tags_builder = FixedSizeListBuilder::new(StringBuilder::new(), 2);

    for idx in 0..6 {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        tags_builder
            .values()
            .append_value(format!("tag{}1", idx + 1));
        tags_builder
            .values()
            .append_value(format!("tag{}2", idx + 1));
        tags_builder.append(true);
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    let expected = [
        "+----------+----------------+",
        "| shape_id | tags           |",
        "+----------+----------------+",
        "| 1        | [tag11, tag12] |",
        "| 2        | [tag21, tag22] |",
        "| 3        | [tag31, tag32] |",
        "| 4        | [tag41, tag42] |",
        "| 5        | [tag51, tag52] |",
        "| 6        | [tag61, tag62] |",
        "+----------+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    let options = UnnestOptions::new().with_preserve_nulls(true);
    let results = df
        .unnest_columns_with_options(&["tags"], options)?
        .collect()
        .await?;
    let expected = vec![
        "+----------+-------+",
        "| shape_id | tags  |",
        "+----------+-------+",
        "| 1        | tag11 |",
        "| 1        | tag12 |",
        "| 2        | tag21 |",
        "| 2        | tag22 |",
        "| 3        | tag31 |",
        "| 3        | tag32 |",
        "| 4        | tag41 |",
        "| 4        | tag42 |",
        "| 5        | tag51 |",
        "| 5        | tag52 |",
        "| 6        | tag61 |",
        "| 6        | tag62 |",
        "+----------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_aggregate_columns() -> Result<()> {
    const NUM_ROWS: usize = 5;

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.select_columns(&["tags"])?.collect().await?;
    let expected = [
        r#"+--------------------+"#,
        r#"| tags               |"#,
        r#"+--------------------+"#,
        r#"| [tag1]             |"#,
        r#"| [tag1, tag2]       |"#,
        r#"|                    |"#,
        r#"| [tag1, tag2, tag3] |"#,
        r#"| [tag1, tag2, tag3] |"#,
        r#"+--------------------+"#,
    ];
    assert_batches_sorted_eq!(expected, &results);

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_columns(&["tags"])?
        .aggregate(vec![], vec![count(col("tags"))])?
        .collect()
        .await?;
    let expected = [
        r#"+-------------+"#,
        r#"| count(tags) |"#,
        r#"+-------------+"#,
        r#"| 9           |"#,
        r#"+-------------+"#,
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_array_agg() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tag_id_builder = UInt32Builder::new();

    for shape_id in 1..=3 {
        for tag_id in 1..=3 {
            shape_id_builder.append_value(shape_id as u32);
            tag_id_builder.append_value((shape_id * 10 + tag_id) as u32);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tag_id", Arc::new(tag_id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    let expected = vec![
        "+----------+--------+",
        "| shape_id | tag_id |",
        "+----------+--------+",
        "| 1        | 11     |",
        "| 1        | 12     |",
        "| 1        | 13     |",
        "| 2        | 21     |",
        "| 2        | 22     |",
        "| 2        | 23     |",
        "| 3        | 31     |",
        "| 3        | 32     |",
        "| 3        | 33     |",
        "+----------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Doing an `array_agg` by `shape_id` produces:
    let results = df
        .clone()
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("tag_id")).alias("tag_id")],
        )?
        .collect()
        .await?;
    let expected = [
        "+----------+--------------+",
        "| shape_id | tag_id       |",
        "+----------+--------------+",
        "| 1        | [11, 12, 13] |",
        "| 2        | [21, 22, 23] |",
        "| 3        | [31, 32, 33] |",
        "+----------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Unnesting again should produce the original batch.
    let results = ctx
        .table("shapes")
        .await?
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("tag_id")).alias("tag_id")],
        )?
        .unnest_columns(&["tag_id"])?
        .collect()
        .await?;
    let expected = vec![
        "+----------+--------+",
        "| shape_id | tag_id |",
        "+----------+--------+",
        "| 1        | 11     |",
        "| 1        | 12     |",
        "| 1        | 13     |",
        "| 2        | 21     |",
        "| 2        | 22     |",
        "| 2        | 23     |",
        "| 3        | 31     |",
        "| 3        | 32     |",
        "| 3        | 33     |",
        "+----------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_with_redundant_columns() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tag_id_builder = UInt32Builder::new();

    for shape_id in 1..=3 {
        for tag_id in 1..=3 {
            shape_id_builder.append_value(shape_id as u32);
            tag_id_builder.append_value((shape_id * 10 + tag_id) as u32);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tag_id", Arc::new(tag_id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    let expected = vec![
        "+----------+--------+",
        "| shape_id | tag_id |",
        "+----------+--------+",
        "| 1        | 11     |",
        "| 1        | 12     |",
        "| 1        | 13     |",
        "| 2        | 21     |",
        "| 2        | 22     |",
        "| 2        | 23     |",
        "| 3        | 31     |",
        "| 3        | 32     |",
        "| 3        | 33     |",
        "+----------+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    // Doing an `array_agg` by `shape_id` produces:
    let df = df
        .clone()
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("shape_id")).alias("shape_id2")],
        )?
        .unnest_columns(&["shape_id2"])?
        .select(vec![col("shape_id")])?;

    let optimized_plan = df.clone().into_optimized_plan()?;
    let expected = vec![
        "Projection: shapes.shape_id [shape_id:UInt32]",
        "  Unnest: lists[shape_id2] structs[] [shape_id:UInt32, shape_id2:UInt32;N]",
        "    Aggregate: groupBy=[[shapes.shape_id]], aggr=[[ARRAY_AGG(shapes.shape_id) AS shape_id2]] [shape_id:UInt32, shape_id2:List(Field { name: \"item\", data_type: UInt32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} })]",
        "      TableScan: shapes projection=[shape_id] [shape_id:UInt32]",
    ];

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let results = df.collect().await?;
    let expected = [
        "+----------+",
        "| shape_id |",
        "+----------+",
        "| 1        |",
        "| 1        |",
        "| 1        |",
        "| 2        |",
        "| 2        |",
        "| 2        |",
        "| 3        |",
        "| 3        |",
        "| 3        |",
        "+----------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_analyze_metrics() -> Result<()> {
    const NUM_ROWS: usize = 5;

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_columns(&["tags"])?
        .explain(false, true)?
        .collect()
        .await?;
    let formatted = arrow::util::pretty::pretty_format_batches(&results)
        .unwrap()
        .to_string();
    assert_contains!(&formatted, "elapsed_compute=");
    assert_contains!(&formatted, "input_batches=1");
    assert_contains!(&formatted, "input_rows=5");
    assert_contains!(&formatted, "output_rows=10");
    assert_contains!(&formatted, "output_batches=1");

    Ok(())
}

#[tokio::test]
async fn unnest_multiple_columns() -> Result<()> {
    let df = table_with_mixed_lists().await?;
    // Default behavior is to preserve nulls.
    let results = df
        .clone()
        .unnest_columns(&["list", "large_list", "fixed_list"])?
        .collect()
        .await?;
    // list:        [1,2,3], null, [null], null,
    // large_list:  [null, 1.1], [2.2, 3.3, 4.4], null, [],
    // fixed_list:  null, [1,2], [3,4], null
    // string:      a, b, c, d
    let expected = [
        "+------+------------+------------+--------+",
        "| list | large_list | fixed_list | string |",
        "+------+------------+------------+--------+",
        "| 1    |            |            | a      |",
        "| 2    | 1.1        |            | a      |",
        "| 3    |            |            | a      |",
        "|      | 2.2        | 1          | b      |",
        "|      | 3.3        | 2          | b      |",
        "|      | 4.4        |            | b      |",
        "|      |            | 3          | c      |",
        "|      |            | 4          | c      |",
        "|      |            |            | d      |",
        "+------+------------+------------+--------+",
    ];
    assert_batches_eq!(expected, &results);

    // Test with `preserve_nulls = false``
    let results = df
        .unnest_columns_with_options(
            &["list", "large_list", "fixed_list"],
            UnnestOptions::new().with_preserve_nulls(false),
        )?
        .collect()
        .await?;
    // list:        [1,2,3], null, [null], null,
    // large_list:  [null, 1.1], [2.2, 3.3, 4.4], null, [],
    // fixed_list:  null, [1,2], [3,4], null
    // string:      a, b, c, d
    let expected = [
        "+------+------------+------------+--------+",
        "| list | large_list | fixed_list | string |",
        "+------+------------+------------+--------+",
        "| 1    |            |            | a      |",
        "| 2    | 1.1        |            | a      |",
        "| 3    |            |            | a      |",
        "|      | 2.2        | 1          | b      |",
        "|      | 3.3        | 2          | b      |",
        "|      | 4.4        |            | b      |",
        "|      |            | 3          | c      |",
        "|      |            | 4          | c      |",
        "+------+------------+------------+--------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

/// Test unnesting a non-nullable list.
#[tokio::test]
async fn unnest_non_nullable_list() -> Result<()> {
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![None]),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c1",
        DataType::new_list(DataType::Int32, true),
        false,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)])?;
    let ctx = SessionContext::new();
    let results = ctx
        .read_batches(vec![batch])?
        .unnest_columns(&["c1"])?
        .collect()
        .await?;

    // Unnesting may produce NULLs even if the list is non-nullable.
    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| c1 |",
        "+----+",
        "| 1  |",
        "| 2  |",
        "|    |",
        "+----+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_read_batches() -> Result<()> {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime);
    let ctx = SessionContext::new_with_state(state);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("number", DataType::Float32, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Float32Array::from(vec![1.12, 3.40, 2.33, 9.10, 6.66])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4, 5])),
                Arc::new(Float32Array::from(vec![1.11, 2.22, 3.33])),
            ],
        )
        .unwrap(),
    ];
    let df = ctx.read_batches(batches).unwrap();
    df.clone().show().await.unwrap();
    let result = df.collect().await?;
    let expected = [
        "+----+--------+",
        "| id | number |",
        "+----+--------+",
        "| 1  | 1.12   |",
        "| 2  | 3.4    |",
        "| 3  | 2.33   |",
        "| 4  | 9.1    |",
        "| 5  | 6.66   |",
        "| 3  | 1.11   |",
        "| 4  | 2.22   |",
        "| 5  | 3.33   |",
        "+----+--------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
    Ok(())
}
#[tokio::test]
async fn test_read_batches_empty() -> Result<()> {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime);
    let ctx = SessionContext::new_with_state(state);

    let batches = vec![];
    let df = ctx.read_batches(batches).unwrap();
    df.clone().show().await.unwrap();
    let result = df.collect().await?;
    let expected = ["++", "++"];
    assert_batches_sorted_eq!(expected, &result);
    Ok(())
}

#[tokio::test]
async fn consecutive_projection_same_schema() -> Result<()> {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime);
    let ctx = SessionContext::new_with_state(state);

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0, 1]))])
            .unwrap();

    let df = ctx.read_batch(batch).unwrap();
    df.clone().show().await.unwrap();

    // Add `t` column full of nulls
    let df = df
        .with_column("t", cast(Expr::Literal(ScalarValue::Null), DataType::Int32))
        .unwrap();
    df.clone().show().await.unwrap();

    let df = df
        // (case when id = 1 then 10 else t) as t
        .with_column(
            "t",
            when(col("id").eq(lit(1)), lit(10))
                .otherwise(col("t"))
                .unwrap(),
        )
        .unwrap()
        // (case when id = 1 then 10 else t) as t2
        .with_column(
            "t2",
            when(col("id").eq(lit(1)), lit(10))
                .otherwise(col("t"))
                .unwrap(),
        )
        .unwrap();

    let results = df.collect().await?;
    let expected = [
        "+----+----+----+",
        "| id | t  | t2 |",
        "+----+----+----+",
        "| 0  |    |    |",
        "| 1  | 10 | 10 |",
        "+----+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

async fn create_test_table(name: &str) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "abcDEF",
                "abc123",
                "CBAdef",
                "123AbcDef",
            ])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch(name, batch)?;

    ctx.table(name).await
}

async fn aggregates_table(ctx: &SessionContext) -> Result<DataFrame> {
    let testdata = datafusion::test_util::arrow_test_data();

    ctx.read_csv(
        format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await
}

fn create_join_context() -> Result<SessionContext> {
    let t1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let t2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        t1,
        vec![
            Arc::new(UInt32Array::from(vec![1, 10, 11, 100])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        t2,
        vec![
            Arc::new(UInt32Array::from(vec![3, 10, 13, 100])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;

    Ok(ctx)
}

/// Create a data frame that contains nested types.
///
/// Create a data frame with nested types, each row contains:
/// - shape_id an integer primary key
/// - points A list of points structs {x, y}
/// - A list of tags.
async fn table_with_nested_types(n: usize) -> Result<DataFrame> {
    use rand::prelude::*;

    let mut shape_id_builder = UInt32Builder::new();
    let mut points_builder = ListBuilder::new(StructBuilder::from_fields(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ],
        5,
    ));
    let mut tags_builder = ListBuilder::new(StringBuilder::new());

    let mut rng = StdRng::seed_from_u64(197);

    for idx in 0..n {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        // Add a random number of points
        let num_points: usize = rng.gen_range(0..4);
        if num_points > 0 {
            for _ in 0..num_points.max(2) {
                // Add x value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(0)
                    .unwrap()
                    .append_value(rng.gen_range(-10..10));
                // Add y value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(1)
                    .unwrap()
                    .append_value(rng.gen_range(-10..10));
                points_builder.values().append(true);
            }
        }

        // Append null if num points is 0.
        points_builder.append(num_points > 0);

        // Append tags.
        let num_tags: usize = rng.gen_range(0..5);
        for id in 0..num_tags {
            tags_builder.values().append_value(format!("tag{}", id + 1));
        }
        tags_builder.append(num_tags > 0);
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("points", Arc::new(points_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    ctx.table("shapes").await
}

fn get_fixed_list_batch() -> Result<RecordBatch, ArrowError> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tags_builder = FixedSizeListBuilder::new(StringBuilder::new(), 2);

    for idx in 0..6 {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        if idx % 3 != 0 {
            tags_builder
                .values()
                .append_value(format!("tag{}1", idx + 1));
            tags_builder
                .values()
                .append_value(format!("tag{}2", idx + 1));
            tags_builder.append(true);
        } else {
            tags_builder.values().append_null();
            tags_builder.values().append_null();
            tags_builder.append(false);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    Ok(batch)
}

/// Create a table with different types of list columns and a string column.
async fn table_with_mixed_lists() -> Result<DataFrame> {
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
        Some(vec![None]),
        None,
    ]);

    let large_list_array =
        LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![None, Some(1.1)]),
            Some(vec![Some(2.2), Some(3.3), Some(4.4)]),
            None,
            Some(vec![]),
        ]);

    let fixed_list_array = FixedSizeListArray::from_iter_primitive::<UInt64Type, _, _>(
        vec![
            None,
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            None,
        ],
        2,
    );

    let string_array = StringArray::from(vec!["a", "b", "c", "d"]);

    let batch = RecordBatch::try_from_iter(vec![
        ("list", Arc::new(list_array) as ArrayRef),
        ("large_list", Arc::new(large_list_array) as ArrayRef),
        ("fixed_list", Arc::new(fixed_list_array) as ArrayRef),
        ("string", Arc::new(string_array) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("mixed_lists", batch)?;
    ctx.table("mixed_lists").await
}

/// A a data frame that a list of integers and string IDs
async fn table_with_lists_and_nulls() -> Result<DataFrame> {
    let mut list_builder = ListBuilder::new(UInt32Builder::new());
    let mut id_builder = StringBuilder::new();

    // [1, 2],  A
    list_builder.values().append_value(1);
    list_builder.values().append_value(2);
    list_builder.append(true);
    id_builder.append_value("A");

    // NULL, B
    list_builder.append(false);
    id_builder.append_value("B");

    // [],  C
    list_builder.append(true);
    id_builder.append_value("C");

    // [3], D
    list_builder.values().append_value(3);
    list_builder.append(true);
    id_builder.append_value("D");

    let batch = RecordBatch::try_from_iter(vec![
        ("list", Arc::new(list_builder.finish()) as ArrayRef),
        ("id", Arc::new(id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    ctx.table("shapes").await
}

pub async fn register_alltypes_tiny_pages_parquet(ctx: &SessionContext) -> Result<()> {
    let testdata = parquet_test_data();
    ctx.register_parquet(
        "alltypes_tiny_pages",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(())
}
#[derive(Debug)]
struct HardcodedIntProvider {}

impl VarProvider for HardcodedIntProvider {
    fn get_value(&self, _var_names: Vec<String>) -> Result<ScalarValue, DataFusionError> {
        Ok(ScalarValue::Int64(Some(1234)))
    }

    fn get_type(&self, _: &[String]) -> Option<TypeRelation> {
        Some(DataType::Int64.into())
    }
}

#[tokio::test]
async fn use_var_provider() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("foo", DataType::Int64, false),
        Field::new("bar", DataType::Int64, false),
    ]));

    let mem_table = Arc::new(MemTable::try_new(schema, vec![])?);

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.optimizer.skip_failed_rules", false);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("csv_table", mem_table)?;
    ctx.register_variable(VarType::UserDefined, Arc::new(HardcodedIntProvider {}));

    let dataframe = ctx
        .sql("SELECT foo FROM csv_table WHERE bar > @var")
        .await?;
    dataframe.collect().await?;
    Ok(())
}

#[tokio::test]
async fn test_array_agg() -> Result<()> {
    let df = create_test_table("test")
        .await?
        .aggregate(vec![], vec![array_agg(col("a"))])?;

    let results = df.collect().await?;

    let expected = [
        "+-------------------------------------+",
        "| ARRAY_AGG(test.a)                   |",
        "+-------------------------------------+",
        "| [abcDEF, abc123, CBAdef, 123AbcDef] |",
        "+-------------------------------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_dataframe_placeholder_missing_param_values() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx
        .read_empty()
        .unwrap()
        .with_column("a", lit(1))
        .unwrap()
        .filter(col("a").eq(placeholder("$0")))
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    let expected = vec![
        "Filter: a = $0 [a:Int32]",
        "  Projection: Int32(1) AS a [a:Int32]",
        "    EmptyRelation []",
    ];
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // The placeholder is not replaced with a value,
    // so the filter data type is not know, i.e. a = $0.
    // Therefore, the optimization fails.
    let optimized_plan = ctx.state().optimize(logical_plan);
    assert!(optimized_plan.is_err());
    assert!(optimized_plan
        .unwrap_err()
        .to_string()
        .contains("Placeholder type could not be resolved. Make sure that the placeholder is bound to a concrete type, e.g. by providing parameter values."));

    // Prodiving a parameter value should resolve the error
    let df = ctx
        .read_empty()
        .unwrap()
        .with_column("a", lit(1))
        .unwrap()
        .filter(col("a").eq(placeholder("$0")))
        .unwrap()
        .with_param_values(vec![("0", ScalarValue::from(3i32))]) // <-- provide parameter value
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    let expected = vec![
        "Filter: a = Int32(3) [a:Int32]",
        "  Projection: Int32(1) AS a [a:Int32]",
        "    EmptyRelation []",
    ];
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let optimized_plan = ctx.state().optimize(logical_plan);
    assert!(optimized_plan.is_ok());

    let actual = optimized_plan.unwrap().display_indent_schema().to_string();
    let expected = "EmptyRelation [a:Int32]";
    assert_eq!(expected, actual);

    Ok(())
}

#[tokio::test]
async fn write_partitioned_parquet_results() -> Result<()> {
    // create partitioned input file and context
    let tmp_dir = TempDir::new()?;

    let ctx = SessionContext::new();

    // Create an in memory table with schema C1 and C2, both strings
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Utf8, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["abc", "def"])),
            Arc::new(StringArray::from(vec!["123", "456"])),
        ],
    )?;

    let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![record_batch]])?);

    // Register the table in the context
    ctx.register_table("test", mem_table)?;

    let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
    let local_url = Url::parse("file://local").unwrap();
    ctx.register_object_store(&local_url, local);

    // execute a simple query and write the results to parquet
    let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
    let out_dir_url = format!("file://{out_dir}");

    // Write the results to parquet with partitioning
    let df = ctx.sql("SELECT c1, c2 FROM test").await?;
    let df_write_options =
        DataFrameWriteOptions::new().with_partition_by(vec![String::from("c2")]);

    df.write_parquet(&out_dir_url, df_write_options, None)
        .await?;

    // Explicitly read the parquet file at c2=123 to verify the physical files are partitioned
    let partitioned_file = format!("{out_dir}/c2=123", out_dir = out_dir);
    let filted_df = ctx
        .read_parquet(&partitioned_file, ParquetReadOptions::default())
        .await?;

    // Check that the c2 column is gone and that c1 is abc.
    let results = filted_df.collect().await?;
    let expected = ["+-----+", "| c1  |", "+-----+", "| abc |", "+-----+"];

    assert_batches_eq!(expected, &results);

    // Read the entire set of parquet files
    let df = ctx
        .read_parquet(
            &out_dir_url,
            ParquetReadOptions::default()
                .table_partition_cols(vec![(String::from("c2"), DataType::Utf8)]),
        )
        .await?;

    // Check that the df has the entire set of data
    let results = df.collect().await?;
    let expected = [
        "+-----+-----+",
        "| c1  | c2  |",
        "+-----+-----+",
        "| abc | 123 |",
        "| def | 456 |",
        "+-----+-----+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn write_parquet_results() -> Result<()> {
    // create partitioned input file and context
    let tmp_dir = TempDir::new()?;
    // let mut ctx = create_ctx(&tmp_dir, 4).await?;
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let schema = populate_csv_partitions(&tmp_dir, 4, ".csv")?;
    // register csv file with the execution context
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    // register a local file system object store for /tmp directory
    let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
    let local_url = Url::parse("file://local").unwrap();
    ctx.register_object_store(&local_url, local);

    // execute a simple query and write the results to parquet
    let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
    let out_dir_url = "file://local/out/";
    let df = ctx.sql("SELECT c1, c2 FROM test").await?;
    df.write_parquet(out_dir_url, DataFrameWriteOptions::new(), None)
        .await?;
    // write_parquet(&mut ctx, "SELECT c1, c2 FROM test", &out_dir, None).await?;

    // create a new context and verify that the results were saved to a partitioned parquet file
    let ctx = SessionContext::new();

    // get write_id
    let mut paths = fs::read_dir(&out_dir).unwrap();
    let path = paths.next();
    let name = path
        .unwrap()?
        .path()
        .file_name()
        .expect("Should be a file name")
        .to_str()
        .expect("Should be a str")
        .to_owned();
    let (parsed_id, _) = name.split_once('_').expect("File should contain _ !");
    let write_id = parsed_id.to_owned();

    // register each partition as well as the top level dir
    ctx.register_parquet(
        "part0",
        &format!("{out_dir}/{write_id}_0.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet("allparts", &out_dir, ParquetReadOptions::default())
        .await?;

    let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
    let allparts = ctx
        .sql("SELECT c1, c2 FROM allparts")
        .await?
        .collect()
        .await?;

    let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

    assert_eq!(part0[0].schema(), allparts[0].schema());

    assert_eq!(allparts_count, 40);

    Ok(())
}
