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

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
};
use arrow_array::types::Int32Type;
use arrow_array::ListArray;
use arrow_schema::SchemaRef;
use std::sync::Arc;

use datafusion::error::Result;

use datafusion::prelude::*;

use datafusion::assert_batches_eq;
use datafusion_common::{assert_snapshot, DFSchema, ScalarValue};
use datafusion_expr::expr::Alias;
use datafusion_expr::ExprSchemable;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
        Field::new("l", DataType::new_list(DataType::Int32, true), true),
    ]))
}

async fn create_test_table() -> Result<DataFrame> {
    let schema = test_schema();

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
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("test", batch)?;

    ctx.table("test").await
}

/// Executes an expression on the test dataframe as a select.
/// Compares formatted output of a record batch with an expected
/// vector of strings, using the assert_batch_eq! macro
macro_rules! assert_fn_batches {
    ($EXPR:expr) => {
        assert_fn_batches!($EXPR, 10)
    };
    ($EXPR:expr, $LIMIT: expr) => {
        let df = create_test_table().await?;
        let df = df.select(vec![$EXPR])?.limit(0, Some($LIMIT))?;
        let batches = df.collect().await?;

        // Once result is updated, run command below for snapshot update
        // `cargo insta test --accept -p datafusion --test core_integration -- dataframe::dataframe_functions`
        datafusion_common::assert_snapshot!(&batches);
    };
}

#[tokio::test]
async fn test_fn_ascii() -> Result<()> {
    let expr = ascii(col("a"));

    assert_fn_batches!(expr, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_bit_length() -> Result<()> {
    let expr = bit_length(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim() -> Result<()> {
    let expr = btrim(vec![lit("      a b c             ")]);

    assert_fn_batches!(expr, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim_with_chars() -> Result<()> {
    let expr = btrim(vec![col("a"), lit("ab")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_nullif() -> Result<()> {
    let expr = nullif(col("a"), lit("abcDEF"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_arrow_cast() -> Result<()> {
    let expr = arrow_typeof(arrow_cast(col("b"), lit("Float64")));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_nvl() -> Result<()> {
    let lit_null = lit(ScalarValue::Utf8(None));
    // nvl(CASE WHEN a = 'abcDEF' THEN NULL ELSE a END, 'TURNED_NULL')
    let expr = nvl(
        when(col("a").eq(lit("abcDEF")), lit_null)
            .otherwise(col("a"))
            .unwrap(),
        lit("TURNED_NULL"),
    )
    .alias("nvl_expr");

    assert_fn_batches!(expr);

    Ok(())
}
#[tokio::test]
async fn test_nvl2() -> Result<()> {
    let lit_null = lit(ScalarValue::Utf8(None));
    // nvl2(CASE WHEN a = 'abcDEF' THEN NULL ELSE a END, 'NON_NUll', 'TURNED_NULL')
    let expr = nvl2(
        when(col("a").eq(lit("abcDEF")), lit_null)
            .otherwise(col("a"))
            .unwrap(),
        lit("NON_NULL"),
        lit("TURNED_NULL"),
    )
    .alias("nvl2_expr");

    assert_fn_batches!(expr);

    Ok(())
}
#[tokio::test]
async fn test_fn_arrow_typeof() -> Result<()> {
    let expr = arrow_typeof(col("l"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_struct() -> Result<()> {
    let expr = r#struct(vec![col("a"), col("b")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_named_struct() -> Result<()> {
    let expr = named_struct(vec![lit("column_a"), col("a"), lit("column_b"), col("b")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_coalesce() -> Result<()> {
    let expr = coalesce(vec![lit(ScalarValue::Utf8(None)), lit("ab")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_approx_median() -> Result<()> {
    let expr = approx_median(col("b"));

    let expected = [
        "+-----------------------+",
        "| APPROX_MEDIAN(test.b) |",
        "+-----------------------+",
        "| 10                    |",
        "+-----------------------+",
    ];

    let df = create_test_table().await?;
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_batches_eq!(expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_fn_approx_percentile_cont() -> Result<()> {
    let expr = approx_percentile_cont(col("b"), lit(0.5));

    let df = create_test_table().await?;
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_snapshot!(&batches);

    // the arg2 parameter is a complex expr, but it can be evaluated to the literal value
    let alias_expr = Expr::Alias(Alias::new(
        cast(lit(0.5), DataType::Float32),
        None::<&str>,
        "arg_2".to_string(),
    ));
    let expr = approx_percentile_cont(col("b"), alias_expr);
    let df = create_test_table().await?;
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_snapshot!(&batches);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_character_length() -> Result<()> {
    let expr = character_length(col("a"));
    assert_fn_batches!(expr);
    Ok(())
}

#[tokio::test]
async fn test_fn_chr() -> Result<()> {
    let expr = chr(lit(128175));

    assert_fn_batches!(expr, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_initcap() -> Result<()> {
    let expr = initcap(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_left() -> Result<()> {
    let expr = left(col("a"), lit(3));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_lower() -> Result<()> {
    let expr = lower(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_lpad() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10)]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_lpad_with_string() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10), lit("*")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim() -> Result<()> {
    let expr = ltrim(vec![lit("      a b c             ")]);

    assert_fn_batches!(expr, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim_with_columns() -> Result<()> {
    let expr = ltrim(vec![col("a")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_md5() -> Result<()> {
    let expr = md5(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_like() -> Result<()> {
    let expr = regexp_like(col("a"), lit("[a-z]"));

    let _expected = [
        "+-----------------------------------+",
        "| regexp_like(test.a,Utf8(\"[a-z]\")) |",
        "+-----------------------------------+",
        "| true                              |",
        "| true                              |",
        "| true                              |",
        "| true                              |",
        "+-----------------------------------+",
    ];

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_match() -> Result<()> {
    let expr = regexp_match(col("a"), lit("[a-z]"));

    let _expected = [
        "+------------------------------------+",
        "| regexp_match(test.a,Utf8(\"[a-z]\")) |",
        "+------------------------------------+",
        "| [a]                                |",
        "| [a]                                |",
        "| [d]                                |",
        "| [b]                                |",
        "+------------------------------------+",
    ];

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_replace() -> Result<()> {
    let expr = regexp_replace(col("a"), lit("[a-z]"), lit("x"), lit("g"));

    let _expected = [
        "+----------------------------------------------------------+",
        "| regexp_replace(test.a,Utf8(\"[a-z]\"),Utf8(\"x\"),Utf8(\"g\")) |",
        "+----------------------------------------------------------+",
        "| xxxDEF                                                   |",
        "| xxx123                                                   |",
        "| CBAxxx                                                   |",
        "| 123AxxDxx                                                |",
        "+----------------------------------------------------------+",
    ];

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_replace() -> Result<()> {
    let expr = replace(col("a"), lit("abc"), lit("x"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_repeat() -> Result<()> {
    let expr = repeat(col("a"), lit(2));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_reverse() -> Result<()> {
    let expr = reverse(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_right() -> Result<()> {
    let expr = right(col("a"), lit(3));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_rpad() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11)]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_rpad_with_characters() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11), lit("x")]);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_sha224() -> Result<()> {
    let expr = sha224(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_split_part() -> Result<()> {
    let expr = split_part(col("a"), lit("b"), lit(1));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_starts_with() -> Result<()> {
    let expr = starts_with(col("a"), lit("abc"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_ends_with() -> Result<()> {
    let expr = ends_with(col("a"), lit("DEF"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_strpos() -> Result<()> {
    let expr = strpos(col("a"), lit("f"));
    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_substr() -> Result<()> {
    let expr = substr(col("a"), lit(2));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_cast() -> Result<()> {
    let expr = cast(col("b"), DataType::Float64);

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_to_hex() -> Result<()> {
    let expr = to_hex(col("b"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_translate() -> Result<()> {
    let expr = translate(col("a"), lit("bc"), lit("xx"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_upper() -> Result<()> {
    let expr = upper(col("a"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_encode() -> Result<()> {
    let expr = encode(col("a"), lit("hex"));

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_decode() -> Result<()> {
    // Note that the decode function returns binary, and the default display of
    // binary is "hexadecimal" and therefore the output looks like decode did
    // nothing. So compare to a constant.
    let df_schema = DFSchema::try_from(test_schema().as_ref().clone())?;
    let expr = decode(encode(col("a"), lit("hex")), lit("hex"))
        // need to cast to utf8 otherwise the default display of binary array is hex
        // so it looks like nothing is done
        .cast_to(&DataType::Utf8, &df_schema)?;

    assert_fn_batches!(expr);

    Ok(())
}

#[tokio::test]
async fn test_fn_array_to_string() -> Result<()> {
    let expr = array_to_string(col("l"), lit("***"));

    assert_fn_batches!(expr);

    Ok(())
}
