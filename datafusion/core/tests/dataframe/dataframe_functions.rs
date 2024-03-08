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

use datafusion::dataframe::DataFrame;

use datafusion::error::Result;

use datafusion::prelude::*;

use datafusion::execution::context::SessionContext;

use datafusion::assert_batches_eq;
use datafusion_common::DFSchema;
use datafusion_expr::expr::Alias;
use datafusion_expr::{approx_median, cast, ExprSchemable};

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
    ($EXPR:expr, $EXPECTED: expr) => {
        assert_fn_batches!($EXPR, $EXPECTED, 10)
    };
    ($EXPR:expr, $EXPECTED: expr, $LIMIT: expr) => {
        let df = create_test_table().await?;
        let df = df.select(vec![$EXPR])?.limit(0, Some($LIMIT))?;
        let batches = df.collect().await?;

        assert_batches_eq!($EXPECTED, &batches);
    };
}

#[tokio::test]
async fn test_fn_ascii() -> Result<()> {
    let expr = ascii(col("a"));

    let expected = [
        "+---------------+",
        "| ascii(test.a) |",
        "+---------------+",
        "| 97            |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_bit_length() -> Result<()> {
    let expr = bit_length(col("a"));

    let expected = [
        "+--------------------+",
        "| bit_length(test.a) |",
        "+--------------------+",
        "| 48                 |",
        "| 48                 |",
        "| 48                 |",
        "| 72                 |",
        "+--------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim() -> Result<()> {
    let expr = btrim(vec![lit("      a b c             ")]);

    let expected = [
        "+-----------------------------------------+",
        "| btrim(Utf8(\"      a b c             \")) |",
        "+-----------------------------------------+",
        "| a b c                                   |",
        "+-----------------------------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim_with_chars() -> Result<()> {
    let expr = btrim(vec![col("a"), lit("ab")]);

    let expected = [
        "+--------------------------+",
        "| btrim(test.a,Utf8(\"ab\")) |",
        "+--------------------------+",
        "| cDEF                     |",
        "| c123                     |",
        "| CBAdef                   |",
        "| 123AbcDef                |",
        "+--------------------------+",
    ];

    assert_fn_batches!(expr, expected);

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

    let expected = [
        "+---------------------------------------------+",
        "| APPROX_PERCENTILE_CONT(test.b,Float64(0.5)) |",
        "+---------------------------------------------+",
        "| 10                                          |",
        "+---------------------------------------------+",
    ];

    let df = create_test_table().await?;
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_batches_eq!(expected, &batches);

    // the arg2 parameter is a complex expr, but it can be evaluated to the literal value
    let alias_expr = Expr::Alias(Alias::new(
        cast(lit(0.5), DataType::Float32),
        None::<&str>,
        "arg_2".to_string(),
    ));
    let expr = approx_percentile_cont(col("b"), alias_expr);
    let df = create_test_table().await?;
    let expected = [
        "+--------------------------------------+",
        "| APPROX_PERCENTILE_CONT(test.b,arg_2) |",
        "+--------------------------------------+",
        "| 10                                   |",
        "+--------------------------------------+",
    ];
    let batches = df.aggregate(vec![], vec![expr]).unwrap().collect().await?;

    assert_batches_eq!(expected, &batches);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_character_length() -> Result<()> {
    let expr = character_length(col("a"));

    let expected = [
        "+--------------------------+",
        "| character_length(test.a) |",
        "+--------------------------+",
        "| 6                        |",
        "| 6                        |",
        "| 6                        |",
        "| 9                        |",
        "+--------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_chr() -> Result<()> {
    let expr = chr(lit(128175));

    let expected = [
        "+--------------------+",
        "| chr(Int32(128175)) |",
        "+--------------------+",
        "| 💯                 |",
        "+--------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_initcap() -> Result<()> {
    let expr = initcap(col("a"));

    let expected = [
        "+-----------------+",
        "| initcap(test.a) |",
        "+-----------------+",
        "| Abcdef          |",
        "| Abc123          |",
        "| Cbadef          |",
        "| 123abcdef       |",
        "+-----------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_left() -> Result<()> {
    let expr = left(col("a"), lit(3));

    let expected = [
        "+-----------------------+",
        "| left(test.a,Int32(3)) |",
        "+-----------------------+",
        "| abc                   |",
        "| abc                   |",
        "| CBA                   |",
        "| 123                   |",
        "+-----------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_lower() -> Result<()> {
    let expr = lower(col("a"));

    let expected = [
        "+---------------+",
        "| lower(test.a) |",
        "+---------------+",
        "| abcdef        |",
        "| abc123        |",
        "| cbadef        |",
        "| 123abcdef     |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_lpad() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10)]);

    let expected = [
        "+------------------------+",
        "| lpad(test.a,Int32(10)) |",
        "+------------------------+",
        "|     abcDEF             |",
        "|     abc123             |",
        "|     CBAdef             |",
        "|  123AbcDef             |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_lpad_with_string() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10), lit("*")]);

    let expected = [
        "+----------------------------------+",
        "| lpad(test.a,Int32(10),Utf8(\"*\")) |",
        "+----------------------------------+",
        "| ****abcDEF                       |",
        "| ****abc123                       |",
        "| ****CBAdef                       |",
        "| *123AbcDef                       |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim() -> Result<()> {
    let expr = ltrim(lit("      a b c             "));

    let expected = [
        "+-----------------------------------------+",
        "| ltrim(Utf8(\"      a b c             \")) |",
        "+-----------------------------------------+",
        "| a b c                                   |",
        "+-----------------------------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim_with_columns() -> Result<()> {
    let expr = ltrim(col("a"));

    let expected = [
        "+---------------+",
        "| ltrim(test.a) |",
        "+---------------+",
        "| abcDEF        |",
        "| abc123        |",
        "| CBAdef        |",
        "| 123AbcDef     |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_md5() -> Result<()> {
    let expr = md5(col("a"));

    let expected = [
        "+----------------------------------+",
        "| md5(test.a)                      |",
        "+----------------------------------+",
        "| ea2de8bd80f3a1f52c754214fc9b0ed1 |",
        "| e99a18c428cb38d5f260853678922e03 |",
        "| 11ed4a6e9985df40913eead67f022e27 |",
        "| 8f5e60e523c9253e623ae38bb58c399a |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_like() -> Result<()> {
    let expr = regexp_like(col("a"), lit("[a-z]"));

    let expected = [
        "+-----------------------------------+",
        "| regexp_like(test.a,Utf8(\"[a-z]\")) |",
        "+-----------------------------------+",
        "| true                              |",
        "| true                              |",
        "| true                              |",
        "| true                              |",
        "+-----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_match() -> Result<()> {
    let expr = regexp_match(col("a"), lit("[a-z]"));

    let expected = [
        "+------------------------------------+",
        "| regexp_match(test.a,Utf8(\"[a-z]\")) |",
        "+------------------------------------+",
        "| [a]                                |",
        "| [a]                                |",
        "| [d]                                |",
        "| [b]                                |",
        "+------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_regexp_replace() -> Result<()> {
    let expr = regexp_replace(col("a"), lit("[a-z]"), lit("x"), lit("g"));

    let expected = [
        "+----------------------------------------------------------+",
        "| regexp_replace(test.a,Utf8(\"[a-z]\"),Utf8(\"x\"),Utf8(\"g\")) |",
        "+----------------------------------------------------------+",
        "| xxxDEF                                                   |",
        "| xxx123                                                   |",
        "| CBAxxx                                                   |",
        "| 123AxxDxx                                                |",
        "+----------------------------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_replace() -> Result<()> {
    let expr = replace(col("a"), lit("abc"), lit("x"));

    let expected = [
        "+---------------------------------------+",
        "| replace(test.a,Utf8(\"abc\"),Utf8(\"x\")) |",
        "+---------------------------------------+",
        "| xDEF                                  |",
        "| x123                                  |",
        "| CBAdef                                |",
        "| 123AbcDef                             |",
        "+---------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_repeat() -> Result<()> {
    let expr = repeat(col("a"), lit(2));

    let expected = [
        "+-------------------------+",
        "| repeat(test.a,Int32(2)) |",
        "+-------------------------+",
        "| abcDEFabcDEF            |",
        "| abc123abc123            |",
        "| CBAdefCBAdef            |",
        "| 123AbcDef123AbcDef      |",
        "+-------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_reverse() -> Result<()> {
    let expr = reverse(col("a"));

    let expected = [
        "+-----------------+",
        "| reverse(test.a) |",
        "+-----------------+",
        "| FEDcba          |",
        "| 321cba          |",
        "| fedABC          |",
        "| feDcbA321       |",
        "+-----------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_right() -> Result<()> {
    let expr = right(col("a"), lit(3));

    let expected = [
        "+------------------------+",
        "| right(test.a,Int32(3)) |",
        "+------------------------+",
        "| DEF                    |",
        "| 123                    |",
        "| def                    |",
        "| Def                    |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_rpad() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11)]);

    let expected = [
        "+------------------------+",
        "| rpad(test.a,Int32(11)) |",
        "+------------------------+",
        "| abcDEF                 |",
        "| abc123                 |",
        "| CBAdef                 |",
        "| 123AbcDef              |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_rpad_with_characters() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11), lit("x")]);

    let expected = [
        "+----------------------------------+",
        "| rpad(test.a,Int32(11),Utf8(\"x\")) |",
        "+----------------------------------+",
        "| abcDEFxxxxx                      |",
        "| abc123xxxxx                      |",
        "| CBAdefxxxxx                      |",
        "| 123AbcDefxx                      |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_sha224() -> Result<()> {
    let expr = sha224(col("a"));

    let expected = [
        "+----------------------------------------------------------+",
        "| sha224(test.a)                                           |",
        "+----------------------------------------------------------+",
        "| 8b9ef961d2b19cfe7ee2a8452e3adeea98c7b22954b4073976bf80ee |",
        "| 5c69bb695cc29b93d655e1a4bb5656cda624080d686f74477ea09349 |",
        "| b3b3783b7470594e7ddb845eca0aec5270746dd6d0bc309bb948ceab |",
        "| fc8a30d59386d78053328440c6670c3b583404a905cbe9bbd491a517 |",
        "+----------------------------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_split_part() -> Result<()> {
    let expr = split_part(col("a"), lit("b"), lit(1));

    let expected = [
        "+---------------------------------------+",
        "| split_part(test.a,Utf8(\"b\"),Int32(1)) |",
        "+---------------------------------------+",
        "| a                                     |",
        "| a                                     |",
        "| CBAdef                                |",
        "| 123A                                  |",
        "+---------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_starts_with() -> Result<()> {
    let expr = starts_with(col("a"), lit("abc"));

    let expected = [
        "+---------------------------------+",
        "| starts_with(test.a,Utf8(\"abc\")) |",
        "+---------------------------------+",
        "| true                            |",
        "| true                            |",
        "| false                           |",
        "| false                           |",
        "+---------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_ends_with() -> Result<()> {
    let expr = ends_with(col("a"), lit("DEF"));

    let expected = [
        "+-------------------------------+",
        "| ends_with(test.a,Utf8(\"DEF\")) |",
        "+-------------------------------+",
        "| true                          |",
        "| false                         |",
        "| false                         |",
        "| false                         |",
        "+-------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_strpos() -> Result<()> {
    let expr = strpos(col("a"), lit("f"));

    let expected = [
        "+--------------------------+",
        "| strpos(test.a,Utf8(\"f\")) |",
        "+--------------------------+",
        "| 0                        |",
        "| 0                        |",
        "| 6                        |",
        "| 9                        |",
        "+--------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_substr() -> Result<()> {
    let expr = substr(col("a"), lit(2));

    let expected = [
        "+-------------------------+",
        "| substr(test.a,Int32(2)) |",
        "+-------------------------+",
        "| bcDEF                   |",
        "| bc123                   |",
        "| BAdef                   |",
        "| 23AbcDef                |",
        "+-------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_cast() -> Result<()> {
    let expr = cast(col("b"), DataType::Float64);
    let expected = [
        "+--------+",
        "| test.b |",
        "+--------+",
        "| 1.0    |",
        "| 10.0   |",
        "| 10.0   |",
        "| 100.0  |",
        "+--------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_to_hex() -> Result<()> {
    let expr = to_hex(col("b"));

    let expected = [
        "+----------------+",
        "| to_hex(test.b) |",
        "+----------------+",
        "| 1              |",
        "| a              |",
        "| a              |",
        "| 64             |",
        "+----------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "unicode_expressions")]
async fn test_fn_translate() -> Result<()> {
    let expr = translate(col("a"), lit("bc"), lit("xx"));

    let expected = [
        "+-----------------------------------------+",
        "| translate(test.a,Utf8(\"bc\"),Utf8(\"xx\")) |",
        "+-----------------------------------------+",
        "| axxDEF                                  |",
        "| axx123                                  |",
        "| CBAdef                                  |",
        "| 123AxxDef                               |",
        "+-----------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_upper() -> Result<()> {
    let expr = upper(col("a"));

    let expected = [
        "+---------------+",
        "| upper(test.a) |",
        "+---------------+",
        "| ABCDEF        |",
        "| ABC123        |",
        "| CBADEF        |",
        "| 123ABCDEF     |",
        "+---------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_encode() -> Result<()> {
    let expr = encode(col("a"), lit("hex"));

    let expected = [
        "+----------------------------+",
        "| encode(test.a,Utf8(\"hex\")) |",
        "+----------------------------+",
        "| 616263444546               |",
        "| 616263313233               |",
        "| 434241646566               |",
        "| 313233416263446566         |",
        "+----------------------------+",
    ];
    assert_fn_batches!(expr, expected);

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

    let expected = [
        "+------------------------------------------------+",
        "| decode(encode(test.a,Utf8(\"hex\")),Utf8(\"hex\")) |",
        "+------------------------------------------------+",
        "| abcDEF                                         |",
        "| abc123                                         |",
        "| CBAdef                                         |",
        "| 123AbcDef                                      |",
        "+------------------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_array_to_string() -> Result<()> {
    let expr = array_to_string(col("l"), lit("***"));

    let expected = [
        "+-------------------------------------+",
        "| array_to_string(test.l,Utf8(\"***\")) |",
        "+-------------------------------------+",
        "| 0***1***2                           |",
        "|                                     |",
        "| 3***5                               |",
        "| 6***7                               |",
        "+-------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_string_to_array() -> Result<()> {
    let expr = string_to_array(lit("abc##def##ghi"), lit("##"), lit("!"));

    let expected = [
        "+-------------------------------------------------------------+",
        "| string_to_array(Utf8(\"abc##def##ghi\"),Utf8(\"##\"),Utf8(\"!\")) |",
        "+-------------------------------------------------------------+",
        "| [abc, def, ghi]                                             |",
        "| [abc, def, ghi]                                             |",
        "| [abc, def, ghi]                                             |",
        "| [abc, def, ghi]                                             |",
        "+-------------------------------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}
