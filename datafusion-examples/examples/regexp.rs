// Licensed to the Apache Software Foundation (ASF) under one
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

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{assert_batches_eq, assert_contains};

/// This example demonstrates how to use the regexp_* functions
///
/// the full list of supported features and
/// syntax can be found at
/// https://docs.rs/regex/latest/regex/#syntax
///
/// Supported flags can be found at
/// https://docs.rs/regex/latest/regex/#grouping-and-flags
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "examples",
        "../../datafusion/physical-expr/tests/data/regex.csv",
        CsvReadOptions::new(),
    )
    .await?;

    //
    //
    //regexp_like examples
    //
    //
    // regexp_like format is (regexp_like(text, regex[, flags])
    //

    // use sql and regexp_like function to test col 'values', against patterns in col 'patterns' without flags
    let result = ctx
        .sql("select regexp_like(values, patterns) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-------------------------------------------------+",
            "| regexp_like(examples.values, examples.patterns) |",
            "+-------------------------------------------------+",
            "| true                                            |",
            "| true                                            |",
            "| false                                           |",
            "| false                                           |",
            "| false                                           |",
            "| false                                           |",
            "| true                                            |",
            "| true                                            |",
            "| true                                            |",
            "| true                                            |",
            "| true                                            |",
            "+-------------------------------------------------+",
        ],
        &result
    );

    // use sql and regexp_like function to test col 'values', against patterns in col 'patterns' with flags
    let result = ctx
        .sql("select regexp_like(values, patterns, flags) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-----------------------------------------------------------------+",
            "| regexp_like(examples.values, examples.patterns, examples.flags) |",
            "+-----------------------------------------------------------------+",
            "| true                                                            |",
            "| true                                                            |",
            "| true                                                            |",
            "| false                                                           |",
            "| false                                                           |",
            "| false                                                           |",
            "| true                                                            |",
            "| true                                                            |",
            "| true                                                            |",
            "| true                                                            |",
            "| true                                                            |",
            "+-----------------------------------------------------------------+"
        ],
        &result
    );

    // literals work as well
    // to match against the entire input use ^ and $ in the regex
    let result = ctx
        .sql("select regexp_like('John Smith', '^.*Smith$'), regexp_like('Smith Jones', '^Smith.*$')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &["+----------------------------------------------------+-----------------------------------------------------+", "| regexp_like(Utf8(\"John Smith\"), Utf8(\"^.*Smith$\")) | regexp_like(Utf8(\"Smith Jones\"), Utf8(\"^Smith.*$\")) |", "+----------------------------------------------------+-----------------------------------------------------+", "| true                                               | true                                                |", "+----------------------------------------------------+-----------------------------------------------------+"],
        &result
    );

    // look-around and back references are not supported for performance
    // reasons.
    // Note that an error may not always be returned but the result
    // if returned will always be false
    let result = ctx
        .sql(r"select regexp_like('(?<=[A-Z]\w )Smith', 'John Smith', 'i') as a")
        .await?
        .collect()
        .await;

    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);

    assert_batches_eq!(
        &[
            "+-------+",
            "| a     |",
            "+-------+",
            "| false |",
            "+-------+",
        ],
        &result
    );

    // invalid flags will result in an error
    let result = ctx
        .sql(r"select regexp_like('\b4(?!000)\d\d\d\b', 4010, 'g')")
        .await?
        .collect()
        .await;

    let expected = "regexp_like() does not support the \"global\" option";
    assert_contains!(result.unwrap_err().to_string(), expected);

    // there is a size limit on the regex during regex compilation
    let result = ctx
        .sql("select regexp_like('aaaaa', 'a{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}')")
        .await?
        .collect()
        .await;

    let expected = "Regular expression did not compile: CompiledTooBig";
    assert_contains!(result.unwrap_err().to_string(), expected);

    //
    //
    //regexp_match examples
    //
    //
    // regexp_match format is (regexp_match(text, regex[, flags])
    //

    let _ = ctx.table("examples").await?;

    // use sql and regexp_match function to test col 'values', against patterns in col 'patterns' without flags
    let result = ctx
        .sql("select regexp_match(values, patterns) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+--------------------------------------------------+",
            "| regexp_match(examples.values, examples.patterns) |",
            "+--------------------------------------------------+",
            "| [a]                                              |",
            "| [A]                                              |",
            "|                                                  |",
            "|                                                  |",
            "|                                                  |",
            "|                                                  |",
            "| [010]                                            |",
            "| [Düsseldorf]                                     |",
            "| [Москва]                                         |",
            "| [Köln]                                           |",
            "| [اليوم]                                          |",
            "+--------------------------------------------------+"
        ],
        &result
    );

    // use dataframe and regexp_match function to test col 'values', against patterns in col 'patterns' with flags
    let result = ctx
        .sql("select regexp_match(values, patterns, flags) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------------------------------------------+",
            "| regexp_match(examples.values, examples.patterns, examples.flags) |",
            "+------------------------------------------------------------------+",
            "| [a]                                                              |",
            "| [A]                                                              |",
            "| [B]                                                              |",
            "|                                                                  |",
            "|                                                                  |",
            "|                                                                  |",
            "| [010]                                                            |",
            "| [Düsseldorf]                                                     |",
            "| [Москва]                                                         |",
            "| [Köln]                                                           |",
            "| [اليوم]                                                          |",
            "+------------------------------------------------------------------+"
        ],
        &result
    );

    // literals work as well
    // to match against the entire input use ^ and $ in the regex
    let result = ctx
        .sql("select regexp_match('John Smith', '^.*Smith$'), regexp_match('Smith Jones', '^Smith.*$')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &["+-----------------------------------------------------+------------------------------------------------------+", "| regexp_match(Utf8(\"John Smith\"), Utf8(\"^.*Smith$\")) | regexp_match(Utf8(\"Smith Jones\"), Utf8(\"^Smith.*$\")) |", "+-----------------------------------------------------+------------------------------------------------------+", "| [John Smith]                                        | [Smith Jones]                                        |", "+-----------------------------------------------------+------------------------------------------------------+"],
        &result
    );

    //
    //
    //regexp_replace examples
    //
    //
    // regexp_replace format is (regexp_replace(text, regex, replace[, flags])
    //

    // use regexp_replace function against tables
    let result = ctx
        .sql("SELECT regexp_replace(values, patterns, replacement, concat('g', flags)) FROM examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &["+-------------------------------------------------------------------------------------------------------------+", "| regexp_replace(examples.values, examples.patterns, examples.replacement, concat(Utf8(\"g\"), examples.flags)) |", "+-------------------------------------------------------------------------------------------------------------+", "| bbabbbc                                                                                                     |", "| B                                                                                                           |", "| aec                                                                                                         |", "| AbC                                                                                                         |", "| aBC                                                                                                         |", "| 4000                                                                                                        |", "| xyz                                                                                                         |", "| München                                                                                                     |", "| Moscow                                                                                                      |", "| Koln                                                                                                        |", "| Today                                                                                                       |", "+-------------------------------------------------------------------------------------------------------------+"],
        &result
    );

    // global flag example
    let result = ctx
        .sql("SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &["+---------------------------------------------------------------------------+", "| regexp_replace(Utf8(\"foobarbaz\"), Utf8(\"b(..)\"), Utf8(\"X\\1Y\"), Utf8(\"g\")) |", "+---------------------------------------------------------------------------+", "| fooXarYXazY                                                               |", "+---------------------------------------------------------------------------+"],
        &result
    );

    // without global flag
    let result = ctx
        .sql("SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----------------------------------------------------------------+",
            "| regexp_replace(Utf8(\"foobarbaz\"), Utf8(\"b(..)\"), Utf8(\"X\\1Y\")) |",
            "+----------------------------------------------------------------+",
            "| fooXarYbaz                                                     |",
            "+----------------------------------------------------------------+"
        ],
        &result
    );

    // null regex means null result
    let result = ctx
        .sql("SELECT regexp_replace('foobarbaz', NULL, 'X\\1Y', 'g')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------------------------------------------+",
            "| regexp_replace(Utf8(\"foobarbaz\"), NULL, Utf8(\"X\\1Y\"), Utf8(\"g\")) |",
            "+------------------------------------------------------------------+",
            "|                                                                  |",
            "+------------------------------------------------------------------+"
        ],
        &result
    );

    Ok(())
}
