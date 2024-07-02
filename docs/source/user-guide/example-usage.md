<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Example Usage

In this example some simple processing is performed on the [`example.csv`](https://github.com/apache/datafusion/blob/main/datafusion/core/tests/data/example.csv) file.

Even [`more code examples`](https://github.com/apache/datafusion/tree/main/datafusion-examples) attached to the project.

## Add published DataFusion dependency

Find latest available Datafusion version on [DataFusion's
crates.io] page. Add the dependency to your `Cargo.toml` file:

```toml
datafusion = "latest_version"
tokio = { version = "1.0", features = ["rt-multi-thread"] }
```

## Add latest non published DataFusion dependency

DataFusion changes are published to `crates.io` according to [release schedule](https://github.com/apache/datafusion/blob/main/dev/release/README.md#release-process)
In case if it is required to test out DataFusion changes which are merged but yet to be published, Cargo supports adding dependency directly to GitHub branch

```toml
datafusion = { git = "https://github.com/apache/datafusion", branch = "main"}
```

Also it works on the package level

```toml
datafusion-common = { git = "https://github.com/apache/datafusion", branch = "main", package = "datafusion-common"}
```

And with features

```toml
datafusion = { git = "https://github.com/apache/datafusion", branch = "main", default-features = false, features = ["unicode_expressions"] }
```

More on [Cargo dependencies](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies)

## Run a SQL query against data stored in a CSV

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

See [the SQL API](../library-user-guide/using-the-sql-api.md) section of the
library guide for more information on the SQL API.

## Use the DataFrame API to process data stored in a CSV

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create the dataframe
  let ctx = SessionContext::new();
  let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

  let df = df.filter(col("a").lt_eq(col("b")))?
           .aggregate(vec![col("a")], vec![min(col("b"))])?
           .limit(0, Some(100))?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Output from both examples

```text
+---+--------+
| a | MIN(b) |
+---+--------+
| 1 | 2      |
+---+--------+
```

## Arrow Versions

Many of DataFusion's public APIs use types from the
[`arrow`] and [`parquet`] crates, so if you use
`arrow` in your project, the `arrow` version must match that used by
DataFusion. You can check the required version on [DataFusion's
crates.io] page.

The easiest way to ensure the versions match is to use the `arrow`
exported by DataFusion, for example:

```rust
use datafusion::arrow::datatypes::Schema;
```

For example, [DataFusion `25.0.0` dependencies] require `arrow`
`39.0.0`. If instead you used `arrow` `40.0.0` in your project you may
see errors such as:

```text
mismatched types [E0308] expected `Schema`, found `arrow_schema::Schema` Note: `arrow_schema::Schema` and `Schema` have similar names, but are actually distinct types Note: `arrow_schema::Schema` is defined in crate `arrow_schema` Note: `Schema` is defined in crate `arrow_schema` Note: perhaps two different versions of crate `arrow_schema` are being used? Note: associated function defined here
```

Or calling `downcast_ref` on an `ArrayRef` may return `None`
unexpectedly.

[`arrow`]: https://docs.rs/arrow/latest/arrow/
[`parquet`]: https://docs.rs/parquet/latest/parquet/
[datafusion's crates.io]: https://crates.io/crates/datafusion
[datafusion `26.0.0` dependencies]: https://crates.io/crates/datafusion/26.0.0/dependencies

## Identifiers and Capitalization

Please be aware that all identifiers are effectively made lower-case in SQL, so if your csv file has capital letters (ex: `Name`) you must put your column name in double quotes or the examples won't work.

To illustrate this behavior, consider the [`capitalized_example.csv`](../../../datafusion/core/tests/data/capitalized_example.csv) file:

## Run a SQL query against data stored in a CSV:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("example", "tests/data/capitalized_example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT \"A\", MIN(b) FROM example WHERE \"A\" <= c GROUP BY \"A\" LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Use the DataFrame API to process data stored in a CSV:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create the dataframe
  let ctx = SessionContext::new();
  let df = ctx.read_csv("tests/data/capitalized_example.csv", CsvReadOptions::new()).await?;

  let df = df
      // col will parse the input string, hence requiring double quotes to maintain the capitalization
      .filter(col("\"A\"").lt_eq(col("c")))?
      // alternatively use ident to pass in an unqualified column name directly without parsing
      .aggregate(vec![ident("A")], vec![min(col("b"))])?
      .limit(0, Some(100))?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Output from both examples

```text
+---+--------+
| A | MIN(b) |
+---+--------+
| 2 | 1      |
| 1 | 2      |
+---+--------+
```

## Extensibility

DataFusion is designed to be extensible at all points. To that end, you can provide your own custom:

- [x] User Defined Functions (UDFs)
- [x] User Defined Aggregate Functions (UDAFs)
- [x] User Defined Table Source (`TableProvider`) for tables
- [x] User Defined `Optimizer` passes (plan rewrites)
- [x] User Defined `LogicalPlan` nodes
- [x] User Defined `ExecutionPlan` nodes

## Optimized Configuration

For an optimized build several steps are required. First, use the below in your `Cargo.toml`. It is
worth noting that using the settings in the `[profile.release]` section will significantly increase the build time.

```toml
[dependencies]
datafusion = { version = "22.0" }
tokio = { version = "^1.0", features = ["rt-multi-thread"] }
snmalloc-rs = "0.3"

[profile.release]
lto = true
codegen-units = 1
```

Then, in `main.rs.` update the memory allocator with the below after your imports:

```rust ,ignore
use datafusion::prelude::*;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  Ok(())
}
```

Based on the instruction set architecture you are building on you will want to configure the `target-cpu` as well, ideally
with `native` or at least `avx2`.

```shell
RUSTFLAGS='-C target-cpu=native' cargo run --release
```

## Enable backtraces

By default Datafusion returns errors as a plain message. There is option to enable more verbose details about the error,
like error backtrace. To enable a backtrace you need to add Datafusion `backtrace` feature to your `Cargo.toml` file:

```toml
datafusion = { version = "31.0.0", features = ["backtrace"]}
```

Set environment [variables](https://doc.rust-lang.org/std/backtrace/index.html#environment-variables)

```bash
RUST_BACKTRACE=1 ./target/debug/datafusion-cli
DataFusion CLI v31.0.0
> select row_numer() over (partition by a order by a) from (select 1 a);
Error during planning: Invalid function 'row_numer'.
Did you mean 'ROW_NUMBER'?

backtrace:    0: std::backtrace_rs::backtrace::libunwind::trace
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/../../backtrace/src/backtrace/libunwind.rs:93:5
   1: std::backtrace_rs::backtrace::trace_unsynchronized
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/../../backtrace/src/backtrace/mod.rs:66:5
   2: std::backtrace::Backtrace::create
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/backtrace.rs:332:13
   3: std::backtrace::Backtrace::capture
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/backtrace.rs:298:9
   4: datafusion_common::error::DataFusionError::get_back_trace
             at /datafusion/datafusion/common/src/error.rs:436:30
   5: datafusion_sql::expr::function::<impl datafusion_sql::planner::SqlToRel<S>>::sql_function_to_expr
   ............
```

The backtraces are useful when debugging code. If there is a test in `datafusion/core/src/physical_planner.rs`

```
#[tokio::test]
async fn test_get_backtrace_for_failed_code() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "
    select row_numer() over (partition by a order by a) from (select 1 a);
    ";

    let _ = ctx.sql(sql).await?.collect().await?;

    Ok(())
}
```

To obtain a backtrace:

```bash
cargo build --features=backtrace
RUST_BACKTRACE=1 cargo test --features=backtrace --package datafusion --lib -- physical_planner::tests::test_get_backtrace_for_failed_code --exact --nocapture
```

Note: The backtrace wrapped into systems calls, so some steps on top of the backtrace can be ignored
