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

# Using the SQL API

DataFusion has a full SQL API that allows you to interact with DataFusion using
SQL query strings. The simplest way to use the SQL API is to use the
[`SessionContext`] struct which provides the highest-level API for executing SQL
queries.

To use SQL, you first register your data as a table and then run queries
using the [`SessionContext::sql`] method. For lower level control such as
preventing DDL, you can use [`SessionContext::sql_with_options`] or the
[`SessionState`] APIs

[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessioncontext::sql`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`sessioncontext::sql_with_options`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql_with_options
[`sessionstate`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html

## Registering Data Sources using `SessionContext::register*`

The `SessionContext::register*` methods tell DataFusion the name of
the source and how to read data. Once registered, you can execute SQL queries
using the `SessionContext::sql` method referring to your data source as a table.

### Read a CSV File

```rust
use datafusion::error::Result;
use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // register the "example" table
    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
    // create a plan to run a SQL query
    let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;
    // execute the plan and collect the results as Vec<RecordBatch>
    let results: Vec<RecordBatch> = df.collect().await?;
    // Use the assert_batches_eq macro to compare the results with expected output
    datafusion::assert_batches_eq!(vec![
        "+---+----------------+",
        "| a | MIN(example.b) |",
        "+---+----------------+",
        "| 1 | 2              |",
        "+---+----------------+",
        ],
        &results
    );
  Ok(())
}
```

### Read an Apache Parquet file

Similarly to CSV, you can register a Parquet file as a table using the `register_parquet` method.

```rust
use datafusion::error::Result;
use datafusion::prelude::*;
#[tokio::main]
async fn main() -> Result<()> {
    // create local session context
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx.sql(
        "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
         FROM alltypes_plain \
         WHERE id > 1 AND tinyint_col < double_col",
    ).await?;

    // execute the plan, and compare to the expected results
    let results = df.collect().await?;
    datafusion::assert_batches_eq!(vec![
        "+---------+------------+--------------------------------+",
        "| int_col | double_col | alltypes_plain.date_string_col |",
        "+---------+------------+--------------------------------+",
        "| 1       | 10.1       | 03/01/09                       |",
        "| 1       | 10.1       | 04/01/09                       |",
        "| 1       | 10.1       | 02/01/09                       |",
        "+---------+------------+--------------------------------+",
        ],
        &results
    );
    Ok(())
}
```

### Read an Apache Avro file

DataFusion can also read Avro files using the `register_avro` method.

```rust
use datafusion::arrow::util::pretty;
use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // find the path to the avro test files
    let testdata = datafusion::test_util::arrow_test_data();
    // register avro file with the execution context
    let avro_file = &format!("{testdata}/avro/alltypes_plain.avro");
    ctx.register_avro("alltypes_plain", avro_file, AvroReadOptions::default()).await?;

    // execute the query
    let df = ctx.sql(
        "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
         FROM alltypes_plain \
         WHERE id > 1 AND tinyint_col < double_col"
      ).await?;

    // execute the plan, and compare to the expected results
    let results = df.collect().await?;
    datafusion::assert_batches_eq!(vec![
        "+---------+------------+--------------------------------+",
        "| int_col | double_col | alltypes_plain.date_string_col |",
        "+---------+------------+--------------------------------+",
        "| 1       | 10.1       | 03/01/09                       |",
        "| 1       | 10.1       | 04/01/09                       |",
        "| 1       | 10.1       | 02/01/09                       |",
        "+---------+------------+--------------------------------+",
        ],
        &results
    );
    Ok(())
}
```

## Reading Multiple Files as a table

It is also possible to read multiple files as a single table. This is done
with the ListingTableProvider which takes a list of file paths and reads them
as a single table, matching schemas as appropriate

Coming Soon

```rust

```

## Using `CREATE EXTERNAL TABLE` to register data sources via SQL

You can also register files using SQL using the [`CREATE EXTERNAL TABLE`]
statement.

[`create external table`]: ../user-guide/sql/ddl.md#create-external-table

```rust
use datafusion::error::Result;
use datafusion::prelude::*;
#[tokio::main]
async fn main() -> Result<()> {
    // create local session context
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file using SQL
    let ddl = format!(
        "CREATE EXTERNAL TABLE alltypes_plain \
        STORED AS PARQUET LOCATION '{testdata}/alltypes_plain.parquet'"
    );
    ctx.sql(&ddl).await?;

    // execute the query referring to the alltypes_plain table we just registered
    let df = ctx.sql(
        "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
         FROM alltypes_plain \
         WHERE id > 1 AND tinyint_col < double_col",
    ).await?;

    // execute the plan, and compare to the expected results
    let results = df.collect().await?;
    datafusion::assert_batches_eq!(vec![
        "+---------+------------+--------------------------------+",
        "| int_col | double_col | alltypes_plain.date_string_col |",
        "+---------+------------+--------------------------------+",
        "| 1       | 10.1       | 03/01/09                       |",
        "| 1       | 10.1       | 04/01/09                       |",
        "| 1       | 10.1       | 02/01/09                       |",
        "+---------+------------+--------------------------------+",
        ],
        &results
    );
    Ok(())
}
```
