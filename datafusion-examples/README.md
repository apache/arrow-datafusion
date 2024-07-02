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

# DataFusion Examples

This crate includes end to end, highly commented examples of how to use
various DataFusion APIs to help you get started.

## Prerequisites:

Run `git submodule update --init` to init test files.

## Running Examples

To run an example, use the `cargo run` command, such as:

```bash
git clone https://github.com/apache/datafusion
cd datafusion
# Download test data
git submodule update --init

# Run the `csv_sql` example:
# ... use the equivalent for other examples
cargo run --example csv_sql
```

## Single Process

- [`advanced_udaf.rs`](examples/advanced_udaf.rs): Define and invoke a more complicated User Defined Aggregate Function (UDAF)
- [`advanced_udf.rs`](examples/advanced_udf.rs): Define and invoke a more complicated User Defined Scalar Function (UDF)
- [`advanced_udwf.rs`](examples/advanced_udwf.rs): Define and invoke a more complicated User Defined Window Function (UDWF)
- [`advanced_parquet_index.rs`](examples/advanced_parquet_index.rs): Creates a detailed secondary index that covers the contents of several parquet files
- [`avro_sql.rs`](examples/avro_sql.rs): Build and run a query plan from a SQL statement against a local AVRO file
- [`catalog.rs`](examples/catalog.rs): Register the table into a custom catalog
- [`composed_extension_codec`](examples/composed_extension_codec.rs): Example of using multiple extension codecs for serialization / deserialization
- [`csv_sql.rs`](examples/csv_sql.rs): Build and run a query plan from a SQL statement against a local CSV file
- [`csv_sql_streaming.rs`](examples/csv_sql_streaming.rs): Build and run a streaming query plan from a SQL statement against a local CSV file
- [`custom_datasource.rs`](examples/custom_datasource.rs): Run queries against a custom datasource (TableProvider)
- [`dataframe-to-s3.rs`](examples/external_dependency/dataframe-to-s3.rs): Run a query using a DataFrame against a parquet file from s3 and writing back to s3
- [`dataframe.rs`](examples/dataframe.rs): Run a query using a DataFrame against a local parquet file
- [`dataframe_in_memory.rs`](examples/dataframe_in_memory.rs): Run a query using a DataFrame against data in memory
- [`dataframe_output.rs`](examples/dataframe_output.rs): Examples of methods which write data out from a DataFrame
- [`deserialize_to_struct.rs`](examples/deserialize_to_struct.rs): Convert query results into rust structs using serde
- [`expr_api.rs`](examples/expr_api.rs): Create, execute, simplify and analyze `Expr`s
- [`file_stream_provider.rs`](examples/file_stream_provider.rs): Run a query on `FileStreamProvider` which implements `StreamProvider` for reading and writing to arbitrary stream sources / sinks.
- [`flight_sql_server.rs`](examples/flight/flight_sql_server.rs): Run DataFusion as a standalone process and execute SQL queries from JDBC clients
- [`function_factory.rs`](examples/function_factory.rs): Register `CREATE FUNCTION` handler to implement SQL macros
- [`make_date.rs`](examples/make_date.rs): Examples of using the make_date function
- [`memtable.rs`](examples/memtable.rs): Create an query data in memory using SQL and `RecordBatch`es
- [`optimizer_rule.rs`](examples/optimizer_rule.rs): Use a custom OptimizerRule to replace certain predicates
- [`parquet_index.rs`](examples/parquet_index.rs): Create an secondary index over several parquet files and use it to speed up queries
- [`parquet_sql.rs`](examples/parquet_sql.rs): Build and run a query plan from a SQL statement against a local Parquet file
- [`parquet_sql_multiple_files.rs`](examples/parquet_sql_multiple_files.rs): Build and run a query plan from a SQL statement against multiple local Parquet files
- [`parquet_exec_visitor.rs`](examples/parquet_exec_visitor.rs): Extract statistics by visiting an ExecutionPlan after execution
- [`parse_sql_expr.rs`](examples/parse_sql_expr.rs): Parse SQL text into Datafusion `Expr`.
- [`plan_to_sql.rs`](examples/plan_to_sql.rs): Generate SQL from Datafusion `Expr` and `LogicalPlan`
- [`pruning.rs`](examples/parquet_sql.rs): Use pruning to rule out files based on statistics
- [`query-aws-s3.rs`](examples/external_dependency/query-aws-s3.rs): Configure `object_store` and run a query against files stored in AWS S3
- [`query-http-csv.rs`](examples/query-http-csv.rs): Configure `object_store` and run a query against files vi HTTP
- [`regexp.rs`](examples/regexp.rs): Examples of using regular expression functions
- [`rewrite_expr.rs`](examples/rewrite_expr.rs): Define and invoke a custom Query Optimizer pass
- [`simple_udaf.rs`](examples/simple_udaf.rs): Define and invoke a User Defined Aggregate Function (UDAF)
- [`simple_udf.rs`](examples/simple_udf.rs): Define and invoke a User Defined Scalar Function (UDF)
- [`simple_udfw.rs`](examples/simple_udwf.rs): Define and invoke a User Defined Window Function (UDWF)
- [`sql_analysis.rs`](examples/sql_analysis.rs): Analyse SQL queries with DataFusion structures
- [`sql_frontend.rs`](examples/sql_frontend.rs): Create LogicalPlans (only) from sql strings
- [`sql_dialect.rs`](examples/sql_dialect.rs): Example of implementing a custom SQL dialect on top of `DFParser`
- [`to_char.rs`](examples/to_char.rs): Examples of using the to_char function
- [`to_timestamp.rs`](examples/to_timestamp.rs): Examples of using to_timestamp functions

## Distributed

- [`flight_client.rs`](examples/flight/flight_client.rs) and [`flight_server.rs`](examples/flight/flight_server.rs): Run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol.
