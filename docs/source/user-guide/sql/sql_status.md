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

# Status

## General

- [x] SQL Parser
- [x] SQL Query Planner
- [x] Query Optimizer
- [x] Constant folding
- [x] Join Reordering
- [x] Limit Pushdown
- [x] Projection push down
- [x] Predicate push down
- [x] Type coercion
- [x] Parallel query execution

## SQL Support

- [x] Projection
- [x] Filter (WHERE)
- [x] Filter post-aggregate (HAVING)
- [x] Limit
- [x] Aggregate
- [x] Common math functions
- [x] cast
- [x] try_cast
- [x] [`VALUES` lists](https://www.postgresql.org/docs/current/queries-values.html)
- Postgres compatible String functions
  - [x] ascii
  - [x] bit_length
  - [x] btrim
  - [x] char_length
  - [x] character_length
  - [x] chr
  - [x] concat
  - [x] concat_ws
  - [x] initcap
  - [x] left
  - [x] length
  - [x] lpad
  - [x] ltrim
  - [x] octet_length
  - [x] regexp_replace
  - [x] repeat
  - [x] replace
  - [x] reverse
  - [x] right
  - [x] rpad
  - [x] rtrim
  - [x] split_part
  - [x] starts_with
  - [x] strpos
  - [x] substr
  - [x] to_hex
  - [x] translate
  - [x] trim
- Conditional expressions
  - [x] nullif
  - [x] case
  - [x] coalesce
- Approximation functions
  - [x] approx_distinct
  - [x] approx_median
  - [x] approx_percentile_cont
  - [x] approx_percentile_cont_with_weight
- Common date/time functions
  - [ ] Basic date functions
  - [ ] Basic time functions
  - [x] Basic timestamp functions
    - [x] [to_timestamp](./datafusion-functions.md#to_timestamp)
    - [x] [to_timestamp_millis](./datafusion-functions.md#to_timestamp_millis)
    - [x] [to_timestamp_micros](./datafusion-functions.md#to_timestamp_micros)
    - [x] [to_timestamp_seconds](./datafusion-functions.md#to_timestamp_seconds)
    - [x] [extract](./datafusion-functions.md#extract)
    - [x] [date_part](./datafusion-functions.md#date_part)
- nested functions
  - [x] Array of columns
- [x] Schema Queries
  - [x] SHOW TABLES
  - [x] SHOW COLUMNS
  - [x] information_schema.{tables, columns, views}
  - [ ] information_schema other views
- [x] Sorting
- [ ] Nested types
- [ ] Lists
- [x] Subqueries
- [x] Common table expressions
- [x] Set Operations
  - [x] UNION ALL
  - [x] UNION
  - [x] INTERSECT
  - [x] INTERSECT ALL
  - [x] EXCEPT
  - [x] EXCEPT ALL
- [x] Joins
  - [x] INNER JOIN
  - [x] LEFT JOIN
  - [x] RIGHT JOIN
  - [x] FULL JOIN
  - [x] CROSS JOIN
- [ ] Window
  - [x] Empty window
  - [x] Common window functions
  - [x] Window with PARTITION BY clause
  - [x] Window with ORDER BY clause
  - [ ] Window with FILTER clause
  - [ ] [Window with custom WINDOW FRAME](https://github.com/apache/arrow-datafusion/issues/361)
  - [ ] UDF and UDAF for window functions

## Data Sources

- [x] CSV
- [x] JSON
- [x] Avro
- [x] Parquet primitive types
- [ ] Parquet nested types

## Extensibility

DataFusion is designed to be extensible at all points. To that end, you can provide your own custom:

- [x] User Defined Functions (UDFs)
- [x] User Defined Aggregate Functions (UDAFs)
- [x] User Defined Table Source (`TableProvider`) for tables
- [x] User Defined `Optimizer` passes (plan rewrites)
- [x] User Defined `LogicalPlan` nodes
- [x] User Defined `ExecutionPlan` nodes

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.

# Supported SQL

This library currently supports many SQL constructs, including

- `CREATE EXTERNAL TABLE X STORED AS PARQUET LOCATION '...';` to register a table's locations
- `SELECT ... FROM ...` together with any expression
- `ALIAS` to name an expression
- `CAST` to change types, including e.g. `Timestamp(Nanosecond, None)`
- Many mathematical unary and binary expressions such as `+`, `/`, `sqrt`, `tan`, `>=`.
- `WHERE` to filter
- `GROUP BY` together with one of the following aggregations: `MIN`, `MAX`, `COUNT`, `SUM`, `AVG`, `CORR`, `VAR`, `COVAR`, `STDDEV` (sample and population)
- `ORDER BY` together with an expression and optional `ASC` or `DESC` and also optional `NULLS FIRST` or `NULLS LAST`

## Supported Functions

DataFusion strives to implement a subset of the [PostgreSQL SQL dialect](https://www.postgresql.org/docs/current/functions.html) where possible. We explicitly choose a single dialect to maximize interoperability with other tools and allow reuse of the PostgreSQL documents and tutorials as much as possible.

Currently, only a subset of the PostgreSQL dialect is implemented, and we will document any deviations.

## Schema Metadata / Information Schema Support

DataFusion supports the showing metadata about the tables available. This information can be accessed using the views of the ISO SQL `information_schema` schema or the DataFusion-specific `SHOW TABLES` and `SHOW COLUMNS` commands.

More information can be found in the [Postgres docs](https://www.postgresql.org/docs/13/infoschema-schema.html)).

To show tables available for use in DataFusion, use the `SHOW TABLES` command or the `information_schema.tables` view:

```sql
> show tables;
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| datafusion    | public             | t          | BASE TABLE |
| datafusion    | information_schema | tables     | VIEW       |
+---------------+--------------------+------------+------------+

> select * from information_schema.tables;

+---------------+--------------------+------------+--------------+
| table_catalog | table_schema       | table_name | table_type   |
+---------------+--------------------+------------+--------------+
| datafusion    | public             | t          | BASE TABLE   |
| datafusion    | information_schema | TABLES     | SYSTEM TABLE |
+---------------+--------------------+------------+--------------+
```

To show views, use the `SHOW VIEWS` command or the or `information_schema.views` view:

```sql
> select * from information_schema.views;

```

To show the schema of a table in DataFusion, use the `SHOW COLUMNS` command or the or `information_schema.columns` view:

```sql
> show columns from t;
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| datafusion    | public       | t          | a           | Int32     | NO          |
| datafusion    | public       | t          | b           | Utf8      | NO          |
| datafusion    | public       | t          | c           | Float32   | NO          |
+---------------+--------------+------------+-------------+-----------+-------------+

>   select table_name, column_name, ordinal_position, is_nullable, data_type from information_schema.columns;
+------------+-------------+------------------+-------------+-----------+
| table_name | column_name | ordinal_position | is_nullable | data_type |
+------------+-------------+------------------+-------------+-----------+
| t          | a           | 0                | NO          | Int32     |
| t          | b           | 1                | NO          | Utf8      |
| t          | c           | 2                | NO          | Float32   |
+------------+-------------+------------------+-------------+-----------+
```

## Supported Data Types

DataFusion uses Arrow, and thus the Arrow type system, for query
execution. The SQL types from
[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/ast/data_type.rs#L27)
are mapped to Arrow types according to the following table

| SQL Data Type | Arrow DataType                    |
| ------------- | --------------------------------- |
| `CHAR`        | `Utf8`                            |
| `VARCHAR`     | `Utf8`                            |
| `UUID`        | _Not yet supported_               |
| `CLOB`        | _Not yet supported_               |
| `BINARY`      | _Not yet supported_               |
| `VARBINARY`   | _Not yet supported_               |
| `DECIMAL`     | `Float64`                         |
| `FLOAT`       | `Float32`                         |
| `SMALLINT`    | `Int16`                           |
| `INT`         | `Int32`                           |
| `BIGINT`      | `Int64`                           |
| `REAL`        | `Float32`                         |
| `DOUBLE`      | `Float64`                         |
| `BOOLEAN`     | `Boolean`                         |
| `DATE`        | `Date32`                          |
| `TIME`        | `Time64(TimeUnit::Millisecond)`   |
| `TIMESTAMP`   | `Timestamp(TimeUnit::Nanosecond)` |
| `INTERVAL`    | _Not yet supported_               |
| `REGCLASS`    | _Not yet supported_               |
| `TEXT`        | _Not yet supported_               |
| `BYTEA`       | _Not yet supported_               |
| `CUSTOM`      | _Not yet supported_               |
| `ARRAY`       | _Not yet supported_               |
