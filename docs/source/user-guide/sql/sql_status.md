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
- Conditional functions
  - [x] nullif
  - [x] case
  - [x] coalesce
- Comparison functions
  - [x] greatest
  - [x] least
- Approximation functions
  - [x] approx_distinct
  - [x] approx_median
  - [x] approx_percentile_cont
  - [x] approx_percentile_cont_with_weight
- Common date/time functions
  - [ ] Basic date functions
  - [ ] Basic time functions
  - [x] Basic timestamp functions
    - [x] [to_timestamp](./scalar_functions.md#to_timestamp)
    - [x] [to_timestamp_millis](./scalar_functions.md#to_timestamp_millis)
    - [x] [to_timestamp_micros](./scalar_functions.md#to_timestamp_micros)
    - [x] [to_timestamp_seconds](./scalar_functions.md#to_timestamp_seconds)
    - [x] [extract](./scalar_functions.md#extract)
    - [x] [date_part](./scalar_functions.md#date_part)
- nested functions
  - [x] Array of columns
- [x] Schema Queries
  - [x] SHOW TABLES
  - [x] SHOW COLUMNS FROM <table/view>
  - [x] SHOW CREATE TABLE <view>
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

In addition to allowing arbitrary datasources via the `TableProvider`
trait, DataFusion includes built in support for the following formats:

- [x] CSV
- [x] Parquet primitive types
- [x] Parquet nested types
- [x] JSON
- [x] Avro
- [x] Arrow
