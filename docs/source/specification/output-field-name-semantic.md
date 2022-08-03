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

# Output Field Name Semantics

This specification documents how field names in output record batches should be
generated based on given user queries. The filed name rules apply to
DataFusion queries planned from both SQL queries and Dataframe APIs.

## Field name rules

- All bare column field names MUST not contain relation/table qualifier.
  - Both `SELECT t1.id`, `SELECT id` and `df.select_columns(&["id"])` SHOULD result in field name: `id`
- All compound column field names MUST contain relation/table qualifier.
  - `SELECT foo + bar` SHOULD result in field name: `table.foo PLUS table.bar`
- Function names MUST be converted to lowercase.
  - `SELECT AVG(c1)` SHOULD result in field name: `avg(table.c1)`
- Literal string MUST not be wrapped with quotes or double quotes.
  - `SELECT 'foo'` SHOULD result in field name: `foo`
- Operator expressions MUST be wrapped with parentheses.
  - `SELECT -2` SHOULD result in field name: `(- 2)`
- Operator and operand MUST be separated by spaces.
  - `SELECT 1+2` SHOULD result in field name: `(1 + 2)`
- Function arguments MUST be separated by a comma `,` and a space.
  - `SELECT f(c1,c2)` and `df.select(vec![f.udf("f")?.call(vec![col("c1"), col("c2")])])` SHOULD result in field name: `f(table.c1, table.c2)`

## Appendices

### Examples and comparison with other systems

Data schema for test sample queries:

```
CREATE TABLE t1 (id INT, a VARCHAR(5));
INSERT INTO t1 (id, a) VALUES (1, 'foo');
INSERT INTO t1 (id, a) VALUES (2, 'bar');

CREATE TABLE t2 (id INT, b VARCHAR(5));
INSERT INTO t2 (id, b) VALUES (1, 'hello');
INSERT INTO t2 (id, b) VALUES (2, 'world');
```

#### Projected columns

Query:

```
SELECT t1.id, a, t2.id, b
FROM t1
JOIN t2 ON t1.id = t2.id
```

DataFusion Arrow record batches output:

| id  | a   | id  | b     |
| --- | --- | --- | ----- |
| 1   | foo | 1   | hello |
| 2   | bar | 2   | world |

Spark, MySQL 8 and PostgreSQL 13 output:

| id  | a   | id  | b     |
| --- | --- | --- | ----- |
| 1   | foo | 1   | hello |
| 2   | bar | 2   | world |

SQLite 3 output:

| id  | a   | b     |
| --- | --- | ----- |
| 1   | foo | hello |
| 2   | bar | world |

#### Function transformed columns

Query:

```
SELECT ABS(t1.id), abs(-id) FROM t1;
```

DataFusion Arrow record batches output:

| abs(t1.id) | abs((- t1.id)) |
| ---------- | -------------- |
| 1          | 1              |
| 2          | 2              |

Spark output:

| abs(id) | abs((- id)) |
| ------- | ----------- |
| 1       | 1           |
| 2       | 2           |

MySQL 8 output:

| ABS(t1.id) | abs(-id) |
| ---------- | -------- |
| 1          | 1        |
| 2          | 2        |

PostgreSQL 13 output:

| abs | abs |
| --- | --- |
| 1   | 1   |
| 2   | 2   |

SQlite 3 output:

| ABS(t1.id) | abs(-id) |
| ---------- | -------- |
| 1          | 1        |
| 2          | 2        |

#### Function with operators

Query:

```
SELECT t1.id + ABS(id), ABS(id * t1.id) FROM t1;
```

DataFusion Arrow record batches output:

| t1.id + abs(t1.id) | abs(t1.id \* t1.id) |
| ------------------ | ------------------- |
| 2                  | 1                   |
| 4                  | 4                   |

Spark output:

| id + abs(id) | abs(id \* id) |
| ------------ | ------------- |
| 2            | 1             |
| 4            | 4             |

MySQL 8 output:

| t1.id + ABS(id) | ABS(id \* t1.id) |
| --------------- | ---------------- |
| 2               | 1                |
| 4               | 4                |

PostgreSQL output:

| ?column? | abs |
| -------- | --- |
| 2        | 1   |
| 4        | 4   |

SQLite output:

| t1.id + ABS(id) | ABS(id \* t1.id) |
| --------------- | ---------------- |
| 2               | 1                |
| 4               | 4                |

#### Project literals

Query:

```
SELECT 1, 2+5, 'foo_bar';
```

DataFusion Arrow record batches output:

| 1   | (2 + 5) | foo_bar |
| --- | ------- | ------- |
| 1   | 7       | foo_bar |

Spark output:

| 1   | (2 + 5) | foo_bar |
| --- | ------- | ------- |
| 1   | 7       | foo_bar |

MySQL output:

| 1   | 2+5 | foo_bar |
| --- | --- | ------- |
| 1   | 7   | foo_bar |

PostgreSQL output:

| ?column? | ?column? | ?column? |
| -------- | -------- | -------- |
| 1        | 7        | foo_bar  |

SQLite 3 output:

| 1   | 2+5 | 'foo_bar' |
| --- | --- | --------- |
| 1   | 7   | foo_bar   |
