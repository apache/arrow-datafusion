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

## DDL

DataFusion is a _query_ engine and supports DDL only for modifying the catalog and registering external tables,
creating tables in memory, or creating views. You can not insert, update, or delete data using DataFusion SQL.

### CREATE DATABASE

<pre>
CREATE DATABASE [ IF NOT EXISTS ] <b><i>database_name</i></b>;
</pre>

#### Description

Creates a database in the catalog.

#### Example

```sql
CREATE DATABASE test;
```

### CREATE SCHEMA

<pre>
CREATE SCHEMA [ IF NOT EXISTS ] <b><i>schema_name</i></b>;
</pre>

#### Description

Creates a schema in a database

#### Example

Create schema in the `test` database.

```sql
CREATE SCHEMA test.schema_1;
```

### CREATE TABLE

```sql
CREATE TABLE test.schema.t1 AS VALUES (1,2,3);
CREATE TABLE t1 AS SELECT * FROM (VALUES (1,2)) as t(a,b);
```

### DROP TABLE

<pre>
DROP TABLE [ IF EXISTS ] <b><i>table_name</i></b>;
</pre>

#### Description

Drops the table from DataFusion's catalog.

#### Example

```sql
DROP TABLE test.schema.t1;
```

### CREATE EXTERNAL TABLE

Parquet data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. It is not necessary
to provide schema information for Parquet files.

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

```sql
CREATE EXTERNAL TABLE test
    STORED AS CSV
    WITH HEADER ROW
    LOCATION 'c:/tmp/test.csv';
```

Create an external table with partitioned CSV files:

```sql
CREATE EXTERNAL TABLE p_test
    STORED AS CSV
    WITH HEADER ROW
    PARTITIONED BY (year)
    LOCATION 'c:/tmp/data';
```

The above statement looks for CSV files in the `c:/tmp/data` directory and creates a table with
the columns and data types inferred from the first 1000 rows of the file, as well as adding a column for the partition:

```
❯ \d p_test
+---------------+--------------+------------+-------------+--------------------------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type                | is_nullable |
+---------------+--------------+------------+-------------+--------------------------+-------------+
| datafusion    | public       | p_test     | Region      | Utf8                     | NO          |
| datafusion    | public       | p_test     | Country     | Utf8                     | NO          |
| datafusion    | public       | p_test     | Units       | Int64                    | NO          |
| datafusion    | public       | p_test     | year        | Dictionary(UInt16, Utf8) | NO          |
+---------------+--------------+------------+-------------+--------------------------+-------------+
4 rows in set. Query took 0.003 seconds.
```

CSV data sources can also be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. The schema
optionally may be specified which prevents DataFusion from attempting to infer the schema.

```sql
CREATE EXTERNAL TABLE test (
    c1  VARCHAR NOT NULL,
    c2  INT NOT NULL,
    c3  SMALLINT NOT NULL,
    c4  SMALLINT NOT NULL,
    c5  INT NOT NULL,
    c6  BIGINT NOT NULL,
    c7  SMALLINT NOT NULL,
    c8  INT NOT NULL,
    c9  BIGINT NOT NULL,
    c10 VARCHAR NOT NULL,
    c11 FLOAT NOT NULL,
    c12 DOUBLE NOT NULL,
    c13 VARCHAR NOT NULL
)
STORED AS CSV
WITH HEADER ROW
LOCATION '/path/to/aggregate_test_100.csv';
```

If data sources are already partitioned in Hive style, `PARTITIONED BY` can be used for partition pruning.

```
/mnt/nyctaxi/year=2022/month=01/tripdata.parquet
/mnt/nyctaxi/year=2021/month=12/tripdata.parquet
/mnt/nyctaxi/year=2021/month=11/tripdata.parquet
```

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
PARTITIONED BY (year, month)
LOCATION '/mnt/nyctaxi';
```
