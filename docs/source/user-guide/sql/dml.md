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

# DML

DML stands for "Data Manipulation Language" and relates to inserting
and modifying data in tables.

## COPY

Copies the contents of a table or query to file(s). Supported file
formats are `parquet`, `csv`, `json`, and `arrow`.

<pre>
COPY { <i><b>table_name</i></b> | <i><b>query</i></b> } 
TO '<i><b>file_name</i></b>'
[ STORED AS <i><b>format</i></b> ]
[ PARTITIONED BY <i><b>column_name</i></b> [, ...] ]
[ OPTIONS( <i><b>option</i></b> [, ... ] ) ]
</pre>

For a detailed list of valid OPTIONS, see [Write Options](write_options).

Copy the contents of `source_table` to `file_name.json` in JSON format:

```sql
> COPY source_table TO 'file_name.json';
+-------+
| count |
+-------+
| 2     |
+-------+
```

Copy the contents of `source_table` to one or more Parquet formatted
files in the `dir_name` directory:

```sql
> COPY source_table TO 'dir_name' STORED AS PARQUET;
+-------+
| count |
+-------+
| 2     |
+-------+
```

Copy the contents of `source_table` to multiple directories
of hive-style partitioned parquet files:

```sql
> COPY source_table TO 'dir_name' STORED AS parquet, PARTITIONED BY (column1, column2);
+-------+
| count |
+-------+
| 2     |
+-------+
```

Run the query `SELECT * from source ORDER BY time` and write the
results (maintaining the order) to a parquet file named
`output.parquet` with a maximum parquet row group size of 10MB:

```sql
> COPY (SELECT * from source ORDER BY time) TO 'output.parquet' OPTIONS (MAX_ROW_GROUP_SIZE 10000000);
+-------+
| count |
+-------+
| 2     |
+-------+
```

The output format is determined by the first match of the following rules:

1. Value of `STORED AS`
2. Value of the `OPTION (FORMAT ..)`
3. Filename extension (e.g. `foo.parquet` implies `PARQUET` format)

## INSERT

Insert values into a table.

<pre>
INSERT INTO <i><b>table_name</i></b> { VALUES ( <i><b>expression</i></b> [, ...] ) [, ...] | <i><b>query</i></b> }
</pre>

```sql
> INSERT INTO target_table VALUES (1, 'Foo'), (2, 'Bar');
+-------+
| count |
+-------+
| 2     |
+-------+
```
