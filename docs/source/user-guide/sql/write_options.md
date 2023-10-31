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

# Write Options

DataFusion supports customizing how data is written out to disk as a result of a `COPY` or `INSERT INTO` query. There are a few special options, file format (e.g. CSV or parquet) specific options, and parquet column specific options. Options can also in some cases be specified in multiple ways with a set order of precedence.

## Specifying Options and Order of Precedence

Write related options can be specified in the following ways:

- Session level config defaults
- `CREATE EXTERNAL TABLE` options
- `COPY` option tuples

For a list of supported session level config defaults see [Configuration Settings](configs). These defaults apply to all write operations but have the lowest level of precedence.

If inserting to an external table, table specific write options can be specified when the table is created using the `OPTIONS` clause:

```sql
CREATE EXTERNAL TABLE
my_table(a bigint, b bigint)
STORED AS csv
COMPRESSION TYPE gzip
WITH HEADER ROW
DELIMITER ';'
LOCATION '/test/location/my_csv_table/'
OPTIONS(
NULL_VALUE 'NAN'
);
```

When running `INSERT INTO my_table ...`, the options from the `CREATE TABLE` will be respected (gzip compression, special delimiter, and header row included). Note that compression, header, and delimiter settings can also be specified within the `OPTIONS` tuple list. Dedicated syntax within the SQL statement always takes precedence over arbitrary option tuples, so if both are specified the `OPTIONS` setting will be ignored. Finally, NULL_VALUE is a CSV format specific option that determines how null values should be encoded within the CSV file.

Finally, options can be passed when running a `COPY` command.

```sql
COPY source_table
TO 'test/table_with_options'
(format parquet,
single_file_output false,
compression snappy,
'compression::col1' 'zstd(5)',
)
```

In this example, we write the entirety of `source_table` out to a folder of parquet files. The option `single_file_output` set to false, indicates that the destination path should be interpreted as a folder to which the query will output multiple files. One parquet file will be written in parallel to the folder for each partition in the query. The next option `compression` set to `snappy` indicates that unless otherwise specified all columns should use the snappy compression codec. The option `compression::col1` sets an override, so that the column `col1` in the parquet file will use `ZSTD` compression codec with compression level `5`. In general, parquet options which support column specific settings can be specified with the syntax `OPTION::COLUMN.NESTED.PATH`.

## Available Options

### COPY Specific Options

The following special options are specific to the `COPY` command.

| Option             | Description                                                                                                                                                                  | Default Value |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| SINGLE_FILE_OUTPUT | If true, COPY query will write output to a single file. Otherwise, multiple files will be written to a directory in parallel.                                                | true          |
| FORMAT             | Specifies the file format COPY query will write out. If single_file_output is false or the format cannot be inferred from the file extension, then FORMAT must be specified. | N/A           |

### CREATE EXTERNAL TABLE Specific Options

The following special options are specific to creating an external table.

| Option      | Description                                                                                                                                                                                                                                | Default Value                                                                |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| SINGLE_FILE | If true, indicates that this external table is backed by a single file. INSERT INTO queries will append to this file.                                                                                                                      | false                                                                        |
| INSERT_MODE | Determines if INSERT INTO queries should append to existing files or append new files to an existing directory. Valid values are append_to_file, append_new_files, and error. Note that "error" will block inserting data into this table. | CSV and JSON default to append_to_file. Parquet defaults to append_new_files |

### JSON Format Specific Options

The following options are available when writing JSON files. Note: If any unsupported option is specified, an error will be raised and the query will fail.

| Option      | Description                                                                                                                        | Default Value |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| COMPRESSION | Sets the compression that should be applied to the entire JSON file. Supported values are GZIP, BZIP2, XZ, ZSTD, and UNCOMPRESSED. | UNCOMPRESSED  |

### CSV Format Specific Options

The following options are available when writing CSV files. Note: if any unsupported options is specified an error will be raised and the query will fail.

| Option          | Description                                                                                                                       | Default Value    |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| COMPRESSION     | Sets the compression that should be applied to the entire CSV file. Supported values are GZIP, BZIP2, XZ, ZSTD, and UNCOMPRESSED. | UNCOMPRESSED     |
| HEADER          | Sets if the CSV file should include column headers                                                                                | false            |
| DATE_FORMAT     | Sets the format that dates should be encoded in within the CSV file                                                               | arrow-rs default |
| DATETIME_FORMAT | Sets the format that datetimes should be encoded in within the CSV file                                                           | arrow-rs default |
| TIME_FORMAT     | Sets the format that times should be encoded in within the CSV file                                                               | arrow-rs default |
| RFC3339         | If true, uses RFC339 format for date and time encodings                                                                           | arrow-rs default |
| NULL_VALUE      | Sets the string which should be used to indicate null values within the CSV file.                                                 | arrow-rs default |
| DELIMITER       | Sets the character which should be used as the column delimiter within the CSV file.                                              | arrow-rs default |

### Parquet Format Specific Options

The following options are available when writing parquet files. If any unsupported option is specified an error will be raised and the query will fail. If a column specific option is specified for a column which does not exist, the option will be ignored without error. For default values, see: [Configuration Settings](https://arrow.apache.org/datafusion/user-guide/configs.html).

| Option                       | Can be Column Specific? | Description                                                                                                   |
| ---------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------- |
| COMPRESSION                  | Yes                     | Sets the compression codec and if applicable compression level to use                                         |
| MAX_ROW_GROUP_SIZE           | No                      | Sets the maximum number of rows that can be encoded in a single row group                                     |
| DATA_PAGESIZE_LIMIT          | No                      | Sets the best effort maximum page size in bytes                                                               |
| WRITE_BATCH_SIZE             | No                      | Maximum number of rows written for each column in a single batch                                              |
| WRITER_VERSION               | No                      | Parquet writer version (1.0 or 2.0)                                                                           |
| DICTIONARY_PAGE_SIZE_LIMIT   | No                      | Sets best effort maximum dictionary page size in bytes                                                        |
| CREATED_BY                   | No                      | Sets the "created by" property in the parquet file                                                            |
| COLUMN_INDEX_TRUNCATE_LENGTH | No                      | Sets the max length of min/max value fields in the column index.                                              |
| DATA_PAGE_ROW_COUNT_LIMIT    | No                      | Sets best effort maximum number of rows in a data page.                                                       |
| BLOOM_FILTER_ENABLED         | Yes                     | Sets whether a bloom filter should be written into the file.                                                  |
| ENCODING                     | Yes                     | Sets the encoding that should be used (e.g. PLAIN or RLE)                                                     |
| DICTIONARY_ENABLED           | Yes                     | Sets if dictionary encoding is enabled. Use this instead of ENCODING to set dictionary encoding.              |
| STATISTICS_ENABLED           | Yes                     | Sets if statistics are enabled at PAGE or ROW_GROUP level.                                                    |
| MAX_STATISTICS_SIZE          | Yes                     | Sets the maximum size in bytes that statistics can take up.                                                   |
| BLOOM_FILTER_FPP             | Yes                     | Sets the false positive probability (fpp) for the bloom filter. Implicitly sets BLOOM_FILTER_ENABLED to true. |
| BLOOM_FILTER_NDV             | Yes                     | Sets the number of distinct values (ndv) for the bloom filter. Implicitly sets bloom_filter_enabled to true.  |
