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

# Local Files / Directories

Files can be queried directly by enclosing the file, directory name
or a remote location in single `'` quotes as shown in the examples.

Create a CSV file to query.

```shell
$ echo "a,b" > data.csv
$ echo "1,2" >> data.csv
```

Query that single file (the CLI also supports parquet, compressed csv, avro, json and more)

```shell
$ datafusion-cli
DataFusion CLI v17.0.0
> select * from 'data.csv';
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.007 seconds.
```

You can also query directories of files with compatible schemas:

```shell
$ ls data_dir/
data.csv   data2.csv
```

```shell
$ datafusion-cli
DataFusion CLI v16.0.0
> select * from 'data_dir';
+---+---+
| a | b |
+---+---+
| 3 | 4 |
| 1 | 2 |
+---+---+
2 rows in set. Query took 0.007 seconds.
```

# Remote Files / Directories

You can also query directly any remote location supported by DataFusion without
registering the location as a table.
For example, to read from a remote parquet file via HTTP(S) you can use the following:

```sql
select count(*) from 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet'
+----------+
| COUNT(*) |
+----------+
| 1000000  |
+----------+
1 row in set. Query took 0.595 seconds.
```

To read from an AWS S3 or GCS, use `s3` or `gs` as a protocol prefix. For
example, to read a file in an S3 bucket named `my-data-bucket` use the URL
`s3://my-data-bucket`and set the relevant access credentials as environmental
variables (e.g. for AWS S3 you need to at least `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`).

```sql
select count(*) from 's3://my-data-bucket/athena_partitioned/hits.parquet'
```

See the [`CREATE EXTERNAL TABLE`](#create-external-table) section for
additional configuration options.

# `CREATE EXTERNAL TABLE`

It is also possible to create a table backed by files or remote locations via
`CREATE EXTERNAL TABLE` as shown below. Note that wildcards (e.g. `*`) are also
supported

For example, to create a table `hits` backed by a local parquet file, use:

```sql
CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION 'hits.parquet';
```

To create a table `hits` backed by a remote parquet file via HTTP(S), use

```sql
CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet';
```

In both cases, `hits` now can be queried as a regular table:

```sql
select count(*) from hits;
+----------+
| COUNT(*) |
+----------+
| 1000000  |
+----------+
1 row in set. Query took 0.344 seconds.
```

# Formats

## Parquet

The schema information for parquet will be derived automatically.

Register a single file parquet datasource

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

Register a single folder parquet datasource. Note: All files inside must be valid
parquet files and have compatible schemas

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/';
```

Register a single folder parquet datasource by specifying a wildcard for files to read

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/*.parquet';
```

## CSV

DataFusion will infer the CSV schema automatically or you can provide it explicitly.

Register a single file csv datasource with a header row.

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
LOCATION '/path/to/aggregate_test_100.csv'
OPTIONS ('has_header' 'true');
```

Register a single file csv datasource with explicitly defined schema.

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
LOCATION '/path/to/aggregate_test_100.csv';
```

# Locations

## HTTP(s)

To read from a remote parquet file via HTTP(S) you can use the following:

```sql
CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet';
```

## S3

[AWS S3](https://aws.amazon.com/s3/) data sources must have connection credentials configured.

To create an external table from a file in an S3 bucket:

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'aws.access_key_id' '******',
    'aws.secret_access_key' '******',
    'aws.region' 'us-east-2'
)
LOCATION 's3://bucket/path/file.parquet';
```

It is also possible to specify the access information using environment variables:

```bash
$ export AWS_DEFAULT_REGION=us-east-2
$ export AWS_SECRET_ACCESS_KEY=******
$ export AWS_ACCESS_KEY_ID=******

$ datafusion-cli
`datafusion-cli v21.0.0
> create external table test stored as parquet location 's3://bucket/path/file.parquet';
0 rows in set. Query took 0.374 seconds.
> select * from test;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set. Query took 0.171 seconds.
```

Supported configuration options are:

| Environment Variable                     | Configuration Option    | Description                                               |
| ---------------------------------------- | ----------------------- | --------------------------------------------------------- |
| `AWS_ACCESS_KEY_ID`                      | `aws.access_key_id`     |                                                           |
| `AWS_SECRET_ACCESS_KEY`                  | `aws.secret_access_key` |                                                           |
| `AWS_DEFAULT_REGION`                     | `aws.region`            |                                                           |
| `AWS_ENDPOINT`                           | `aws.endpoint`          |                                                           |
| `AWS_SESSION_TOKEN`                      | `aws.token`             |                                                           |
| `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` |                         | See [IAM Roles]                                           |
| `AWS_ALLOW_HTTP`                         |                         | set to "true" to permit HTTP connections without TLS      |
| `AWS_PROFILE`                            |                         | Support for using a [named profile] to supply credentials |

[named profile]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
[iam roles]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html

## OSS

[Alibaba cloud OSS](https://www.alibabacloud.com/product/object-storage-service) data sources must have connection credentials configured

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'aws.access_key_id' '******',
    'aws.secret_access_key' '******',
    'aws.oss.endpoint' 'https://bucket.oss-cn-hangzhou.aliyuncs.com'
)
LOCATION 'oss://bucket/path/file.parquet';
```

The supported OPTIONS are

- access_key_id
- secret_access_key
- endpoint

Note that the `endpoint` format of oss needs to be: `https://{bucket}.{oss-region-endpoint}`

## COS

[Tencent cloud COS](https://cloud.tencent.com/product/cos) data sources data sources must have connection credentials configured

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'aws.access_key_id' '******',
    'aws.secret_access_key' '******',
    'aws.cos.endpoint' 'https://cos.ap-singapore.myqcloud.com'
)
LOCATION 'cos://bucket/path/file.parquet';
```

The supported OPTIONS are:

- access_key_id
- secret_access_key
- endpoint

Note that the `endpoint` format of urls must be: `https://cos.{cos-region-endpoint}`

## GCS

[Google Cloud Storage](https://cloud.google.com/storage) data sources must have connection credentials configured

For example, to create an external table from a file in a GCS bucket

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'gcp.service_account_path' '/tmp/gcs.json',
)
LOCATION 'gs://bucket/path/file.parquet';
```

It is also possible to specify the access information using environment variables:

```bash
$ export GOOGLE_SERVICE_ACCOUNT=/tmp/gcs.json

$ datafusion-cli
DataFusion CLI v21.0.0
> create external table test stored as parquet location 'gs://bucket/path/file.parquet';
0 rows in set. Query took 0.374 seconds.
> select * from test;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set. Query took 0.171 seconds.
```

Supported configuration options are:

| Environment Variable             | Configuration Option               | Description                              |
| -------------------------------- | ---------------------------------- | ---------------------------------------- |
| `GOOGLE_SERVICE_ACCOUNT`         | `gcp.service_account_path`         | location of service account file         |
| `GOOGLE_SERVICE_ACCOUNT_PATH`    | `gcp.service_account_path`         | (alias) location of service account file |
| `SERVICE_ACCOUNT`                | `gcp.service_account_path`         | (alias) location of service account file |
| `GOOGLE_SERVICE_ACCOUNT_KEY`     | `gcp.service_account_key`          | JSON serialized service account key      |
| `GOOGLE_APPLICATION_CREDENTIALS` | `gcp.application_credentials_path` | location of application credentials file |
| `GOOGLE_BUCKET`                  |                                    | bucket name                              |
| `GOOGLE_BUCKET_NAME`             |                                    | (alias) bucket name                      |

## Hugging Face

The `datafusion-cli` supports querying datasets from the [Hugging Face Hub](https://huggingface.co/datasets) for both public and private datasets.

For example, to query directly a public dataset from the Hugging Face Hub:

```sql
SELECT question, answer
FROM "hf://datasets/cais/mmlu/astronomy/dev-00000-of-00001.parquet";
```

It is also possible to query a list of files from a dataset:

```sql
CREATE EXTERNAL TABLE astronomy
STORED AS parquet
LOCATION "hf://datasets/cais/mmlu/astronomy/";
```

and then

```sql
SELECT question, answer
FROM astronomy;
```

To query a private dataset, you need to set the either the hf.user_access_token or the HF_USER_ACCESS_TOKEN environment variable:

```sql
CREATE EXTERNAL TABLE astronomy
OPTIONS (
    'hf.user_access_token' '******'
)
STORED AS parquet
LOCATION "hf://datasets/cais/mmlu/astronomy/";
```

or

```bash
$ export HF_USER_ACCESS_TOKEN=******

$ datafusion-cli
DataFusion CLI v38.0.0

> CREATE EXTERNAL TABLE astronomy
STORED AS parquet
LOCATION "hf://datasets/cais/mmlu/astronomy/";
```

Supported configuration options are:

| Environment Variable   | Configuration Option   | Description                    |
| ---------------------- | ---------------------- | ------------------------------ |
| `HF_ENDPOINT`          | `hf.endpoint`          | Hugging Face endpoint          |
| `HF_USER_ACCESS_TOKEN` | `hf.user_access_token` | Hugging Face user access token |
