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

# DataFusion Benchmarks

This crate contains benchmarks based on popular public data sets and
open source benchmark suites, to help with performance and scalability
testing of DataFusion.


## Other engines

The benchmarks measure changes to DataFusion itself, rather than
its performance against other engines. For competitive benchmarking,
DataFusion is included in the benchmark setups for several popular
benchmarks that compare performance with other engines. For example:

* [ClickBench] scripts are in the [ClickBench repo](https://github.com/ClickHouse/ClickBench/tree/main/datafusion)
* [H2o.ai `db-benchmark`] scripts are in [db-benchmark](db-benchmark) directory

[ClickBench]: https://github.com/ClickHouse/ClickBench/tree/main
[H2o.ai `db-benchmark`]: https://github.com/h2oai/db-benchmark

# Running the benchmarks

## `bench.sh`

The easiest way to run benchmarks is the [bench.sh](bench.sh)
script. Usage instructions can be found with:

```shell
# show usage
./bench.sh
```

## Generating data

You can create / download the data for these benchmarks using the [bench.sh](bench.sh) script:

Create / download all datasets

```shell
./bench.sh data
```

Create / download a specific dataset (TPCH)

```shell
./bench.sh data tpch
```

Data is placed in the `data` subdirectory.

## Comparing performance of main and a branch

```shell
git checkout main

# Create the data
./benchmarks/bench.sh data

# Gather baseline data for tpch benchmark
./benchmarks/bench.sh run tpch

# Switch to the branch the branch name is mybranch and gather data
git checkout mybranch
./benchmarks/bench.sh run tpch

# Compare results in the two branches:
./bench.sh compare main mybranch
```

This produces results like:

```shell
Comparing main and mybranch
--------------------
Benchmark tpch.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃         main ┃     mybranch ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ QQuery 1     │    2520.52ms │    2795.09ms │  1.11x slower │
│ QQuery 2     │     222.37ms │     216.01ms │     no change │
│ QQuery 3     │     248.41ms │     239.07ms │     no change │
│ QQuery 4     │     144.01ms │     129.28ms │ +1.11x faster │
│ QQuery 5     │     339.54ms │     327.53ms │     no change │
│ QQuery 6     │     147.59ms │     138.73ms │ +1.06x faster │
│ QQuery 7     │     605.72ms │     631.23ms │     no change │
│ QQuery 8     │     326.35ms │     372.12ms │  1.14x slower │
│ QQuery 9     │     579.02ms │     634.73ms │  1.10x slower │
│ QQuery 10    │     403.38ms │     420.39ms │     no change │
│ QQuery 11    │     201.94ms │     212.12ms │  1.05x slower │
│ QQuery 12    │     235.94ms │     254.58ms │  1.08x slower │
│ QQuery 13    │     738.40ms │     789.67ms │  1.07x slower │
│ QQuery 14    │     198.73ms │     206.96ms │     no change │
│ QQuery 15    │     183.32ms │     179.53ms │     no change │
│ QQuery 16    │     168.57ms │     186.43ms │  1.11x slower │
│ QQuery 17    │    2032.57ms │    2108.12ms │     no change │
│ QQuery 18    │    1912.80ms │    2134.82ms │  1.12x slower │
│ QQuery 19    │     391.64ms │     368.53ms │ +1.06x faster │
│ QQuery 20    │     648.22ms │     691.41ms │  1.07x slower │
│ QQuery 21    │     866.25ms │    1020.37ms │  1.18x slower │
│ QQuery 22    │     115.94ms │     117.27ms │     no change │
└──────────────┴──────────────┴──────────────┴───────────────┘
--------------------
Benchmark tpch_mem.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃         main ┃     mybranch ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ QQuery 1     │    2182.44ms │    2390.39ms │  1.10x slower │
│ QQuery 2     │     181.16ms │     153.94ms │ +1.18x faster │
│ QQuery 3     │      98.89ms │      95.51ms │     no change │
│ QQuery 4     │      61.43ms │      66.15ms │  1.08x slower │
│ QQuery 5     │     260.20ms │     283.65ms │  1.09x slower │
│ QQuery 6     │      24.24ms │      23.39ms │     no change │
│ QQuery 7     │     545.87ms │     653.34ms │  1.20x slower │
│ QQuery 8     │     147.48ms │     136.00ms │ +1.08x faster │
│ QQuery 9     │     371.53ms │     363.61ms │     no change │
│ QQuery 10    │     197.91ms │     190.37ms │     no change │
│ QQuery 11    │     197.91ms │     183.70ms │ +1.08x faster │
│ QQuery 12    │     100.32ms │     103.08ms │     no change │
│ QQuery 13    │     428.02ms │     440.26ms │     no change │
│ QQuery 14    │      38.50ms │      27.11ms │ +1.42x faster │
│ QQuery 15    │     101.15ms │      63.25ms │ +1.60x faster │
│ QQuery 16    │     171.15ms │     142.44ms │ +1.20x faster │
│ QQuery 17    │    1885.05ms │    1953.58ms │     no change │
│ QQuery 18    │    1549.92ms │    1914.06ms │  1.23x slower │
│ QQuery 19    │     106.53ms │     104.28ms │     no change │
│ QQuery 20    │     532.11ms │     610.62ms │  1.15x slower │
│ QQuery 21    │     723.39ms │     823.34ms │  1.14x slower │
│ QQuery 22    │      91.84ms │      89.89ms │     no change │
└──────────────┴──────────────┴──────────────┴───────────────┘
```

Note that you can also execute an automatic comparison of the changes in a given PR against the base 
just by including the trigger `/benchmark` in any comment.

### Running Benchmarks Manually

Assuming data in the `data` directory, the `tpch` benchmark can be run with a command like this

```bash
cargo run --release --bin dfbench -- tpch --iterations 3 --path ./data --format tbl --query 1 --batch-size 4096
```

See the help for more details

### Different features

You can enable `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as `--features`. For example

```shell
cargo run --release --features "mimalloc" --bin tpch -- benchmark datafusion --iterations 3 --path ./data --format tbl --query 1 --batch-size 4096
```

The benchmark program also supports CSV and Parquet input file formats and a utility is provided to convert from `tbl`
(generated by the `dbgen` utility) to CSV and Parquet.

```bash
cargo run --release --bin tpch -- convert --input ./data --output /mnt/tpch-parquet --format parquet
```

Or if you want to verify and run all the queries in the benchmark, you can just run `cargo test`.

### Comparing results between runs

Any `dfbench` execution with `-o <dir>` argument will produce a
summary JSON in the specified directory. This file contains a
serialized form of all the runs that happened and runtime
metadata (number of cores, DataFusion version, etc.).

```shell
$ git checkout main
# generate an output script in /tmp/output_main
$ mkdir -p /tmp/output_main
$ cargo run --release --bin tpch -- benchmark datafusion --iterations 5 --path ./data --format parquet -o /tmp/output_main
# generate an output script in /tmp/output_branch
$ mkdir -p /tmp/output_branch
$ git checkout my_branch
$ cargo run --release --bin tpch -- benchmark datafusion --iterations 5 --path ./data --format parquet -o /tmp/output_branch
# compare the results:
./compare.py /tmp/output_main/tpch-summary--1679330119.json  /tmp/output_branch/tpch-summary--1679328405.json
```

This will produce output like

```
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃ /home/alamb… ┃ /home/alamb… ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ Q1           │   16252.56ms │   16031.82ms │     no change │
│ Q2           │    3994.56ms │    4353.75ms │  1.09x slower │
│ Q3           │    5572.06ms │    5620.27ms │     no change │
│ Q4           │    2144.14ms │    2194.67ms │     no change │
│ Q5           │    7796.93ms │    7646.74ms │     no change │
│ Q6           │    4382.32ms │    4327.16ms │     no change │
│ Q7           │   18702.50ms │   19922.74ms │  1.07x slower │
│ Q8           │    7383.74ms │    7616.21ms │     no change │
│ Q9           │   13855.17ms │   14408.42ms │     no change │
│ Q10          │    7446.05ms │    8030.00ms │  1.08x slower │
│ Q11          │    3414.81ms │    3850.34ms │  1.13x slower │
│ Q12          │    3027.16ms │    3085.89ms │     no change │
│ Q13          │   18859.06ms │   18627.02ms │     no change │
│ Q14          │    4157.91ms │    4140.22ms │     no change │
│ Q15          │    5293.05ms │    5369.17ms │     no change │
│ Q16          │    6512.42ms │    3011.58ms │ +2.16x faster │
│ Q17          │   86253.33ms │   76036.06ms │ +1.13x faster │
│ Q18          │   45101.99ms │   49717.76ms │  1.10x slower │
│ Q19          │    7323.15ms │    7409.85ms │     no change │
│ Q20          │   19902.39ms │   20965.94ms │  1.05x slower │
│ Q21          │   22040.06ms │   23184.84ms │  1.05x slower │
│ Q22          │    2011.87ms │    2143.62ms │  1.07x slower │
└──────────────┴──────────────┴──────────────┴───────────────┘
```

# Benchmark Runner

The `dfbench` program contains subcommands to run the various
benchmarks. When benchmarking, it should always be built in release
mode using `--release`.

Full help for each benchmark can be found in the relevant sub
command. For example to get help for tpch, run

```shell
cargo run --release --bin dfbench  --help
...
datafusion-benchmarks 27.0.0
benchmark command

USAGE:
    dfbench <SUBCOMMAND>

SUBCOMMANDS:
    clickbench        Run the clickbench benchmark
    help              Prints this message or the help of the given subcommand(s)
    parquet-filter    Test performance of parquet filter pushdown
    sort              Test performance of parquet filter pushdown
    tpch              Run the tpch benchmark.
    tpch-convert      Convert tpch .slt files to .parquet or .csv files

```

# Benchmarks

The output of `dfbench` help includes a description of each benchmark, which is reproduced here for convenience

## ClickBench

The ClickBench[1] benchmarks are widely cited in the industry and
focus on grouping / aggregation / filtering. This runner uses the
scripts and queries from [2].

[1]: https://github.com/ClickHouse/ClickBench
[2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion

## Parquet Filter

Test performance of parquet filter pushdown

The queries are executed on a synthetic dataset generated during
the benchmark execution and designed to simulate web server access
logs.

Example

dfbench parquet-filter  --path ./data --scale-factor 1.0

generates the synthetic dataset at `./data/logs.parquet`. The size
of the dataset can be controlled through the `size_factor`
(with the default value of `1.0` generating a ~1GB parquet file).

For each filter we will run the query using different
`ParquetScanOption` settings.

Example output:

```
Running benchmarks with the following options: Opt { debug: false, iterations: 3, partitions: 2, path: "./data",
batch_size: 8192, scale_factor: 1.0 }
Generated test dataset with 10699521 rows
Executing with filter 'request_method = Utf8("GET")'
Using scan options ParquetScanOptions { pushdown_filters: false, reorder_predicates: false, enable_page_index: false }
Iteration 0 returned 10699521 rows in 1303 ms
Iteration 1 returned 10699521 rows in 1288 ms
Iteration 2 returned 10699521 rows in 1266 ms
Using scan options ParquetScanOptions { pushdown_filters: true, reorder_predicates: true, enable_page_index: true }
Iteration 0 returned 1781686 rows in 1970 ms
Iteration 1 returned 1781686 rows in 2002 ms
Iteration 2 returned 1781686 rows in 1988 ms
Using scan options ParquetScanOptions { pushdown_filters: true, reorder_predicates: false, enable_page_index: true }
Iteration 0 returned 1781686 rows in 1940 ms
Iteration 1 returned 1781686 rows in 1986 ms
Iteration 2 returned 1781686 rows in 1947 ms
...
```

## Sort
Test performance of sorting large datasets

This test sorts a a synthetic dataset generated during the
benchmark execution, designed to simulate sorting web server
access logs. Such sorting is often done during data transformation
steps.

The tests sort the entire dataset using several different sort
orders.

## TPCH

Run the tpch benchmark.

This benchmarks is derived from the [TPC-H][1] version
[2.17.1]. The data and answers are generated using `tpch-gen` from
[2].

[1]: http://www.tpc.org/tpch/
[2]: https://github.com/databricks/tpch-dbgen.git,
[2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf


# Older Benchmarks

## h2o benchmarks

```bash
cargo run --release --bin h2o group-by --query 1 --path /mnt/bigdata/h2oai/N_1e7_K_1e2_single.csv --mem-table --debug
```

Example run:

```
Running benchmarks with the following options: GroupBy(GroupBy { query: 1, path: "/mnt/bigdata/h2oai/N_1e7_K_1e2_single.csv", debug: false })
Executing select id1, sum(v1) as v1 from x group by id1
+-------+--------+
| id1   | v1     |
+-------+--------+
| id063 | 199420 |
| id094 | 200127 |
| id044 | 198886 |
...
| id093 | 200132 |
| id003 | 199047 |
+-------+--------+

h2o groupby query 1 took 1669 ms
```

[1]: http://www.tpc.org/tpch/
[2]: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
