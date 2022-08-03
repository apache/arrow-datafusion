<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Roadmap

This document describes high level goals of the DataFusion and
Ballista development community. It is not meant to restrict
possibilities, but rather help newcomers understand the broader
context of where the community is headed, and inspire
additional contributions.

DataFusion and Ballista are part of the [Apache
Arrow](https://arrow.apache.org/) project and governed by the Apache
Software Foundation governance model. These projects are entirely
driven by volunteers, and we welcome contributions for items not on
this roadmap. However, before submitting a large PR, we strongly
suggest you start a conversation using a github issue or the
dev@arrow.apache.org mailing list to make review efficient and avoid
surprises.

## DataFusion

DataFusion's goal is to become the embedded query engine of choice
for new analytic applications, by leveraging the unique features of
[Rust](https://www.rust-lang.org/) and [Apache Arrow](https://arrow.apache.org/)
to provide:

1. Best-in-class single node query performance
2. A Declarative SQL query interface compatible with PostgreSQL
3. A Dataframe API, similar to those offered by Pandas and Spark
4. A Procedural API for programmatically creating and running execution plans
5. High performance, data race free, ergonomic extensibility points at at every layer

### Additional SQL Language Features

- Decimal Support [#122](https://github.com/apache/arrow-datafusion/issues/122)
- Complete support list on [status](https://github.com/apache/arrow-datafusion/blob/master/README.md#status)
- Timestamp Arithmetic [#194](https://github.com/apache/arrow-datafusion/issues/194)
- SQL Parser extension point [#533](https://github.com/apache/arrow-datafusion/issues/533)
- Support for nested structures (fields, lists, structs) [#119](https://github.com/apache/arrow-datafusion/issues/119)
- Run all queries from the TPCH benchmark (see [milestone](https://github.com/apache/arrow-datafusion/milestone/2) for more details)

### Query Optimizer

- More sophisticated cost based optimizer for join ordering
- Implement advanced query optimization framework (Tokomak) #440
- Finer optimizations for group by and aggregate functions

### Datasources

- Better support for reading data from remote filesystems (e.g. S3) without caching it locally [#907](https://github.com/apache/arrow-datafusion/issues/907) [#1060](https://github.com/apache/arrow-datafusion/issues/1060)
- Improve performances of file format datasources (parallelize file listings, async Arrow readers, file chunk prefetching capability...)

### Runtime / Infrastructure

- Migrate to some sort of arrow2 based implementation (see [milestone](https://github.com/apache/arrow-datafusion/milestone/3) for more details)
- Add DataFusion to h2oai/db-benchmark [147](https://github.com/apache/arrow-datafusion/issues/147)
- Improve build time [348](https://github.com/apache/arrow-datafusion/issues/348)

### Resource Management

- Finer grain control and limit of runtime memory [#587](https://github.com/apache/arrow-datafusion/issues/587) and CPU usage [#54](https://github.com/apache/arrow-datafusion/issues/64)

### Python Interface

TBD

### DataFusion CLI (`datafusion-cli`)

Note: There are some additional thoughts on a datafusion-cli vision on [#1096](https://github.com/apache/arrow-datafusion/issues/1096#issuecomment-939418770).

- Better abstraction between REPL parsing and queries so that commands are separated and handled correctly
- Connect to the `Statistics` subsystem and have the cli print out more stats for query debugging, etc.
- Improved error handling for interactive use and shell scripting usage
- publishing to apt, brew, and possible NuGet registry so that people can use it more easily
- adopt a shorter name, like dfcli?

## Ballista

Ballista is a distributed compute platform based on Apache Arrow and DataFusion. It provides a query scheduler that
breaks a physical plan into stages and tasks and then schedules tasks for execution across the available executors
in the cluster.

Having Ballista as part of the DataFusion codebase helps ensure that DataFusion remains suitable for distributed
compute. For example, it helps ensure that physical query plans can be serialized to protobuf format and that they
remain language-agnostic so that executors can be built in languages other than Rust.

### Ballista Roadmap

### Move query scheduler into DataFusion

The Ballista scheduler has some advantages over DataFusion query execution because it doesn't try to eagerly execute
the entire query at once but breaks it down into a directionally-acyclic graph (DAG) of stages and executes a
configurable number of stages and tasks concurrently. It should be possible to push some of this logic down to
DataFusion so that the same scheduler can be used to scale across cores in-process and across nodes in a cluster.

### Implement execution-time cost-based optimizations based on statistics

After the execution of a query stage, accurate statistics are available for the resulting data. These statistics
could be leveraged by the scheduler to optimize the query during execution. For example, when performing a hash join
it is desirable to load the smaller side of the join into memory and in some cases we cannot predict which side will
be smaller until execution time.
