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

# What is DataFusion?

DataFusion is an extensible query planning, optimization, and execution framework, written in
Rust, that uses [Apache Arrow's](https://arrow.apache.org) memory model and computational kernels.
It is designed to run within a single process, using threads for parallel query execution.

DataFusion supports both an SQL and a DataFrame API for building
logical query plans as well as a query optimizer and execution engine
capable of parallel execution against partitioned data sources (CSV
and Parquet).

TODO: add a diagram here of where DataFusion fits into the ecosystem/stack

## Features

- Query execution engine with support for **SQL** and **DataFrame** APIs
- Fully functional SQL, including support for CTEs, window functions, UDFs, UDAFs
- Native support for **Parquet**, **CSV**, **JSON**, and **Avro** file formats. Custom
  file formats can be supported by implementing your own data source via a `TableProvider` trait.
  - Supports partitioned files, in Hive format or otherwise
- Supports popular object stores, including **AWS S3**, **Azure Blob
  Storage**, and **Google Cloud Storage**. There are extension points for implementing
  custom object stores.

## Use Cases

DataFusion is modular in design with many extension points and can be
used without modification as an embedded query engine or can provide
a foundation for building new systems. Here are some example use cases:

- DataFusion can be used as an SQL query planner and query optimizer, providing
  optimized logical plans that can then be mapped to other execution engines.
- DataFusion is used to create modern, fast and efficient data
  pipelines, ETL processes, and database systems, which need the
  performance of Rust and Apache Arrow and want to provide their users
  the convenience of an SQL interface or a DataFrame API.

## Why DataFusion?

- _High Performance_: Leveraging Rust and Arrow's memory model and optimized computational kernels, DataFusion achieves very high performance
- _Easy to Connect_: Being part of the Apache Arrow ecosystem (Arrow, Parquet, and Flight), DataFusion works well with the rest of the big data ecosystem
- _Easy to Embed_: Allowing extension at almost any point in its design, DataFusion can be tailored for your specific use case
- _High Quality_: Extensively tested, both by itself and with the rest of the Arrow ecosystem, DataFusion can be used as the foundation for production systems

## Getting Started
