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

# Quarterly Roadmap

A quarterly roadmap will be published to give the DataFusion community visibility into the priorities of the projects contributors. This roadmap is not binding.

## 2022 Q2

### DataFusion Core

- IO Improvements
  - Reading, registering, and writing more file formats from both DataFrame API and SQL
  - Additional options for IO including partitioning and metadata support
- Work Scheduling
  - Improve predictability, observability and performance of IO and CPU-bound work
  - Develop a more explicit story for managing parallelism during plan execution
- Memory Management
  - Add more operators for memory limited execution
- Performance
  - Incorporate row-format into operators such as aggregate
  - Add row-format benchmarks
  - Explore JIT-compiling complex expressions
  - Explore LLVM for JIT, with inline Rust functions as the primary goal
  - Improve performance of Sort and Merge using Row Format / JIT expressions
- Documentation
  - General improvements to DataFusion website
  - Publish design documents
- Streaming
  - Create `StreamProvider` trait

### Ballista

- Make production ready
  - Shuffle file cleanup
  - Fill functional gaps between DataFusion and Ballista
  - Improve task scheduling and data exchange efficiency
  - Better error handling
    - Task failure
    - Executor lost
    - Schedule restart
  - Improve monitoring and logging
  - Auto scaling support
- Support for multi-scheduler deployments. Initially for resiliency and fault tolerance but ultimately to support sharding for scalability and more efficient caching.
- Executor deployment grouping based on resource allocation

### Extensions ([datafusion-contrib](https://github.com/datafusion-contrib]))

#### [DataFusion-Python](https://github.com/datafusion-contrib/datafusion-python)

- Add missing functionality to DataFrame and SessionContext
- Improve documentation

#### [DataFusion-S3](https://github.com/datafusion-contrib/datafusion-objectstore-s3)

- Create Python bindings to use with datafusion-python

#### [DataFusion-Tui](https://github.com/datafusion-contrib/datafusion-tui)

- Create multiple SQL editors
- Expose more Context and query metadata
- Support new data sources
  - BigTable, HDFS, HTTP APIs

#### [DataFusion-BigTable](https://github.com/datafusion-contrib/datafusion-bigtable)

- Python binding to use with datafusion-python
- Timestamp range predicate pushdown
- Multi-threaded partition aware execution
- Production ready Rust SDK

#### [DataFusion-Streams](https://github.com/datafusion-contrib/datafusion-streams)

- Create experimental implementation of `StreamProvider` trait
