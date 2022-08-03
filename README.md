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

# DataFusion

<img src="docs/source/_static/images/DataFusion-Logo-Background-White.svg" width="256" alt="logo"/>

DataFusion is an extensible query planning, optimization, and execution framework, written in
Rust, that uses [Apache Arrow](https://arrow.apache.org) as its
in-memory format.

## Features

- SQL query planner with support for multiple SQL dialects
- DataFrame API
- Parquet, CSV, JSON, and Avro file formats are supported natively. Custom
  file formats can be supported by implementing a `TableProvider` trait.
- Supports popular object stores, including AWS S3, Azure Blob
  Storage, and Google Cloud Storage. There are extension points for implementing
  custom object stores.

## Use Cases

DataFusion is modular in design with many extension points and can be
used without modification as an embedded query engine and can also provide
a foundation for building new systems. Here are some example use cases:

- DataFusion can be used as a SQL query planner and query optimizer, providing
  optimized logical plans that can then be mapped to other execution engines.
- DataFusion is used to create modern, fast and efficient data
  pipelines, ETL processes, and database systems, which need the
  performance of Rust and Apache Arrow and want to provide their users
  the convenience of an SQL interface or a DataFrame API.

## Why DataFusion?

- _High Performance_: Leveraging Rust and Arrow's memory model, DataFusion achieves very high performance
- _Easy to Connect_: Being part of the Apache Arrow ecosystem (Arrow, Parquet and Flight), DataFusion works well with the rest of the big data ecosystem
- _Easy to Embed_: Allowing extension at almost any point in its design, DataFusion can be tailored for your specific use case
- _High Quality_: Extensively tested, both by itself and with the rest of the Arrow ecosystem, DataFusion can be used as the foundation for production systems.

## DataFusion Community Extensions

There are a number of community projects that extend DataFusion or provide integrations with other systems.

### Language Bindings

- [datafusion-c](https://github.com/datafusion-contrib/datafusion-c)
- [datafusion-python](https://github.com/apache/arrow-datafusion-python)
- [datafusion-ruby](https://github.com/datafusion-contrib/datafusion-ruby)
- [datafusion-java](https://github.com/datafusion-contrib/datafusion-java)

### Integrations

- [datafusion-bigtable](https://github.com/datafusion-contrib/datafusion-bigtable)
- [datafusion-catalogprovider-glue](https://github.com/datafusion-contrib/datafusion-catalogprovider-glue)
- [datafusion-substrait](https://github.com/datafusion-contrib/datafusion-substrait)

## Known Uses

Here are some of the projects known to use DataFusion:

- [Ballista](https://github.com/apache/arrow-ballista) Distributed SQL Query Engine
- [Blaze](https://github.com/blaze-init/blaze) Spark accelerator with DataFusion at its core
- [Cloudfuse Buzz](https://github.com/cloudfuse-io/buzz-rust)
- [CnosDB](https://github.com/cnosdb/cnosdb) Open Source Distributed Time Series Database
- [Cube Store](https://github.com/cube-js/cube.js/tree/master/rust)
- [datafusion-tui](https://github.com/datafusion-contrib/datafusion-tui) Text UI for DataFusion
- [delta-rs](https://github.com/delta-io/delta-rs) Native Rust implementation of Delta Lake
- [Flock](https://github.com/flock-lab/flock)
- [InfluxDB IOx](https://github.com/influxdata/influxdb_iox) Time Series Database
- [qv](https://github.com/timvw/qv) Quickly view your data
- [ROAPI](https://github.com/roapi/roapi)
- [Tensorbase](https://github.com/tensorbase/tensorbase)
- [VegaFusion](https://vegafusion.io/) Server-side acceleration for the [Vega](https://vega.github.io/) visualization grammar

(if you know of another project, please submit a PR to add a link!)

## Example Usage

Please see [example usage](https://arrow.apache.org/datafusion/user-guide/example-usage.html) to find how to use DataFusion.

## Roadmap

Please see [Roadmap](docs/source/specification/roadmap.md) for information of where the project is headed.

## Architecture Overview

There is no formal document describing DataFusion's architecture yet, but the following presentations offer a good overview of its different components and how they interact together.

- (July 2022): DataFusion and Arrow: Supercharge Your Data Analytical Tool with a Rusty Query Engine: [recording](https://www.youtube.com/watch?v=Rii1VTn3seQ) and [slides](https://docs.google.com/presentation/d/1q1bPibvu64k2b7LPi7Yyb0k3gA1BiUYiUbEklqW1Ckc/view#slide=id.g11054eeab4c_0_1165)
- (March 2021): The DataFusion architecture is described in _Query Engine Design and the Rust-Based DataFusion in Apache Arrow_: [recording](https://www.youtube.com/watch?v=K6eCAVEk4kU) (DataFusion content starts [~ 15 minutes in](https://www.youtube.com/watch?v=K6eCAVEk4kU&t=875s)) and [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)
- (February 2021): How DataFusion is used within the Ballista Project is described in \*Ballista: Distributed Compute with Rust and Apache Arrow: [recording](https://www.youtube.com/watch?v=ZZHQaOap9pQ)

## User's guide

Please see [User Guide](https://arrow.apache.org/datafusion/) for more information about DataFusion.

## Contribution Guide

Please see [Contribution Guide](CONTRIBUTING.md) for information about contributing to DataFusion.
