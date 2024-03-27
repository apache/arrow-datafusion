// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DataFusion benchmark runner
use datafusion::error::Result;

use structopt::StructOpt;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use datafusion_benchmarks::{access_log_gen, clickbench, parquet_filter, sort, tpch};

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Tpch(tpch::RunOpt),
    TpchConvert(tpch::ConvertOpt),
    Clickbench(clickbench::RunOpt),
    ParquetFilter(parquet_filter::RunOpt),
    AccessLogGen(access_log_gen::RunOpt),
    Sort(sort::RunOpt),
}

// Main benchmark runner entrypoint
#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Tpch(opt) => opt.run().await,
        Options::TpchConvert(opt) => opt.run().await,
        Options::Clickbench(opt) => opt.run().await,
        Options::ParquetFilter(opt) => opt.run().await,
        Options::AccessLogGen(opt) => opt.run().await,
        Options::Sort(opt) => opt.run().await,
    }
}
