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

use crate::AccessLogOpt;
use crate::BenchmarkRun;
use crate::CommonOpt;
use datafusion::arrow::util::pretty;
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::test_util::parquet::TestParquetFile;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;

/// Test performance of sorting large datasets
///
/// This test sorts a a synthetic dataset generated during the
/// benchmark execution, designed to simulate sorting web server
/// access logs. Such sorting is often done during data transformation
/// steps.
///
/// The tests sort the entire dataset using several different sort
/// orders.
///
/// Example:
///
/// dfbench sort --path ./data --scale-factor 1.0
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Create data files
    #[structopt(flatten)]
    access_log: AccessLogOpt,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let test_file = self.access_log.build()?;

        use datafusion::physical_expr::expressions::col;
        let mut rundata = BenchmarkRun::new();
        let schema = test_file.schema();
        let sort_cases = vec![
            (
                "sort utf8",
                vec![PhysicalSortExpr {
                    expr: col("request_method", &schema)?,
                    options: Default::default(),
                }],
            ),
            (
                "sort int",
                vec![PhysicalSortExpr {
                    expr: col("request_bytes", &schema)?,
                    options: Default::default(),
                }],
            ),
            (
                "sort decimal",
                vec![
                    // sort decimal
                    PhysicalSortExpr {
                        expr: col("decimal_price", &schema)?,
                        options: Default::default(),
                    },
                ],
            ),
            (
                "sort integer tuple",
                vec![
                    PhysicalSortExpr {
                        expr: col("request_bytes", &schema)?,
                        options: Default::default(),
                    },
                    PhysicalSortExpr {
                        expr: col("response_bytes", &schema)?,
                        options: Default::default(),
                    },
                ],
            ),
            (
                "sort utf8 tuple",
                vec![
                    // sort utf8 tuple
                    PhysicalSortExpr {
                        expr: col("service", &schema)?,
                        options: Default::default(),
                    },
                    PhysicalSortExpr {
                        expr: col("host", &schema)?,
                        options: Default::default(),
                    },
                    PhysicalSortExpr {
                        expr: col("pod", &schema)?,
                        options: Default::default(),
                    },
                    PhysicalSortExpr {
                        expr: col("image", &schema)?,
                        options: Default::default(),
                    },
                ],
            ),
            (
                "sort mixed tuple",
                vec![
                    PhysicalSortExpr {
                        expr: col("service", &schema)?,
                        options: Default::default(),
                    },
                    PhysicalSortExpr {
                        expr: col("request_bytes", &schema)?,
                        options: Default::default(),
                    },
                    PhysicalSortExpr {
                        expr: col("decimal_price", &schema)?,
                        options: Default::default(),
                    },
                ],
            ),
        ];
        for (title, expr) in sort_cases {
            println!("Executing '{title}' (sorting by: {expr:?})");
            rundata.start_new_case(title);
            for i in 0..self.common.iterations {
                let config = SessionConfig::new().with_target_partitions(
                    self.common.partitions.unwrap_or(num_cpus::get()),
                );
                let ctx = SessionContext::new_with_config(config);
                let (rows, elapsed) =
                    exec_sort(&ctx, &expr, &test_file, self.common.debug).await?;
                let ms = elapsed.as_secs_f64() * 1000.0;
                println!("Iteration {i} finished in {ms} ms");
                rundata.write_iter(elapsed, rows);
            }
            println!("\n");
        }
        if let Some(path) = &self.output_path {
            std::fs::write(path, rundata.to_json())?;
        }
        Ok(())
    }
}

async fn exec_sort(
    ctx: &SessionContext,
    expr: &[PhysicalSortExpr],
    test_file: &TestParquetFile,
    debug: bool,
) -> Result<(usize, std::time::Duration)> {
    let start = Instant::now();
    let scan = test_file.create_scan(None).await?;
    let exec = Arc::new(SortExec::new(expr.to_owned(), scan));
    let task_ctx = ctx.task_ctx();
    let result = collect(exec, task_ctx).await?;
    let elapsed = start.elapsed();
    if debug {
        pretty::print_batches(&result)?;
    }
    let rows = result.iter().map(|b| b.num_rows()).sum();
    Ok((rows, elapsed))
}
