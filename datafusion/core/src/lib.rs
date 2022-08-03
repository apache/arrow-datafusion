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
#![warn(missing_docs, clippy::needless_borrow)]

//! [DataFusion](https://github.com/apache/arrow-datafusion)
//! is an extensible query execution framework that uses
//! [Apache Arrow](https://arrow.apache.org) as its in-memory format.
//!
//! DataFusion supports both an SQL and a DataFrame API for building logical query plans
//! as well as a query optimizer and execution engine capable of parallel execution
//! against partitioned data sources (CSV and Parquet) using threads.
//!
//! Below is an example of how to execute a query against data stored
//! in a CSV file using a [`DataFrame`](dataframe::DataFrame):
//!
//! ```rust
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use datafusion::arrow::record_batch::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//!
//! // create the dataframe
//! let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
//!
//! // create a plan
//! let df = df.filter(col("a").lt_eq(col("b")))?
//!            .aggregate(vec![col("a")], vec![min(col("b"))])?
//!            .limit(None, Some(100))?;
//!
//! // execute the plan
//! let results: Vec<RecordBatch> = df.collect().await?;
//!
//! // format the results
//! let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?
//!    .to_string();
//!
//! let expected = vec![
//!     "+---+----------------+",
//!     "| a | MIN(?table?.b) |",
//!     "+---+----------------+",
//!     "| 1 | 2              |",
//!     "+---+----------------+"
//! ];
//!
//! assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
//! # Ok(())
//! # }
//! ```
//!
//! and how to execute a query against a CSV using SQL:
//!
//! ```
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use datafusion::arrow::record_batch::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//!
//! ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await?;
//!
//! // create a plan
//! let df = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;
//!
//! // execute the plan
//! let results: Vec<RecordBatch> = df.collect().await?;
//!
//! // format the results
//! let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?
//!   .to_string();
//!
//! let expected = vec![
//!     "+---+----------------+",
//!     "| a | MIN(example.b) |",
//!     "+---+----------------+",
//!     "| 1 | 2              |",
//!     "+---+----------------+"
//! ];
//!
//! assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
//! # Ok(())
//! # }
//! ```
//!
//! ## Parse, Plan, Optimize, Execute
//!
//! DataFusion is a fully fledged query engine capable of performing complex operations.
//! Specifically, when DataFusion receives an SQL query, there are different steps
//! that it passes through until a result is obtained. Broadly, they are:
//!
//! 1. The string is parsed to an Abstract syntax tree (AST) using [sqlparser](https://docs.rs/sqlparser/latest/sqlparser/).
//! 2. The planner [`SqlToRel`](sql::planner::SqlToRel) converts logical expressions on the AST to logical expressions [`Expr`s](logical_plan::Expr).
//! 3. The planner [`SqlToRel`](sql::planner::SqlToRel) converts logical nodes on the AST to a [`LogicalPlan`](logical_plan::LogicalPlan).
//! 4. [`OptimizerRules`](optimizer::optimizer::OptimizerRule) are applied to the [`LogicalPlan`](logical_plan::LogicalPlan) to optimize it.
//! 5. The [`LogicalPlan`](logical_plan::LogicalPlan) is converted to an [`ExecutionPlan`](physical_plan::ExecutionPlan) by a [`PhysicalPlanner`](physical_plan::PhysicalPlanner)
//! 6. The [`ExecutionPlan`](physical_plan::ExecutionPlan) is executed against data through the [`SessionContext`](execution::context::SessionContext)
//!
//! With a [`DataFrame`](dataframe::DataFrame) API, steps 1-3 are not used as the DataFrame builds the [`LogicalPlan`](logical_plan::LogicalPlan) directly.
//!
//! Phases 1-5 are typically cheap when compared to phase 6, and thus DataFusion puts a
//! lot of effort to ensure that phase 6 runs efficiently and without errors.
//!
//! DataFusion's planning is divided in two main parts: logical planning and physical planning.
//!
//! ### Logical plan
//!
//! Logical planning yields [`logical plans`](logical_plan::LogicalPlan) and [`logical expressions`](logical_plan::Expr).
//! These are [`Schema`](arrow::datatypes::Schema)-aware traits that represent statements whose result is independent of how it should physically be executed.
//!
//! A [`LogicalPlan`](logical_plan::LogicalPlan) is a Directed Acyclic Graph (DAG) of other [`LogicalPlan`s](logical_plan::LogicalPlan) and each node contains logical expressions ([`Expr`s](logical_plan::Expr)).
//! All of these are located in [`logical_plan`](logical_plan).
//!
//! ### Physical plan
//!
//! A Physical plan ([`ExecutionPlan`](physical_plan::ExecutionPlan)) is a plan that can be executed against data.
//! Contrarily to a logical plan, the physical plan has concrete information about how the calculation
//! should be performed (e.g. what Rust functions are used) and how data should be loaded into memory.
//!
//! [`ExecutionPlan`](physical_plan::ExecutionPlan) uses the Arrow format as its in-memory representation of data, through the [arrow] crate.
//! We recommend going through [its documentation](arrow) for details on how the data is physically represented.
//!
//! A [`ExecutionPlan`](physical_plan::ExecutionPlan) is composed by nodes (implement the trait [`ExecutionPlan`](physical_plan::ExecutionPlan)),
//! and each node is composed by physical expressions ([`PhysicalExpr`](physical_plan::PhysicalExpr))
//! or aggregate expressions ([`AggregateExpr`](physical_plan::AggregateExpr)).
//! All of these are located in the module [`physical_plan`](physical_plan).
//!
//! Broadly speaking,
//!
//! * an [`ExecutionPlan`](physical_plan::ExecutionPlan) receives a partition number and asynchronously returns
//!   an iterator over [`RecordBatch`](arrow::record_batch::RecordBatch)
//!   (a node-specific struct that implements [`RecordBatchReader`](arrow::record_batch::RecordBatchReader))
//! * a [`PhysicalExpr`](physical_plan::PhysicalExpr) receives a [`RecordBatch`](arrow::record_batch::RecordBatch)
//!   and returns an [`Array`](arrow::array::Array)
//! * an [`AggregateExpr`](physical_plan::AggregateExpr) receives [`RecordBatch`es](arrow::record_batch::RecordBatch)
//!   and returns a [`RecordBatch`](arrow::record_batch::RecordBatch) of a single row(*)
//!
//! (*) Technically, it aggregates the results on each partition and then merges the results into a single partition.
//!
//! The following physical nodes are currently implemented:
//!
//! * Projection: [`ProjectionExec`](physical_plan::projection::ProjectionExec)
//! * Filter: [`FilterExec`](physical_plan::filter::FilterExec)
//! * Grouped and non-grouped aggregations: [`AggregateExec`](physical_plan::aggregates::AggregateExec)
//! * Sort: [`SortExec`](physical_plan::sorts::sort::SortExec)
//! * Coalesce partitions: [`CoalescePartitionsExec`](physical_plan::coalesce_partitions::CoalescePartitionsExec)
//! * Limit: [`LocalLimitExec`](physical_plan::limit::LocalLimitExec) and [`GlobalLimitExec`](physical_plan::limit::GlobalLimitExec)
//! * Scan a CSV: [`CsvExec`](physical_plan::file_format::CsvExec)
//! * Scan a Parquet: [`ParquetExec`](physical_plan::file_format::ParquetExec)
//! * Scan from memory: [`MemoryExec`](physical_plan::memory::MemoryExec)
//! * Explain the plan: [`ExplainExec`](physical_plan::explain::ExplainExec)
//!
//! ## Customize
//!
//! DataFusion allows users to
//! * extend the planner to use user-defined logical and physical nodes ([`QueryPlanner`](execution::context::QueryPlanner))
//! * declare and use user-defined scalar functions ([`ScalarUDF`](physical_plan::udf::ScalarUDF))
//! * declare and use user-defined aggregate functions ([`AggregateUDF`](physical_plan::udaf::AggregateUDF))
//!
//! you can find examples of each of them in examples section.
//!
//! ## Examples
//!
//! Examples are located in [datafusion-examples directory](https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples)
//!
//! Here's how to run them
//!
//! ```bash
//! git clone https://github.com/apache/arrow-datafusion
//! cd arrow-datafusion
//! # Download test data
//! git submodule update --init
//!
//! cargo run --example csv_sql
//!
//! cargo run --example parquet_sql
//!
//! cargo run --example dataframe
//!
//! cargo run --example dataframe_in_memory
//!
//! cargo run --example simple_udaf
//!
//! cargo run --example simple_udf
//! ```

/// DataFusion crate version
pub const DATAFUSION_VERSION: &str = env!("CARGO_PKG_VERSION");

extern crate sqlparser;

pub mod avro_to_arrow;
pub mod catalog;
pub mod config;
pub mod dataframe;
pub mod datasource;
pub mod error;
pub mod execution;
pub mod logical_plan;
pub mod physical_optimizer;
pub mod physical_plan;
pub mod prelude;
pub mod scalar;
#[cfg(feature = "scheduler")]
pub mod scheduler;
pub mod variable;

// re-export dependencies from arrow-rs to minimise version maintenance for crate users
pub use arrow;
pub use parquet;

// re-export DataFusion crates
pub use datafusion_common as common;
pub use datafusion_expr as logical_expr;
pub use datafusion_optimizer as optimizer;
pub use datafusion_physical_expr as physical_expr;
pub use datafusion_row as row;
pub use datafusion_sql as sql;

#[cfg(feature = "jit")]
pub use datafusion_jit as jit;

pub use common::from_slice;

#[cfg(test)]
pub mod test;
pub mod test_util;

#[cfg(doctest)]
doc_comment::doctest!("../../../README.md", readme_example_test);

#[cfg(doctest)]
doc_comment::doctest!(
    "../../../docs/source/user-guide/example-usage.md",
    user_guide_example_usage
);
