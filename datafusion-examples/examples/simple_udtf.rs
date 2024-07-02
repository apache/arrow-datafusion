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

use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{plan_err, ScalarValue};
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{Expr, TableType};
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;
use datafusion_common::logical_type::schema::LogicalSchemaRef;
// To define your own table function, you only need to do the following 3 things:
// 1. Implement your own [`TableProvider`]
// 2. Implement your own [`TableFunctionImpl`] and return your [`TableProvider`]
// 3. Register the function using [`SessionContext::register_udtf`]

/// This example demonstrates how to register a TableFunction
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // register the table function that will be called in SQL statements by `read_csv`
    ctx.register_udtf("read_csv", Arc::new(LocalCsvTableFunc {}));

    let testdata = datafusion::test_util::arrow_test_data();
    let csv_file = format!("{testdata}/csv/aggregate_test_100.csv");

    // Pass 2 arguments, read csv with at most 2 rows (simplify logic makes 1+1 --> 2)
    let df = ctx
        .sql(format!("SELECT * FROM read_csv('{csv_file}', 1 + 1);").as_str())
        .await?;
    df.show().await?;

    // just run, return all rows
    let df = ctx
        .sql(format!("SELECT * FROM read_csv('{csv_file}');").as_str())
        .await?;
    df.show().await?;

    Ok(())
}

/// Table Function that mimics the [`read_csv`] function in DuckDB.
///
/// Usage: `read_csv(filename, [limit])`
///
/// [`read_csv`]: https://duckdb.org/docs/data/csv/overview.html
struct LocalCsvTable {
    schema: SchemaRef,
    limit: Option<usize>,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for LocalCsvTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.schema.clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batches = if let Some(max_return_lines) = self.limit {
            // get max return rows from self.batches
            let mut batches = vec![];
            let mut lines = 0;
            for batch in &self.batches {
                let batch_lines = batch.num_rows();
                if lines + batch_lines > max_return_lines {
                    let batch_lines = max_return_lines - lines;
                    batches.push(batch.slice(0, batch_lines));
                    break;
                } else {
                    batches.push(batch.clone());
                    lines += batch_lines;
                }
            }
            batches
        } else {
            self.batches.clone()
        };
        Ok(Arc::new(MemoryExec::try_new(
            &[batches],
            self.schema.clone(),
            projection.cloned(),
        )?))
    }
}

struct LocalCsvTableFunc {}

impl TableFunctionImpl for LocalCsvTableFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(ref path)))) = exprs.first() else {
            return plan_err!("read_csv requires at least one string argument");
        };

        let limit = exprs
            .get(1)
            .map(|expr| {
                // try to simpify the expression, so 1+2 becomes 3, for example
                let execution_props = ExecutionProps::new();
                let info = SimplifyContext::new(&execution_props);
                let expr = ExprSimplifier::new(info).simplify(expr.clone())?;

                if let Expr::Literal(ScalarValue::Int64(Some(limit))) = expr {
                    Ok(limit as usize)
                } else {
                    plan_err!("Limit must be an integer")
                }
            })
            .transpose()?;

        let (schema, batches) = read_csv_batches(path)?;

        let table = LocalCsvTable {
            schema,
            limit,
            batches,
        };
        Ok(Arc::new(table))
    }
}

fn read_csv_batches(csv_path: impl AsRef<Path>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut file = File::open(csv_path)?;
    let (schema, _) = Format::default().infer_schema(&mut file, None)?;
    file.rewind()?;

    let reader = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_header(true)
        .build(file)?;
    let mut batches = vec![];
    for bacth in reader {
        batches.push(bacth?);
    }
    let schema = Arc::new(schema);
    Ok((schema, batches))
}
