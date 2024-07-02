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

use arrow::array::Int64Array;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::{assert_batches_eq, DFSchema, ScalarValue};
use datafusion_expr::{EmptyRelation, Expr, LogicalPlan, Projection, TableType};
use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;
use datafusion_common::logical_type::schema::LogicalSchemaRef;

/// test simple udtf with define read_csv with parameters
#[tokio::test]
async fn test_simple_read_csv_udtf() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_udtf("read_csv", Arc::new(SimpleCsvTableFunc {}));

    let csv_file = "tests/tpch-csv/nation.csv";
    // read csv with at most 5 rows
    let rbs = ctx
        .sql(format!("SELECT * FROM read_csv('{csv_file}', 5);").as_str())
        .await?
        .collect()
        .await?;

    let excepted = [
        "+-------------+-----------+-------------+-------------------------------------------------------------------------------------------------------------+",
        "| n_nationkey | n_name    | n_regionkey | n_comment                                                                                                   |",
        "+-------------+-----------+-------------+-------------------------------------------------------------------------------------------------------------+",
        "| 1           | ARGENTINA | 1           | al foxes promise slyly according to the regular accounts. bold requests alon                                |",
        "| 2           | BRAZIL    | 1           | y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special  |",
        "| 3           | CANADA    | 1           | eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold       |",
        "| 4           | EGYPT     | 4           | y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d         |",
        "| 5           | ETHIOPIA  | 0           | ven packages wake quickly. regu                                                                             |",
        "+-------------+-----------+-------------+-------------------------------------------------------------------------------------------------------------+",    ];
    assert_batches_eq!(excepted, &rbs);

    // just run, return all rows
    let rbs = ctx
        .sql(format!("SELECT * FROM read_csv('{csv_file}');").as_str())
        .await?
        .collect()
        .await?;
    let excepted = [
        "+-------------+-----------+-------------+--------------------------------------------------------------------------------------------------------------------+", 
        "| n_nationkey | n_name    | n_regionkey | n_comment                                                                                                          |",
        "+-------------+-----------+-------------+--------------------------------------------------------------------------------------------------------------------+",
        "| 1           | ARGENTINA | 1           | al foxes promise slyly according to the regular accounts. bold requests alon                                       |",
        "| 2           | BRAZIL    | 1           | y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special         |", 
        "| 3           | CANADA    | 1           | eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold              |", 
        "| 4           | EGYPT     | 4           | y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d                |", 
        "| 5           | ETHIOPIA  | 0           | ven packages wake quickly. regu                                                                                    |", 
        "| 6           | FRANCE    | 3           | refully final requests. regular, ironi                                                                             |", 
        "| 7           | GERMANY   | 3           | l platelets. regular accounts x-ray: unusual, regular acco                                                         |", 
        "| 8           | INDIA     | 2           | ss excuses cajole slyly across the packages. deposits print aroun                                                  |", 
        "| 9           | INDONESIA | 2           |  slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull |", 
        "| 10          | IRAN      | 4           | efully alongside of the slyly final dependencies.                                                                  |",
        "+-------------+-----------+-------------+--------------------------------------------------------------------------------------------------------------------+"
    ];
    assert_batches_eq!(excepted, &rbs);

    Ok(())
}

#[tokio::test]
async fn test_deregister_udtf() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_udtf("read_csv", Arc::new(SimpleCsvTableFunc {}));

    assert!(ctx.state().table_functions().contains_key("read_csv"));

    ctx.deregister_udtf("read_csv");

    assert!(!ctx.state().table_functions().contains_key("read_csv"));

    Ok(())
}

struct SimpleCsvTable {
    schema: SchemaRef,
    exprs: Vec<Expr>,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for SimpleCsvTable {
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batches = if !self.exprs.is_empty() {
            let max_return_lines = self.interpreter_expr(state).await?;
            // get max return rows from self.batches
            let mut batches = vec![];
            let mut lines = 0;
            for batch in &self.batches {
                let batch_lines = batch.num_rows();
                if lines + batch_lines > max_return_lines as usize {
                    let batch_lines = max_return_lines as usize - lines;
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

impl SimpleCsvTable {
    async fn interpreter_expr(&self, state: &SessionState) -> Result<i64> {
        use datafusion::logical_expr::expr_rewriter::normalize_col;
        use datafusion::logical_expr::utils::columnize_expr;
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        });
        let logical_plan = Projection::try_new(
            vec![columnize_expr(
                normalize_col(self.exprs[0].clone(), &plan)?,
                &plan,
            )?],
            Arc::new(plan),
        )
        .map(LogicalPlan::Projection)?;
        let rbs = collect(
            state.create_physical_plan(&logical_plan).await?,
            Arc::new(TaskContext::from(state)),
        )
        .await?;
        let limit = rbs[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        Ok(limit)
    }
}

struct SimpleCsvTableFunc {}

impl TableFunctionImpl for SimpleCsvTableFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let mut new_exprs = vec![];
        let mut filepath = String::new();
        for expr in exprs {
            match expr {
                Expr::Literal(ScalarValue::Utf8(Some(ref path))) => {
                    filepath.clone_from(path);
                }
                expr => new_exprs.push(expr.clone()),
            }
        }
        let (schema, batches) = read_csv_batches(filepath)?;
        let table = SimpleCsvTable {
            schema,
            exprs: new_exprs.clone(),
            batches,
        };
        Ok(Arc::new(table))
    }
}

fn read_csv_batches(csv_path: impl AsRef<Path>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut file = File::open(csv_path)?;
    let (schema, _) = Format::default()
        .with_header(true)
        .infer_schema(&mut file, None)?;
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
