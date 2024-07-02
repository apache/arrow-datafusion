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

//! Utility functions to make testing DataFusion based crates easier

#[cfg(feature = "parquet")]
pub mod parquet;

use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::dataframe::DataFrame;
use crate::datasource::provider::TableProviderFactory;
use crate::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
use crate::datasource::{empty::EmptyTable, provider_as_source, TableProvider};
use crate::error::Result;
use crate::execution::context::{SessionState, TaskContext};
use crate::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use crate::prelude::{CsvReadOptions, SessionContext};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::TableReference;
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use futures::Stream;
use tempfile::TempDir;
use datafusion_common::logical_type::schema::LogicalSchemaRef;
// backwards compatibility
#[cfg(feature = "parquet")]
pub use datafusion_common::test_util::parquet_test_data;
pub use datafusion_common::test_util::{arrow_test_data, get_data_dir};

/// Scan an empty data source, mainly used in tests
pub fn scan_empty(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema));
    let name = TableReference::bare(name.unwrap_or(UNNAMED_TABLE));
    LogicalPlanBuilder::scan(name, provider_as_source(provider), projection)
}

/// Scan an empty data source with configured partition, mainly used in tests.
pub fn scan_empty_with_partitions(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
    partitions: usize,
) -> Result<LogicalPlanBuilder> {
    let table_schema = Arc::new(table_schema.clone());
    let provider = Arc::new(EmptyTable::new(table_schema).with_partitions(partitions));
    let name = TableReference::bare(name.unwrap_or(UNNAMED_TABLE));
    LogicalPlanBuilder::scan(name, provider_as_source(provider), projection)
}

/// Get the schema for the aggregate_test_* csv files
pub fn aggr_test_schema() -> SchemaRef {
    let mut f1 = Field::new("c1", DataType::Utf8, false);
    f1.set_metadata(HashMap::from_iter(vec![("testing".into(), "test".into())]));
    let schema = Schema::new(vec![
        f1,
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]);

    Arc::new(schema)
}

/// Register session context for the aggregate_test_100.csv file
pub async fn register_aggregate_csv(
    ctx: &mut SessionContext,
    table_name: &str,
) -> Result<()> {
    let schema = aggr_test_schema();
    let testdata = arrow_test_data();
    ctx.register_csv(
        table_name,
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new().schema(schema.as_ref()),
    )
    .await?;
    Ok(())
}

/// Create a table from the aggregate_test_100.csv file with the specified name
pub async fn test_table_with_name(name: &str) -> Result<DataFrame> {
    let mut ctx = SessionContext::new();
    register_aggregate_csv(&mut ctx, name).await?;
    ctx.table(name).await
}

/// Create a table from the aggregate_test_100.csv file with the name "aggregate_test_100"
pub async fn test_table() -> Result<DataFrame> {
    test_table_with_name("aggregate_test_100").await
}

/// Execute SQL and return results
pub async fn plan_and_collect(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Generate CSV partitions within the supplied directory
pub fn populate_csv_partitions(
    tmp_dir: &TempDir,
    partition_count: usize,
    file_extension: &str,
) -> Result<SchemaRef> {
    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes())?;
        }
    }

    Ok(schema)
}

/// TableFactory for tests
pub struct TestTableFactory {}

#[async_trait]
impl TableProviderFactory for TestTableFactory {
    async fn create(
        &self,
        _: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(TestTableProvider {
            url: cmd.location.to_string(),
            schema: Arc::new(cmd.schema.as_ref().into()),
        }))
    }
}

/// TableProvider for testing purposes
pub struct TestTableProvider {
    /// URL of table files or folder
    pub url: String,
    /// test table schema
    pub schema: SchemaRef,
}

impl TestTableProvider {}

#[async_trait]
impl TableProvider for TestTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.schema.clone().into())
    }

    fn table_type(&self) -> TableType {
        unimplemented!("TestTableProvider is a stub for testing.")
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("TestTableProvider is a stub for testing.")
    }
}

/// A mock execution plan that simply returns the provided data source characteristic
#[derive(Debug, Clone)]
pub struct UnboundedExec {
    batch_produce: Option<usize>,
    batch: RecordBatch,
    cache: PlanProperties,
}
impl UnboundedExec {
    /// Create new exec that clones the given record batch to its output.
    ///
    /// Set `batch_produce` to `Some(n)` to emit exactly `n` batches per partition.
    pub fn new(
        batch_produce: Option<usize>,
        batch: RecordBatch,
        partitions: usize,
    ) -> Self {
        let cache = Self::compute_properties(batch.schema(), batch_produce, partitions);
        Self {
            batch_produce,
            batch,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        batch_produce: Option<usize>,
        n_partitions: usize,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        let mode = if batch_produce.is_none() {
            ExecutionMode::Unbounded
        } else {
            ExecutionMode::Bounded
        };
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(n_partitions),
            mode,
        )
    }
}

impl DisplayAs for UnboundedExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "UnboundableExec: unbounded={}",
                    self.batch_produce.is_none(),
                )
            }
        }
    }
}

impl ExecutionPlan for UnboundedExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(UnboundedStream {
            batch_produce: self.batch_produce,
            count: 0,
            batch: self.batch.clone(),
        }))
    }
}

#[derive(Debug)]
struct UnboundedStream {
    batch_produce: Option<usize>,
    count: usize,
    batch: RecordBatch,
}

impl Stream for UnboundedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(val) = self.batch_produce {
            if val <= self.count {
                return Poll::Ready(None);
            }
        }
        self.count += 1;
        Poll::Ready(Some(Ok(self.batch.clone())))
    }
}

impl RecordBatchStream for UnboundedStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

/// This function creates an unbounded sorted file for testing purposes.
pub fn register_unbounded_file_with_ordering(
    ctx: &SessionContext,
    schema: SchemaRef,
    file_path: &Path,
    table_name: &str,
    file_sort_order: Vec<Vec<Expr>>,
) -> Result<()> {
    let source = FileStreamProvider::new_file(schema, file_path.into());
    let config = StreamConfig::new(Arc::new(source)).with_order(file_sort_order);

    // Register table:
    ctx.register_table(table_name, Arc::new(StreamTable::new(Arc::new(config))))?;
    Ok(())
}
