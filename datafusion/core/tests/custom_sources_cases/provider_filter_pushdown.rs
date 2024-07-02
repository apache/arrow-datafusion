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

use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{Int32Builder, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::provider::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{internal_err, not_impl_err};
use datafusion_expr::expr::{BinaryExpr, Cast};
use datafusion_functions_aggregate::expr_fn::count;
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use datafusion_common::logical_type::schema::LogicalSchemaRef;

fn create_batch(value: i32, num_rows: usize) -> Result<RecordBatch> {
    let mut builder = Int32Builder::with_capacity(num_rows);
    for _ in 0..num_rows {
        builder.append_value(value);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(builder.finish())],
    )?)
}

#[derive(Debug)]
struct CustomPlan {
    batches: Vec<RecordBatch>,
    cache: PlanProperties,
}

impl CustomPlan {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let cache = Self::compute_properties(schema);
        Self { batches, cache }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for CustomPlan {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CustomPlan: batch_size={}", self.batches.len(),)
            }
        }
    }
}

impl ExecutionPlan for CustomPlan {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // CustomPlan has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::iter(self.batches.clone().into_iter().map(Ok)),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        // here we could provide more accurate statistics
        // but we want to test the filter pushdown not the CBOs
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

#[derive(Clone)]
struct CustomProvider {
    zero_batch: RecordBatch,
    one_batch: RecordBatch,
}

#[async_trait]
impl TableProvider for CustomProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.zero_batch.schema().clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let empty = Vec::new();
        let projection = projection.unwrap_or(&empty);
        match &filters[0] {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => {
                let int_value = match &**right {
                    Expr::Literal(ScalarValue::Int8(Some(i))) => *i as i64,
                    Expr::Literal(ScalarValue::Int16(Some(i))) => *i as i64,
                    Expr::Literal(ScalarValue::Int32(Some(i))) => *i as i64,
                    Expr::Literal(ScalarValue::Int64(Some(i))) => *i,
                    Expr::Cast(Cast { expr, data_type: _ }) => match expr.deref() {
                        Expr::Literal(lit_value) => match lit_value {
                            ScalarValue::Int8(Some(v)) => *v as i64,
                            ScalarValue::Int16(Some(v)) => *v as i64,
                            ScalarValue::Int32(Some(v)) => *v as i64,
                            ScalarValue::Int64(Some(v)) => *v,
                            other_value => {
                                return not_impl_err!(
                                    "Do not support value {other_value:?}"
                                );
                            }
                        },
                        other_expr => {
                            return not_impl_err!("Do not support expr {other_expr:?}");
                        }
                    },
                    other_expr => {
                        return not_impl_err!("Do not support expr {other_expr:?}");
                    }
                };

                Ok(Arc::new(CustomPlan::new(
                    match projection.is_empty() {
                        true => Arc::new(Schema::empty()),
                        false => self.zero_batch.schema(),
                    },
                    match int_value {
                        0 => vec![self.zero_batch.clone()],
                        1 => vec![self.one_batch.clone()],
                        _ => vec![],
                    },
                )))
            }
            _ => Ok(Arc::new(CustomPlan::new(
                match projection.is_empty() {
                    true => Arc::new(Schema::empty()),
                    false => self.zero_batch.schema(),
                },
                vec![],
            ))),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

async fn assert_provider_row_count(value: i64, expected_count: i64) -> Result<()> {
    let provider = CustomProvider {
        zero_batch: create_batch(0, 10)?,
        one_batch: create_batch(1, 5)?,
    };

    let ctx = SessionContext::new();
    let df = ctx
        .read_table(Arc::new(provider.clone()))?
        .filter(col("flag").eq(lit(value)))?
        .aggregate(vec![], vec![count(col("flag"))])?;

    let results = df.collect().await?;
    let result_col: &Int64Array = as_primitive_array(results[0].column(0))?;
    assert_eq!(result_col.value(0), expected_count);

    ctx.register_table("data", Arc::new(provider))?;
    let sql_results = ctx
        .sql(&format!("select count(*) from data where flag = {value}"))
        .await?
        .collect()
        .await?;

    let sql_result_col: &Int64Array = as_primitive_array(sql_results[0].column(0))?;
    assert_eq!(sql_result_col.value(0), expected_count);

    Ok(())
}

#[tokio::test]
async fn test_filter_pushdown_results() -> Result<()> {
    assert_provider_row_count(0, 10).await?;
    assert_provider_row_count(1, 5).await?;
    assert_provider_row_count(2, 0).await?;
    Ok(())
}
