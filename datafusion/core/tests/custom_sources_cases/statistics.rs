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

//! This module contains end to end tests of statistics propagation

use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::{
    datasource::{TableProvider, TableType},
    error::Result,
    logical_expr::Expr,
    physical_plan::{
        ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
        Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
    scalar::ScalarValue,
};
use datafusion_common::{project_schema, stats::Precision};
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use datafusion_common::logical_type::schema::LogicalSchemaRef;

/// This is a testing structure for statistics
/// It will act both as a table provider and execution plan
#[derive(Debug, Clone)]
struct StatisticsValidation {
    stats: Statistics,
    schema: Arc<Schema>,
    cache: PlanProperties,
}

impl StatisticsValidation {
    fn new(stats: Statistics, schema: SchemaRef) -> Self {
        assert_eq!(
            stats.column_statistics.len(),
            schema.fields().len(),
            "the column statistics vector length should be the number of fields"
        );
        let cache = Self::compute_properties(schema.clone());
        Self {
            stats,
            schema,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(2),
            ExecutionMode::Bounded,
        )
    }
}

#[async_trait]
impl TableProvider for StatisticsValidation {
    fn as_any(&self) -> &dyn Any {
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
        filters: &[Expr],
        // limit is ignored because it is not mandatory for a `TableProvider` to honor it
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Filters should not be pushed down as they are marked as unsupported by default.
        assert_eq!(
            0,
            filters.len(),
            "Unsupported expressions should not be pushed down"
        );
        let projection = match projection.cloned() {
            Some(p) => p,
            None => (0..self.schema.fields().len()).collect(),
        };
        let projected_schema = project_schema(&self.schema, Some(&projection))?;

        let current_stat = self.stats.clone();

        let proj_col_stats = projection
            .iter()
            .map(|i| current_stat.column_statistics[*i].clone())
            .collect();
        Ok(Arc::new(Self::new(
            Statistics {
                num_rows: current_stat.num_rows,
                column_statistics: proj_col_stats,
                // TODO stats: knowing the type of the new columns we can guess the output size
                total_byte_size: Precision::Absent,
            },
            projected_schema,
        )))
    }
}

impl DisplayAs for StatisticsValidation {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StatisticsValidation: col_count={}, row_count={:?}",
                    self.schema.fields().len(),
                    self.stats.num_rows,
                )
            }
        }
    }
}

impl ExecutionPlan for StatisticsValidation {
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
        unimplemented!("This plan only serves for testing statistics")
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.stats.clone())
    }
}

fn init_ctx(stats: Statistics, schema: Schema) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider: Arc<dyn TableProvider> =
        Arc::new(StatisticsValidation::new(stats, Arc::new(schema)));
    ctx.register_table("stats_table", provider)?;
    Ok(ctx)
}

fn fully_defined() -> (Statistics, Schema) {
    (
        Statistics {
            num_rows: Precision::Exact(13),
            total_byte_size: Precision::Absent, // ignore byte size for now
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(1023))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(-24))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(13),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(5486))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-6783))),
                    null_count: Precision::Exact(5),
                },
            ],
        },
        Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int64, false),
        ]),
    )
}

#[tokio::test]
async fn sql_basic() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx.sql("SELECT * from stats_table").await.unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();

    // the statistics should be those of the source
    assert_eq!(stats, physical_plan.statistics()?);

    Ok(())
}

#[tokio::test]
async fn sql_filter() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats, schema)?;

    let df = ctx
        .sql("SELECT * FROM stats_table WHERE c1 = 5")
        .await
        .unwrap();

    let physical_plan = df.create_physical_plan().await.unwrap();
    let stats = physical_plan.statistics()?;
    assert_eq!(stats.num_rows, Precision::Inexact(1));

    Ok(())
}

#[tokio::test]
async fn sql_limit() -> Result<()> {
    let (stats, schema) = fully_defined();
    let col_stats = Statistics::unknown_column(&schema);
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx.sql("SELECT * FROM stats_table LIMIT 5").await.unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();
    // when the limit is smaller than the original number of lines
    // we loose all statistics except the for number of rows which becomes the limit
    assert_eq!(
        Statistics {
            num_rows: Precision::Exact(5),
            column_statistics: col_stats,
            total_byte_size: Precision::Absent
        },
        physical_plan.statistics()?
    );

    let df = ctx
        .sql("SELECT * FROM stats_table LIMIT 100")
        .await
        .unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();
    // when the limit is larger than the original number of lines, statistics remain unchanged
    assert_eq!(stats, physical_plan.statistics()?);

    Ok(())
}

#[tokio::test]
async fn sql_window() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx
        .sql("SELECT c2, sum(c1) over (partition by c2) FROM stats_table")
        .await
        .unwrap();

    let physical_plan = df.create_physical_plan().await.unwrap();

    let result = physical_plan.statistics()?;

    assert_eq!(stats.num_rows, result.num_rows);
    let col_stats = result.column_statistics;
    assert_eq!(2, col_stats.len());
    assert_eq!(stats.column_statistics[1], col_stats[0]);

    Ok(())
}
