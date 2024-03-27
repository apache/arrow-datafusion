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

//! View data source which uses a LogicalPlan as it's input.

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Column;
use datafusion_expr::{LogicalPlanBuilder, TableProviderFilterPushDown};

use crate::{
    error::Result,
    logical_expr::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
};

use crate::datasource::{TableProvider, TableType};
use crate::execution::context::SessionState;

/// An implementation of `TableProvider` that uses another logical plan.
pub struct ViewTable {
    /// LogicalPlan of the view
    logical_plan: LogicalPlan,
    /// File fields + partition columns
    table_schema: SchemaRef,
    /// SQL used to create the view, if available
    definition: Option<String>,
}

impl ViewTable {
    /// Create new view that is executed at query runtime.
    /// Takes a `LogicalPlan` and an optional create statement as input.
    pub fn try_new(
        logical_plan: LogicalPlan,
        definition: Option<String>,
    ) -> Result<Self> {
        let table_schema = logical_plan.schema().as_ref().to_owned().into();

        let view = Self {
            logical_plan,
            table_schema,
            definition,
        };

        Ok(view)
    }

    /// Get definition ref
    pub fn definition(&self) -> Option<&String> {
        self.definition.as_ref()
    }

    /// Get logical_plan ref
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

#[async_trait]
impl TableProvider for ViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        Some(&self.logical_plan)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        // A filter is added on the View when given
        Ok(TableProviderFilterPushDown::Exact)
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        let plan = self.logical_plan().clone();
        let mut plan = LogicalPlanBuilder::from(plan);

        if let Some(filter) = filter {
            plan = plan.filter(filter)?;
        }

        let mut plan = if let Some(projection) = projection {
            // avoiding adding a redundant projection (e.g. SELECT * FROM view)
            let current_projection =
                (0..plan.schema().fields().len()).collect::<Vec<usize>>();
            if projection == &current_projection {
                plan
            } else {
                let fields: Vec<Expr> = projection
                    .iter()
                    .map(|i| {
                        Expr::Column(Column::from(
                            self.logical_plan.schema().qualified_field(*i),
                        ))
                    })
                    .collect();
                plan.project(fields)?
            }
        } else {
            plan
        };

        if let Some(limit) = limit {
            plan = plan.limit(0, Some(limit))?;
        }

        state.create_physical_plan(&plan.build()?).await
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{col, lit};

    use crate::execution::options::ParquetReadOptions;
    use crate::prelude::SessionContext;
    use crate::test_util::parquet_test_data;
    use crate::{assert_batches_eq, execution::context::SessionConfig};

    use super::*;

    #[tokio::test]
    async fn issue_3242() -> Result<()> {
        // regression test for https://github.com/apache/arrow-datafusion/pull/3242
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("create view v as select 1 as a, 2 as b, 3 as c")
            .await?
            .collect()
            .await?;

        let results = session_ctx
            .sql("select * from (select b from v)")
            .await?
            .collect()
            .await?;

        let expected = ["+---+", "| b |", "+---+", "| 2 |", "+---+"];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn create_view_return_empty_dataframe() -> Result<()> {
        let session_ctx = SessionContext::new();

        let df = session_ctx
            .sql("CREATE VIEW xyz AS SELECT 1")
            .await?
            .collect()
            .await?;

        assert!(df.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn query_view() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------+---------+---------+",
            "| column1 | column2 | column3 |",
            "+---------+---------+---------+",
            "| 1       | 2       | 3       |",
            "| 4       | 5       | 6       |",
            "+---------+---------+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_alias() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(SessionConfig::new());

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1 AS column1_alias, column2 AS column2_alias FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("SELECT column1_alias FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------------+",
            "| column1_alias |",
            "+---------------+",
            "| 1             |",
            "| 4             |",
            "+---------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_inline_alias() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(SessionConfig::new());

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz (column1_alias, column2_alias) AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("SELECT column2_alias, column1_alias FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------------+---------------+",
            "| column2_alias | column1_alias |",
            "+---------------+---------------+",
            "| 2             | 1             |",
            "| 5             | 4             |",
            "+---------------+---------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_projection() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT column1 FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 4       |",
            "+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT column1 FROM xyz WHERE column2 = 5")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 4       |",
            "+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn query_join_views() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let view_sql = "CREATE VIEW lmn AS SELECT column1, column3 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND (table_name = 'xyz' OR table_name = 'lmn')").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 2);

        let results = session_ctx
            .sql("SELECT * FROM xyz JOIN lmn USING (column1) ORDER BY column2")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------+---------+---------+",
            "| column2 | column1 | column3 |",
            "+---------+---------+---------+",
            "| 2       | 1       | 3       |",
            "| 5       | 4       | 6       |",
            "+---------+---------+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn filter_pushdown_view() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain.snappy.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_table("t1", ctx.table("test").await?.into_view())?;

        ctx.sql("CREATE VIEW t2 as SELECT * FROM t1").await?;

        let df = ctx
            .table("t2")
            .await?
            .filter(col("id").eq(lit(1)))?
            .select_columns(&["bool_col", "int_col"])?;

        let plan = df.explain(false, false)?.collect().await?;

        // Filters all the way to Parquet
        let formatted = arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(formatted.contains("FilterExec: id@0 = 1"));
        Ok(())
    }

    #[tokio::test]
    async fn limit_pushdown_view() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain.snappy.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_table("t1", ctx.table("test").await?.into_view())?;

        ctx.sql("CREATE VIEW t2 as SELECT * FROM t1").await?;

        let df = ctx
            .table("t2")
            .await?
            .limit(0, Some(10))?
            .select_columns(&["bool_col", "int_col"])?;

        let plan = df.explain(false, false)?.collect().await?;
        // Limit is included in ParquetExec
        let formatted = arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(formatted.contains("ParquetExec: "));
        assert!(formatted.contains("projection=[bool_col, int_col], limit=10"));
        Ok(())
    }

    #[tokio::test]
    async fn create_view_plan() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let dataframe = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT * FROM abc")
            .await?;
        let plan = dataframe.into_optimized_plan()?;
        let actual = format!("{}", plan.display_indent());
        let expected = "\
        Explain\
        \n  CreateView: Bare { table: \"xyz\" }\
        \n    TableScan: abc projection=[column1, column2, column3]";
        assert_eq!(expected, actual);

        let dataframe = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT * FROM abc WHERE column2 = 5")
            .await?;
        let plan = dataframe.into_optimized_plan()?;
        let actual = format!("{}", plan.display_indent());
        let expected = "\
        Explain\
        \n  CreateView: Bare { table: \"xyz\" }\
        \n    Filter: abc.column2 = Int64(5)\
        \n      TableScan: abc projection=[column1, column2, column3]";
        assert_eq!(expected, actual);

        let dataframe = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT column1, column2 FROM abc WHERE column2 = 5")
            .await?;
        let plan = dataframe.into_optimized_plan()?;
        let actual = format!("{}", plan.display_indent());
        let expected = "\
        Explain\
        \n  CreateView: Bare { table: \"xyz\" }\
        \n    Filter: abc.column2 = Int64(5)\
        \n      TableScan: abc projection=[column1, column2]";
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn create_or_replace_view() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let view_sql = "CREATE OR REPLACE VIEW xyz AS SELECT column1 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 4       |",
            "+---------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
