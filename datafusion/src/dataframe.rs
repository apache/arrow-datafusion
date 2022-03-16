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

//! DataFrame API for building and executing query plans.

use crate::arrow::ipc::writer::IpcWriteOptions;
use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::logical_plan::{
    col, DFSchema, Expr, FunctionRegistry, JoinType, LogicalPlan, LogicalPlanBuilder,
    Partitioning,
};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

use crate::physical_plan::SendableRecordBatchStream;
use async_trait::async_trait;

use crate::arrow::datatypes::Schema;
use crate::arrow::datatypes::SchemaRef;
use crate::arrow::util::pretty;
use crate::datasource::TableProvider;
use crate::datasource::TableType;
use crate::execution::context::{ExecutionContext, ExecutionContextState};
use crate::physical_plan::file_format::{plan_to_csv, plan_to_ipc, plan_to_parquet};
use crate::physical_plan::{collect, collect_partitioned};
use crate::physical_plan::{execute_stream, execute_stream_partitioned, ExecutionPlan};
use crate::scalar::ScalarValue;
use crate::sql::utils::find_window_exprs;
use parking_lot::Mutex;
use std::any::Any;

/// DataFrame represents a logical set of rows with the same named columns.
/// Similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or
/// [Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
///
/// DataFrames are typically created by the `read_csv` and `read_parquet` methods on the
/// [ExecutionContext](../execution/context/struct.ExecutionContext.html) and can then be modified
/// by calling the transformation methods, such as `filter`, `select`, `aggregate`, and `limit`
/// to build up a query definition.
///
/// The query can be executed by calling the `collect` method.
///
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut ctx = ExecutionContext::new();
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(100)?;
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
pub struct DataFrame {
    ctx_state: Arc<Mutex<ExecutionContextState>>,
    plan: LogicalPlan,
}

impl DataFrame {
    /// Create a new Table based on an existing logical plan
    pub fn new(ctx_state: Arc<Mutex<ExecutionContextState>>, plan: &LogicalPlan) -> Self {
        Self {
            ctx_state,
            plan: plan.clone(),
        }
    }

    /// Create a physical plan
    pub async fn create_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let state = self.ctx_state.lock().clone();
        let ctx = ExecutionContext::from(Arc::new(Mutex::new(state)));
        let plan = ctx.optimize(&self.plan)?;
        ctx.create_physical_plan(&plan).await
    }

    /// Filter the DataFrame by column. Returns a new DataFrame only containing the
    /// specified columns.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select_columns(&["a", "b"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select_columns(&self, columns: &[&str]) -> Result<Arc<DataFrame>> {
        let fields = columns
            .iter()
            .map(|name| self.plan.schema().field_with_unqualified_name(name))
            .collect::<Result<Vec<_>>>()?;
        let expr: Vec<Expr> = fields.iter().map(|f| col(f.name())).collect();
        self.select(expr)
    }

    /// Create a projection based on arbitrary expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select(vec![col("a") * col("b"), col("c")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select(&self, expr_list: Vec<Expr>) -> Result<Arc<DataFrame>> {
        let window_func_exprs = find_window_exprs(&expr_list);
        let plan = if window_func_exprs.is_empty() {
            self.to_logical_plan()
        } else {
            LogicalPlanBuilder::window_plan(self.to_logical_plan(), window_func_exprs)?
        };
        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;

        Ok(Arc::new(DataFrame::new(
            self.ctx_state.clone(),
            &project_plan,
        )))
    }

    /// Filter a DataFrame to only include rows that match the specified filter expression.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.filter(col("a").lt_eq(col("b")))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(&self, predicate: Expr) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .filter(predicate)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Perform an aggregate query with optional grouping expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let _ = df.aggregate(vec![col("a")], vec![min(col("b"))])?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let _ = df.aggregate(vec![], vec![min(col("b"))])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Limit the number of rows returned from this DataFrame.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.limit(100)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(&self, n: usize) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .limit(n)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Calculate the union two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.union(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .union(dataframe.to_logical_plan())?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Calculate the union distinct two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.union(df.clone())?;
    /// let df = df.distinct()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn distinct(&self) -> Result<Arc<DataFrame>> {
        Ok(Arc::new(DataFrame::new(
            self.ctx_state.clone(),
            &LogicalPlanBuilder::from(self.to_logical_plan())
                .distinct()?
                .build()?,
        )))
    }

    /// Sort the DataFrame by the specified sorting expressions. Any expression can be turned into
    /// a sort expression by calling its [sort](../logical_plan/enum.Expr.html#method.sort) method.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.sort(vec![col("a").sort(true, true), col("b").sort(false, false)])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .sort(expr)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Join this DataFrame with another DataFrame using the specified columns as join keys
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let left = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let right = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?
    ///   .select(vec![
    ///     col("a").alias("a2"),
    ///     col("b").alias("b2"),
    ///     col("c").alias("c2")])?;
    /// let join = left.join(right, JoinType::Inner, &["a", "b"], &["a2", "b2"])?;
    /// let batches = join.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn join(
        &self,
        right: Arc<DataFrame>,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
    ) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .join(
                &right.to_logical_plan(),
                join_type,
                (left_cols.to_vec(), right_cols.to_vec()),
            )?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    // TODO: add join_using

    /// Repartition a DataFrame based on a logical partitioning scheme.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df1 = df.repartition(Partitioning::RoundRobinBatch(4))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn repartition(
        &self,
        partitioning_scheme: Partitioning,
    ) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .repartition(partitioning_scheme)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Convert the logical plan represented by this DataFrame into a physical plan and
    /// execute it, collecting all resulting batches into memory
    /// Executes this DataFrame and collects all results into a vector of RecordBatch.
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let plan = self.create_physical_plan().await?;
        let runtime = self.ctx_state.lock().runtime_env.clone();
        Ok(collect(plan, runtime).await?)
    }

    /// Print results.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show(&self) -> Result<()> {
        let results = self.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Print results and limit rows.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// df.show_limit(10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_limit(&self, num: usize) -> Result<()> {
        let results = self.limit(num)?.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Executes this DataFrame and returns a stream over a single partition
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let stream = df.execute_stream().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_stream(&self) -> Result<SendableRecordBatchStream> {
        let plan = self.create_physical_plan().await?;
        let runtime = self.ctx_state.lock().runtime_env.clone();
        execute_stream(plan, runtime).await
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_partitioned(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let plan = self.create_physical_plan().await?;
        let runtime = self.ctx_state.lock().runtime_env.clone();
        Ok(collect_partitioned(plan, runtime).await?)
    }

    /// Executes this DataFrame and returns one stream per partition.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.execute_stream_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_stream_partitioned(
        &self,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let plan = self.create_physical_plan().await?;
        let runtime = self.ctx_state.lock().runtime_env.clone();
        Ok(execute_stream_partitioned(plan, runtime).await?)
    }

    /// Returns the schema describing the output of this DataFrame in terms of columns returned,
    /// where each column has a name, data type, and nullability attribute.

    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let schema = df.schema();
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    /// Return the logical plan represented by this DataFrame.
    pub fn to_logical_plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    /// Return a DataFrame with the explanation of its plan so far.
    ///
    /// if `analyze` is specified, runs the plan and reports metrics
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.limit(100)?.explain(false, false)?.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(&self, verbose: bool, analyze: bool) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .explain(verbose, analyze)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.ctx_state.clone(), &plan)))
    }

    /// Return a `FunctionRegistry` used to plan udf's calls
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let f = df.registry();
    /// // use f.udf("name", vec![...]) to use the udf
    /// # Ok(())
    /// # }
    /// ```
    pub fn registry(&self) -> Arc<dyn FunctionRegistry> {
        let registry = self.ctx_state.lock().clone();
        Arc::new(registry)
    }

    /// Calculate the intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.intersect(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersect(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        let left_plan = self.to_logical_plan();
        let right_plan = dataframe.to_logical_plan();
        Ok(Arc::new(DataFrame::new(
            self.ctx_state.clone(),
            &LogicalPlanBuilder::intersect(left_plan, right_plan, true)?,
        )))
    }

    /// Calculate the exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = ExecutionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.except(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn except(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        let left_plan = self.to_logical_plan();
        let right_plan = dataframe.to_logical_plan();

        Ok(Arc::new(DataFrame::new(
            self.ctx_state.clone(),
            &LogicalPlanBuilder::except(left_plan, right_plan, true)?,
        )))
    }

    /// Write a `DataFrame` to a CSV file.
    pub async fn write_csv(&self, path: &str) -> Result<()> {
        let plan = self.create_physical_plan().await?;
        let state = self.ctx_state.lock().clone();
        let ctx = ExecutionContext::from(Arc::new(Mutex::new(state)));
        plan_to_csv(&ctx, plan, path).await
    }

    /// Write a `DataFrame` to a Parquet file.
    pub async fn write_parquet(
        &self,
        path: &str,
        writer_properties: Option<WriterProperties>,
    ) -> Result<()> {
        let plan = self.create_physical_plan().await?;
        let state = self.ctx_state.lock().clone();
        let ctx = ExecutionContext::from(Arc::new(Mutex::new(state)));
        plan_to_parquet(&ctx, plan, path, writer_properties).await
    }

    /// Write a `DataFrame` to a IPC file.
    pub async fn write_ipc(
        &self,
        path: &str,
        writer_properties: IpcWriteOptions,
    ) -> Result<()> {
        let plan = self.create_physical_plan().await?;
        let state = self.ctx_state.lock().clone();
        let ctx = ExecutionContext::from(Arc::new(Mutex::new(state)));
        plan_to_ipc(&ctx, plan, path, writer_properties).await
    }
}

#[async_trait]
impl TableProvider for DataFrame {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema: Schema = self.plan.schema().as_ref().into();
        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let expr = projection
            .as_ref()
            // construct projections
            .map_or_else(
                || Ok(Arc::new(Self::new(self.ctx_state.clone(), &self.plan)) as Arc<_>),
                |projection| {
                    let schema = TableProvider::schema(self).project(projection)?;
                    let names = schema
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>();
                    self.select_columns(names.as_slice())
                },
            )?
            // add predicates, otherwise use `true` as the predicate
            .filter(filters.iter().cloned().fold(
                Expr::Literal(ScalarValue::Boolean(Some(true))),
                |acc, new| acc.and(new),
            ))?;
        // add a limit if given
        Self::new(
            self.ctx_state.clone(),
            &limit
                .map_or_else(|| Ok(expr.clone()), |n| expr.limit(n))?
                .to_logical_plan(),
        )
        .create_physical_plan()
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::execution::options::CsvReadOptions;
    use crate::physical_plan::{window_functions, ColumnarValue};
    use crate::{assert_batches_sorted_eq, execution::context::ExecutionContext};
    use crate::{logical_plan::*, test_util};
    use arrow::datatypes::DataType;
    use datafusion_expr::ScalarFunctionImplementation;
    use datafusion_expr::Volatility;

    #[tokio::test]
    async fn select_columns() -> Result<()> {
        // build plan using Table API

        let t = test_table().await?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn select_expr() -> Result<()> {
        // build plan using Table API
        let t = test_table().await?;
        let t2 = t.select(vec![col("c1"), col("c2"), col("c11")])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn select_with_window_exprs() -> Result<()> {
        // build plan using Table API
        let t = test_table().await?;
        let first_row = Expr::WindowFunction {
            fun: window_functions::WindowFunction::BuiltInWindowFunction(
                window_functions::BuiltInWindowFunction::FirstValue,
            ),
            args: vec![col("aggregate_test_100.c1")],
            partition_by: vec![col("aggregate_test_100.c2")],
            order_by: vec![],
            window_frame: None,
        };
        let t2 = t.select(vec![col("c1"), first_row])?;
        let plan = t2.to_logical_plan();

        let sql_plan = create_plan(
            "select c1, first_value(c1) over (partition by c2) from aggregate_test_100",
        )
        .await?;

        assert_same_plan(&plan, &sql_plan);
        Ok(())
    }

    #[tokio::test]
    async fn aggregate() -> Result<()> {
        // build plan using DataFrame API
        let df = test_table().await?;
        let group_expr = vec![col("c1")];
        let aggr_expr = vec![
            min(col("c12")),
            max(col("c12")),
            avg(col("c12")),
            sum(col("c12")),
            count(col("c12")),
            count_distinct(col("c12")),
        ];

        let df: Vec<RecordBatch> = df.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) | AVG(aggregate_test_100.c12) | SUM(aggregate_test_100.c12) | COUNT(aggregate_test_100.c12) | COUNT(DISTINCT aggregate_test_100.c12) |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| a  | 0.02182578039211991         | 0.9800193410444061          | 0.48754517466109415         | 10.238448667882977          | 21                            | 21                                     |",
                "| b  | 0.04893135681998029         | 0.9185813970744787          | 0.41040709263815384         | 7.797734760124923           | 19                            | 19                                     |",
                "| c  | 0.0494924465469434          | 0.991517828651004           | 0.6600456536439784          | 13.860958726523545          | 21                            | 21                                     |",
                "| d  | 0.061029375346466685        | 0.9748360509016578          | 0.48855379387549824         | 8.793968289758968           | 18                            | 18                                     |",
                "| e  | 0.01479305307777301         | 0.9965400387585364          | 0.48600669271341534         | 10.206140546981722          | 21                            | 21                                     |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
            ],
            &df
        );

        Ok(())
    }

    #[tokio::test]
    async fn join() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c3"])?;
        let left_rows = left.collect().await?;
        let right_rows = right.collect().await?;
        let join = left.join(right, JoinType::Inner, &["c1"], &["c1"])?;
        let join_rows = join.collect().await?;
        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(2008, join_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        Ok(())
    }

    #[tokio::test]
    async fn limit() -> Result<()> {
        // build query using Table API
        let t = test_table().await?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?.limit(10)?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn explain() -> Result<()> {
        // build query using Table API
        let df = test_table().await?;
        let df = df
            .select_columns(&["c1", "c2", "c11"])?
            .limit(10)?
            .explain(false, false)?;
        let plan = df.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("EXPLAIN SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")
                .await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn registry() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;

        // declare the udf
        let my_fn: ScalarFunctionImplementation =
            Arc::new(|_: &[ColumnarValue]| unimplemented!("my_fn is not implemented"));

        // create and register the udf
        ctx.register_udf(create_udf(
            "my_fn",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            my_fn,
        ));

        // build query with a UDF using DataFrame API
        let df = ctx.table("aggregate_test_100")?;

        let f = df.registry();

        let df = df.select(vec![f.udf("my_fn")?.call(vec![col("c12")])])?;
        let plan = df.to_logical_plan();

        // build query using SQL
        let sql_plan =
            ctx.create_logical_plan("SELECT my_fn(c12) FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn sendable() {
        let df = test_table().await.unwrap();
        // dataframes should be sendable between threads/tasks
        let task = tokio::task::spawn(async move {
            df.select_columns(&["c1"])
                .expect("should be usable in a task")
        });
        task.await.expect("task completed successfully");
    }

    #[tokio::test]
    async fn intersect() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c3"])?;
        let plan = df.intersect(df.clone())?;
        let result = plan.to_logical_plan();
        let expected = create_plan(
            "SELECT c1, c3 FROM aggregate_test_100
            INTERSECT ALL SELECT c1, c3 FROM aggregate_test_100",
        )
        .await?;
        assert_same_plan(&result, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn except() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c3"])?;
        let plan = df.except(df.clone())?;
        let result = plan.to_logical_plan();
        let expected = create_plan(
            "SELECT c1, c3 FROM aggregate_test_100
            EXCEPT ALL SELECT c1, c3 FROM aggregate_test_100",
        )
        .await?;
        assert_same_plan(&result, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn register_table() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c12"])?;
        let mut ctx = ExecutionContext::new();
        let df_impl = Arc::new(DataFrame::new(ctx.state.clone(), &df.to_logical_plan()));

        // register a dataframe as a table
        ctx.register_table("test_table", df_impl.clone())?;

        // pull the table out
        let table = ctx.table("test_table")?;

        let group_expr = vec![col("c1")];
        let aggr_expr = vec![sum(col("c12"))];

        // check that we correctly read from the table
        let df_results = &df_impl
            .aggregate(group_expr.clone(), aggr_expr.clone())?
            .collect()
            .await?;
        let table_results = &table.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+-----------------------------+",
                "| c1 | SUM(aggregate_test_100.c12) |",
                "+----+-----------------------------+",
                "| a  | 10.238448667882977          |",
                "| b  | 7.797734760124923           |",
                "| c  | 13.860958726523545          |",
                "| d  | 8.793968289758968           |",
                "| e  | 10.206140546981722          |",
                "+----+-----------------------------+",
            ],
            df_results
        );

        // the results are the same as the results from the view, modulo the leaf table name
        assert_batches_sorted_eq!(
            vec![
                "+----+---------------------+",
                "| c1 | SUM(test_table.c12) |",
                "+----+---------------------+",
                "| a  | 10.238448667882977  |",
                "| b  | 7.797734760124923   |",
                "| c  | 13.860958726523545  |",
                "| d  | 8.793968289758968   |",
                "| e  | 10.206140546981722  |",
                "+----+---------------------+",
            ],
            table_results
        );
        Ok(())
    }
    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{:?}", plan1), format!("{:?}", plan2));
    }

    /// Create a logical plan from a SQL query
    async fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;
        ctx.create_logical_plan(sql)
    }

    async fn test_table_with_name(name: &str) -> Result<Arc<DataFrame>> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx, name).await?;
        ctx.table(name)
    }

    async fn test_table() -> Result<Arc<DataFrame>> {
        test_table_with_name("aggregate_test_100").await
    }

    async fn register_aggregate_csv(
        ctx: &mut ExecutionContext,
        table_name: &str,
    ) -> Result<()> {
        let schema = test_util::aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        ctx.register_csv(
            table_name,
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            CsvReadOptions::new().schema(schema.as_ref()),
        )
        .await?;
        Ok(())
    }
}
