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

use crate::error::Result;
use crate::logical_plan::{
    DFSchema, Expr, FunctionRegistry, JoinType, LogicalPlan, Partitioning,
};
use crate::record_batch::RecordBatch;
use std::sync::Arc;

use crate::physical_plan::SendableRecordBatchStream;
use async_trait::async_trait;

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
#[async_trait]
pub trait DataFrame: Send + Sync {
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
    fn select_columns(&self, columns: &[&str]) -> Result<Arc<dyn DataFrame>>;

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
    fn select(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>>;

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
    fn filter(&self, expr: Expr) -> Result<Arc<dyn DataFrame>>;

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
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>>;

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
    fn limit(&self, n: usize) -> Result<Arc<dyn DataFrame>>;

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
    fn union(&self, dataframe: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>;

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
    fn distinct(&self) -> Result<Arc<dyn DataFrame>>;

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
    fn sort(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>>;

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
    fn join(
        &self,
        right: Arc<dyn DataFrame>,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
    ) -> Result<Arc<dyn DataFrame>>;

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
    fn repartition(
        &self,
        partitioning_scheme: Partitioning,
    ) -> Result<Arc<dyn DataFrame>>;

    /// Executes this DataFrame and collects all results into a vector of RecordBatch.
    ///
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
    async fn collect(&self) -> Result<Vec<RecordBatch>>;

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
    async fn show(&self) -> Result<()>;

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
    async fn show_limit(&self, n: usize) -> Result<()>;

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
    async fn execute_stream(&self) -> Result<SendableRecordBatchStream>;

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
    async fn collect_partitioned(&self) -> Result<Vec<Vec<RecordBatch>>>;

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
    async fn execute_stream_partitioned(&self) -> Result<Vec<SendableRecordBatchStream>>;

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
    fn schema(&self) -> &DFSchema;

    /// Return the logical plan represented by this DataFrame.
    fn to_logical_plan(&self) -> LogicalPlan;

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
    fn explain(&self, verbose: bool, analyze: bool) -> Result<Arc<dyn DataFrame>>;

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
    fn registry(&self) -> Arc<dyn FunctionRegistry>;

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
    fn intersect(&self, dataframe: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>;

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
    fn except(&self, dataframe: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>;

    /// Write a `DataFrame` to a CSV file.
    async fn write_csv(&self, path: &str) -> Result<()>;

    /// Write a `DataFrame` to a Parquet file.
    async fn write_parquet(
        &self,
        path: &str,
        writer_properties: Option<arrow::io::parquet::write::WriteOptions>,
    ) -> Result<()>;
}
