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

//! [`DataFrame`] API for building and executing query plans.

#[cfg(feature = "parquet")]
mod parquet;

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty;
use crate::datasource::{provider_as_source, MemTable, TableProvider};
use crate::error::Result;
use crate::execution::context::{SessionState, TaskContext};
use crate::execution::FunctionRegistry;
use crate::logical_expr::utils::find_window_exprs;
use crate::logical_expr::{
    col, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Partitioning, TableType,
};
use crate::physical_plan::{
    collect, collect_partitioned, execute_stream, execute_stream_partitioned,
    ExecutionPlan, SendableRecordBatchStream,
};
use crate::prelude::SessionContext;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::config::{CsvOptions, FormatOptions, JsonOptions};
use datafusion_common::{
    plan_err, Column, DFSchema, DataFusionError, ParamValues, SchemaError, UnnestOptions,
};
use datafusion_expr::{
    avg, count, is_null, max, median, min, stddev, utils::COUNT_STAR_EXPANSION,
    TableProviderFilterPushDown, UNNAMED_TABLE,
};

use async_trait::async_trait;

/// Contains options that control how data is
/// written out from a DataFrame
pub struct DataFrameWriteOptions {
    /// Controls if existing data should be overwritten
    overwrite: bool,
    /// Controls if all partitions should be coalesced into a single output file
    /// Generally will have slower performance when set to true.
    single_file_output: bool,
    /// Sets which columns should be used for hive-style partitioned writes by name.
    /// Can be set to empty vec![] for non-partitioned writes.
    partition_by: Vec<String>,
}

impl DataFrameWriteOptions {
    /// Create a new DataFrameWriteOptions with default values
    pub fn new() -> Self {
        DataFrameWriteOptions {
            overwrite: false,
            single_file_output: false,
            partition_by: vec![],
        }
    }
    /// Set the overwrite option to true or false
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Set the single_file_output value to true or false
    pub fn with_single_file_output(mut self, single_file_output: bool) -> Self {
        self.single_file_output = single_file_output;
        self
    }

    /// Sets the partition_by columns for output partitioning
    pub fn with_partition_by(mut self, partition_by: Vec<String>) -> Self {
        self.partition_by = partition_by;
        self
    }
}

impl Default for DataFrameWriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a logical set of rows with the same named columns.
///
/// Similar to a [Pandas DataFrame] or [Spark DataFrame], a DataFusion DataFrame
/// represents a 2 dimensional table of rows and columns.
///
/// The typical workflow using DataFrames looks like
///
/// 1. Create a DataFrame via methods on [SessionContext], such as [`read_csv`]
/// and [`read_parquet`].
///
/// 2. Build a desired calculation by calling methods such as [`filter`],
/// [`select`], [`aggregate`], and [`limit`]
///
/// 3. Execute into [`RecordBatch`]es by calling [`collect`]
///
/// A `DataFrame` is a wrapper around a [`LogicalPlan`] and the [`SessionState`]
/// required for execution.
///
/// DataFrames are "lazy" in the sense that most methods do not actually compute
/// anything, they just build up a plan. Calling [`collect`] executes the plan
/// using the same DataFusion planning and execution process used to execute SQL
/// and other queries.
///
/// [Pandas DataFrame]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
/// [Spark DataFrame]: https://spark.apache.org/docs/latest/sql-programming-guide.html
/// [`read_csv`]: SessionContext::read_csv
/// [`read_parquet`]: SessionContext::read_parquet
/// [`filter`]: DataFrame::filter
/// [`select`]: DataFrame::select
/// [`aggregate`]: DataFrame::aggregate
/// [`limit`]: DataFrame::limit
/// [`collect`]: DataFrame::collect
///
/// # Example
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// // Read the data from a csv file
/// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
/// // create a new dataframe that computes the equivalent of
/// // `SELECT a, MIN(b) FROM df WHERE a <= b GROUP BY a LIMIT 100;`
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(0, Some(100))?;
/// // Perform the actual computation
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DataFrame {
    session_state: SessionState,
    plan: LogicalPlan,
}

impl DataFrame {
    /// Create a new `DataFrame ` based on an existing `LogicalPlan`
    ///
    /// This is a low-level method and is not typically used by end users. See
    /// [`SessionContext::read_csv`] and other methods for creating a
    /// `DataFrame` from an existing datasource.
    pub fn new(session_state: SessionState, plan: LogicalPlan) -> Self {
        Self {
            session_state,
            plan,
        }
    }

    /// Consume the DataFrame and produce a physical plan
    pub async fn create_physical_plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.session_state.create_physical_plan(&self.plan).await
    }

    /// Filter the DataFrame by column. Returns a new DataFrame only containing the
    /// specified columns.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select_columns(&["a", "b"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select_columns(self, columns: &[&str]) -> Result<DataFrame> {
        let fields = columns
            .iter()
            .map(|name| {
                self.plan
                    .schema()
                    .qualified_field_with_unqualified_name(name)
            })
            .collect::<Result<Vec<_>>>()?;
        let expr: Vec<Expr> = fields
            .into_iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        self.select(expr)
    }

    /// Project arbitrary expressions (like SQL SELECT expressions) into a new
    /// `DataFrame`.
    ///
    /// The output `DataFrame` has one column for each element in `expr_list`.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select(vec![col("a") * col("b"), col("c")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select(self, expr_list: Vec<Expr>) -> Result<DataFrame> {
        let window_func_exprs = find_window_exprs(&expr_list);
        let plan = if window_func_exprs.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::window_plan(self.plan, window_func_exprs)?
        };
        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;

        Ok(DataFrame::new(self.session_state, project_plan))
    }

    /// Expand each list element of a column to multiple rows.
    ///
    /// See also:
    ///
    /// 1. [`UnnestOptions`] documentation for the behavior of `unnest`
    /// 2. [`Self::unnest_column_with_options`]
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.unnest_column("a")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unnest_column(self, column: &str) -> Result<DataFrame> {
        self.unnest_column_with_options(column, UnnestOptions::new())
    }

    /// Expand each list element of a column to multiple rows, with
    /// behavior controlled by [`UnnestOptions`].
    ///
    /// Please see the documentation on [`UnnestOptions`] for more
    /// details about the meaning of unnest.
    pub fn unnest_column_with_options(
        self,
        column: &str,
        options: UnnestOptions,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .unnest_column_with_options(column, options)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Return a DataFrame with only rows for which `predicate` evaluates to
    /// `true`.
    ///
    /// Rows for which `predicate` evaluates to `false` or `null`
    /// are filtered out.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.filter(col("a").lt_eq(col("b")))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(self, predicate: Expr) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .filter(predicate)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Return a new `DataFrame` that aggregates the rows of the current
    /// `DataFrame`, first optionally grouping by the given expressions.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let _ = df.clone().aggregate(vec![col("a")], vec![min(col("b"))])?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let _ = df.aggregate(vec![], vec![min(col("b"))])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn aggregate(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Return a new DataFrame that adds the result of evaluating one or more
    /// window functions ([`Expr::WindowFunction`]) to the existing columns
    pub fn window(self, window_exprs: Vec<Expr>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .window(window_exprs)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Returns a new `DataFrame` with a limited number of rows.
    ///
    /// # Arguments
    /// `skip` - Number of rows to skip before fetch any row
    /// `fetch` - Maximum number of rows to return, after skipping `skip` rows.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.limit(0, Some(100))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .limit(skip, fetch)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Calculate the union of two [`DataFrame`]s, preserving duplicate rows.
    ///
    /// The two [`DataFrame`]s must have exactly the same schema
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.union(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union(self, dataframe: DataFrame) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .union(dataframe.plan)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Calculate the distinct union of two [`DataFrame`]s.
    ///
    /// The two [`DataFrame`]s must have exactly the same schema. Any duplicate
    /// rows are discarded.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.union_distinct(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union_distinct(self, dataframe: DataFrame) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::from(self.plan)
                .union_distinct(dataframe.plan)?
                .build()?,
        ))
    }

    /// Return a new `DataFrame` with all duplicated rows removed.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.distinct()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn distinct(self) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::from(self.plan).distinct()?.build()?,
        ))
    }

    /// Return a new `DataFrame` that has statistics for a DataFrame.
    ///
    /// Only summarizes numeric datatypes at the moment and returns nulls for
    /// non numeric datatypes. The output format is modeled after pandas
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use arrow::util::pretty;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/tpch-csv/customer.csv", CsvReadOptions::new()).await?;
    /// df.describe().await.unwrap();
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn describe(self) -> Result<Self> {
        //the functions now supported
        let supported_describe_functions =
            vec!["count", "null_count", "mean", "std", "min", "max", "median"];

        let original_schema_fields = self.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));

        //collect recordBatch
        let describe_record_batch = vec![
            // count aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // null_count aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(is_null(col(f.name()))).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // mean aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| avg(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // std aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| stddev(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // min aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| {
                        !matches!(f.data_type(), DataType::Binary | DataType::Boolean)
                    })
                    .map(|f| min(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // max aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| {
                        !matches!(f.data_type(), DataType::Binary | DataType::Boolean)
                    })
                    .map(|f| max(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // median aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| median(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
        ];

        // first column with function names
        let mut array_ref_vec: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            supported_describe_functions.clone(),
        ))];
        for field in original_schema_fields {
            let mut array_datas = vec![];
            for result in describe_record_batch.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batchs = df.clone().collect().await;
                        match batchs {
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0]
                                        .column_by_name(field.name())
                                        .is_some() =>
                            {
                                let column =
                                    batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                            Aggregate requires at least one grouping \
                                            or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
                };
                array_datas.push(array_ref);
            }
            array_ref_vec.push(concat(
                array_datas
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }

        let describe_record_batch =
            RecordBatch::try_new(Arc::new(Schema::new(describe_schemas)), array_ref_vec)?;

        let provider = MemTable::try_new(
            describe_record_batch.schema(),
            vec![vec![describe_record_batch]],
        )?;
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        ))
    }

    /// Sort the DataFrame by the specified sorting expressions.
    ///
    /// Note that any expression can be turned into
    /// a sort expression by calling its [sort](Expr::sort) method.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.sort(vec![
    ///   col("a").sort(true, true),   // a ASC, nulls first
    ///   col("b").sort(false, false), // b DESC, nulls last
    ///  ])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sort(self, expr: Vec<Expr>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan).sort(expr)?.build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Join this `DataFrame` with another `DataFrame` using explicitly specified
    /// columns and an optional filter expression.
    ///
    /// See [`join_on`](Self::join_on) for a more concise way to specify the
    /// join condition. Since DataFusion will automatically identify and
    /// optimize equality predicates there is no performance difference between
    /// this function and `join_on`
    ///
    /// `left_cols` and `right_cols` are used to form "equijoin" predicates (see
    /// example below), which are then combined with the optional `filter`
    /// expression.
    ///
    /// Note that in case of outer join, the `filter` is applied to only matched rows.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let left = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let right = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
    ///   .select(vec![
    ///     col("a").alias("a2"),
    ///     col("b").alias("b2"),
    ///     col("c").alias("c2")])?;
    /// // Perform the equivalent of `left INNER JOIN right ON (a = a2 AND b = b2)`
    /// // finding all pairs of rows from `left` and `right` where `a = a2` and `b = b2`.
    /// let join = left.join(right, JoinType::Inner, &["a", "b"], &["a2", "b2"], None)?;
    /// let batches = join.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn join(
        self,
        right: DataFrame,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .join(
                right.plan,
                join_type,
                (left_cols.to_vec(), right_cols.to_vec()),
                filter,
            )?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Join this `DataFrame` with another `DataFrame` using the specified
    /// expressions.
    ///
    /// Note that DataFusion automatically optimizes joins, including
    /// identifying and optimizing equality predicates.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let left = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let right = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     .select(vec![
    ///         col("a").alias("a2"),
    ///         col("b").alias("b2"),
    ///         col("c").alias("c2"),
    ///     ])?;
    ///
    /// // Perform the equivalent of `left INNER JOIN right ON (a != a2 AND b != b2)`
    /// // finding all pairs of rows from `left` and `right` where
    /// // where `a != a2` and `b != b2`.
    /// let join_on = left.join_on(
    ///     right,
    ///     JoinType::Inner,
    ///     [col("a").not_eq(col("a2")), col("b").not_eq(col("b2"))],
    /// )?;
    /// let batches = join_on.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn join_on(
        self,
        right: DataFrame,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<DataFrame> {
        let expr = on_exprs.into_iter().reduce(Expr::and);
        let plan = LogicalPlanBuilder::from(self.plan)
            .join_on(right.plan, join_type, expr)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Repartition a DataFrame based on a logical partitioning scheme.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df1 = df.repartition(Partitioning::RoundRobinBatch(4))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn repartition(self, partitioning_scheme: Partitioning) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .repartition(partitioning_scheme)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Return the total number of rows in this `DataFrame`.
    ///
    /// Note that this method will actually run a plan to calculate the count,
    /// which may be slow for large or complicated DataFrames.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let count = df.count().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count(self) -> Result<usize> {
        let rows = self
            .aggregate(
                vec![],
                vec![datafusion_expr::count(Expr::Literal(COUNT_STAR_EXPANSION))],
            )?
            .collect()
            .await?;
        let len = *rows
            .first()
            .and_then(|r| r.columns().first())
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .and_then(|a| a.values().first())
            .ok_or(DataFusionError::Internal(
                "Unexpected output when collecting for count()".to_string(),
            ))? as usize;
        Ok(len)
    }

    /// Execute this `DataFrame` and buffer all resulting `RecordBatch`es  into memory.
    ///
    /// Prior to calling `collect`, modifying a DataFrame simply updates a plan
    /// (no actual computation is performed). `collect` triggers the computation.
    ///
    /// See [`Self::execute_stream`] to execute a DataFrame without buffering.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        collect(plan, task_ctx).await
    }

    /// Execute the `DataFrame` and print the results to the console.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show(self) -> Result<()> {
        let results = self.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Execute the `DataFrame` and print only the first `num` rows of the
    /// result to the console.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// df.show_limit(10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_limit(self, num: usize) -> Result<()> {
        let results = self.limit(0, Some(num))?.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Return a new [`TaskContext`] which would be used to execute this DataFrame
    pub fn task_ctx(&self) -> TaskContext {
        TaskContext::from(&self.session_state)
    }

    /// Executes this DataFrame and returns a stream over a single partition
    ///
    /// See [Self::collect] to buffer the `RecordBatch`es in memory.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let stream = df.execute_stream().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Aborting Execution
    ///
    /// Dropping the stream will abort the execution of the query, and free up
    /// any allocated resources
    pub async fn execute_stream(self) -> Result<SendableRecordBatchStream> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        execute_stream(plan, task_ctx)
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_partitioned(self) -> Result<Vec<Vec<RecordBatch>>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        collect_partitioned(plan, task_ctx).await
    }

    /// Executes this DataFrame and returns one stream per partition.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.execute_stream_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    /// # Aborting Execution
    ///
    /// Dropping the stream will abort the execution of the query, and free up
    /// any allocated resources
    pub async fn execute_stream_partitioned(
        self,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        execute_stream_partitioned(plan, task_ctx)
    }

    /// Returns the `DFSchema` describing the output of this DataFrame.
    ///
    /// The output `DFSchema` contains information on the name, data type, and
    /// nullability for each column.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let schema = df.schema();
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    /// Return a reference to the unoptimized [`LogicalPlan`] that comprises
    /// this DataFrame. See [`Self::into_unoptimized_plan`] for more details.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Returns both the [`LogicalPlan`] and [`SessionState`] that comprise this [`DataFrame`]
    pub fn into_parts(self) -> (SessionState, LogicalPlan) {
        (self.session_state, self.plan)
    }

    /// Return the [`LogicalPlan`] represented by this DataFrame without running
    /// any optimizers
    ///
    /// Note: This method should not be used outside testing, as it loses the
    /// snapshot of the [`SessionState`] attached to this [`DataFrame`] and
    /// consequently subsequent operations may take place against a different
    /// state (e.g. a different value of `now()`)
    pub fn into_unoptimized_plan(self) -> LogicalPlan {
        self.plan
    }

    /// Return the optimized [`LogicalPlan`] represented by this DataFrame.
    ///
    /// Note: This method should not be used outside testing -- see
    /// [`Self::into_optimized_plan`] for more details.
    pub fn into_optimized_plan(self) -> Result<LogicalPlan> {
        // Optimize the plan first for better UX
        self.session_state.optimize(&self.plan)
    }

    /// Converts this [`DataFrame`] into a [`TableProvider`] that can be registered
    /// as a table view using [`SessionContext::register_table`].
    ///
    /// Note: This discards the [`SessionState`] associated with this
    /// [`DataFrame`] in favour of the one passed to [`TableProvider::scan`]
    pub fn into_view(self) -> Arc<dyn TableProvider> {
        Arc::new(DataFrameTableProvider { plan: self.plan })
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
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.limit(0, Some(100))?.explain(false, false)?.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<DataFrame> {
        if matches!(self.plan, LogicalPlan::Explain(_)) {
            return plan_err!("Nested EXPLAINs are not supported");
        }
        let plan = LogicalPlanBuilder::from(self.plan)
            .explain(verbose, analyze)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Return a `FunctionRegistry` used to plan udf's calls
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let f = df.registry();
    /// // use f.udf("name", vec![...]) to use the udf
    /// # Ok(())
    /// # }
    /// ```
    pub fn registry(&self) -> &dyn FunctionRegistry {
        &self.session_state
    }

    /// Calculate the intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.intersect(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersect(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::intersect(left_plan, right_plan, true)?,
        ))
    }

    /// Calculate the exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.except(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn except(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;

        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::except(left_plan, right_plan, true)?,
        ))
    }

    /// Execute this `DataFrame` and write the results to `table_name`.
    ///
    /// Returns a single [RecordBatch] containing a single column and
    /// row representing the count of total rows written.
    ///
    /// Unlike most other `DataFrame` methods, this method executes eagerly.
    /// Data is written to the table using the [`TableProvider::insert_into`]
    /// method. This is the same underlying implementation used by SQL `INSERT
    /// INTO` statements.
    pub async fn write_table(
        self,
        table_name: &str,
        write_options: DataFrameWriteOptions,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let arrow_schema = Schema::from(self.schema());
        let plan = LogicalPlanBuilder::insert_into(
            self.plan,
            table_name.to_owned(),
            &arrow_schema,
            write_options.overwrite,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Execute the `DataFrame` and write the results to CSV file(s).
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use std::fs;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion::dataframe::DataFrameWriteOptions;
    /// let ctx = SessionContext::new();
    /// // Sort the data by column "b" and write it to a new location
    /// ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
    ///   .sort(vec![col("b").sort(true, true)])? // sort by b asc, nulls first
    ///   .write_csv(
    ///     "output.csv",
    ///     DataFrameWriteOptions::new(),
    ///     None, // can also specify CSV writing options here
    /// ).await?;
    /// # fs::remove_file("output.csv")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_csv(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<CsvOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_csv.".to_owned(),
            ));
        }
        let props = writer_options
            .unwrap_or_else(|| self.session_state.default_table_options().csv);

        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FormatOptions::CSV(props),
            HashMap::new(),
            options.partition_by,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Execute the `DataFrame` and write the results to JSON file(s).
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use std::fs;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion::dataframe::DataFrameWriteOptions;
    /// let ctx = SessionContext::new();
    /// // Sort the data by column "b" and write it to a new location
    /// ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
    ///   .sort(vec![col("b").sort(true, true)])? // sort by b asc, nulls first
    ///   .write_json(
    ///     "output.json",
    ///     DataFrameWriteOptions::new(),
    ///     None
    /// ).await?;
    /// # fs::remove_file("output.json")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_json(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<JsonOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_json.".to_owned(),
            ));
        }

        let props = writer_options
            .unwrap_or_else(|| self.session_state.default_table_options().json);

        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FormatOptions::JSON(props),
            Default::default(),
            options.partition_by,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Add an additional column to the DataFrame.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.with_column("ab_sum", col("a") + col("b"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column(self, name: &str, expr: Expr) -> Result<DataFrame> {
        let window_func_exprs = find_window_exprs(&[expr.clone()]);
        let plan = if window_func_exprs.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::window_plan(self.plan, window_func_exprs)?
        };

        let new_column = expr.alias(name);
        let mut col_exists = false;
        let mut fields: Vec<Expr> = plan
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                if field.name() == name {
                    col_exists = true;
                    new_column.clone()
                } else {
                    col(Column::from((qualifier, field.as_ref())))
                }
            })
            .collect();

        if !col_exists {
            fields.push(new_column);
        }

        let project_plan = LogicalPlanBuilder::from(plan).project(fields)?.build()?;

        Ok(DataFrame::new(self.session_state, project_plan))
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    ///
    /// The method supports case sensitive rename with wrapping column name into one of following symbols (  "  or  '  or  `  )
    ///
    /// Alternatively setting Datafusion param `datafusion.sql_parser.enable_ident_normalization` to `false` will enable  
    /// case sensitive rename without need to wrap column name into special symbols
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.with_column_renamed("ab_sum", "total")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column_renamed(
        self,
        old_name: impl Into<String>,
        new_name: &str,
    ) -> Result<DataFrame> {
        let ident_opts = self
            .session_state
            .config_options()
            .sql_parser
            .enable_ident_normalization;
        let old_column: Column = if ident_opts {
            Column::from_qualified_name(old_name)
        } else {
            Column::from_qualified_name_ignore_case(old_name)
        };

        let (qualifier_rename, field_rename) =
            match self.plan.schema().qualified_field_from_column(&old_column) {
                Ok(qualifier_and_field) => qualifier_and_field,
                // no-op if field not found
                Err(DataFusionError::SchemaError(
                    SchemaError::FieldNotFound { .. },
                    _,
                )) => return Ok(self),
                Err(err) => return Err(err),
            };
        let projection = self
            .plan
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                if qualifier.eq(&qualifier_rename) && field.as_ref() == field_rename {
                    col(Column::from((qualifier, field.as_ref()))).alias(new_name)
                } else {
                    col(Column::from((qualifier, field.as_ref())))
                }
            })
            .collect::<Vec<_>>();
        let project_plan = LogicalPlanBuilder::from(self.plan)
            .project(projection)?
            .build()?;
        Ok(DataFrame::new(self.session_state, project_plan))
    }

    /// Replace all parameters in logical plan with the specified
    /// values, in preparation for execution.
    ///
    /// # Example
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// # use datafusion::{error::Result, assert_batches_eq};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # use datafusion_common::ScalarValue;
    /// let mut ctx = SessionContext::new();
    /// # ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let results = ctx
    ///   .sql("SELECT a FROM example WHERE b = $1")
    ///   .await?
    ///    // replace $1 with value 2
    ///   .with_param_values(vec![
    ///      // value at index 0 --> $1
    ///      ScalarValue::from(2i64)
    ///    ])?
    ///   .collect()
    ///   .await?;
    /// assert_batches_eq!(
    ///  &[
    ///    "+---+",
    ///    "| a |",
    ///    "+---+",
    ///    "| 1 |",
    ///    "+---+",
    ///  ],
    ///  &results
    /// );
    /// // Note you can also provide named parameters
    /// let results = ctx
    ///   .sql("SELECT a FROM example WHERE b = $my_param")
    ///   .await?
    ///    // replace $my_param with value 2
    ///    // Note you can also use a HashMap as well
    ///   .with_param_values(vec![
    ///       ("my_param", ScalarValue::from(2i64))
    ///    ])?
    ///   .collect()
    ///   .await?;
    /// assert_batches_eq!(
    ///  &[
    ///    "+---+",
    ///    "| a |",
    ///    "+---+",
    ///    "| 1 |",
    ///    "+---+",
    ///  ],
    ///  &results
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_param_values(self, query_values: impl Into<ParamValues>) -> Result<Self> {
        let plan = self.plan.with_param_values(query_values)?;
        Ok(Self::new(self.session_state, plan))
    }

    /// Cache DataFrame as a memory table.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.cache().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cache(self) -> Result<DataFrame> {
        let context = SessionContext::new_with_state(self.session_state.clone());
        // The schema is consistent with the output
        let plan = self.clone().create_physical_plan().await?;
        let schema = plan.schema();
        let task_ctx = Arc::new(self.task_ctx());
        let partitions = collect_partitioned(plan, task_ctx).await?;
        let mem_table = MemTable::try_new(schema, partitions)?;
        context.read_table(Arc::new(mem_table))
    }
}

struct DataFrameTableProvider {
    plan: LogicalPlan,
}

#[async_trait]
impl TableProvider for DataFrameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        Some(&self.plan)
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        // A filter is added on the DataFrame when given
        Ok(TableProviderFilterPushDown::Exact)
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut expr = LogicalPlanBuilder::from(self.plan.clone());
        // Add filter when given
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(filter) = filter {
            expr = expr.filter(filter)?
        }

        if let Some(p) = projection {
            expr = expr.select(p.iter().copied())?
        }

        // add a limit if given
        if let Some(l) = limit {
            expr = expr.limit(0, Some(l))?
        }
        let plan = expr.build()?;
        state.create_physical_plan(&plan).await
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::execution::context::SessionConfig;
    use crate::physical_plan::{ColumnarValue, Partitioning, PhysicalExpr};
    use crate::test_util::{register_aggregate_csv, test_table, test_table_with_name};
    use crate::{assert_batches_sorted_eq, execution::context::SessionContext};

    use arrow::array::{self, Int32Array};
    use arrow::datatypes::DataType;
    use datafusion_common::{Constraint, Constraints};
    use datafusion_common_runtime::SpawnedTask;
    use datafusion_expr::{
        avg, cast, count, count_distinct, create_udf, expr, lit, max, min, sum,
        BuiltInWindowFunction, ScalarFunctionImplementation, Volatility, WindowFrame,
        WindowFunctionDefinition,
    };
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_plan::{get_plan_string, ExecutionPlanProperties};

    // Get string representation of the plan
    async fn assert_physical_plan(df: &DataFrame, expected: Vec<&str>) {
        let physical_plan = df
            .clone()
            .create_physical_plan()
            .await
            .expect("Error creating physical plan");

        let actual = get_plan_string(&physical_plan);
        assert_eq!(
            expected, actual,
            "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
    }

    pub fn table_with_constraints() -> Arc<dyn TableProvider> {
        let dual_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            dual_schema.clone(),
            vec![
                Arc::new(array::Int32Array::from(vec![1])),
                Arc::new(array::StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let provider = MemTable::try_new(dual_schema, vec![vec![batch]])
            .unwrap()
            .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]));
        Arc::new(provider)
    }

    async fn assert_logical_expr_schema_eq_physical_expr_schema(
        df: DataFrame,
    ) -> Result<()> {
        let logical_expr_dfschema = df.schema();
        let logical_expr_schema = SchemaRef::from(logical_expr_dfschema.to_owned());
        let batches = df.collect().await?;
        let physical_expr_schema = batches[0].schema();
        assert_eq!(logical_expr_schema, physical_expr_schema);
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_ord_schema() -> Result<()> {
        let ctx = SessionContext::new();

        let create_table_query = r#"
            CREATE TABLE test_table (
                "double_field" DOUBLE,
                "string_field" VARCHAR
            ) AS VALUES
                (1.0, 'a'),
                (2.0, 'b'),
                (3.0, 'c')
        "#;
        ctx.sql(create_table_query).await?;

        let query = r#"SELECT
        array_agg("double_field" ORDER BY "string_field") as "double_field",
        array_agg("string_field" ORDER BY "string_field") as "string_field"
    FROM test_table"#;

        let result = ctx.sql(query).await?;
        assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_schema() -> Result<()> {
        let ctx = SessionContext::new();

        let create_table_query = r#"
            CREATE TABLE test_table (
                "double_field" DOUBLE,
                "string_field" VARCHAR
            ) AS VALUES
                (1.0, 'a'),
                (2.0, 'b'),
                (3.0, 'c')
        "#;
        ctx.sql(create_table_query).await?;

        let query = r#"SELECT
        array_agg("double_field") as "double_field",
        array_agg("string_field") as "string_field"
    FROM test_table"#;

        let result = ctx.sql(query).await?;
        assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_distinct_schema() -> Result<()> {
        let ctx = SessionContext::new();

        let create_table_query = r#"
            CREATE TABLE test_table (
                "double_field" DOUBLE,
                "string_field" VARCHAR
            ) AS VALUES
                (1.0, 'a'),
                (2.0, 'b'),
                (2.0, 'a')
        "#;
        ctx.sql(create_table_query).await?;

        let query = r#"SELECT
        array_agg(distinct "double_field") as "double_field",
        array_agg(distinct "string_field") as "string_field"
    FROM test_table"#;

        let result = ctx.sql(query).await?;
        assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
        Ok(())
    }

    #[tokio::test]
    async fn select_columns() -> Result<()> {
        // build plan using Table API

        let t = test_table().await?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?;
        let plan = t2.plan.clone();

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
        let plan = t2.plan.clone();

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
        let first_row = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunctionDefinition::BuiltInWindowFunction(
                BuiltInWindowFunction::FirstValue,
            ),
            vec![col("aggregate_test_100.c1")],
            vec![col("aggregate_test_100.c2")],
            vec![],
            WindowFrame::new(None),
            None,
        ));
        let t2 = t.select(vec![col("c1"), first_row])?;
        let plan = t2.plan.clone();

        let sql_plan = create_plan(
            "select c1, first_value(c1) over (partition by c2) from aggregate_test_100",
        )
        .await?;

        assert_same_plan(&plan, &sql_plan);
        Ok(())
    }

    #[tokio::test]
    async fn select_with_periods() -> Result<()> {
        // define data with a column name that has a "." in it:
        let array: Int32Array = [1, 10].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![("f.c1", Arc::new(array) as _)])?;

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch)?;

        let df = ctx.table("t").await?.select_columns(&["f.c1"])?;

        let df_results = df.collect().await?;

        assert_batches_sorted_eq!(
            ["+------+", "| f.c1 |", "+------+", "| 1    |", "| 10   |", "+------+"],
            &df_results
        );

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
            ["+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) | AVG(aggregate_test_100.c12) | SUM(aggregate_test_100.c12) | COUNT(aggregate_test_100.c12) | COUNT(DISTINCT aggregate_test_100.c12) |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| a  | 0.02182578039211991         | 0.9800193410444061          | 0.48754517466109415         | 10.238448667882977          | 21                            | 21                                     |",
                "| b  | 0.04893135681998029         | 0.9185813970744787          | 0.41040709263815384         | 7.797734760124923           | 19                            | 19                                     |",
                "| c  | 0.0494924465469434          | 0.991517828651004           | 0.6600456536439784          | 13.860958726523545          | 21                            | 21                                     |",
                "| d  | 0.061029375346466685        | 0.9748360509016578          | 0.48855379387549824         | 8.793968289758968           | 18                            | 18                                     |",
                "| e  | 0.01479305307777301         | 0.9965400387585364          | 0.48600669271341534         | 10.206140546981722          | 21                            | 21                                     |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+"],
            &df
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_with_pk() -> Result<()> {
        // create the dataframe
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        let df = ctx.read_table(table_with_constraints())?;

        // GROUP BY id
        let group_expr = vec![col("id")];
        let aggr_expr = vec![];
        let df = df.aggregate(group_expr, aggr_expr)?;

        // Since id and name are functionally dependant, we can use name among
        // expression even if it is not part of the group by expression and can
        // select "name" column even though it wasn't explicitly grouped
        let df = df.select(vec![col("id"), col("name")])?;
        assert_physical_plan(
            &df,
            vec![
                "AggregateExec: mode=Single, gby=[id@0 as id, name@1 as name], aggr=[]",
                "  MemoryExec: partitions=1, partition_sizes=[1]",
            ],
        )
        .await;

        let df_results = df.collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!([
             "+----+------+",
             "| id | name |",
             "+----+------+",
             "| 1  | a    |",
             "+----+------+"
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_with_pk2() -> Result<()> {
        // create the dataframe
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        let df = ctx.read_table(table_with_constraints())?;

        // GROUP BY id
        let group_expr = vec![col("id")];
        let aggr_expr = vec![];
        let df = df.aggregate(group_expr, aggr_expr)?;

        // Predicate refers to id, and name fields:
        // id = 1 AND name = 'a'
        let predicate = col("id").eq(lit(1i32)).and(col("name").eq(lit("a")));
        let df = df.filter(predicate)?;
        assert_physical_plan(
            &df,
            vec![
            "CoalesceBatchesExec: target_batch_size=8192",
            "  FilterExec: id@0 = 1 AND name@1 = a",
            "    AggregateExec: mode=Single, gby=[id@0 as id, name@1 as name], aggr=[]",
            "      MemoryExec: partitions=1, partition_sizes=[1]",
        ],
        )
        .await;

        // Since id and name are functionally dependant, we can use name among expression
        // even if it is not part of the group by expression.
        let df_results = df.collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!(
            ["+----+------+",
             "| id | name |",
             "+----+------+",
             "| 1  | a    |",
             "+----+------+",],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_with_pk3() -> Result<()> {
        // create the dataframe
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        let df = ctx.read_table(table_with_constraints())?;

        // GROUP BY id
        let group_expr = vec![col("id")];
        let aggr_expr = vec![];
        // group by id,
        let df = df.aggregate(group_expr, aggr_expr)?;

        // Predicate refers to id field
        // id = 1
        let predicate = col("id").eq(lit(1i32));
        let df = df.filter(predicate)?;
        // Select expression refers to id, and name columns.
        // id, name
        let df = df.select(vec![col("id"), col("name")])?;
        assert_physical_plan(
            &df,
            vec![
            "CoalesceBatchesExec: target_batch_size=8192",
            "  FilterExec: id@0 = 1",
            "    AggregateExec: mode=Single, gby=[id@0 as id, name@1 as name], aggr=[]",
            "      MemoryExec: partitions=1, partition_sizes=[1]",
        ],
        )
        .await;

        // Since id and name are functionally dependant, we can use name among expression
        // even if it is not part of the group by expression.
        let df_results = df.collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!(
            ["+----+------+",
             "| id | name |",
             "+----+------+",
             "| 1  | a    |",
             "+----+------+",],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_with_pk4() -> Result<()> {
        // create the dataframe
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        let df = ctx.read_table(table_with_constraints())?;

        // GROUP BY id
        let group_expr = vec![col("id")];
        let aggr_expr = vec![];
        let df = df.aggregate(group_expr, aggr_expr)?;

        // Predicate refers to id field
        // id = 1
        let predicate = col("id").eq(lit(1i32));
        let df = df.filter(predicate)?;
        // Select expression refers to id column.
        // id
        let df = df.select(vec![col("id")])?;

        // In this case aggregate shouldn't be expanded, since these
        // columns are not used.
        assert_physical_plan(
            &df,
            vec![
                "CoalesceBatchesExec: target_batch_size=8192",
                "  FilterExec: id@0 = 1",
                "    AggregateExec: mode=Single, gby=[id@0 as id], aggr=[]",
                "      MemoryExec: partitions=1, partition_sizes=[1]",
            ],
        )
        .await;

        let df_results = df.collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!([
                "+----+",
                "| id |",
                "+----+",
                "| 1  |",
                "+----+",],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate_alias() -> Result<()> {
        let df = test_table().await?;

        let df = df
            // GROUP BY `c2 + 1`
            .aggregate(vec![col("c2") + lit(1)], vec![])?
            // SELECT `c2 + 1` as c2
            .select(vec![(col("c2") + lit(1)).alias("c2")])?
            // GROUP BY c2 as "c2" (alias in expr is not supported by SQL)
            .aggregate(vec![col("c2").alias("c2")], vec![])?;

        let df_results = df.collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!([
                "+----+",
                "| c2 |",
                "+----+",
                "| 2  |",
                "| 3  |",
                "| 4  |",
                "| 5  |",
                "| 6  |",
                "+----+",
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_distinct() -> Result<()> {
        let t = test_table().await?;
        let plan = t
            .select(vec![col("c1")])
            .unwrap()
            .distinct()
            .unwrap()
            .plan
            .clone();

        let sql_plan = create_plan("select distinct c1 from aggregate_test_100").await?;

        assert_same_plan(&plan, &sql_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_distinct_sort_by() -> Result<()> {
        let t = test_table().await?;
        let plan = t
            .select(vec![col("c1")])
            .unwrap()
            .distinct()
            .unwrap()
            .sort(vec![col("c1").sort(true, true)])
            .unwrap();

        let df_results = plan.clone().collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!(
            ["+----+",
                "| c1 |",
                "+----+",
                "| a  |",
                "| b  |",
                "| c  |",
                "| d  |",
                "| e  |",
                "+----+"],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_distinct_sort_by_unprojected() -> Result<()> {
        let t = test_table().await?;
        let err = t
            .select(vec![col("c1")])
            .unwrap()
            .distinct()
            .unwrap()
            // try to sort on some value not present in input to distinct
            .sort(vec![col("c2").sort(true, true)])
            .unwrap_err();
        assert_eq!(err.strip_backtrace(), "Error during planning: For SELECT DISTINCT, ORDER BY expressions c2 must appear in select list");

        Ok(())
    }

    #[tokio::test]
    async fn join() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c3"])?;
        let left_rows = left.clone().collect().await?;
        let right_rows = right.clone().collect().await?;
        let join = left.join(right, JoinType::Inner, &["c1"], &["c1"], None)?;
        let join_rows = join.collect().await?;
        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(2008, join_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        Ok(())
    }

    #[tokio::test]
    async fn join_on() -> Result<()> {
        let left = test_table_with_name("a")
            .await?
            .select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("b")
            .await?
            .select_columns(&["c1", "c2"])?;
        let join = left.join_on(
            right,
            JoinType::Inner,
            [col("a.c1").not_eq(col("b.c1")), col("a.c2").eq(col("b.c2"))],
        )?;

        let expected_plan = "Inner Join:  Filter: a.c1 != b.c1 AND a.c2 = b.c2\
        \n  Projection: a.c1, a.c2\
        \n    TableScan: a\
        \n  Projection: b.c1, b.c2\
        \n    TableScan: b";
        assert_eq!(expected_plan, format!("{:?}", join.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn join_ambiguous_filter() -> Result<()> {
        let left = test_table_with_name("a")
            .await?
            .select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("b")
            .await?
            .select_columns(&["c1", "c2"])?;

        let join = left
            .join_on(right, JoinType::Inner, [col("c1").eq(col("c1"))])
            .expect_err("join didn't fail check");
        let expected = "Schema error: Ambiguous reference to unqualified field c1";
        assert_eq!(join.strip_backtrace(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn limit() -> Result<()> {
        // build query using Table API
        let t = test_table().await?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?.limit(0, Some(10))?;
        let plan = t2.plan.clone();

        // build query using SQL
        let sql_plan =
            create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn df_count() -> Result<()> {
        let count = test_table().await?.count().await?;
        assert_eq!(100, count);
        Ok(())
    }

    #[tokio::test]
    async fn explain() -> Result<()> {
        // build query using Table API
        let df = test_table().await?;
        let df = df
            .select_columns(&["c1", "c2", "c11"])?
            .limit(0, Some(10))?
            .explain(false, false)?;
        let plan = df.plan.clone();

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
        let mut ctx = SessionContext::new();
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
        let df = ctx.table("aggregate_test_100").await?;

        let expr = df.registry().udf("my_fn")?.call(vec![col("c12")]);
        let df = df.select(vec![expr])?;

        // build query using SQL
        let sql_plan = ctx.sql("SELECT my_fn(c12) FROM aggregate_test_100").await?;

        // the two plans should be identical
        assert_same_plan(&df.plan, &sql_plan.plan);

        Ok(())
    }

    #[tokio::test]
    async fn sendable() {
        let df = test_table().await.unwrap();
        // dataframes should be sendable between threads/tasks
        let task = SpawnedTask::spawn(async move {
            df.select_columns(&["c1"])
                .expect("should be usable in a task")
        });
        task.join().await.expect("task completed successfully");
    }

    #[tokio::test]
    async fn intersect() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c3"])?;
        let d2 = df.clone();
        let plan = df.intersect(d2)?;
        let result = plan.plan.clone();
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
        let d2 = df.clone();
        let plan = df.except(d2)?;
        let result = plan.plan.clone();
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
        let ctx = SessionContext::new();
        let df_impl = DataFrame::new(ctx.state(), df.plan.clone());

        // register a dataframe as a table
        ctx.register_table("test_table", df_impl.clone().into_view())?;

        // pull the table out
        let table = ctx.table("test_table").await?;

        let group_expr = vec![col("c1")];
        let aggr_expr = vec![sum(col("c12"))];

        // check that we correctly read from the table
        let df_results = df_impl
            .aggregate(group_expr.clone(), aggr_expr.clone())?
            .collect()
            .await?;
        let table_results = &table.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+----+-----------------------------+",
                "| c1 | SUM(aggregate_test_100.c12) |",
                "+----+-----------------------------+",
                "| a  | 10.238448667882977          |",
                "| b  | 7.797734760124923           |",
                "| c  | 13.860958726523545          |",
                "| d  | 8.793968289758968           |",
                "| e  | 10.206140546981722          |",
                "+----+-----------------------------+"
            ],
            &df_results
        );

        // the results are the same as the results from the view, modulo the leaf table name
        assert_batches_sorted_eq!(
            [
                "+----+---------------------+",
                "| c1 | SUM(test_table.c12) |",
                "+----+---------------------+",
                "| a  | 10.238448667882977  |",
                "| b  | 7.797734760124923   |",
                "| c  | 13.860958726523545  |",
                "| d  | 8.793968289758968   |",
                "| e  | 10.206140546981722  |",
                "+----+---------------------+"
            ],
            table_results
        );
        Ok(())
    }

    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{plan1:?}"), format!("{plan2:?}"));
    }

    /// Create a logical plan from a SQL query
    async fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = SessionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;
        Ok(ctx.sql(sql).await?.into_unoptimized_plan())
    }

    #[tokio::test]
    async fn with_column() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();
        let df_impl = DataFrame::new(ctx.state(), df.plan.clone());

        let df = df_impl
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .with_column("sum", col("c2") + col("c3"))?;

        // check that new column added
        let df_results = df.clone().collect().await?;

        assert_batches_sorted_eq!(
            [
                "+----+----+-----+-----+",
                "| c1 | c2 | c3  | sum |",
                "+----+----+-----+-----+",
                "| a  | 3  | -12 | -9  |",
                "| a  | 3  | -72 | -69 |",
                "| a  | 3  | 13  | 16  |",
                "| a  | 3  | 13  | 16  |",
                "| a  | 3  | 14  | 17  |",
                "| a  | 3  | 17  | 20  |",
                "+----+----+-----+-----+"
            ],
            &df_results
        );

        // check that col with the same name ovwewritten
        let df_results_overwrite = df
            .clone()
            .with_column("c1", col("c2") + col("c3"))?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+-----+----+-----+-----+",
                "| c1  | c2 | c3  | sum |",
                "+-----+----+-----+-----+",
                "| -69 | 3  | -72 | -69 |",
                "| -9  | 3  | -12 | -9  |",
                "| 16  | 3  | 13  | 16  |",
                "| 16  | 3  | 13  | 16  |",
                "| 17  | 3  | 14  | 17  |",
                "| 20  | 3  | 17  | 20  |",
                "+-----+----+-----+-----+"
            ],
            &df_results_overwrite
        );

        // check that col with the same name ovwewritten using same name as reference
        let df_results_overwrite_self = df
            .clone()
            .with_column("c2", col("c2") + lit(1))?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+----+----+-----+-----+",
                "| c1 | c2 | c3  | sum |",
                "+----+----+-----+-----+",
                "| a  | 4  | -12 | -9  |",
                "| a  | 4  | -72 | -69 |",
                "| a  | 4  | 13  | 16  |",
                "| a  | 4  | 13  | 16  |",
                "| a  | 4  | 14  | 17  |",
                "| a  | 4  | 17  | 20  |",
                "+----+----+-----+-----+"
            ],
            &df_results_overwrite_self
        );

        Ok(())
    }

    // Test issue: https://github.com/apache/arrow-datafusion/issues/7790
    // The join operation outputs two identical column names, but they belong to different relations.
    #[tokio::test]
    async fn with_column_join_same_columns() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1"])?;
        let ctx = SessionContext::new();

        let table = df.into_view();
        ctx.register_table("t1", table.clone())?;
        ctx.register_table("t2", table)?;
        let df = ctx
            .table("t1")
            .await?
            .join(
                ctx.table("t2").await?,
                JoinType::Inner,
                &["c1"],
                &["c1"],
                None,
            )?
            .sort(vec![
                // make the test deterministic
                col("t1.c1").sort(true, true),
            ])?
            .limit(0, Some(1))?;

        let df_results = df.clone().collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+",
                "| c1 | c1 |",
                "+----+----+",
                "| a  | a  |",
                "+----+----+",
            ],
            &df_results
        );

        let df_with_column = df.clone().with_column("new_column", lit(true))?;

        assert_eq!(
            "\
        Projection: t1.c1, t2.c1, Boolean(true) AS new_column\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: t1.c1 ASC NULLS FIRST\
        \n      Inner Join: t1.c1 = t2.c1\
        \n        TableScan: t1\
        \n        TableScan: t2",
            format!("{:?}", df_with_column.logical_plan())
        );

        assert_eq!(
            "\
        Projection: t1.c1, t2.c1, Boolean(true) AS new_column\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: t1.c1 ASC NULLS FIRST, fetch=1\
        \n      Inner Join: t1.c1 = t2.c1\
        \n        SubqueryAlias: t1\
        \n          TableScan: aggregate_test_100 projection=[c1]\
        \n        SubqueryAlias: t2\
        \n          TableScan: aggregate_test_100 projection=[c1]",
            format!("{:?}", df_with_column.clone().into_optimized_plan()?)
        );

        let df_results = df_with_column.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+----+----+------------+",
                "| c1 | c1 | new_column |",
                "+----+----+------------+",
                "| a  | a  | true       |",
                "+----+----+------------+",
            ],
            &df_results
        );
        Ok(())
    }

    // Table 't1' self join
    // Supplementary test of issue: https://github.com/apache/arrow-datafusion/issues/7790
    #[tokio::test]
    async fn with_column_self_join() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1"])?;
        let ctx = SessionContext::new();

        ctx.register_table("t1", df.into_view())?;

        let df = ctx
            .table("t1")
            .await?
            .join(
                ctx.table("t1").await?,
                JoinType::Inner,
                &["c1"],
                &["c1"],
                None,
            )?
            .sort(vec![
                // make the test deterministic
                col("t1.c1").sort(true, true),
            ])?
            .limit(0, Some(1))?;

        let df_results = df.clone().collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+",
                "| c1 | c1 |",
                "+----+----+",
                "| a  | a  |",
                "+----+----+",
            ],
            &df_results
        );

        let actual_err = df.clone().with_column("new_column", lit(true)).unwrap_err();
        let expected_err = "Error during planning: Projections require unique expression names \
            but the expression \"t1.c1\" at position 0 and \"t1.c1\" at position 1 have the same name. \
            Consider aliasing (\"AS\") one of them.";
        assert_eq!(actual_err.strip_backtrace(), expected_err);

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c1", "c2", "c3"])?
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .limit(0, Some(1))?
            .sort(vec![
                // make the test deterministic
                col("c1").sort(true, true),
                col("c2").sort(true, true),
                col("c3").sort(true, true),
            ])?
            .with_column("sum", col("c2") + col("c3"))?;

        let df_sum_renamed = df
            .with_column_renamed("sum", "total")?
            // table qualifier optional
            .with_column_renamed("c1", "one")?
            // accepts table qualifier
            .with_column_renamed("aggregate_test_100.c2", "two")?
            // no-op for missing column
            .with_column_renamed("c4", "boom")?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+-----+-----+----+-------+",
                "| one | two | c3 | total |",
                "+-----+-----+----+-------+",
                "| a   | 3   | 13 | 16    |",
                "+-----+-----+----+-------+"
            ],
            &df_sum_renamed
        );

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_ambiguous() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();

        let table = df.into_view();
        ctx.register_table("t1", table.clone())?;
        ctx.register_table("t2", table)?;

        let actual_err = ctx
            .table("t1")
            .await?
            .join(
                ctx.table("t2").await?,
                JoinType::Inner,
                &["c1"],
                &["c1"],
                None,
            )?
            // can be t1.c2 or t2.c2
            .with_column_renamed("c2", "AAA")
            .unwrap_err();
        let expected_err = "Schema error: Ambiguous reference to unqualified field c2";
        assert_eq!(actual_err.strip_backtrace(), expected_err);

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_join() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();

        let table = df.into_view();
        ctx.register_table("t1", table.clone())?;
        ctx.register_table("t2", table)?;
        let df = ctx
            .table("t1")
            .await?
            .join(
                ctx.table("t2").await?,
                JoinType::Inner,
                &["c1"],
                &["c1"],
                None,
            )?
            .sort(vec![
                // make the test deterministic
                col("t1.c1").sort(true, true),
                col("t1.c2").sort(true, true),
                col("t1.c3").sort(true, true),
                col("t2.c1").sort(true, true),
                col("t2.c2").sort(true, true),
                col("t2.c3").sort(true, true),
            ])?
            .limit(0, Some(1))?;

        let df_results = df.clone().collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+----+----+-----+",
                "| c1 | c2 | c3  | c1 | c2 | c3  |",
                "+----+----+-----+----+----+-----+",
                "| a  | 1  | -85 | a  | 1  | -85 |",
                "+----+----+-----+----+----+-----+"
            ],
            &df_results
        );

        let df_renamed = df.clone().with_column_renamed("t1.c1", "AAA")?;

        assert_eq!("\
        Projection: t1.c1 AS AAA, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: t1.c1 ASC NULLS FIRST, t1.c2 ASC NULLS FIRST, t1.c3 ASC NULLS FIRST, t2.c1 ASC NULLS FIRST, t2.c2 ASC NULLS FIRST, t2.c3 ASC NULLS FIRST\
        \n      Inner Join: t1.c1 = t2.c1\
        \n        TableScan: t1\
        \n        TableScan: t2",
                   format!("{:?}", df_renamed.logical_plan())
        );

        assert_eq!("\
        Projection: t1.c1 AS AAA, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: t1.c1 ASC NULLS FIRST, t1.c2 ASC NULLS FIRST, t1.c3 ASC NULLS FIRST, t2.c1 ASC NULLS FIRST, t2.c2 ASC NULLS FIRST, t2.c3 ASC NULLS FIRST, fetch=1\
        \n      Inner Join: t1.c1 = t2.c1\
        \n        SubqueryAlias: t1\
        \n          TableScan: aggregate_test_100 projection=[c1, c2, c3]\
        \n        SubqueryAlias: t2\
        \n          TableScan: aggregate_test_100 projection=[c1, c2, c3]",
                   format!("{:?}", df_renamed.clone().into_optimized_plan()?)
        );

        let df_results = df_renamed.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+-----+----+-----+----+----+-----+",
                "| AAA | c2 | c3  | c1 | c2 | c3  |",
                "+-----+----+-----+----+----+-----+",
                "| a   | 1  | -85 | a  | 1  | -85 |",
                "+-----+----+-----+----+----+-----+"
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_case_sensitive() -> Result<()> {
        let config =
            SessionConfig::from_string_hash_map(std::collections::HashMap::from([(
                "datafusion.sql_parser.enable_ident_normalization".to_owned(),
                "false".to_owned(),
            )]))?;
        let mut ctx = SessionContext::new_with_config(config);
        let name = "aggregate_test_100";
        register_aggregate_csv(&mut ctx, name).await?;
        let df = ctx.table(name);

        let df = df
            .await?
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .limit(0, Some(1))?
            .sort(vec![
                // make the test deterministic
                col("c1").sort(true, true),
                col("c2").sort(true, true),
                col("c3").sort(true, true),
            ])?
            .select_columns(&["c1"])?;

        let df_renamed = df.clone().with_column_renamed("c1", "CoLuMn1")?;

        let res = &df_renamed.clone().collect().await?;

        assert_batches_sorted_eq!(
            [
                "+---------+",
                "| CoLuMn1 |",
                "+---------+",
                "| a       |",
                "+---------+"
            ],
            res
        );

        let df_renamed = df_renamed
            .with_column_renamed("CoLuMn1", "c1")?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            ["+----+", "| c1 |", "+----+", "| a  |", "+----+"],
            &df_renamed
        );

        Ok(())
    }

    #[tokio::test]
    async fn cast_expr_test() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(1))?
            .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

        let df_results = df.clone().collect().await?;
        df.clone().show().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+",
                "| c2 | c3 | sum |",
                "+----+----+-----+",
                "| 2  | 1  | 3   |",
                "+----+----+-----+"
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn row_writer_resize_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "column_1",
            DataType::Utf8,
            false,
        )]));

        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("2a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
                    Some("3a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800"),
                ]))
            ],
        )?;

        let ctx = SessionContext::new();
        ctx.register_batch("test", data)?;

        let sql = r#"
        SELECT
            COUNT(1)
        FROM
            test
        GROUP BY
            column_1"#;

        let df = ctx.sql(sql).await?;
        df.show_limit(10).await?;

        Ok(())
    }

    #[tokio::test]
    async fn with_column_name() -> Result<()> {
        // define data with a column name that has a "." in it:
        let array: Int32Array = [1, 10].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![("f.c1", Arc::new(array) as _)])?;

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch)?;

        let df = ctx
            .table("t")
            .await?
            // try and create a column with a '.' in it
            .with_column("f.c2", lit("hello"))?;

        let df_results = df.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+------+-------+",
                "| f.c1 | f.c2  |",
                "+------+-------+",
                "| 1    | hello |",
                "| 10   | hello |",
                "+------+-------+"
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_mismatch() -> Result<()> {
        let ctx = SessionContext::new();
        let df = ctx
            .sql("SELECT CASE WHEN true THEN NULL ELSE 1 END")
            .await?;
        let cache_df = df.cache().await;
        assert!(cache_df.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn cache_test() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(1))?
            .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

        let cached_df = df.clone().cache().await?;

        assert_eq!(
            "TableScan: ?table? projection=[c2, c3, sum]",
            format!("{:?}", cached_df.clone().into_optimized_plan()?)
        );

        let df_results = df.collect().await?;
        let cached_df_results = cached_df.collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+",
                "| c2 | c3 | sum |",
                "+----+----+-----+",
                "| 2  | 1  | 3   |",
                "+----+----+-----+"
            ],
            &cached_df_results
        );

        assert_eq!(&df_results, &cached_df_results);

        Ok(())
    }

    #[tokio::test]
    async fn partition_aware_union() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c3"])?
            .with_column_renamed("c2.c1", "c2_c1")?;

        let left_rows = left.clone().collect().await?;
        let right_rows = right.clone().collect().await?;
        let join1 = left.clone().join(
            right.clone(),
            JoinType::Inner,
            &["c1"],
            &["c2_c1"],
            None,
        )?;
        let join2 = left.join(right, JoinType::Inner, &["c1"], &["c2_c1"], None)?;

        let union = join1.union(join2)?;

        let union_rows = union.clone().collect().await?;

        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(4016, union_rows.iter().map(|x| x.num_rows()).sum::<usize>());

        let physical_plan = union.create_physical_plan().await?;
        let default_partition_count = SessionConfig::new().target_partitions();

        // For partition aware union, the output partition count should not be changed.
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            default_partition_count
        );
        // For partition aware union, the output partition is the same with the union's inputs
        for child in physical_plan.children() {
            assert_eq!(
                physical_plan.output_partitioning(),
                child.output_partitioning()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn non_partition_aware_union() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c2"])?
            .with_column_renamed("c2.c1", "c2_c1")?
            .with_column_renamed("c2.c2", "c2_c2")?;

        let left_rows = left.clone().collect().await?;
        let right_rows = right.clone().collect().await?;
        let join1 = left.clone().join(
            right.clone(),
            JoinType::Inner,
            &["c1", "c2"],
            &["c2_c1", "c2_c2"],
            None,
        )?;

        // join key ordering is different
        let join2 = left.join(
            right,
            JoinType::Inner,
            &["c2", "c1"],
            &["c2_c2", "c2_c1"],
            None,
        )?;

        let union = join1.union(join2)?;

        let union_rows = union.clone().collect().await?;

        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(916, union_rows.iter().map(|x| x.num_rows()).sum::<usize>());

        let physical_plan = union.create_physical_plan().await?;
        let default_partition_count = SessionConfig::new().target_partitions();

        // For non-partition aware union, the output partitioning count should be the combination of all output partitions count
        assert!(matches!(
            physical_plan.output_partitioning(),
            Partitioning::UnknownPartitioning(partition_count) if *partition_count == default_partition_count * 2));
        Ok(())
    }

    #[tokio::test]
    async fn verify_join_output_partitioning() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c2"])?
            .with_column_renamed("c2.c1", "c2_c1")?
            .with_column_renamed("c2.c2", "c2_c2")?;

        let all_join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ];

        let default_partition_count = SessionConfig::new().target_partitions();

        for join_type in all_join_types {
            let join = left.clone().join(
                right.clone(),
                join_type,
                &["c1", "c2"],
                &["c2_c1", "c2_c2"],
                None,
            )?;
            let physical_plan = join.create_physical_plan().await?;
            let out_partitioning = physical_plan.output_partitioning();
            let join_schema = physical_plan.schema();

            match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    let left_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                        Arc::new(Column::new_with_schema("c1", &join_schema)?),
                        Arc::new(Column::new_with_schema("c2", &join_schema)?),
                    ];
                    assert_eq!(
                        out_partitioning,
                        &Partitioning::Hash(left_exprs, default_partition_count)
                    );
                }
                JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                    let right_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                        Arc::new(Column::new_with_schema("c2_c1", &join_schema)?),
                        Arc::new(Column::new_with_schema("c2_c2", &join_schema)?),
                    ];
                    assert_eq!(
                        out_partitioning,
                        &Partitioning::Hash(right_exprs, default_partition_count)
                    );
                }
                JoinType::Full => {
                    assert!(matches!(
                        out_partitioning,
                    &Partitioning::UnknownPartitioning(partition_count) if partition_count == default_partition_count));
                }
            }
        }

        Ok(())
    }
    #[tokio::test]
    async fn nested_explain_should_fail() -> Result<()> {
        let ctx = SessionContext::new();
        // must be error
        let mut result = ctx.sql("explain select 1").await?.explain(false, false);
        assert!(result.is_err());
        // must be error
        result = ctx.sql("explain explain select 1").await;
        assert!(result.is_err());
        Ok(())
    }
}
