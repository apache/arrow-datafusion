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

//! [`SessionContext`] API for registering data sources and executing queries

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, Weak};

use super::options::ReadOptions;
use crate::{
    catalog::listing_schema::ListingSchemaProvider,
    catalog::schema::MemorySchemaProvider,
    catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider},
    dataframe::DataFrame,
    datasource::{
        function::{TableFunction, TableFunctionImpl},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        provider::TableProviderFactory,
    },
    datasource::{provider_as_source, MemTable, TableProvider, ViewTable},
    error::{DataFusionError, Result},
    execution::{options::ArrowReadOptions, runtime_env::RuntimeEnv, FunctionRegistry},
    logical_expr::AggregateUDF,
    logical_expr::ScalarUDF,
    logical_expr::{
        CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateFunction,
        CreateMemoryTable, CreateView, DropCatalogSchema, DropFunction, DropTable,
        DropView, LogicalPlan, LogicalPlanBuilder, SetVariable, TableType, UNNAMED_TABLE,
    },
    physical_expr::PhysicalExpr,
    physical_plan::ExecutionPlan,
    variable::{VarProvider, VarType},
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use datafusion_common::{
    config::{ConfigExtension, TableOptions},
    exec_err, not_impl_err, plan_datafusion_err, plan_err,
    tree_node::{TreeNodeRecursion, TreeNodeVisitor},
    DFSchema, SchemaReference, TableReference,
};
use datafusion_execution::registry::SerializerRegistry;
use datafusion_expr::{
    expr_rewriter::FunctionRewrite,
    logical_plan::{DdlStatement, Statement},
    Expr, UserDefinedLogicalNode, WindowUDF,
};

// backwards compatibility
pub use crate::execution::session_state::SessionState;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use parking_lot::RwLock;
use url::Url;

pub use datafusion_execution::config::SessionConfig;
pub use datafusion_execution::TaskContext;
pub use datafusion_expr::execution_props::ExecutionProps;
use datafusion_optimizer::{AnalyzerRule, OptimizerRule};

mod avro;
mod csv;
mod json;
#[cfg(feature = "parquet")]
mod parquet;

/// DataFilePaths adds a method to convert strings and vector of strings to vector of [`ListingTableUrl`] URLs.
/// This allows methods such [`SessionContext::read_csv`] and [`SessionContext::read_avro`]
/// to take either a single file or multiple files.
pub trait DataFilePaths {
    /// Parse to a vector of [`ListingTableUrl`] URLs.
    fn to_urls(self) -> Result<Vec<ListingTableUrl>>;
}

impl DataFilePaths for &str {
    fn to_urls(self) -> Result<Vec<ListingTableUrl>> {
        Ok(vec![ListingTableUrl::parse(self)?])
    }
}

impl DataFilePaths for String {
    fn to_urls(self) -> Result<Vec<ListingTableUrl>> {
        Ok(vec![ListingTableUrl::parse(self)?])
    }
}

impl DataFilePaths for &String {
    fn to_urls(self) -> Result<Vec<ListingTableUrl>> {
        Ok(vec![ListingTableUrl::parse(self)?])
    }
}

impl<P> DataFilePaths for Vec<P>
where
    P: AsRef<str>,
{
    fn to_urls(self) -> Result<Vec<ListingTableUrl>> {
        self.iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<ListingTableUrl>>>()
    }
}

/// Main interface for executing queries with DataFusion. Maintains
/// the state of the connection between a user and an instance of the
/// DataFusion engine.
///
/// # Overview
///
/// [`SessionContext`] provides the following functionality:
///
/// * Create a [`DataFrame`] from a CSV or Parquet data source.
/// * Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
/// * Register a custom data source that can be referenced from a SQL query.
/// * Execution a SQL query
///
/// # Example: DataFrame API
///
/// The following example demonstrates how to use the context to execute a query against a CSV
/// data source using the [`DataFrame`] API:
///
/// ```
/// use datafusion::prelude::*;
/// # use datafusion::{error::Result, assert_batches_eq};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(0, Some(100))?;
/// let results = df
///   .collect()
///   .await?;
/// assert_batches_eq!(
///  &[
///    "+---+----------------+",
///    "| a | MIN(?table?.b) |",
///    "+---+----------------+",
///    "| 1 | 2              |",
///    "+---+----------------+",
///  ],
///  &results
/// );
/// # Ok(())
/// # }
/// ```
///
/// # Example: SQL API
///
/// The following example demonstrates how to execute the same query using SQL:
///
/// ```
/// use datafusion::prelude::*;
/// # use datafusion::{error::Result, assert_batches_eq};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut ctx = SessionContext::new();
/// ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
/// let results = ctx
///   .sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100")
///   .await?
///   .collect()
///   .await?;
/// assert_batches_eq!(
///  &[
///    "+---+----------------+",
///    "| a | MIN(example.b) |",
///    "+---+----------------+",
///    "| 1 | 2              |",
///    "+---+----------------+",
///  ],
///  &results
/// );
/// # Ok(())
/// # }
/// ```
///
/// # `SessionContext`, `SessionState`, and `TaskContext`
///
/// The state required to optimize, and evaluate queries is
/// broken into three levels to allow tailoring
///
/// The objects are:
///
/// 1. [`SessionContext`]: Most users should use a `SessionContext`. It contains
/// all information required to execute queries including  high level APIs such
/// as [`SessionContext::sql`]. All queries run with the same `SessionContext`
/// share the same configuration and resources (e.g. memory limits).
///
/// 2. [`SessionState`]: contains information required to plan and execute an
/// individual query (e.g. creating a [`LogicalPlan`] or [`ExecutionPlan`]).
/// Each query is planned and executed using its own `SessionState`, which can
/// be created with [`SessionContext::state`]. `SessionState` allows finer
/// grained control over query execution, for example disallowing DDL operations
/// such as `CREATE TABLE`.
///
/// 3. [`TaskContext`] contains the state required for query execution (e.g.
/// [`ExecutionPlan::execute`]). It contains a subset of information in
/// [`SessionState`]. `TaskContext` allows executing [`ExecutionPlan`]s
/// [`PhysicalExpr`]s without requiring a full [`SessionState`].
///
/// [`PhysicalExpr`]: crate::physical_expr::PhysicalExpr
#[derive(Clone)]
pub struct SessionContext {
    /// UUID for the session
    session_id: String,
    /// Session start time
    session_start_time: DateTime<Utc>,
    /// Shared session state for the session
    state: Arc<RwLock<SessionState>>,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContext {
    /// Creates a new `SessionContext` using the default [`SessionConfig`].
    pub fn new() -> Self {
        Self::new_with_config(SessionConfig::new())
    }

    /// Finds any [`ListingSchemaProvider`]s and instructs them to reload tables from "disk"
    pub async fn refresh_catalogs(&self) -> Result<()> {
        let cat_names = self.catalog_names().clone();
        for cat_name in cat_names.iter() {
            let cat = self.catalog(cat_name.as_str()).ok_or_else(|| {
                DataFusionError::Internal("Catalog not found!".to_string())
            })?;
            for schema_name in cat.schema_names() {
                let schema = cat.schema(schema_name.as_str()).ok_or_else(|| {
                    DataFusionError::Internal("Schema not found!".to_string())
                })?;
                let lister = schema.as_any().downcast_ref::<ListingSchemaProvider>();
                if let Some(lister) = lister {
                    lister.refresh(&self.state()).await?;
                }
            }
        }
        Ok(())
    }

    /// Creates a new `SessionContext` using the provided
    /// [`SessionConfig`] and a new [`RuntimeEnv`].
    ///
    /// See [`Self::new_with_config_rt`] for more details on resource
    /// limits.
    pub fn new_with_config(config: SessionConfig) -> Self {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::new_with_config_rt(config, runtime)
    }

    /// Creates a new `SessionContext` using the provided
    /// [`SessionConfig`] and a new [`RuntimeEnv`].
    #[deprecated(since = "32.0.0", note = "Use SessionContext::new_with_config")]
    pub fn with_config(config: SessionConfig) -> Self {
        Self::new_with_config(config)
    }

    /// Creates a new `SessionContext` using the provided
    /// [`SessionConfig`] and a [`RuntimeEnv`].
    ///
    /// # Resource Limits
    ///
    /// By default, each new `SessionContext` creates a new
    /// `RuntimeEnv`, and therefore will not enforce memory or disk
    /// limits for queries run on different `SessionContext`s.
    ///
    /// To enforce resource limits (e.g. to limit the total amount of
    /// memory used) across all DataFusion queries in a process,
    /// all `SessionContext`'s should be configured with the
    /// same `RuntimeEnv`.
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let state = SessionState::new_with_config_rt(config, runtime);
        Self::new_with_state(state)
    }

    /// Creates a new `SessionContext` using the provided
    /// [`SessionConfig`] and a [`RuntimeEnv`].
    #[deprecated(since = "32.0.0", note = "Use SessionState::new_with_config_rt")]
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self::new_with_config_rt(config, runtime)
    }

    /// Creates a new `SessionContext` using the provided [`SessionState`]
    pub fn new_with_state(state: SessionState) -> Self {
        Self {
            session_id: state.session_id().to_string(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Creates a new `SessionContext` using the provided [`SessionState`]
    #[deprecated(since = "32.0.0", note = "Use SessionState::new_with_state")]
    pub fn with_state(state: SessionState) -> Self {
        Self::new_with_state(state)
    }
    /// Returns the time this `SessionContext` was created
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time
    }

    /// Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
    pub fn with_function_factory(
        self,
        function_factory: Arc<dyn FunctionFactory>,
    ) -> Self {
        self.state.write().set_function_factory(function_factory);
        self
    }

    /// Adds an optimizer rule to the end of the existing rules.
    ///
    /// See [`SessionState`] for more control of when the rule is applied.
    pub fn add_optimizer_rule(
        &self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) {
        self.state.write().append_optimizer_rule(optimizer_rule);
    }

    /// Adds an analyzer rule to the end of the existing rules.
    ///
    /// See [`SessionState`] for more control of when the rule is applied.
    pub fn add_analyzer_rule(&self, analyzer_rule: Arc<dyn AnalyzerRule + Send + Sync>) {
        self.state.write().add_analyzer_rule(analyzer_rule);
    }

    /// Registers an [`ObjectStore`] to be used with a specific URL prefix.
    ///
    /// See [`RuntimeEnv::register_object_store`] for more details.
    ///
    /// # Example: register a local object store for the "file://" URL prefix
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion_execution::object_store::ObjectStoreUrl;
    /// let object_store_url = ObjectStoreUrl::parse("file://").unwrap();
    /// let object_store = object_store::local::LocalFileSystem::new();
    /// let mut ctx = SessionContext::new();
    /// // All files with the file:// url prefix will be read from the local file system
    /// ctx.register_object_store(object_store_url.as_ref(), Arc::new(object_store));
    /// ```
    pub fn register_object_store(
        &self,
        url: &Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.runtime_env().register_object_store(url, object_store)
    }

    /// Registers the [`RecordBatch`] as the specified table name
    pub fn register_batch(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        self.register_table(
            TableReference::Bare {
                table: table_name.into(),
            },
            Arc::new(table),
        )
    }

    /// Return the [RuntimeEnv] used to run queries with this `SessionContext`
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        Arc::clone(self.state.read().runtime_env())
    }

    /// Returns an id that uniquely identifies this `SessionContext`.
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the [`TableProviderFactory`] that is registered for the
    /// specified file type, if any.
    pub fn table_factory(
        &self,
        file_type: &str,
    ) -> Option<Arc<dyn TableProviderFactory>> {
        self.state.read().table_factories().get(file_type).cloned()
    }

    /// Return the `enable_ident_normalization` of this Session
    pub fn enable_ident_normalization(&self) -> bool {
        self.state
            .read()
            .config()
            .options()
            .sql_parser
            .enable_ident_normalization
    }

    /// Return a copied version of config for this Session
    pub fn copied_config(&self) -> SessionConfig {
        self.state.read().config().clone()
    }

    /// Return a copied version of table options for this Session
    pub fn copied_table_options(&self) -> TableOptions {
        self.state.read().default_table_options()
    }

    /// Creates a [`DataFrame`] from SQL query text.
    ///
    /// Note: This API implements DDL statements such as `CREATE TABLE` and
    /// `CREATE VIEW` and DML statements such as `INSERT INTO` with in-memory
    /// default implementations. See [`Self::sql_with_options`].
    ///
    /// # Example: Running SQL queries
    ///
    /// See the example on [`Self`]
    ///
    /// # Example: Creating a Table with SQL
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// # use datafusion::{error::Result, assert_batches_eq};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = SessionContext::new();
    /// ctx
    ///   .sql("CREATE TABLE foo (x INTEGER)")
    ///   .await?
    ///   .collect()
    ///   .await?;
    /// assert!(ctx.table_exist("foo").unwrap());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.sql_with_options(sql, SQLOptions::new()).await
    }

    /// Creates a [`DataFrame`] from SQL query text, first validating
    /// that the queries are allowed by `options`
    ///
    /// # Example: Preventing Creating a Table with SQL
    ///
    /// If you want to avoid creating tables, or modifying data or the
    /// session, set [`SQLOptions`] appropriately:
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// # use datafusion::{error::Result};
    /// # use datafusion::physical_plan::collect;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let mut ctx = SessionContext::new();
    /// let options = SQLOptions::new()
    ///   .with_allow_ddl(false);
    /// let err = ctx.sql_with_options("CREATE TABLE foo (x INTEGER)", options)
    ///   .await
    ///   .unwrap_err();
    /// assert!(
    ///   err.to_string().starts_with("Error during planning: DDL not supported: CreateMemoryTable")
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sql_with_options(
        &self,
        sql: &str,
        options: SQLOptions,
    ) -> Result<DataFrame> {
        let plan = self.state().create_logical_plan(sql).await?;
        options.verify_plan(&plan)?;

        self.execute_logical_plan(plan).await
    }

    /// Creates logical expresssions from SQL query text.
    ///
    /// # Example: Parsing SQL queries
    ///
    /// ```
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion::prelude::*;
    /// # use datafusion_common::{DFSchema, Result};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // datafusion will parse number as i64 first.
    /// let sql = "a > 10";
    /// let expected = col("a").gt(lit(10 as i64));
    /// // provide type information that `a` is an Int32
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    /// let df_schema = DFSchema::try_from(schema).unwrap();
    /// let expr = SessionContext::new()
    ///  .parse_sql_expr(sql, &df_schema)?;
    /// assert_eq!(expected, expr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse_sql_expr(&self, sql: &str, df_schema: &DFSchema) -> Result<Expr> {
        self.state.read().create_logical_expr(sql, df_schema)
    }

    /// Execute the [`LogicalPlan`], return a [`DataFrame`]. This API
    /// is not featured limited (so all SQL such as `CREATE TABLE` and
    /// `COPY` will be run).
    ///
    /// If you wish to limit the type of plan that can be run from
    /// SQL, see [`Self::sql_with_options`] and
    /// [`SQLOptions::verify_plan`].
    pub async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame> {
        match plan {
            LogicalPlan::Ddl(ddl) => {
                // Box::pin avoids allocating the stack space within this function's frame
                // for every one of these individual async functions, decreasing the risk of
                // stack overflows.
                match ddl {
                    DdlStatement::CreateExternalTable(cmd) => {
                        Box::pin(async move { self.create_external_table(&cmd).await })
                            as std::pin::Pin<Box<dyn futures::Future<Output = _> + Send>>
                    }
                    DdlStatement::CreateMemoryTable(cmd) => {
                        Box::pin(self.create_memory_table(cmd))
                    }
                    DdlStatement::CreateView(cmd) => Box::pin(self.create_view(cmd)),
                    DdlStatement::CreateCatalogSchema(cmd) => {
                        Box::pin(self.create_catalog_schema(cmd))
                    }
                    DdlStatement::CreateCatalog(cmd) => {
                        Box::pin(self.create_catalog(cmd))
                    }
                    DdlStatement::DropTable(cmd) => Box::pin(self.drop_table(cmd)),
                    DdlStatement::DropView(cmd) => Box::pin(self.drop_view(cmd)),
                    DdlStatement::DropCatalogSchema(cmd) => {
                        Box::pin(self.drop_schema(cmd))
                    }
                    DdlStatement::CreateFunction(cmd) => {
                        Box::pin(self.create_function(cmd))
                    }
                    DdlStatement::DropFunction(cmd) => Box::pin(self.drop_function(cmd)),
                }
                .await
            }
            // TODO what about the other statements (like TransactionStart and TransactionEnd)
            LogicalPlan::Statement(Statement::SetVariable(stmt)) => {
                self.set_variable(stmt).await
            }

            plan => Ok(DataFrame::new(self.state(), plan)),
        }
    }

    /// Create a [`PhysicalExpr`] from an [`Expr`] after applying type
    /// coercion and function rewrites.
    ///
    /// Note: The expression is not [simplified] or otherwise optimized:  `a = 1
    /// + 2` will not be simplified to `a = 3` as this is a more involved process.
    /// See the [expr_api] example for how to simplify expressions.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion::prelude::*;
    /// # use datafusion_common::DFSchema;
    /// // a = 1 (i64)
    /// let expr = col("a").eq(lit(1i64));
    /// // provide type information that `a` is an Int32
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    /// let df_schema = DFSchema::try_from(schema).unwrap();
    /// // Create a PhysicalExpr. Note DataFusion automatically coerces (casts) `1i64` to `1i32`
    /// let physical_expr = SessionContext::new()
    ///   .create_physical_expr(expr, &df_schema).unwrap();
    /// ```
    /// # See Also
    /// * [`SessionState::create_physical_expr`] for a lower level API
    ///
    /// [simplified]: datafusion_optimizer::simplify_expressions
    /// [expr_api]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs
    pub fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.state.read().create_physical_expr(expr, df_schema)
    }

    // return an empty dataframe
    fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.state(), plan))
    }

    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<DataFrame> {
        let exist = self.table_exist(cmd.name.clone())?;
        if exist {
            match cmd.if_not_exists {
                true => return self.return_empty_dataframe(),
                false => {
                    return exec_err!("Table '{}' already exists", cmd.name);
                }
            }
        }

        let table_provider: Arc<dyn TableProvider> =
            self.create_custom_table(cmd).await?;
        self.register_table(cmd.name.clone(), table_provider)?;
        self.return_empty_dataframe()
    }

    async fn create_memory_table(&self, cmd: CreateMemoryTable) -> Result<DataFrame> {
        let CreateMemoryTable {
            name,
            input,
            if_not_exists,
            or_replace,
            constraints,
            column_defaults,
        } = cmd;

        let input = Arc::try_unwrap(input).unwrap_or_else(|e| e.as_ref().clone());
        let input = self.state().optimize(&input)?;
        let table = self.table(name.clone()).await;
        match (if_not_exists, or_replace, table) {
            (true, false, Ok(_)) => self.return_empty_dataframe(),
            (false, true, Ok(_)) => {
                self.deregister_table(name.clone())?;
                let schema = Arc::new(input.schema().as_ref().into());
                let physical = DataFrame::new(self.state(), input);

                let batches: Vec<_> = physical.collect_partitioned().await?;
                let table = Arc::new(
                    // pass constraints and column defaults to the mem table.
                    MemTable::try_new(schema, batches)?
                        .with_constraints(constraints)
                        .with_column_defaults(column_defaults.into_iter().collect()),
                );

                self.register_table(name.clone(), table)?;
                self.return_empty_dataframe()
            }
            (true, true, Ok(_)) => {
                exec_err!("'IF NOT EXISTS' cannot coexist with 'REPLACE'")
            }
            (_, _, Err(_)) => {
                let df_schema = input.schema();
                let schema = Arc::new(df_schema.as_ref().into());
                let physical = DataFrame::new(self.state(), input);

                let batches: Vec<_> = physical.collect_partitioned().await?;
                let table = Arc::new(
                    // pass constraints and column defaults to the mem table.
                    MemTable::try_new(schema, batches)?
                        .with_constraints(constraints)
                        .with_column_defaults(column_defaults.into_iter().collect()),
                );

                self.register_table(name, table)?;
                self.return_empty_dataframe()
            }
            (false, false, Ok(_)) => exec_err!("Table '{name}' already exists"),
        }
    }

    async fn create_view(&self, cmd: CreateView) -> Result<DataFrame> {
        let CreateView {
            name,
            input,
            or_replace,
            definition,
        } = cmd;

        let view = self.table(name.clone()).await;

        match (or_replace, view) {
            (true, Ok(_)) => {
                self.deregister_table(name.clone())?;
                let table = Arc::new(ViewTable::try_new((*input).clone(), definition)?);

                self.register_table(name, table)?;
                self.return_empty_dataframe()
            }
            (_, Err(_)) => {
                let table = Arc::new(ViewTable::try_new((*input).clone(), definition)?);

                self.register_table(name, table)?;
                self.return_empty_dataframe()
            }
            (false, Ok(_)) => exec_err!("Table '{name}' already exists"),
        }
    }

    async fn create_catalog_schema(&self, cmd: CreateCatalogSchema) -> Result<DataFrame> {
        let CreateCatalogSchema {
            schema_name,
            if_not_exists,
            ..
        } = cmd;

        // sqlparser doesnt accept database / catalog as parameter to CREATE SCHEMA
        // so for now, we default to default catalog
        let tokens: Vec<&str> = schema_name.split('.').collect();
        let (catalog, schema_name) = match tokens.len() {
            1 => {
                let state = self.state.read();
                let name = &state.config().options().catalog.default_catalog;
                let catalog = state.catalog_list().catalog(name).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Missing default catalog '{name}'"
                    ))
                })?;
                (catalog, tokens[0])
            }
            2 => {
                let name = &tokens[0];
                let catalog = self.catalog(name).ok_or_else(|| {
                    DataFusionError::Execution(format!("Missing catalog '{name}'"))
                })?;
                (catalog, tokens[1])
            }
            _ => return exec_err!("Unable to parse catalog from {schema_name}"),
        };
        let schema = catalog.schema(schema_name);

        match (if_not_exists, schema) {
            (true, Some(_)) => self.return_empty_dataframe(),
            (true, None) | (false, None) => {
                let schema = Arc::new(MemorySchemaProvider::new());
                catalog.register_schema(schema_name, schema)?;
                self.return_empty_dataframe()
            }
            (false, Some(_)) => exec_err!("Schema '{schema_name}' already exists"),
        }
    }

    async fn create_catalog(&self, cmd: CreateCatalog) -> Result<DataFrame> {
        let CreateCatalog {
            catalog_name,
            if_not_exists,
            ..
        } = cmd;
        let catalog = self.catalog(catalog_name.as_str());

        match (if_not_exists, catalog) {
            (true, Some(_)) => self.return_empty_dataframe(),
            (true, None) | (false, None) => {
                let new_catalog = Arc::new(MemoryCatalogProvider::new());
                self.state
                    .write()
                    .catalog_list()
                    .register_catalog(catalog_name, new_catalog);
                self.return_empty_dataframe()
            }
            (false, Some(_)) => exec_err!("Catalog '{catalog_name}' already exists"),
        }
    }

    async fn drop_table(&self, cmd: DropTable) -> Result<DataFrame> {
        let DropTable {
            name, if_exists, ..
        } = cmd;
        let result = self
            .find_and_deregister(name.clone(), TableType::Base)
            .await;
        match (result, if_exists) {
            (Ok(true), _) => self.return_empty_dataframe(),
            (_, true) => self.return_empty_dataframe(),
            (_, _) => exec_err!("Table '{name}' doesn't exist."),
        }
    }

    async fn drop_view(&self, cmd: DropView) -> Result<DataFrame> {
        let DropView {
            name, if_exists, ..
        } = cmd;
        let result = self
            .find_and_deregister(name.clone(), TableType::View)
            .await;
        match (result, if_exists) {
            (Ok(true), _) => self.return_empty_dataframe(),
            (_, true) => self.return_empty_dataframe(),
            (_, _) => exec_err!("View '{name}' doesn't exist."),
        }
    }

    async fn drop_schema(&self, cmd: DropCatalogSchema) -> Result<DataFrame> {
        let DropCatalogSchema {
            name,
            if_exists: allow_missing,
            cascade,
            schema: _,
        } = cmd;
        let catalog = {
            let state = self.state.read();
            let catalog_name = match &name {
                SchemaReference::Full { catalog, .. } => catalog.to_string(),
                SchemaReference::Bare { .. } => {
                    state.config_options().catalog.default_catalog.to_string()
                }
            };
            if let Some(catalog) = state.catalog_list().catalog(&catalog_name) {
                catalog
            } else if allow_missing {
                return self.return_empty_dataframe();
            } else {
                return self.schema_doesnt_exist_err(name);
            }
        };
        let dereg = catalog.deregister_schema(name.schema_name(), cascade)?;
        match (dereg, allow_missing) {
            (None, true) => self.return_empty_dataframe(),
            (None, false) => self.schema_doesnt_exist_err(name),
            (Some(_), _) => self.return_empty_dataframe(),
        }
    }

    fn schema_doesnt_exist_err(&self, schemaref: SchemaReference) -> Result<DataFrame> {
        exec_err!("Schema '{schemaref}' doesn't exist.")
    }

    async fn set_variable(&self, stmt: SetVariable) -> Result<DataFrame> {
        let SetVariable {
            variable, value, ..
        } = stmt;

        let mut state = self.state.write();
        state.config_mut().options_mut().set(&variable, &value)?;
        drop(state);

        self.return_empty_dataframe()
    }

    async fn create_custom_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.state.read().clone();
        let file_type = cmd.file_type.to_uppercase();
        let factory =
            state
                .table_factories()
                .get(file_type.as_str())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Unable to find factory for {}",
                        cmd.file_type
                    ))
                })?;
        let table = (*factory).create(&state, cmd).await?;
        Ok(table)
    }

    async fn find_and_deregister<'a>(
        &self,
        table_ref: impl Into<TableReference>,
        table_type: TableType,
    ) -> Result<bool> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_owned();
        let maybe_schema = {
            let state = self.state.read();
            let resolved = state.resolve_table_ref(table_ref);
            state
                .catalog_list()
                .catalog(&resolved.catalog)
                .and_then(|c| c.schema(&resolved.schema))
        };

        if let Some(schema) = maybe_schema {
            if let Some(table_provider) = schema.table(&table).await? {
                if table_provider.table_type() == table_type {
                    schema.deregister_table(&table)?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    async fn create_function(&self, stmt: CreateFunction) -> Result<DataFrame> {
        let function = {
            let state = self.state.read().clone();
            let function_factory = state.function_factory();

            match function_factory {
                Some(f) => f.create(&state, stmt).await?,
                _ => Err(DataFusionError::Configuration(
                    "Function factory has not been configured".into(),
                ))?,
            }
        };

        match function {
            RegisterFunction::Scalar(f) => {
                self.state.write().register_udf(f)?;
            }
            RegisterFunction::Aggregate(f) => {
                self.state.write().register_udaf(f)?;
            }
            RegisterFunction::Window(f) => {
                self.state.write().register_udwf(f)?;
            }
            RegisterFunction::Table(name, f) => self.register_udtf(&name, f),
        };

        self.return_empty_dataframe()
    }

    async fn drop_function(&self, stmt: DropFunction) -> Result<DataFrame> {
        // we don't know function type at this point
        // decision has been made to drop all functions
        let mut dropped = false;
        dropped |= self.state.write().deregister_udf(&stmt.name)?.is_some();
        dropped |= self.state.write().deregister_udaf(&stmt.name)?.is_some();
        dropped |= self.state.write().deregister_udwf(&stmt.name)?.is_some();
        dropped |= self.state.write().deregister_udtf(&stmt.name)?.is_some();

        // DROP FUNCTION IF EXISTS drops the specified function only if that
        // function exists and in this way, it avoids error. While the DROP FUNCTION
        // statement also performs the same function, it throws an
        // error if the function does not exist.

        if !stmt.if_exists && !dropped {
            exec_err!("Function does not exist")
        } else {
            self.return_empty_dataframe()
        }
    }

    /// Registers a variable provider within this context.
    pub fn register_variable(
        &self,
        variable_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) {
        self.state
            .write()
            .execution_props_mut()
            .add_var_provider(variable_type, provider);
    }

    /// Register a table UDF with this context
    pub fn register_udtf(&self, name: &str, fun: Arc<dyn TableFunctionImpl>) {
        self.state.write().register_udtf(name, fun)
    }

    /// Registers a scalar UDF within this context.
    ///
    /// Note in SQL queries, function names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// - `SELECT MY_FUNC(x)...` will look for a function named `"my_func"`
    /// - `SELECT "my_FUNC"(x)` will look for a function named `"my_FUNC"`
    /// Any functions registered with the udf name or its aliases will be overwritten with this new function
    pub fn register_udf(&self, f: ScalarUDF) {
        let mut state = self.state.write();
        state.register_udf(Arc::new(f)).ok();
    }

    /// Registers an aggregate UDF within this context.
    ///
    /// Note in SQL queries, aggregate names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// - `SELECT MY_UDAF(x)...` will look for an aggregate named `"my_udaf"`
    /// - `SELECT "my_UDAF"(x)` will look for an aggregate named `"my_UDAF"`
    pub fn register_udaf(&self, f: AggregateUDF) {
        self.state.write().register_udaf(Arc::new(f)).ok();
    }

    /// Registers a window UDF within this context.
    ///
    /// Note in SQL queries, window function names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// - `SELECT MY_UDWF(x)...` will look for a window function named `"my_udwf"`
    /// - `SELECT "my_UDWF"(x)` will look for a window function named `"my_UDWF"`
    pub fn register_udwf(&self, f: WindowUDF) {
        self.state.write().register_udwf(Arc::new(f)).ok();
    }

    /// Deregisters a UDF within this context.
    pub fn deregister_udf(&self, name: &str) {
        self.state.write().deregister_udf(name).ok();
    }

    /// Deregisters a UDAF within this context.
    pub fn deregister_udaf(&self, name: &str) {
        self.state.write().deregister_udaf(name).ok();
    }

    /// Deregisters a UDWF within this context.
    pub fn deregister_udwf(&self, name: &str) {
        self.state.write().deregister_udwf(name).ok();
    }

    /// Deregisters a UDTF within this context.
    pub fn deregister_udtf(&self, name: &str) {
        self.state.write().deregister_udtf(name).ok();
    }

    /// Creates a [`DataFrame`] for reading a data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`ListingTable`].
    async fn _read_type<'a, P: DataFilePaths>(
        &self,
        table_paths: P,
        options: impl ReadOptions<'a>,
    ) -> Result<DataFrame> {
        let table_paths = table_paths.to_urls()?;
        let session_config = self.copied_config();
        let listing_options =
            options.to_listing_options(&session_config, self.copied_table_options());

        let option_extension = listing_options.file_extension.clone();

        if table_paths.is_empty() {
            return exec_err!("No table paths were provided");
        }

        // check if the file extension matches the expected extension
        for path in &table_paths {
            let file_path = path.as_str();
            if !file_path.ends_with(option_extension.clone().as_str())
                && !path.is_collection()
            {
                return exec_err!(
                    "File path '{file_path}' does not match the expected extension '{option_extension}'"
                );
            }
        }

        let resolved_schema = options
            .get_resolved_schema(&session_config, self.state(), table_paths[0].clone())
            .await?;
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;
        self.read_table(Arc::new(provider))
    }

    /// Creates a [`DataFrame`] for reading an Arrow data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`ListingTable`].
    ///
    /// For an example, see [`read_csv`](Self::read_csv)
    pub async fn read_arrow<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: ArrowReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
    }

    /// Creates an empty DataFrame.
    pub fn read_empty(&self) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::empty(true).build()?,
        ))
    }

    /// Creates a [`DataFrame`] for a [`TableProvider`] such as a
    /// [`ListingTable`] or a custom user defined provider.
    pub fn read_table(&self, provider: Arc<dyn TableProvider>) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(provider), None)?
                .build()?,
        ))
    }

    /// Creates a [`DataFrame`] for reading a [`RecordBatch`]
    pub fn read_batch(&self, batch: RecordBatch) -> Result<DataFrame> {
        let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        ))
    }
    /// Create a [`DataFrame`] for reading a [`Vec[`RecordBatch`]`]
    pub fn read_batches(
        &self,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<DataFrame> {
        // check schema uniqueness
        let mut batches = batches.into_iter().peekable();
        let schema = if let Some(batch) = batches.peek() {
            Arc::clone(&batch.schema())
        } else {
            Arc::new(Schema::empty())
        };
        let provider = MemTable::try_new(schema, vec![batches.collect()])?;
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        ))
    }
    /// Registers a [`ListingTable`] that can assemble multiple files
    /// from locations in an [`ObjectStore`] instance into a single
    /// table.
    ///
    /// This method is `async` because it might need to resolve the schema.
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    pub async fn register_listing_table(
        &self,
        name: &str,
        table_path: impl AsRef<str>,
        options: ListingOptions,
        provided_schema: Option<SchemaRef>,
        sql_definition: Option<String>,
    ) -> Result<()> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let resolved_schema = match provided_schema {
            Some(s) => s,
            None => options.infer_schema(&self.state(), &table_path).await?,
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?.with_definition(sql_definition);
        self.register_table(
            TableReference::Bare { table: name.into() },
            Arc::new(table),
        )?;
        Ok(())
    }

    /// Registers an Arrow file as a table that can be referenced from
    /// SQL statements executed against this context.
    pub async fn register_arrow(
        &self,
        name: &str,
        table_path: &str,
        options: ArrowReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options
            .to_listing_options(&self.copied_config(), self.copied_table_options());

        self.register_listing_table(
            name,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;
        Ok(())
    }

    /// Registers a named catalog using a custom `CatalogProvider` so that
    /// it can be referenced from SQL statements executed against this
    /// context.
    ///
    /// Returns the [`CatalogProvider`] previously registered for this
    /// name, if any
    pub fn register_catalog(
        &self,
        name: impl Into<String>,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let name = name.into();
        self.state
            .read()
            .catalog_list()
            .register_catalog(name, catalog)
    }

    /// Retrieves the list of available catalog names.
    pub fn catalog_names(&self) -> Vec<String> {
        self.state.read().catalog_list().catalog_names()
    }

    /// Retrieves a [`CatalogProvider`] instance by name
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.state.read().catalog_list().catalog(name)
    }

    /// Registers a [`TableProvider`] as a table that can be
    /// referenced from SQL statements executed against this context.
    ///
    /// Returns the [`TableProvider`] previously registered for this
    /// reference, if any
    pub fn register_table(
        &self,
        table_ref: impl Into<TableReference>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref: TableReference = table_ref.into();
        let table = table_ref.table().to_owned();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table, provider)
    }

    /// Deregisters the given table.
    ///
    /// Returns the registered provider, if any
    pub fn deregister_table(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_owned();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .deregister_table(&table)
    }

    /// Return `true` if the specified table exists in the schema provider.
    pub fn table_exist(&self, table_ref: impl Into<TableReference>) -> Result<bool> {
        let table_ref: TableReference = table_ref.into();
        let table = table_ref.table();
        let table_ref = table_ref.clone();
        Ok(self
            .state
            .read()
            .schema_for_ref(table_ref)?
            .table_exist(table))
    }

    /// Retrieves a [`DataFrame`] representing a table previously
    /// registered by calling the [`register_table`] function.
    ///
    /// Returns an error if no table has been registered with the
    /// provided reference.
    ///
    /// [`register_table`]: SessionContext::register_table
    pub async fn table<'a>(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> Result<DataFrame> {
        let table_ref: TableReference = table_ref.into();
        let provider = self.table_provider(table_ref.clone()).await?;
        let plan = LogicalPlanBuilder::scan(
            table_ref,
            provider_as_source(Arc::clone(&provider)),
            None,
        )?
        .build()?;
        Ok(DataFrame::new(self.state(), plan))
    }

    /// Retrieves a [`TableFunction`] reference by name.
    ///
    /// Returns an error if no table function has been registered with the provided name.
    ///
    /// [`register_udtf`]: SessionContext::register_udtf
    pub fn table_function(&self, name: &str) -> Result<Arc<TableFunction>> {
        self.state
            .read()
            .table_functions()
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Table function '{name}' not found"))
    }

    /// Return a [`TableProvider`] for the specified table.
    pub async fn table_provider<'a>(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_string();
        let schema = self.state.read().schema_for_ref(table_ref)?;
        match schema.table(&table).await? {
            Some(ref provider) => Ok(Arc::clone(provider)),
            _ => plan_err!("No table named '{table}'"),
        }
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }

    /// Return a new  [`SessionState`] suitable for executing a single query.
    ///
    /// Notes:
    ///
    /// 1. `query_execution_start_time` is set to the current time for the
    /// returned state.
    ///
    /// 2. The returned state is not shared with the current session state
    /// and this changes to the returned `SessionState` such as changing
    /// [`ConfigOptions`] will not be reflected in this `SessionContext`.
    ///
    /// [`ConfigOptions`]: crate::config::ConfigOptions
    pub fn state(&self) -> SessionState {
        let mut state = self.state.read().clone();
        state.execution_props_mut().start_execution();
        state
    }

    /// Get reference to [`SessionState`]
    pub fn state_ref(&self) -> Arc<RwLock<SessionState>> {
        Arc::clone(&self.state)
    }

    /// Get weak reference to [`SessionState`]
    pub fn state_weak_ref(&self) -> Weak<RwLock<SessionState>> {
        Arc::downgrade(&self.state)
    }

    /// Register [`CatalogProviderList`] in [`SessionState`]
    pub fn register_catalog_list(&mut self, catalog_list: Arc<dyn CatalogProviderList>) {
        self.state.write().register_catalog_list(catalog_list)
    }

    /// Registers a [`ConfigExtension`] as a table option extention that can be
    /// referenced from SQL statements executed against this context.
    pub fn register_table_options_extension<T: ConfigExtension>(&self, extension: T) {
        self.state
            .write()
            .register_table_options_extension(extension)
    }
}

impl FunctionRegistry for SessionContext {
    fn udfs(&self) -> HashSet<String> {
        self.state.read().udfs()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.state.read().udf(name)
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.state.read().udaf(name)
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        self.state.read().udwf(name)
    }
    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        self.state.write().register_udf(udf)
    }
    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> Result<Option<Arc<AggregateUDF>>> {
        self.state.write().register_udaf(udaf)
    }
    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
        self.state.write().register_udwf(udwf)
    }

    fn register_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> Result<()> {
        self.state.write().register_function_rewrite(rewrite)
    }
}

/// Create a new task context instance from SessionContext
impl From<&SessionContext> for TaskContext {
    fn from(session: &SessionContext) -> Self {
        TaskContext::from(&*session.state.read())
    }
}

/// A planner used to add extensions to DataFusion logical and physical plans.
#[async_trait]
pub trait QueryPlanner {
    /// Given a `LogicalPlan`, create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// A pluggable interface to handle `CREATE FUNCTION` statements
/// and interact with [SessionState] to registers new udf, udaf or udwf.

#[async_trait]
pub trait FunctionFactory: Sync + Send {
    /// Handles creation of user defined function specified in [CreateFunction] statement
    async fn create(
        &self,
        state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction>;
}

/// Type of function to create
pub enum RegisterFunction {
    /// Scalar user defined function
    Scalar(Arc<ScalarUDF>),
    /// Aggregate user defined function
    Aggregate(Arc<AggregateUDF>),
    /// Window user defined function
    Window(Arc<WindowUDF>),
    /// Table user defined function
    Table(String, Arc<dyn TableFunctionImpl>),
}

/// Default implementation of [SerializerRegistry] that throws unimplemented error
/// for all requests.
pub struct EmptySerializerRegistry;

impl SerializerRegistry for EmptySerializerRegistry {
    fn serialize_logical_plan(
        &self,
        node: &dyn UserDefinedLogicalNode,
    ) -> Result<Vec<u8>> {
        not_impl_err!(
            "Serializing user defined logical plan node `{}` is not supported",
            node.name()
        )
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        _bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        not_impl_err!(
            "Deserializing user defined logical plan node `{name}` is not supported"
        )
    }
}

/// Describes which SQL statements can be run.
///
/// See [`SessionContext::sql_with_options`] for more details.
#[derive(Clone, Debug, Copy)]
pub struct SQLOptions {
    /// See [`Self::with_allow_ddl`]
    allow_ddl: bool,
    /// See [`Self::with_allow_dml`]
    allow_dml: bool,
    /// See [`Self::with_allow_statements`]
    allow_statements: bool,
}

impl Default for SQLOptions {
    fn default() -> Self {
        Self {
            allow_ddl: true,
            allow_dml: true,
            allow_statements: true,
        }
    }
}

impl SQLOptions {
    /// Create a new `SQLOptions` with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Should DDL data definition commands  (e.g. `CREATE TABLE`) be run? Defaults to `true`.
    pub fn with_allow_ddl(mut self, allow: bool) -> Self {
        self.allow_ddl = allow;
        self
    }

    /// Should DML data modification commands (e.g. `INSERT` and `COPY`) be run? Defaults to `true`
    pub fn with_allow_dml(mut self, allow: bool) -> Self {
        self.allow_dml = allow;
        self
    }

    /// Should Statements such as (e.g. `SET VARIABLE and `BEGIN TRANSACTION` ...`) be run?. Defaults to `true`
    pub fn with_allow_statements(mut self, allow: bool) -> Self {
        self.allow_statements = allow;
        self
    }

    /// Return an error if the [`LogicalPlan`] has any nodes that are
    /// incompatible with this [`SQLOptions`].
    pub fn verify_plan(&self, plan: &LogicalPlan) -> Result<()> {
        plan.visit_with_subqueries(&mut BadPlanVisitor::new(self))?;
        Ok(())
    }
}

struct BadPlanVisitor<'a> {
    options: &'a SQLOptions,
}
impl<'a> BadPlanVisitor<'a> {
    fn new(options: &'a SQLOptions) -> Self {
        Self { options }
    }
}

impl<'n, 'a> TreeNodeVisitor<'n> for BadPlanVisitor<'a> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Ddl(ddl) if !self.options.allow_ddl => {
                plan_err!("DDL not supported: {}", ddl.name())
            }
            LogicalPlan::Dml(dml) if !self.options.allow_dml => {
                plan_err!("DML not supported: {}", dml.op)
            }
            LogicalPlan::Copy(_) if !self.options.allow_dml => {
                plan_err!("DML not supported: COPY")
            }
            LogicalPlan::Statement(stmt) if !self.options.allow_statements => {
                plan_err!("Statement not supported: {}", stmt.name())
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::{super::options::CsvReadOptions, *};
    use crate::assert_batches_eq;
    use crate::execution::memory_pool::MemoryConsumer;
    use crate::execution::runtime_env::RuntimeConfig;
    use crate::test;
    use crate::test_util::{plan_and_collect, populate_csv_partitions};

    use datafusion_common_runtime::SpawnedTask;

    use crate::catalog::schema::SchemaProvider;
    use crate::physical_planner::PhysicalPlanner;
    use async_trait::async_trait;
    use tempfile::TempDir;

    #[tokio::test]
    async fn shared_memory_and_disk_manager() {
        // Demonstrate the ability to share DiskManager and
        // MemoryPool between two different executions.
        let ctx1 = SessionContext::new();

        // configure with same memory / disk manager
        let memory_pool = ctx1.runtime_env().memory_pool.clone();

        let mut reservation = MemoryConsumer::new("test").register(&memory_pool);
        reservation.grow(100);

        let disk_manager = ctx1.runtime_env().disk_manager.clone();

        let ctx2 =
            SessionContext::new_with_config_rt(SessionConfig::new(), ctx1.runtime_env());

        assert_eq!(ctx1.runtime_env().memory_pool.reserved(), 100);
        assert_eq!(ctx2.runtime_env().memory_pool.reserved(), 100);

        drop(reservation);

        assert_eq!(ctx1.runtime_env().memory_pool.reserved(), 0);
        assert_eq!(ctx2.runtime_env().memory_pool.reserved(), 0);

        assert!(std::ptr::eq(
            Arc::as_ptr(&disk_manager),
            Arc::as_ptr(&ctx1.runtime_env().disk_manager)
        ));
        assert!(std::ptr::eq(
            Arc::as_ptr(&disk_manager),
            Arc::as_ptr(&ctx2.runtime_env().disk_manager)
        ));
    }

    #[tokio::test]
    async fn create_variable_expr() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = create_ctx(&tmp_dir, partition_count).await?;

        let variable_provider = test::variable::SystemVar::new();
        ctx.register_variable(VarType::System, Arc::new(variable_provider));
        let variable_provider = test::variable::UserDefinedVar::new();
        ctx.register_variable(VarType::UserDefined, Arc::new(variable_provider));

        let provider = test::create_table_dual();
        ctx.register_table("dual", provider)?;

        let results =
            plan_and_collect(&ctx, "SELECT @@version, @name, @integer + 1 FROM dual")
                .await?;

        let expected = [
            "+----------------------+------------------------+---------------------+",
            "| @@version            | @name                  | @integer + Int64(1) |",
            "+----------------------+------------------------+---------------------+",
            "| system-var-@@version | user-defined-var-@name | 42                  |",
            "+----------------------+------------------------+---------------------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn create_variable_err() -> Result<()> {
        let ctx = SessionContext::new();

        let err = plan_and_collect(&ctx, "SElECT @=   X3").await.unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: variable [\"@=\"] has no type information"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_deregister() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = create_ctx(&tmp_dir, partition_count).await?;

        let provider = test::create_table_dual();
        ctx.register_table("dual", provider)?;

        assert!(ctx.deregister_table("dual")?.is_some());
        assert!(ctx.deregister_table("dual")?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn send_context_to_threads() -> Result<()> {
        // ensure SessionContexts can be used in a multi-threaded
        // environment. Usecase is for concurrent planing.
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = Arc::new(create_ctx(&tmp_dir, partition_count).await?);

        let threads: Vec<_> = (0..2)
            .map(|_| ctx.clone())
            .map(|ctx| {
                SpawnedTask::spawn(async move {
                    // Ensure we can create logical plan code on a separate thread.
                    ctx.sql("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")
                        .await
                })
            })
            .collect();

        for handle in threads {
            handle.join().await.unwrap().unwrap();
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_listing_schema_provider() -> Result<()> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = path.join("tests/tpch-csv");
        let url = format!("file://{}", path.display());

        let rt_cfg = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(rt_cfg).unwrap());
        let cfg = SessionConfig::new()
            .set_str("datafusion.catalog.location", url.as_str())
            .set_str("datafusion.catalog.format", "CSV")
            .set_str("datafusion.catalog.has_header", "true");
        let session_state = SessionState::new_with_config_rt(cfg, runtime);
        let ctx = SessionContext::new_with_state(session_state);
        ctx.refresh_catalogs().await?;

        let result =
            plan_and_collect(&ctx, "select c_name from default.customer limit 3;")
                .await?;

        let actual = arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();
        let expected = r#"+--------------------+
| c_name             |
+--------------------+
| Customer#000000002 |
| Customer#000000003 |
| Customer#000000004 |
+--------------------+"#;
        assert_eq!(actual, expected);

        Ok(())
    }

    #[tokio::test]
    async fn custom_query_planner() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let session_state =
            SessionState::new_with_config_rt(SessionConfig::new(), runtime)
                .with_query_planner(Arc::new(MyQueryPlanner {}));
        let ctx = SessionContext::new_with_state(session_state);

        let df = ctx.sql("SELECT 1").await?;
        df.collect().await.expect_err("query not supported");
        Ok(())
    }

    #[tokio::test]
    async fn disabled_default_catalog_and_schema() -> Result<()> {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_create_default_catalog_and_schema(false),
        );

        assert!(matches!(
            ctx.register_table("test", test::table_with_sequence(1, 1)?),
            Err(DataFusionError::Plan(_))
        ));

        assert!(matches!(
            ctx.sql("select * from datafusion.public.test").await,
            Err(DataFusionError::Plan(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn custom_catalog_and_schema() {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    #[tokio::test]
    async fn custom_catalog_and_schema_no_default() {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(false)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    #[tokio::test]
    async fn custom_catalog_and_schema_and_information_schema() {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    async fn catalog_and_schema_test(config: SessionConfig) {
        let ctx = SessionContext::new_with_config(config);
        let catalog = MemoryCatalogProvider::new();
        let schema = MemorySchemaProvider::new();
        schema
            .register_table("test".to_owned(), test::table_with_sequence(1, 1).unwrap())
            .unwrap();
        catalog
            .register_schema("my_schema", Arc::new(schema))
            .unwrap();
        ctx.register_catalog("my_catalog", Arc::new(catalog));

        for table_ref in &["my_catalog.my_schema.test", "my_schema.test", "test"] {
            let result = plan_and_collect(
                &ctx,
                &format!("SELECT COUNT(*) AS count FROM {table_ref}"),
            )
            .await
            .unwrap();

            let expected = [
                "+-------+",
                "| count |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ];
            assert_batches_eq!(expected, &result);
        }
    }

    #[tokio::test]
    async fn cross_catalog_access() -> Result<()> {
        let ctx = SessionContext::new();

        let catalog_a = MemoryCatalogProvider::new();
        let schema_a = MemorySchemaProvider::new();
        schema_a
            .register_table("table_a".to_owned(), test::table_with_sequence(1, 1)?)?;
        catalog_a.register_schema("schema_a", Arc::new(schema_a))?;
        ctx.register_catalog("catalog_a", Arc::new(catalog_a));

        let catalog_b = MemoryCatalogProvider::new();
        let schema_b = MemorySchemaProvider::new();
        schema_b
            .register_table("table_b".to_owned(), test::table_with_sequence(1, 2)?)?;
        catalog_b.register_schema("schema_b", Arc::new(schema_b))?;
        ctx.register_catalog("catalog_b", Arc::new(catalog_b));

        let result = plan_and_collect(
            &ctx,
            "SELECT cat, SUM(i) AS total FROM (
                    SELECT i, 'a' AS cat FROM catalog_a.schema_a.table_a
                    UNION ALL
                    SELECT i, 'b' AS cat FROM catalog_b.schema_b.table_b
                ) AS all
                GROUP BY cat
                ORDER BY cat
                ",
        )
        .await?;

        let expected = [
            "+-----+-------+",
            "| cat | total |",
            "+-----+-------+",
            "| a   | 1     |",
            "| b   | 3     |",
            "+-----+-------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn catalogs_not_leaked() {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // register a single catalog
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let catalog_weak = Arc::downgrade(&catalog);
        ctx.register_catalog("my_catalog", catalog);

        let catalog_list_weak = {
            let state = ctx.state.read();
            Arc::downgrade(state.catalog_list())
        };

        drop(ctx);

        assert_eq!(Weak::strong_count(&catalog_list_weak), 0);
        assert_eq!(Weak::strong_count(&catalog_weak), 0);
    }

    #[tokio::test]
    async fn sql_create_schema() -> Result<()> {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // Create schema
        ctx.sql("CREATE SCHEMA abc").await?.collect().await?;

        // Add table to schema
        ctx.sql("CREATE TABLE abc.y AS VALUES (1,2,3)")
            .await?
            .collect()
            .await?;

        // Check table exists in schema
        let results = ctx.sql("SELECT * FROM information_schema.tables WHERE table_schema='abc' AND table_name = 'y'").await.unwrap().collect().await.unwrap();

        assert_eq!(results[0].num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sql_create_catalog() -> Result<()> {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // Create catalog
        ctx.sql("CREATE DATABASE test").await?.collect().await?;

        // Create schema
        ctx.sql("CREATE SCHEMA test.abc").await?.collect().await?;

        // Add table to schema
        ctx.sql("CREATE TABLE test.abc.y AS VALUES (1,2,3)")
            .await?
            .collect()
            .await?;

        // Check table exists in schema
        let results = ctx.sql("SELECT * FROM information_schema.tables WHERE table_catalog='test' AND table_schema='abc' AND table_name = 'y'").await.unwrap().collect().await.unwrap();

        assert_eq!(results[0].num_rows(), 1);
        Ok(())
    }

    struct MyPhysicalPlanner {}

    #[async_trait]
    impl PhysicalPlanner for MyPhysicalPlanner {
        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _session_state: &SessionState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            not_impl_err!("query not supported")
        }

        fn create_physical_expr(
            &self,
            _expr: &Expr,
            _input_dfschema: &crate::common::DFSchema,
            _session_state: &SessionState,
        ) -> Result<Arc<dyn crate::physical_plan::PhysicalExpr>> {
            unimplemented!()
        }
    }

    struct MyQueryPlanner {}

    #[async_trait]
    impl QueryPlanner for MyQueryPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session_state: &SessionState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let physical_planner = MyPhysicalPlanner {};
            physical_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        }
    }

    /// Generate a partitioned CSV file and register it with an execution context
    async fn create_ctx(
        tmp_dir: &TempDir,
        partition_count: usize,
    ) -> Result<SessionContext> {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(8),
        );

        let schema = populate_csv_partitions(tmp_dir, partition_count, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        Ok(ctx)
    }
}
