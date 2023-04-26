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

//! [`SessionContext`] contains methods for registering data sources and executing queries
use crate::{
    catalog::catalog::{CatalogList, MemoryCatalogList},
    datasource::{
        datasource::TableProviderFactory,
        listing::{ListingOptions, ListingTable},
        listing_table_factory::ListingTableFactory,
    },
    datasource::{MemTable, ViewTable},
    logical_expr::{PlanType, ToStringifiedPlan},
    optimizer::optimizer::Optimizer,
    physical_optimizer::{
        aggregate_statistics::AggregateStatistics, join_selection::JoinSelection,
        optimizer::PhysicalOptimizerRule,
    },
    physical_plan::file_format::FileWriterSaveMode,
};
use datafusion_expr::{
    logical_plan::Statement, DescribeTable, DmlStatement, StringifiedPlan, WriteOp,
};
pub use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::var_provider::is_system_variables;
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::string::String;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};
use std::{ops::ControlFlow, sync::Weak};

use arrow::record_batch::RecordBatch;
use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
};

use crate::catalog::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    schema::{MemorySchemaProvider, SchemaProvider},
};
use crate::dataframe::DataFrame;
use crate::datasource::{
    listing::{ListingTableConfig, ListingTableUrl},
    provider_as_source, TableProvider,
};
use crate::error::{DataFusionError, Result};
use crate::logical_expr::{
    CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
    CreateView, DropTable, DropView, Explain, LogicalPlan, LogicalPlanBuilder,
    SetVariable, TableSource, TableType, UNNAMED_TABLE,
};
use crate::optimizer::OptimizerRule;
use datafusion_sql::{planner::ParserOptions, ResolvedTableReference, TableReference};

use crate::physical_optimizer::coalesce_batches::CoalesceBatches;
use crate::physical_optimizer::repartition::Repartition;

use crate::config::ConfigOptions;
use crate::execution::{runtime_env::RuntimeEnv, FunctionRegistry};
use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
use crate::physical_plan::file_format::{plan_to_csv, plan_to_json, plan_to_parquet};
use crate::physical_plan::planner::DefaultPhysicalPlanner;
use crate::physical_plan::udaf::AggregateUDF;
use crate::physical_plan::udf::ScalarUDF;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::PhysicalPlanner;
use crate::variable::{VarProvider, VarType};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_common::OwnedTableReference;
use datafusion_sql::{
    parser::DFParser,
    planner::{ContextProvider, SqlToRel},
};
use parquet::file::properties::WriterProperties;
use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, ClickHouseDialect, Dialect, GenericDialect,
    HiveDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, RedshiftSqlDialect,
    SQLiteDialect, SnowflakeDialect,
};
use url::Url;

use crate::catalog::information_schema::{InformationSchemaProvider, INFORMATION_SCHEMA};
use crate::catalog::listing_schema::ListingSchemaProvider;
use crate::datasource::object_store::ObjectStoreUrl;
use crate::physical_optimizer::global_sort_selection::GlobalSortSelection;
use crate::physical_optimizer::pipeline_checker::PipelineChecker;
use crate::physical_optimizer::pipeline_fixer::PipelineFixer;
use crate::physical_optimizer::sort_enforcement::EnforceSorting;
use datafusion_optimizer::{
    analyzer::{Analyzer, AnalyzerRule},
    OptimizerConfig,
};
use datafusion_sql::planner::object_name_to_table_reference;
use uuid::Uuid;

// backwards compatibility
use crate::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
pub use datafusion_execution::config::SessionConfig;
pub use datafusion_execution::TaskContext;

use super::options::{
    AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, ReadOptions,
};

/// DataFilePaths adds a method to convert strings and vector of strings to vector of [`ListingTableUrl`] URLs.
/// This allows methods such [`SessionContext::read_csv`] and `[`SessionContext::read_avro`]
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
/// * Create a DataFrame from a CSV or Parquet data source.
/// * Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
/// * Register a custom data source that can be referenced from a SQL query.
/// * Execution a SQL query
///
/// The following example demonstrates how to use the context to execute a query against a CSV
/// data source using the DataFrame API:
///
/// ```
/// use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(0, Some(100))?;
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
///
/// The following example demonstrates how to execute the same query using SQL:
///
/// ```
/// use datafusion::prelude::*;
///
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut ctx = SessionContext::new();
/// ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;
/// # Ok(())
/// # }
/// ```
///
/// # `SessionContext`, `SessionState`, and `TaskContext`
///
/// A [`SessionContext`] can be created from a [`SessionConfig`] and
/// stores the state for a particular query session. A single
/// [`SessionContext`] can run multiple queries.
///
/// [`SessionState`] contains information available during query
/// planning (creating [`LogicalPlan`]s and [`ExecutionPlan`]s).
///
/// [`TaskContext`] contains the state available during query
/// execution [`ExecutionPlan::execute`].  It contains a subset of the
/// information in[`SessionState`] and is created from a
/// [`SessionContext`] or a [`SessionState`].
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
        Self::with_config(SessionConfig::new())
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
    /// See [`Self::with_config_rt`] for more details on resource
    /// limits.
    pub fn with_config(config: SessionConfig) -> Self {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt(config, runtime)
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
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let state = SessionState::with_config_rt(config, runtime);
        Self::with_state(state)
    }

    /// Creates a new `SessionContext` using the provided [`SessionState`]
    pub fn with_state(state: SessionState) -> Self {
        Self {
            session_id: state.session_id.clone(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Returns the time this `SessionContext` was created
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time
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
        self.state.read().runtime_env.clone()
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
            .config
            .options()
            .sql_parser
            .enable_ident_normalization
    }

    /// Return a copied version of config for this Session
    pub fn copied_config(&self) -> SessionConfig {
        self.state.read().config.clone()
    }

    /// Creates a [`DataFrame`] that will execute a SQL query.
    ///
    /// Note: This API implements DDL statements such as `CREATE TABLE` and
    /// `CREATE VIEW` and DML statements such as `INSERT INTO` with in-memory
    /// default implementations.
    ///
    /// If this is not desirable, consider using [`SessionState::create_logical_plan()`] which
    /// does not mutate the state based on such statements.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        // create a query planner
        let plan = self.state().create_logical_plan(sql).await?;

        self.execute_logical_plan(plan).await
    }

    /// Execute the [`LogicalPlan`], return a [`DataFrame`]
    pub async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame> {
        match plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Insert,
                input,
                ..
            }) => {
                if self.table_exist(&table_name)? {
                    let name = table_name.table();
                    let provider = self.table_provider(name).await?;
                    provider.insert_into(&self.state(), &input).await?;
                } else {
                    return Err(DataFusionError::Execution(format!(
                        "Table '{table_name}' does not exist"
                    )));
                }
                self.return_empty_dataframe()
            }
            LogicalPlan::CreateExternalTable(cmd) => {
                self.create_external_table(&cmd).await
            }

            LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists,
                or_replace,
                primary_key,
            }) => {
                if !primary_key.is_empty() {
                    Err(DataFusionError::Execution(
                        "Primary keys on MemoryTables are not currently supported!"
                            .to_string(),
                    ))?;
                }

                let input = Arc::try_unwrap(input).unwrap_or_else(|e| e.as_ref().clone());
                let table = self.table(&name).await;

                match (if_not_exists, or_replace, table) {
                    (true, false, Ok(_)) => self.return_empty_dataframe(),
                    (false, true, Ok(_)) => {
                        self.deregister_table(&name)?;
                        let schema = Arc::new(input.schema().as_ref().into());
                        let physical = DataFrame::new(self.state(), input);

                        let batches: Vec<_> = physical.collect_partitioned().await?;
                        let table = Arc::new(MemTable::try_new(schema, batches)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (true, true, Ok(_)) => Err(DataFusionError::Execution(
                        "'IF NOT EXISTS' cannot coexist with 'REPLACE'".to_string(),
                    )),
                    (_, _, Err(_)) => {
                        let schema = Arc::new(input.schema().as_ref().into());
                        let physical = DataFrame::new(self.state(), input);

                        let batches: Vec<_> = physical.collect_partitioned().await?;
                        let table = Arc::new(MemTable::try_new(schema, batches)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (false, false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{name}' already exists"
                    ))),
                }
            }

            LogicalPlan::CreateView(CreateView {
                name,
                input,
                or_replace,
                definition,
            }) => {
                let view = self.table(&name).await;

                match (or_replace, view) {
                    (true, Ok(_)) => {
                        self.deregister_table(&name)?;
                        let table =
                            Arc::new(ViewTable::try_new((*input).clone(), definition)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (_, Err(_)) => {
                        let table =
                            Arc::new(ViewTable::try_new((*input).clone(), definition)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{name}' already exists"
                    ))),
                }
            }

            LogicalPlan::DropTable(DropTable {
                name, if_exists, ..
            }) => {
                let result = self.find_and_deregister(&name, TableType::Base).await;
                match (result, if_exists) {
                    (Ok(true), _) => self.return_empty_dataframe(),
                    (_, true) => self.return_empty_dataframe(),
                    (_, _) => Err(DataFusionError::Execution(format!(
                        "Table '{name}' doesn't exist."
                    ))),
                }
            }

            LogicalPlan::DropView(DropView {
                name, if_exists, ..
            }) => {
                let result = self.find_and_deregister(&name, TableType::View).await;
                match (result, if_exists) {
                    (Ok(true), _) => self.return_empty_dataframe(),
                    (_, true) => self.return_empty_dataframe(),
                    (_, _) => Err(DataFusionError::Execution(format!(
                        "View '{name}' doesn't exist."
                    ))),
                }
            }

            LogicalPlan::Statement(Statement::SetVariable(SetVariable {
                variable,
                value,
                ..
            })) => {
                let mut state = self.state.write();
                state.config.options_mut().set(&variable, &value)?;
                drop(state);

                self.return_empty_dataframe()
            }

            LogicalPlan::DescribeTable(DescribeTable { schema, .. }) => {
                self.return_describe_table_dataframe(schema).await
            }

            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists,
                ..
            }) => {
                // sqlparser doesnt accept database / catalog as parameter to CREATE SCHEMA
                // so for now, we default to default catalog
                let tokens: Vec<&str> = schema_name.split('.').collect();
                let (catalog, schema_name) = match tokens.len() {
                    1 => {
                        let state = self.state.read();
                        let name = &state.config.options().catalog.default_catalog;
                        let catalog =
                            state.catalog_list.catalog(name).ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Missing default catalog '{name}'"
                                ))
                            })?;
                        (catalog, tokens[0])
                    }
                    2 => {
                        let name = &tokens[0];
                        let catalog = self.catalog(name).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Missing catalog '{name}'"
                            ))
                        })?;
                        (catalog, tokens[1])
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unable to parse catalog from {schema_name}"
                        )))
                    }
                };
                let schema = catalog.schema(schema_name);

                match (if_not_exists, schema) {
                    (true, Some(_)) => self.return_empty_dataframe(),
                    (true, None) | (false, None) => {
                        let schema = Arc::new(MemorySchemaProvider::new());
                        catalog.register_schema(schema_name, schema)?;
                        self.return_empty_dataframe()
                    }
                    (false, Some(_)) => Err(DataFusionError::Execution(format!(
                        "Schema '{schema_name}' already exists"
                    ))),
                }
            }
            LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                ..
            }) => {
                let catalog = self.catalog(catalog_name.as_str());

                match (if_not_exists, catalog) {
                    (true, Some(_)) => self.return_empty_dataframe(),
                    (true, None) | (false, None) => {
                        let new_catalog = Arc::new(MemoryCatalogProvider::new());
                        self.state
                            .write()
                            .catalog_list
                            .register_catalog(catalog_name, new_catalog);
                        self.return_empty_dataframe()
                    }
                    (false, Some(_)) => Err(DataFusionError::Execution(format!(
                        "Catalog '{catalog_name}' already exists"
                    ))),
                }
            }

            plan => Ok(DataFrame::new(self.state(), plan)),
        }
    }

    // return an empty dataframe
    fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.state(), plan))
    }

    // return an record_batch which describe table
    async fn return_describe_table_record_batch(
        &self,
        schema: Arc<Schema>,
    ) -> Result<RecordBatch> {
        let record_batch_schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]));

        let mut column_names = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = StringBuilder::new();
        for (_, field) in schema.fields().iter().enumerate() {
            column_names.append_value(field.name());

            // "System supplied type" --> Use debug format of the datatype
            let data_type = field.data_type();
            data_types.append_value(format!("{data_type:?}"));

            // "YES if the column is possibly nullable, NO if it is known not nullable. "
            let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
            is_nullables.append_value(nullable_str);
        }

        let record_batch = RecordBatch::try_new(
            record_batch_schema,
            vec![
                Arc::new(column_names.finish()),
                Arc::new(data_types.finish()),
                Arc::new(is_nullables.finish()),
            ],
        )?;

        Ok(record_batch)
    }

    // return an dataframe which describe file
    async fn return_describe_table_dataframe(
        &self,
        schema: Arc<Schema>,
    ) -> Result<DataFrame> {
        let record_batch = self.return_describe_table_record_batch(schema).await?;
        self.read_batch(record_batch)
    }

    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<DataFrame> {
        let exist = self.table_exist(&cmd.name)?;
        if exist {
            match cmd.if_not_exists {
                true => return self.return_empty_dataframe(),
                false => {
                    return Err(DataFusionError::Execution(format!(
                        "Table '{}' already exists",
                        cmd.name
                    )));
                }
            }
        }

        let table_provider: Arc<dyn TableProvider> =
            self.create_custom_table(cmd).await?;
        self.register_table(&cmd.name, table_provider)?;
        self.return_empty_dataframe()
    }

    async fn create_custom_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.state.read().clone();
        let file_type = cmd.file_type.to_uppercase();
        let factory =
            &state
                .table_factories
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
        table_ref: impl Into<TableReference<'a>>,
        table_type: TableType,
    ) -> Result<bool> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_owned();
        let maybe_schema = {
            let state = self.state.read();
            let resolved = state.resolve_table_ref(table_ref);
            state
                .catalog_list
                .catalog(&resolved.catalog)
                .and_then(|c| c.schema(&resolved.schema))
        };

        if let Some(schema) = maybe_schema {
            if let Some(table_provider) = schema.table(&table).await {
                if table_provider.table_type() == table_type {
                    schema.deregister_table(&table)?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Registers a variable provider within this context.
    pub fn register_variable(
        &self,
        variable_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) {
        self.state
            .write()
            .execution_props
            .add_var_provider(variable_type, provider);
    }

    /// Registers a scalar UDF within this context.
    ///
    /// Note in SQL queries, function names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_FUNC(x)...` will look for a function named `"my_func"`
    /// `SELECT "my_FUNC"(x)` will look for a function named `"my_FUNC"`
    pub fn register_udf(&self, f: ScalarUDF) {
        self.state
            .write()
            .scalar_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Registers an aggregate UDF within this context.
    ///
    /// Note in SQL queries, aggregate names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_UDAF(x)...` will look for an aggregate named `"my_udaf"`
    /// `SELECT "my_UDAF"(x)` will look for an aggregate named `"my_UDAF"`
    pub fn register_udaf(&self, f: AggregateUDF) {
        self.state
            .write()
            .aggregate_functions
            .insert(f.name.clone(), Arc::new(f));
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
        let listing_options = options.to_listing_options(&session_config);
        let resolved_schema = options
            .get_resolved_schema(&session_config, self.state(), table_paths[0].clone())
            .await?;
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;
        self.read_table(Arc::new(provider))
    }

    /// Creates a [`DataFrame`] for reading an Avro data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`ListingTable`].
    ///
    /// For an example, see [`read_csv`](Self::read_csv)
    pub async fn read_avro<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: AvroReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
    }

    /// Creates a [`DataFrame`] for reading an JSON data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`ListingTable`].
    ///
    /// For an example, see [`read_csv`](Self::read_csv)
    pub async fn read_json<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: NdJsonReadOptions<'_>,
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

    /// Creates a [`DataFrame`] for reading a CSV data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`ListingTable`].
    ///
    /// Example usage is given below:
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// // You can read a single file using `read_csv`
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// // you can also read multiple files:
    /// let df = ctx.read_csv(vec!["tests/data/example.csv", "tests/data/example.csv"], CsvReadOptions::new()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_csv<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
    }

    /// Creates a [`DataFrame`] for reading a Parquet data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`ListingTable`].
    ///
    /// For an example, see [`read_csv`](Self::read_csv)
    pub async fn read_parquet<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: ParquetReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
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

    /// Registers a [`ListingTable]` that can assemble multiple files
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
        let resolved_schema = match (provided_schema, options.infinite_source) {
            (Some(s), _) => s,
            (None, false) => options.infer_schema(&self.state(), &table_path).await?,
            (None, true) => {
                return Err(DataFusionError::Plan(
                    "Schema inference for infinite data sources is not supported."
                        .to_string(),
                ))
            }
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

    /// Registers a CSV file as a table which can referenced from SQL
    /// statements executed against this context.
    pub async fn register_csv(
        &self,
        name: &str,
        table_path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options.to_listing_options(&self.copied_config());

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

    /// Registers a JSON file as a table that it can be referenced
    /// from SQL statements executed against this context.
    pub async fn register_json(
        &self,
        name: &str,
        table_path: &str,
        options: NdJsonReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options.to_listing_options(&self.copied_config());

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

    /// Registers a Parquet file as a table that can be referenced from SQL
    /// statements executed against this context.
    pub async fn register_parquet(
        &self,
        name: &str,
        table_path: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options.to_listing_options(&self.state.read().config);

        self.register_listing_table(name, table_path, listing_options, None, None)
            .await?;
        Ok(())
    }

    /// Registers an Avro file as a table that can be referenced from
    /// SQL statements executed against this context.
    pub async fn register_avro(
        &self,
        name: &str,
        table_path: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options.to_listing_options(&self.copied_config());

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
            .catalog_list
            .register_catalog(name, catalog)
    }

    /// Retrieves the list of available catalog names.
    pub fn catalog_names(&self) -> Vec<String> {
        self.state.read().catalog_list.catalog_names()
    }

    /// Retrieves a [`CatalogProvider`] instance by name
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.state.read().catalog_list.catalog(name)
    }

    /// Registers a [`TableProvider`] as a table that can be
    /// referenced from SQL statements executed against this context.
    ///
    /// Returns the [`TableProvider`] previously registered for this
    /// reference, if any
    pub fn register_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_owned();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table, provider)
    }

    /// Deregisters the given table.
    ///
    /// Returns the registered provider, if any
    pub fn deregister_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_owned();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .deregister_table(&table)
    }

    /// Return `true` if the specified table exists in the schema provider.
    pub fn table_exist<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<bool> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_owned();
        Ok(self
            .state
            .read()
            .schema_for_ref(table_ref)?
            .table_exist(&table))
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
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<DataFrame> {
        let table_ref = table_ref.into();
        let provider = self.table_provider(table_ref.to_owned_reference()).await?;
        let plan = LogicalPlanBuilder::scan(
            table_ref.to_owned_reference(),
            provider_as_source(Arc::clone(&provider)),
            None,
        )?
        .build()?;
        Ok(DataFrame::new(self.state(), plan))
    }

    /// Return a [`TableProvider`] for the specified table.
    pub async fn table_provider<'a>(
        &self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_ref = table_ref.into();
        let table = table_ref.table().to_string();
        let schema = self.state.read().schema_for_ref(table_ref)?;
        match schema.table(&table).await {
            Some(ref provider) => Ok(Arc::clone(provider)),
            _ => Err(DataFusionError::Plan(format!("No table named '{table}'"))),
        }
    }

    /// Returns the set of available tables in the default catalog and
    /// schema.
    ///
    /// Use [`table`] to get a specific table.
    ///
    /// [`table`]: SessionContext::table
    #[deprecated(
        note = "Please use the catalog provider interface (`SessionContext::catalog`) to examine available catalogs, schemas, and tables"
    )]
    pub fn tables(&self) -> Result<HashSet<String>> {
        Ok(self
            .state
            .read()
            // a bare reference will always resolve to the default catalog and schema
            .schema_for_ref(TableReference::Bare { table: "".into() })?
            .table_names()
            .iter()
            .cloned()
            .collect())
    }

    /// Optimizes the logical plan by applying optimizer rules.
    #[deprecated(
        note = "Use SessionState::optimize to ensure a consistent state for planning and execution"
    )]
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.state.read().optimize(plan)
    }

    /// Creates a physical plan from a logical plan.
    #[deprecated(
        note = "Use SessionState::create_physical_plan or DataFrame::create_physical_plan to ensure a consistent state for planning and execution"
    )]
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.state().create_physical_plan(logical_plan).await
    }

    /// Executes a query and writes the results to a partitioned CSV file.
    pub async fn write_csv(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        plan_to_csv(self.task_ctx(), plan, path).await
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    pub async fn write_json(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        plan_to_json(self.task_ctx(), plan, path).await
    }

    /// Executes a query and writes the results to a partitioned Parquet file.
    pub async fn write_parquet(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
        writer_properties: Option<WriterProperties>,
        save_mode: FileWriterSaveMode,
    ) -> Result<()> {
        plan_to_parquet(self.task_ctx(), plan, path, writer_properties, save_mode).await
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }

    /// Snapshots the [`SessionState`] of this [`SessionContext`] setting the
    /// `query_execution_start_time` to the current time
    pub fn state(&self) -> SessionState {
        let mut state = self.state.read().clone();
        state.execution_props.start_execution();
        state
    }

    /// Get weak reference to [`SessionState`]
    pub fn state_weak_ref(&self) -> Weak<RwLock<SessionState>> {
        Arc::downgrade(&self.state)
    }

    /// Register [`CatalogList`] in [`SessionState`]
    pub fn register_catalog_list(&mut self, catalog_list: Arc<dyn CatalogList>) {
        self.state.write().catalog_list = catalog_list;
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
}

/// A planner used to add extensions to DataFusion logical and physical plans.
#[async_trait]
pub trait QueryPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// The query planner used if no user defined planner is provided
struct DefaultQueryPlanner {}

#[async_trait]
impl QueryPlanner for DefaultQueryPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Execution context for registering data sources and executing queries
#[derive(Clone)]
pub struct SessionState {
    /// A unique UUID that identifies the session
    session_id: String,
    /// Responsible for analyzing and rewrite a logical plan before optimization
    analyzer: Analyzer,
    /// Responsible for optimizing a logical plan
    optimizer: Optimizer,
    /// Responsible for optimizing a physical execution plan
    physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    /// Collection of catalogs containing schemas and ultimately TableProviders
    catalog_list: Arc<dyn CatalogList>,
    /// Scalar functions that are registered with the context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Session configuration
    config: SessionConfig,
    /// Execution properties
    execution_props: ExecutionProps,
    /// TableProviderFactories for different file formats.
    ///
    /// Maps strings like "JSON" to an instance of  [`TableProviderFactory`]
    ///
    /// This is used to create [`TableProvider`] instances for the
    /// `CREATE EXTERNAL TABLE ... STORED AS <FORMAT>` for custom file
    /// formats other than those built into DataFusion
    table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
    /// Runtime environment
    runtime_env: Arc<RuntimeEnv>,
}

impl Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("session_id", &self.session_id)
            // TODO should we print out more?
            .finish()
    }
}

/// Default session builder using the provided configuration
pub fn default_session_builder(config: SessionConfig) -> SessionState {
    SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()))
}

impl SessionState {
    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`] and [`RuntimeEnv`].
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let catalog_list = Arc::new(MemoryCatalogList::new()) as Arc<dyn CatalogList>;
        Self::with_config_rt_and_catalog_list(config, runtime, catalog_list)
    }

    /// Returns new SessionState using the provided configuration, runtime and catalog list.
    pub fn with_config_rt_and_catalog_list(
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        catalog_list: Arc<dyn CatalogList>,
    ) -> Self {
        let session_id = Uuid::new_v4().to_string();

        // Create table_factories for all default formats
        let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
            HashMap::new();
        table_factories.insert("PARQUET".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("CSV".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("JSON".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("NDJSON".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("AVRO".into(), Arc::new(ListingTableFactory::new()));

        if config.create_default_catalog_and_schema() {
            let default_catalog = MemoryCatalogProvider::new();

            default_catalog
                .register_schema(
                    &config.options().catalog.default_schema,
                    Arc::new(MemorySchemaProvider::new()),
                )
                .expect("memory catalog provider can register schema");

            Self::register_default_schema(
                &config,
                &table_factories,
                &runtime,
                &default_catalog,
            );

            catalog_list.register_catalog(
                config.options().catalog.default_catalog.clone(),
                Arc::new(default_catalog),
            );
        }

        // We need to take care of the rule ordering. They may influence each other.
        let physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            // If the query is processing infinite inputs, the PipelineFixer rule applies the
            // necessary transformations to make the query runnable (if it is not already runnable).
            // If the query can not be made runnable, the rule emits an error with a diagnostic message.
            // Since the transformations it applies may alter output partitioning properties of operators
            // (e.g. by swapping hash join sides), this rule runs before EnforceDistribution.
            Arc::new(PipelineFixer::new()),
            // In order to increase the parallelism, the Repartition rule will change the
            // output partitioning of some operators in the plan tree, which will influence
            // other rules. Therefore, it should run as soon as possible. It is optional because:
            // - It's not used for the distributed engine, Ballista.
            // - It's conflicted with some parts of the EnforceDistribution, since it will
            //   introduce additional repartitioning while EnforceDistribution aims to
            //   reduce unnecessary repartitioning.
            Arc::new(Repartition::new()),
            // - Currently it will depend on the partition number to decide whether to change the
            // single node sort to parallel local sort and merge. Therefore, GlobalSortSelection
            // should run after the Repartition.
            // - Since it will change the output ordering of some operators, it should run
            // before JoinSelection and EnforceSorting, which may depend on that.
            Arc::new(GlobalSortSelection::new()),
            // The EnforceDistribution rule is for adding essential repartition to satisfy the required
            // distribution. Please make sure that the whole plan tree is determined before this rule.
            Arc::new(EnforceDistribution::new()),
            // The EnforceSorting rule is for adding essential local sorting to satisfy the required
            // ordering. Please make sure that the whole plan tree is determined before this rule.
            // Note that one should always run this rule after running the EnforceDistribution rule
            // as the latter may break local sorting requirements.
            Arc::new(EnforceSorting::new()),
            // The CombinePartialFinalAggregate rule should be applied after the EnforceDistribution
            // and EnforceSorting rules
            Arc::new(CombinePartialFinalAggregate::new()),
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
            // The PipelineChecker rule will reject non-runnable query plans that use
            // pipeline-breaking operators on infinite input(s). The rule generates a
            // diagnostic error message when this happens. It makes no changes to the
            // given query plan; i.e. it only acts as a final gatekeeping rule.
            Arc::new(PipelineChecker::new()),
        ];

        SessionState {
            session_id,
            analyzer: Analyzer::new(),
            optimizer: Optimizer::new(),
            physical_optimizers,
            query_planner: Arc::new(DefaultQueryPlanner {}),
            catalog_list,
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            config,
            execution_props: ExecutionProps::new(),
            runtime_env: runtime,
            table_factories,
        }
    }

    fn register_default_schema(
        config: &SessionConfig,
        table_factories: &HashMap<String, Arc<dyn TableProviderFactory>>,
        runtime: &Arc<RuntimeEnv>,
        default_catalog: &MemoryCatalogProvider,
    ) {
        let url = config.options().catalog.location.as_ref();
        let format = config.options().catalog.format.as_ref();
        let (url, format) = match (url, format) {
            (Some(url), Some(format)) => (url, format),
            _ => return,
        };
        let url = url.to_string();
        let format = format.to_string();

        let has_header = config.options().catalog.has_header;
        let url = Url::parse(url.as_str()).expect("Invalid default catalog location!");
        let authority = match url.host_str() {
            Some(host) => format!("{}://{}", url.scheme(), host),
            None => format!("{}://", url.scheme()),
        };
        let path = &url.as_str()[authority.len()..];
        let path = object_store::path::Path::parse(path).expect("Can't parse path");
        let store = ObjectStoreUrl::parse(authority.as_str())
            .expect("Invalid default catalog url");
        let store = match runtime.object_store(store) {
            Ok(store) => store,
            _ => return,
        };
        let factory = match table_factories.get(format.as_str()) {
            Some(factory) => factory,
            _ => return,
        };
        let schema = ListingSchemaProvider::new(
            authority,
            path,
            factory.clone(),
            store,
            format,
            has_header,
        );
        let _ = default_catalog
            .register_schema("default", Arc::new(schema))
            .expect("Failed to register default schema");
    }

    fn resolve_table_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> ResolvedTableReference<'a> {
        let catalog = &self.config_options().catalog;
        table_ref
            .into()
            .resolve(&catalog.default_catalog, &catalog.default_schema)
    }

    fn schema_for_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        if self.config.information_schema() && resolved_ref.schema == INFORMATION_SCHEMA {
            return Ok(Arc::new(InformationSchemaProvider::new(
                self.catalog_list.clone(),
            )));
        }

        self.catalog_list
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                ))
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve schema: {}",
                    resolved_ref.schema
                ))
            })
    }

    /// Replace the random session id.
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = session_id;
        self
    }

    /// Replace the default query planner
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = query_planner;
        self
    }

    /// Replace the analyzer rules
    pub fn with_analyzer_rules(
        mut self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.analyzer = Analyzer::with_rules(rules);
        self
    }

    /// Replace the optimizer rules
    pub fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizer = Optimizer::with_rules(rules);
        self
    }

    /// Replace the physical optimizer rules
    pub fn with_physical_optimizer_rules(
        mut self,
        physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.physical_optimizers = physical_optimizers;
        self
    }

    /// Adds a new [`AnalyzerRule`]
    pub fn add_analyzer_rule(
        mut self,
        analyzer_rule: Arc<dyn AnalyzerRule + Send + Sync>,
    ) -> Self {
        self.analyzer.rules.push(analyzer_rule);
        self
    }

    /// Adds a new [`OptimizerRule`]
    pub fn add_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        self.optimizer.rules.push(optimizer_rule);
        self
    }

    /// Adds a new [`PhysicalOptimizerRule`]
    pub fn add_physical_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizers.push(optimizer_rule);
        self
    }

    /// Get the table factories
    pub fn table_factories(&self) -> &HashMap<String, Arc<dyn TableProviderFactory>> {
        &self.table_factories
    }

    /// Get the table factories
    pub fn table_factories_mut(
        &mut self,
    ) -> &mut HashMap<String, Arc<dyn TableProviderFactory>> {
        &mut self.table_factories
    }

    /// Convert a SQL string into an AST Statement
    pub fn sql_to_statement(
        &self,
        sql: &str,
        dialect: &str,
    ) -> Result<datafusion_sql::parser::Statement> {
        let dialect = create_dialect_from_str(dialect)?;
        let mut statements = DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?;
        if statements.len() > 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }
        let statement = statements.pop_front().ok_or_else(|| {
            DataFusionError::NotImplemented(
                "The context requires a statement!".to_string(),
            )
        })?;
        Ok(statement)
    }

    /// Resolve all table references in the SQL statement.
    pub fn resolve_table_references(
        &self,
        statement: &datafusion_sql::parser::Statement,
    ) -> Result<Vec<OwnedTableReference>> {
        use crate::catalog::information_schema::INFORMATION_SCHEMA_TABLES;
        use datafusion_sql::parser::Statement as DFStatement;
        use sqlparser::ast::*;

        // Getting `TableProviders` is async but planing is not -- thus pre-fetch
        // table providers for all relations referenced in this query
        let mut relations = hashbrown::HashSet::with_capacity(10);

        match statement {
            DFStatement::Statement(s) => {
                struct RelationVisitor<'a>(&'a mut hashbrown::HashSet<ObjectName>);

                impl<'a> Visitor for RelationVisitor<'a> {
                    type Break = ();

                    fn pre_visit_relation(
                        &mut self,
                        relation: &ObjectName,
                    ) -> ControlFlow<()> {
                        self.0.get_or_insert_with(relation, |_| relation.clone());
                        ControlFlow::Continue(())
                    }

                    fn pre_visit_statement(
                        &mut self,
                        statement: &Statement,
                    ) -> ControlFlow<()> {
                        if let Statement::ShowCreate {
                            obj_type: ShowCreateObject::Table | ShowCreateObject::View,
                            obj_name,
                        } = statement
                        {
                            self.0.get_or_insert_with(obj_name, |_| obj_name.clone());
                        }
                        ControlFlow::Continue(())
                    }
                }
                let mut visitor = RelationVisitor(&mut relations);
                let _ = s.as_ref().visit(&mut visitor);
            }
            DFStatement::CreateExternalTable(table) => {
                relations.insert(ObjectName(vec![Ident::from(table.name.as_str())]));
            }
            DFStatement::DescribeTableStmt(table) => {
                relations
                    .get_or_insert_with(&table.table_name, |_| table.table_name.clone());
            }
        }

        // Always include information_schema if available
        if self.config.information_schema() {
            for s in INFORMATION_SCHEMA_TABLES {
                relations.insert(ObjectName(vec![
                    Ident::new(INFORMATION_SCHEMA),
                    Ident::new(*s),
                ]));
            }
        }

        let enable_ident_normalization =
            self.config.options().sql_parser.enable_ident_normalization;
        relations
            .into_iter()
            .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
            .collect::<Result<_>>()
    }

    /// Convert an AST Statement into a LogicalPlan
    pub async fn statement_to_plan(
        &self,
        statement: datafusion_sql::parser::Statement,
    ) -> Result<LogicalPlan> {
        let references = self.resolve_table_references(&statement)?;

        let mut provider = SessionContextProvider {
            state: self,
            tables: HashMap::with_capacity(references.len()),
        };

        let enable_ident_normalization =
            self.config.options().sql_parser.enable_ident_normalization;
        let parse_float_as_decimal =
            self.config.options().sql_parser.parse_float_as_decimal;
        for reference in references {
            let table = reference.table();
            let resolved = self.resolve_table_ref(&reference);
            if let Entry::Vacant(v) = provider.tables.entry(resolved.to_string()) {
                if let Ok(schema) = self.schema_for_ref(resolved) {
                    if let Some(table) = schema.table(table).await {
                        v.insert(provider_as_source(table));
                    }
                }
            }
        }

        let query = SqlToRel::new_with_options(
            &provider,
            ParserOptions {
                parse_float_as_decimal,
                enable_ident_normalization,
            },
        );
        query.statement_to_plan(statement)
    }

    /// Creates a [`LogicalPlan`] from the provided SQL string
    ///
    /// See [`SessionContext::sql`] for a higher-level interface that also handles DDL
    pub async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let dialect = self.config.options().sql_parser.dialect.as_str();
        let statement = self.sql_to_statement(sql, dialect)?;
        let plan = self.statement_to_plan(statement).await?;
        Ok(plan)
    }

    /// Optimizes the logical plan by applying optimizer rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        if let LogicalPlan::Explain(e) = plan {
            let mut stringified_plans = e.stringified_plans.clone();

            // analyze & capture output of each rule
            let analyzed_plan = match self.analyzer.execute_and_check(
                e.plan.as_ref(),
                self.options(),
                |analyzed_plan, analyzer| {
                    let analyzer_name = analyzer.name().to_string();
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans.push(analyzed_plan.to_stringified(plan_type));
                },
            ) {
                Ok(plan) => plan,
                Err(DataFusionError::Context(analyzer_name, err)) => {
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans
                        .push(StringifiedPlan::new(plan_type, err.to_string()));

                    return Ok(LogicalPlan::Explain(Explain {
                        verbose: e.verbose,
                        plan: e.plan.clone(),
                        stringified_plans,
                        schema: e.schema.clone(),
                        logical_optimization_succeeded: false,
                    }));
                }
                Err(e) => return Err(e),
            };

            // to delineate the analyzer & optimizer phases in explain output
            stringified_plans
                .push(analyzed_plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan));

            // optimize the child plan, capturing the output of each optimizer
            let (plan, logical_optimization_succeeded) = match self.optimizer.optimize(
                &analyzed_plan,
                self,
                |optimized_plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(optimized_plan.to_stringified(plan_type));
                },
            ) {
                Ok(plan) => (Arc::new(plan), true),
                Err(DataFusionError::Context(optimizer_name, err)) => {
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans
                        .push(StringifiedPlan::new(plan_type, err.to_string()));
                    (e.plan.clone(), false)
                }
                Err(e) => return Err(e),
            };

            Ok(LogicalPlan::Explain(Explain {
                verbose: e.verbose,
                plan,
                stringified_plans,
                schema: e.schema.clone(),
                logical_optimization_succeeded,
            }))
        } else {
            let analyzed_plan =
                self.analyzer
                    .execute_and_check(plan, self.options(), |_, _| {})?;
            self.optimizer.optimize(&analyzed_plan, self, |_, _| {})
        }
    }

    /// Creates a physical plan from a logical plan.
    ///
    /// Note: this first calls [`Self::optimize`] on the provided plan
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.optimize(logical_plan)?;
        self.query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }

    /// Return the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Return the runtime env
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    /// Return the execution properties
    pub fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    /// Return the [`SessionConfig`]
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Return the physical optimizers
    pub fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_optimizers
    }

    /// return the configuration options
    pub fn config_options(&self) -> &ConfigOptions {
        self.config.options()
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }

    /// Return catalog list
    pub fn catalog_list(&self) -> Arc<dyn CatalogList> {
        self.catalog_list.clone()
    }

    /// Return reference to scalar_functions
    pub fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    /// Return reference to aggregate_functions
    pub fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    /// Return version of the cargo package that produced this query
    pub fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
}

struct SessionContextProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl<'a> ContextProvider for SessionContextProvider<'a> {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let name = self.state.resolve_table_ref(name).to_string();
        self.tables
            .get(&name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("table '{name}' not found")))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.state
            .execution_props
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
}

impl FunctionRegistry for SessionState {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "There is no UDF named \"{name}\" in the registry"
            ))
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "There is no UDAF named \"{name}\" in the registry"
            ))
        })
    }
}

impl OptimizerConfig for SessionState {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.execution_props.query_execution_start_time
    }

    fn options(&self) -> &ConfigOptions {
        self.config_options()
    }
}

/// Create a new task context instance from SessionContext
impl From<&SessionContext> for TaskContext {
    fn from(session: &SessionContext) -> Self {
        TaskContext::from(&*session.state.read())
    }
}

/// Create a new task context instance from SessionState
impl From<&SessionState> for TaskContext {
    fn from(state: &SessionState) -> Self {
        let task_id = None;
        TaskContext::new(
            task_id,
            state.session_id.clone(),
            state.config.clone(),
            state.scalar_functions.clone(),
            state.aggregate_functions.clone(),
            state.runtime_env.clone(),
        )
    }
}

// TODO: remove when https://github.com/sqlparser-rs/sqlparser-rs/pull/848 is released
fn create_dialect_from_str(dialect_name: &str) -> Result<Box<dyn Dialect>> {
    match dialect_name.to_lowercase().as_str() {
        "generic" => Ok(Box::new(GenericDialect)),
        "mysql" => Ok(Box::new(MySqlDialect {})),
        "postgresql" | "postgres" => Ok(Box::new(PostgreSqlDialect {})),
        "hive" => Ok(Box::new(HiveDialect {})),
        "sqlite" => Ok(Box::new(SQLiteDialect {})),
        "snowflake" => Ok(Box::new(SnowflakeDialect)),
        "redshift" => Ok(Box::new(RedshiftSqlDialect {})),
        "mssql" => Ok(Box::new(MsSqlDialect {})),
        "clickhouse" => Ok(Box::new(ClickHouseDialect {})),
        "bigquery" => Ok(Box::new(BigQueryDialect)),
        "ansi" => Ok(Box::new(AnsiDialect {})),
        _ => {
            Err(DataFusionError::Internal(format!(
                "Unsupported SQL dialect: {dialect_name}. Available dialects: Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, MsSQL, ClickHouse, BigQuery, Ansi."
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_batches_eq;
    use crate::execution::context::QueryPlanner;
    use crate::execution::memory_pool::MemoryConsumer;
    use crate::execution::runtime_env::RuntimeConfig;
    use crate::physical_plan::expressions::AvgAccumulator;
    use crate::test;
    use crate::test_util::parquet_test_data;
    use crate::variable::VarType;
    use arrow::array::ArrayRef;
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion_expr::{create_udaf, create_udf, Expr, Volatility};
    use datafusion_physical_expr::functions::make_scalar_function;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Weak;
    use std::{env, io::prelude::*};
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
            SessionContext::with_config_rt(SessionConfig::new(), ctx1.runtime_env());

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

        let expected = vec![
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

        let err = plan_and_collect(&ctx, "SElECT @=   X#=?!~ 5")
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Execution error: variable [\"@\"] has no type information"
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
    async fn case_sensitive_identifiers_user_defined_functions() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        let myfunc = |args: &[ArrayRef]| Ok(Arc::clone(&args[0]));
        let myfunc = make_scalar_function(myfunc);

        ctx.register_udf(create_udf(
            "MY_FUNC",
            vec![DataType::Int32],
            Arc::new(DataType::Int32),
            Volatility::Immutable,
            myfunc,
        ));

        // doesn't work as it was registered with non lowercase
        let err = plan_and_collect(&ctx, "SELECT MY_FUNC(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function \'my_func\'"
        );

        // Can call it if you put quotes
        let result = plan_and_collect(&ctx, "SELECT \"MY_FUNC\"(i) FROM t").await?;

        let expected = vec![
            "+--------------+",
            "| MY_FUNC(t.i) |",
            "+--------------+",
            "| 1            |",
            "+--------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_user_defined_aggregates() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        // Note capitalization
        let my_avg = create_udaf(
            "MY_AVG",
            DataType::Float64,
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|_| {
                Ok(Box::new(AvgAccumulator::try_new(
                    &DataType::Float64,
                    &DataType::Float64,
                )?))
            }),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        );

        ctx.register_udaf(my_avg);

        // doesn't work as it was registered as non lowercase
        let err = plan_and_collect(&ctx, "SELECT MY_AVG(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function \'my_avg\'"
        );

        // Can call it if you put quotes
        let result = plan_and_collect(&ctx, "SELECT \"MY_AVG\"(i) FROM t").await?;

        let expected = vec![
            "+-------------+",
            "| MY_AVG(t.i) |",
            "+-------------+",
            "| 1.0         |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn query_csv_with_custom_partition_extension() -> Result<()> {
        let tmp_dir = TempDir::new()?;

        // The main stipulation of this test: use a file extension that isn't .csv.
        let file_extension = ".tst";

        let ctx = SessionContext::new();
        let schema = populate_csv_partitions(&tmp_dir, 2, file_extension)?;
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new()
                .schema(&schema)
                .file_extension(file_extension),
        )
        .await?;
        let results =
            plan_and_collect(&ctx, "SELECT SUM(c1), SUM(c2), COUNT(*) FROM test").await?;

        assert_eq!(results.len(), 1);
        let expected = vec![
            "+--------------+--------------+-----------------+",
            "| SUM(test.c1) | SUM(test.c2) | COUNT(UInt8(1)) |",
            "+--------------+--------------+-----------------+",
            "| 10           | 110          | 20              |",
            "+--------------+--------------+-----------------+",
        ];
        assert_batches_eq!(expected, &results);

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
                tokio::spawn(async move {
                    // Ensure we can create logical plan code on a separate thread.
                    ctx.sql("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")
                        .await
                })
            })
            .collect();

        for handle in threads {
            handle.await.unwrap().unwrap();
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
        let session_state = SessionState::with_config_rt(cfg, runtime);
        let ctx = SessionContext::with_state(session_state);
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
        let session_state = SessionState::with_config_rt(SessionConfig::new(), runtime)
            .with_query_planner(Arc::new(MyQueryPlanner {}));
        let ctx = SessionContext::with_state(session_state);

        let df = ctx.sql("SELECT 1").await?;
        df.collect().await.expect_err("query not supported");
        Ok(())
    }

    #[tokio::test]
    async fn disabled_default_catalog_and_schema() -> Result<()> {
        let ctx = SessionContext::with_config(
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
        let ctx = SessionContext::with_config(config);
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

            let expected = vec![
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

        let expected = vec![
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
        let ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // register a single catalog
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let catalog_weak = Arc::downgrade(&catalog);
        ctx.register_catalog("my_catalog", catalog);

        let catalog_list_weak = {
            let state = ctx.state.read();
            Arc::downgrade(&state.catalog_list)
        };

        drop(ctx);

        assert_eq!(Weak::strong_count(&catalog_list_weak), 0);
        assert_eq!(Weak::strong_count(&catalog_weak), 0);
    }

    #[tokio::test]
    async fn sql_create_schema() -> Result<()> {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::with_config(
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
        let ctx = SessionContext::with_config(
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

    #[tokio::test]
    async fn read_with_glob_path() -> Result<()> {
        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(
                format!("{}/alltypes_plain*.parquet", parquet_test_data()),
                ParquetReadOptions::default(),
            )
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
        // alltypes_plain.parquet = 8 rows, alltypes_plain.snappy.parquet = 2 rows, alltypes_dictionary.parquet = 2 rows
        assert_eq!(total_rows, 10);
        Ok(())
    }

    #[tokio::test]
    async fn read_with_glob_path_issue_2465() -> Result<()> {
        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(
                // it was reported that when a path contains // (two consecutive separator) no files were found
                // in this test, regardless of parquet_test_data() value, our path now contains a //
                format!("{}/..//*/alltypes_plain*.parquet", parquet_test_data()),
                ParquetReadOptions::default(),
            )
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
        // alltypes_plain.parquet = 8 rows, alltypes_plain.snappy.parquet = 2 rows, alltypes_dictionary.parquet = 2 rows
        assert_eq!(total_rows, 10);
        Ok(())
    }

    #[tokio::test]
    async fn read_from_registered_table_with_glob_path() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain*.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;
        let df = ctx.sql("SELECT * FROM test").await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
        // alltypes_plain.parquet = 8 rows, alltypes_plain.snappy.parquet = 2 rows, alltypes_dictionary.parquet = 2 rows
        assert_eq!(total_rows, 10);
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
            Err(DataFusionError::NotImplemented(
                "query not supported".to_string(),
            ))
        }

        fn create_physical_expr(
            &self,
            _expr: &Expr,
            _input_dfschema: &crate::common::DFSchema,
            _input_schema: &Schema,
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

    /// Execute SQL and return results
    async fn plan_and_collect(
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        ctx.sql(sql).await?.collect().await
    }

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
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

    /// Generate a partitioned CSV file and register it with an execution context
    async fn create_ctx(
        tmp_dir: &TempDir,
        partition_count: usize,
    ) -> Result<SessionContext> {
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(8));

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

    // Test for compilation error when calling read_* functions from an #[async_trait] function.
    // See https://github.com/apache/arrow-datafusion/issues/1154
    #[async_trait]
    trait CallReadTrait {
        async fn call_read_csv(&self) -> DataFrame;
        async fn call_read_avro(&self) -> DataFrame;
        async fn call_read_parquet(&self) -> DataFrame;
    }

    struct CallRead {}

    #[async_trait]
    impl CallReadTrait for CallRead {
        async fn call_read_csv(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_csv("dummy", CsvReadOptions::new()).await.unwrap()
        }

        async fn call_read_avro(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_avro("dummy", AvroReadOptions::default())
                .await
                .unwrap()
        }

        async fn call_read_parquet(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_parquet("dummy", ParquetReadOptions::default())
                .await
                .unwrap()
        }
    }
}
