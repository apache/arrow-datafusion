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

//! SessionContext contains methods for registering data sources and executing queries
use crate::{
    catalog::{
        catalog::{CatalogList, MemoryCatalogList},
        information_schema::CatalogWithInformationSchema,
    },
    datasource::listing::{ListingOptions, ListingTable},
    datasource::{
        file_format::{
            avro::{AvroFormat, DEFAULT_AVRO_EXTENSION},
            csv::{CsvFormat, DEFAULT_CSV_EXTENSION},
            json::{JsonFormat, DEFAULT_JSON_EXTENSION},
            parquet::{ParquetFormat, DEFAULT_PARQUET_EXTENSION},
            FileFormat,
        },
        MemTable,
    },
    logical_plan::{PlanType, ToStringifiedPlan},
    optimizer::eliminate_filter::EliminateFilter,
    optimizer::eliminate_limit::EliminateLimit,
    physical_optimizer::{
        aggregate_statistics::AggregateStatistics,
        hash_build_probe_order::HashBuildProbeOrder, optimizer::PhysicalOptimizerRule,
    },
};
use log::{debug, trace};
use parking_lot::RwLock;
use std::string::String;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use arrow::datatypes::{DataType, SchemaRef};

use crate::catalog::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    schema::{MemorySchemaProvider, SchemaProvider},
    ResolvedTableReference, TableReference,
};
use crate::dataframe::DataFrame;
use crate::datasource::listing::ListingTableConfig;
use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};
use crate::logical_plan::{
    CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
    DropTable, FileType, FunctionRegistry, LogicalPlan, LogicalPlanBuilder,
    UNNAMED_TABLE,
};
use crate::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use crate::optimizer::filter_push_down::FilterPushDown;
use crate::optimizer::limit_push_down::LimitPushDown;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::projection_push_down::ProjectionPushDown;
use crate::optimizer::simplify_expressions::SimplifyExpressions;
use crate::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use crate::optimizer::to_approx_perc::ToApproxPerc;

use crate::physical_optimizer::coalesce_batches::CoalesceBatches;
use crate::physical_optimizer::merge_exec::AddCoalescePartitionsExec;
use crate::physical_optimizer::repartition::Repartition;

use crate::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use crate::logical_plan::plan::Explain;
use crate::physical_plan::file_format::{plan_to_csv, plan_to_json, plan_to_parquet};
use crate::physical_plan::planner::DefaultPhysicalPlanner;
use crate::physical_plan::udaf::AggregateUDF;
use crate::physical_plan::udf::ScalarUDF;
use crate::physical_plan::udtf::TableUDF;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::PhysicalPlanner;
use crate::sql::{
    parser::DFParser,
    planner::{ContextProvider, SqlToRel},
};
use crate::variable::{VarProvider, VarType};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::options::{
    AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions,
};

/// The default catalog name - this impacts what SQL queries use if not specified
const DEFAULT_CATALOG: &str = "datafusion";
/// The default schema name - this impacts what SQL queries use if not specified
const DEFAULT_SCHEMA: &str = "public";

/// SessionContext is the main interface for executing queries with DataFusion. It stands for
/// the connection between user and DataFusion/Ballista cluster.
/// The context provides the following functionality
///
/// * Create DataFrame from a CSV or Parquet data source.
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
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(100)?;
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
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await?;
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SessionContext {
    /// Uuid for the session
    session_id: String,
    /// Session start time
    pub session_start_time: DateTime<Utc>,
    /// Shared session state for the session
    pub state: Arc<RwLock<SessionState>>,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContext {
    /// Creates a new execution context using a default session configuration.
    pub fn new() -> Self {
        Self::with_config(SessionConfig::new())
    }

    /// Creates a new session context using the provided session configuration.
    pub fn with_config(config: SessionConfig) -> Self {
        let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap());
        Self::with_config_rt(config, runtime)
    }

    /// Creates a new session context using the provided configuration and RuntimeEnv.
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let state = SessionState::with_config_rt(config, runtime);
        Self {
            session_id: state.session_id.clone(),
            session_start_time: chrono::Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Creates a new session context using the provided session state.
    pub fn with_state(state: SessionState) -> Self {
        Self {
            session_id: state.session_id.clone(),
            session_start_time: chrono::Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Return the [RuntimeEnv] used to run queries with this [SessionContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.state.read().runtime_env.clone()
    }

    /// Return the session_id of this Session
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return a copied version of config for this Session
    pub fn copied_config(&self) -> SessionConfig {
        self.state.read().config.clone()
    }

    /// Creates a dataframe that will execute a SQL query.
    ///
    /// This method is `async` because queries of type `CREATE EXTERNAL TABLE`
    /// might require the schema to be inferred.
    pub async fn sql(&self, sql: &str) -> Result<Arc<DataFrame>> {
        let plan = self.create_logical_plan(sql)?;
        match plan {
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref has_header,
                ref delimiter,
                ref table_partition_cols,
                ref if_not_exists,
            }) => {
                let (file_format, file_extension) = match file_type {
                    FileType::CSV => (
                        Arc::new(
                            CsvFormat::default()
                                .with_has_header(*has_header)
                                .with_delimiter(*delimiter as u8),
                        ) as Arc<dyn FileFormat>,
                        DEFAULT_CSV_EXTENSION,
                    ),
                    FileType::Parquet => (
                        Arc::new(ParquetFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_PARQUET_EXTENSION,
                    ),
                    FileType::Avro => (
                        Arc::new(AvroFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_AVRO_EXTENSION,
                    ),
                    FileType::NdJson => (
                        Arc::new(JsonFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_JSON_EXTENSION,
                    ),
                };
                let table = self.table(name.as_str());
                match (if_not_exists, table) {
                    (true, Ok(_)) => {
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (_, Err(_)) => {
                        // TODO make schema in CreateExternalTable optional instead of empty
                        let provided_schema = if schema.fields().is_empty() {
                            None
                        } else {
                            Some(Arc::new(schema.as_ref().to_owned().into()))
                        };
                        let options = ListingOptions {
                            format: file_format,
                            collect_stat: false,
                            file_extension: file_extension.to_owned(),
                            target_partitions: self.copied_config().target_partitions,
                            table_partition_cols: table_partition_cols.clone(),
                        };
                        self.register_listing_table(
                            name,
                            location,
                            options,
                            provided_schema,
                        )
                        .await?;
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{:?}' already exists",
                        name
                    ))),
                }
            }

            LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists,
            }) => {
                let table = self.table(name.as_str());

                match (if_not_exists, table) {
                    (true, Ok(_)) => {
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (_, Err(_)) => {
                        let plan = self.optimize(&input)?;
                        let physical =
                            Arc::new(DataFrame::new(self.state.clone(), &plan));

                        let batches: Vec<_> = physical.collect_partitioned().await?;
                        let table = Arc::new(MemTable::try_new(
                            Arc::new(plan.schema().as_ref().into()),
                            batches,
                        )?);

                        self.register_table(name.as_str(), table)?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{:?}' already exists",
                        name
                    ))),
                }
            }

            LogicalPlan::DropTable(DropTable {
                name, if_exists, ..
            }) => {
                let returned = self.deregister_table(name.as_str())?;
                if !if_exists && returned.is_none() {
                    Err(DataFusionError::Execution(format!(
                        "Memory table {:?} doesn't exist.",
                        name
                    )))
                } else {
                    let plan = LogicalPlanBuilder::empty(false).build()?;
                    Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                }
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
                    1 => Ok((DEFAULT_CATALOG, schema_name.as_str())),
                    2 => Ok((tokens[0], tokens[1])),
                    _ => Err(DataFusionError::Execution(format!(
                        "Unable to parse catalog from {}",
                        schema_name
                    ))),
                }?;
                let catalog = self.catalog(catalog).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Missing '{}' catalog",
                        DEFAULT_CATALOG
                    ))
                })?;

                let schema = catalog.schema(schema_name);

                match (if_not_exists, schema) {
                    (true, Some(_)) => {
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (true, None) | (false, None) => {
                        let schema = Arc::new(MemorySchemaProvider::new());
                        catalog.register_schema(schema_name, schema)?;
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (false, Some(_)) => Err(DataFusionError::Execution(format!(
                        "Schema '{:?}' already exists",
                        schema_name
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
                    (true, Some(_)) => {
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (true, None) | (false, None) => {
                        let new_catalog = Arc::new(MemoryCatalogProvider::new());
                        self.state
                            .write()
                            .catalog_list
                            .register_catalog(catalog_name, new_catalog);
                        let plan = LogicalPlanBuilder::empty(false).build()?;
                        Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
                    }
                    (false, Some(_)) => Err(DataFusionError::Execution(format!(
                        "Catalog '{:?}' already exists",
                        catalog_name
                    ))),
                }
            }

            plan => Ok(Arc::new(DataFrame::new(
                self.state.clone(),
                &self.optimize(&plan)?,
            ))),
        }
    }

    /// Creates a logical plan.
    ///
    /// This function is intended for internal use and should not be called directly.
    pub fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        // create a query planner
        let state = self.state.read().clone();
        let query_planner = SqlToRel::new(&state);
        query_planner.statement_to_plan(statements.pop_front().unwrap())
    }

    /// Registers a variable provider within this context.
    pub fn register_variable(
        &mut self,
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
    pub fn register_udf(&mut self, f: ScalarUDF) {
        self.state
            .write()
            .scalar_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Registers a table UDF within this context.
    ///
    /// Note in SQL queries, function names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_FUNC(x)...` will look for a function named `"my_func"`
    /// `SELECT "my_FUNC"(x)` will look for a function named `"my_FUNC"`
    pub fn register_udtf(&mut self, f: TableUDF) {
        self.state
            .write()
            .table_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Registers an aggregate UDF within this context.
    ///
    /// Note in SQL queries, aggregate names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_UDAF(x)...` will look for an aggregate named `"my_udaf"`
    /// `SELECT "my_UDAF"(x)` will look for an aggregate named `"my_UDAF"`
    pub fn register_udaf(&mut self, f: AggregateUDF) {
        self.state
            .write()
            .aggregate_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Creates a DataFrame for reading an Avro data source.
    pub async fn read_avro(
        &self,
        uri: impl Into<String>,
        options: AvroReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.runtime_env().object_store(&uri)?;
        let target_partitions = self.copied_config().target_partitions;
        Ok(Arc::new(DataFrame::new(
            self.state.clone(),
            &LogicalPlanBuilder::scan_avro(
                object_store,
                path,
                options,
                None,
                target_partitions,
            )
            .await?
            .build()?,
        )))
    }

    /// Creates a DataFrame for reading an Json data source.
    pub async fn read_json(
        &mut self,
        uri: impl Into<String>,
        options: NdJsonReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.runtime_env().object_store(&uri)?;
        let target_partitions = self.copied_config().target_partitions;
        Ok(Arc::new(DataFrame::new(
            self.state.clone(),
            &LogicalPlanBuilder::scan_json(
                object_store,
                path,
                options,
                None,
                target_partitions,
            )
            .await?
            .build()?,
        )))
    }

    /// Creates an empty DataFrame.
    pub fn read_empty(&self) -> Result<Arc<DataFrame>> {
        Ok(Arc::new(DataFrame::new(
            self.state.clone(),
            &LogicalPlanBuilder::empty(true).build()?,
        )))
    }

    /// Creates a DataFrame for reading a CSV data source.
    pub async fn read_csv(
        &self,
        uri: impl Into<String>,
        options: CsvReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.runtime_env().object_store(&uri)?;
        let target_partitions = self.copied_config().target_partitions;
        Ok(Arc::new(DataFrame::new(
            self.state.clone(),
            &LogicalPlanBuilder::scan_csv(
                object_store,
                path,
                options,
                None,
                target_partitions,
            )
            .await?
            .build()?,
        )))
    }

    /// Creates a DataFrame for reading a Parquet data source.
    pub async fn read_parquet(
        &self,
        uri: impl Into<String>,
        options: ParquetReadOptions<'_>,
    ) -> Result<Arc<DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.runtime_env().object_store(&uri)?;
        let target_partitions = self.copied_config().target_partitions;
        let logical_plan = LogicalPlanBuilder::scan_parquet(
            object_store,
            path,
            options,
            None,
            target_partitions,
        )
        .await?
        .build()?;
        Ok(Arc::new(DataFrame::new(self.state.clone(), &logical_plan)))
    }

    /// Creates a DataFrame for reading a custom TableProvider.
    pub fn read_table(&self, provider: Arc<dyn TableProvider>) -> Result<Arc<DataFrame>> {
        Ok(Arc::new(DataFrame::new(
            self.state.clone(),
            &LogicalPlanBuilder::scan(UNNAMED_TABLE, provider, None)?.build()?,
        )))
    }

    /// Registers a table that uses the listing feature of the object store to
    /// find the files to be processed
    /// This is async because it might need to resolve the schema.
    pub async fn register_listing_table<'a>(
        &'a self,
        name: &'a str,
        uri: &'a str,
        options: ListingOptions,
        provided_schema: Option<SchemaRef>,
    ) -> Result<()> {
        let (object_store, path) = self.runtime_env().object_store(uri)?;
        let resolved_schema = match provided_schema {
            None => {
                options
                    .infer_schema(Arc::clone(&object_store), path)
                    .await?
            }
            Some(s) => s,
        };
        let config = ListingTableConfig::new(object_store, path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?;
        self.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Registers a CSV data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub async fn register_csv(
        &self,
        name: &str,
        uri: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.copied_config().target_partitions);

        self.register_listing_table(
            name,
            uri,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
        )
        .await?;

        Ok(())
    }

    // Registers a Json data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub async fn register_json(
        &self,
        name: &str,
        uri: &str,
        options: NdJsonReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.copied_config().target_partitions);

        self.register_listing_table(name, uri, listing_options, options.schema)
            .await?;
        Ok(())
    }

    /// Registers a Parquet data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub async fn register_parquet(
        &self,
        name: &str,
        uri: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        let (target_partitions, parquet_pruning) = {
            let conf = self.copied_config();
            (conf.target_partitions, conf.parquet_pruning)
        };
        let listing_options = options
            .parquet_pruning(parquet_pruning)
            .to_listing_options(target_partitions);

        self.register_listing_table(name, uri, listing_options, None)
            .await?;
        Ok(())
    }

    /// Registers an Avro data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub async fn register_avro(
        &self,
        name: &str,
        uri: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.copied_config().target_partitions);

        self.register_listing_table(name, uri, listing_options, options.schema)
            .await?;
        Ok(())
    }

    /// Registers a named catalog using a custom `CatalogProvider` so that
    /// it can be referenced from SQL statements executed against this
    /// context.
    ///
    /// Returns the `CatalogProvider` previously registered for this
    /// name, if any
    pub fn register_catalog(
        &self,
        name: impl Into<String>,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let name = name.into();
        let information_schema = self.copied_config().information_schema;
        let state = self.state.read();
        let catalog = if information_schema {
            Arc::new(CatalogWithInformationSchema::new(
                Arc::downgrade(&state.catalog_list),
                catalog,
            ))
        } else {
            catalog
        };

        state.catalog_list.register_catalog(name, catalog)
    }

    /// Retrieves a `CatalogProvider` instance by name
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.state.read().catalog_list.catalog(name)
    }

    /// Registers a table using a custom `TableProvider` so that
    /// it can be referenced from SQL statements executed against this
    /// context.
    ///
    /// Returns the `TableProvider` previously registered for this
    /// reference, if any
    pub fn register_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table_ref.table().to_owned(), provider)
    }

    /// Deregisters the given table.
    ///
    /// Returns the registered provider, if any
    pub fn deregister_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .deregister_table(table_ref.table())
    }

    /// Check whether the given table exists in the schema provider or not
    /// Returns true if the table exists.
    pub fn table_exist<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<bool> {
        let table_ref = table_ref.into();
        Ok(self
            .state
            .read()
            .schema_for_ref(table_ref)?
            .table_exist(table_ref.table()))
    }

    /// Retrieves a DataFrame representing a table previously registered by calling the
    /// register_table function.
    ///
    /// Returns an error if no table has been registered with the provided reference.
    pub fn table<'a>(
        &self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<DataFrame>> {
        let table_ref = table_ref.into();
        let schema = self.state.read().schema_for_ref(table_ref)?;
        match schema.table(table_ref.table()) {
            Some(ref provider) => {
                let plan = LogicalPlanBuilder::scan(
                    table_ref.table(),
                    Arc::clone(provider),
                    None,
                )?
                .build()?;
                Ok(Arc::new(DataFrame::new(self.state.clone(), &plan)))
            }
            _ => Err(DataFusionError::Plan(format!(
                "No table named '{}'",
                table_ref.table()
            ))),
        }
    }

    /// Returns the set of available tables in the default catalog and schema.
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
            .schema_for_ref(TableReference::Bare { table: "" })?
            .table_names()
            .iter()
            .cloned()
            .collect())
    }

    /// Optimizes the logical plan by applying optimizer rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.state.read().optimize(plan)
    }

    /// Creates a physical plan from a logical plan.
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state_cloned = {
            let mut state = self.state.write();
            state.execution_props.start_execution();

            // We need to clone `state` to release the lock that is not `Send`. We could
            // make the lock `Send` by using `tokio::sync::Mutex`, but that would require to
            // propagate async even to the `LogicalPlan` building methods.
            // Cloning `state` here is fine as we then pass it as immutable `&state`, which
            // means that we avoid write consistency issues as the cloned version will not
            // be written to. As for eventual modifications that would be applied to the
            // original state after it has been cloned, they will not be picked up by the
            // clone but that is okay, as it is equivalent to postponing the state update
            // by keeping the lock until the end of the function scope.
            state.clone()
        };

        state_cloned.create_physical_plan(logical_plan).await
    }

    /// Executes a query and writes the results to a partitioned CSV file.
    pub async fn write_csv(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        let state = self.state.read().clone();
        plan_to_csv(&state, plan, path).await
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    pub async fn write_json(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        let state = self.state.read().clone();
        plan_to_json(&state, plan, path).await
    }

    /// Executes a query and writes the results to a partitioned Parquet file.
    pub async fn write_parquet(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
        writer_properties: Option<WriterProperties>,
    ) -> Result<()> {
        let state = self.state.read().clone();
        plan_to_parquet(&state, plan, path, writer_properties).await
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
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

/// Session Configuration entry name for 'BATCH_SIZE'
pub const BATCH_SIZE: &str = "batch_size";
/// Session Configuration entry name for 'TARGET_PARTITIONS'
pub const TARGET_PARTITIONS: &str = "target_partitions";
/// Session Configuration entry name for 'REPARTITION_JOINS'
pub const REPARTITION_JOINS: &str = "repartition_joins";
/// Session Configuration entry name for 'REPARTITION_AGGREGATIONS'
pub const REPARTITION_AGGREGATIONS: &str = "repartition_aggregations";
/// Session Configuration entry name for 'REPARTITION_WINDOWS'
pub const REPARTITION_WINDOWS: &str = "repartition_windows";
/// Session Configuration entry name for 'PARQUET_PRUNING'
pub const PARQUET_PRUNING: &str = "parquet_pruning";

/// Configuration options for session context
#[derive(Clone)]
pub struct SessionConfig {
    /// Default batch size while creating new batches, it's especially useful
    /// for buffer-in-memory batches since creating tiny batches would results
    /// in too much metadata memory consumption.
    pub batch_size: usize,
    /// Number of partitions for query execution. Increasing partitions can increase concurrency.
    pub target_partitions: usize,
    /// Default catalog name for table resolution
    default_catalog: String,
    /// Default schema name for table resolution
    default_schema: String,
    /// Whether the default catalog and schema should be created automatically
    create_default_catalog_and_schema: bool,
    /// Should DataFusion provide access to `information_schema`
    /// virtual tables for displaying schema information
    information_schema: bool,
    /// Should DataFusion repartition data using the join keys to execute joins in parallel
    /// using the provided `target_partitions` level
    pub repartition_joins: bool,
    /// Should DataFusion repartition data using the aggregate keys to execute aggregates in parallel
    /// using the provided `target_partitions` level
    pub repartition_aggregations: bool,
    /// Should DataFusion repartition data using the partition keys to execute window functions in
    /// parallel using the provided `target_partitions` level
    pub repartition_windows: bool,
    /// Should DataFusion parquet reader using the predicate to prune data
    pub parquet_pruning: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            target_partitions: num_cpus::get(),
            default_catalog: DEFAULT_CATALOG.to_owned(),
            default_schema: DEFAULT_SCHEMA.to_owned(),
            create_default_catalog_and_schema: true,
            information_schema: false,
            repartition_joins: true,
            repartition_aggregations: true,
            repartition_windows: true,
            parquet_pruning: true,
        }
    }
}

impl SessionConfig {
    /// Create an execution config with default setting
    pub fn new() -> Self {
        Default::default()
    }

    /// Customize batch size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.batch_size = n;
        self
    }

    /// Customize target_partitions
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        // partition count must be greater than zero
        assert!(n > 0);
        self.target_partitions = n;
        self
    }

    /// Selects a name for the default catalog and schema
    pub fn with_default_catalog_and_schema(
        mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        self.default_catalog = catalog.into();
        self.default_schema = schema.into();
        self
    }

    /// Controls whether the default catalog and schema will be automatically created
    pub fn create_default_catalog_and_schema(mut self, create: bool) -> Self {
        self.create_default_catalog_and_schema = create;
        self
    }

    /// Enables or disables the inclusion of `information_schema` virtual tables
    pub fn with_information_schema(mut self, enabled: bool) -> Self {
        self.information_schema = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for joins to improve parallelism
    pub fn with_repartition_joins(mut self, enabled: bool) -> Self {
        self.repartition_joins = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for aggregations to improve parallelism
    pub fn with_repartition_aggregations(mut self, enabled: bool) -> Self {
        self.repartition_aggregations = enabled;
        self
    }

    /// Enables or disables the use of repartitioning for window functions to improve parallelism
    pub fn with_repartition_windows(mut self, enabled: bool) -> Self {
        self.repartition_windows = enabled;
        self
    }

    /// Enables or disables the use of pruning predicate for parquet readers to skip row groups
    pub fn with_parquet_pruning(mut self, enabled: bool) -> Self {
        self.parquet_pruning = enabled;
        self
    }

    /// Convert configuration to name-value pairs
    pub fn to_props(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert(BATCH_SIZE.to_owned(), format!("{}", self.batch_size));
        map.insert(
            TARGET_PARTITIONS.to_owned(),
            format!("{}", self.target_partitions),
        );
        map.insert(
            REPARTITION_JOINS.to_owned(),
            format!("{}", self.repartition_joins),
        );
        map.insert(
            REPARTITION_AGGREGATIONS.to_owned(),
            format!("{}", self.repartition_aggregations),
        );
        map.insert(
            REPARTITION_WINDOWS.to_owned(),
            format!("{}", self.repartition_windows),
        );
        map.insert(
            PARQUET_PRUNING.to_owned(),
            format!("{}", self.parquet_pruning),
        );
        map
    }
}

/// Holds per-execution properties and data (such as starting timestamps, etc).
/// An instance of this struct is created each time a [`LogicalPlan`] is prepared for
/// execution (optimized). If the same plan is optimized multiple times, a new
/// `ExecutionProps` is created each time.
///
/// It is important that this structure be cheap to create as it is
/// done so during predicate pruning and expression simplification
#[derive(Clone)]
pub struct ExecutionProps {
    pub(crate) query_execution_start_time: DateTime<Utc>,
    /// providers for scalar variables
    pub var_providers: Option<HashMap<VarType, Arc<dyn VarProvider + Send + Sync>>>,
}

impl Default for ExecutionProps {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionProps {
    /// Creates a new execution props
    pub fn new() -> Self {
        ExecutionProps {
            query_execution_start_time: chrono::Utc::now(),
            var_providers: None,
        }
    }

    /// Marks the execution of query started timestamp
    pub fn start_execution(&mut self) -> &Self {
        self.query_execution_start_time = chrono::Utc::now();
        &*self
    }

    /// Registers a variable provider, returning the existing
    /// provider, if any
    pub fn add_var_provider(
        &mut self,
        var_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) -> Option<Arc<dyn VarProvider + Send + Sync>> {
        let mut var_providers = self.var_providers.take().unwrap_or_default();

        let old_provider = var_providers.insert(var_type, provider);

        self.var_providers = Some(var_providers);

        old_provider
    }

    /// Returns the provider for the var_type, if any
    pub fn get_var_provider(
        &self,
        var_type: VarType,
    ) -> Option<Arc<dyn VarProvider + Send + Sync>> {
        self.var_providers
            .as_ref()
            .and_then(|var_providers| var_providers.get(&var_type).map(Arc::clone))
    }
}

/// Execution context for registering data sources and executing queries
#[derive(Clone)]
pub struct SessionState {
    /// Uuid for the session
    pub session_id: String,
    /// Responsible for optimizing a logical plan
    pub optimizers: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    /// Responsible for optimizing a physical execution plan
    pub physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    pub query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalog_list: Arc<dyn CatalogList>,
    /// Scalar functions that are registered with the context
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Table functions that are registered with the context
    pub table_functions: HashMap<String, Arc<TableUDF>>,
    /// Aggregate functions registered in the context
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Session configuration
    pub config: SessionConfig,
    /// Execution properties
    pub execution_props: ExecutionProps,
    /// Runtime environment
    pub runtime_env: Arc<RuntimeEnv>,
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
    SessionState::with_config_rt(
        config,
        Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap()),
    )
}

impl SessionState {
    /// Returns new SessionState using the provided configuration and runtime
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let session_id = Uuid::new_v4().to_string();

        let catalog_list = Arc::new(MemoryCatalogList::new()) as Arc<dyn CatalogList>;
        if config.create_default_catalog_and_schema {
            let default_catalog = MemoryCatalogProvider::new();

            default_catalog
                .register_schema(
                    &config.default_schema,
                    Arc::new(MemorySchemaProvider::new()),
                )
                .expect("memory catalog provider can register schema");

            let default_catalog: Arc<dyn CatalogProvider> = if config.information_schema {
                Arc::new(CatalogWithInformationSchema::new(
                    Arc::downgrade(&catalog_list),
                    Arc::new(default_catalog),
                ))
            } else {
                Arc::new(default_catalog)
            };
            catalog_list
                .register_catalog(config.default_catalog.clone(), default_catalog);
        }

        SessionState {
            session_id,
            optimizers: vec![
                // Simplify expressions first to maximize the chance
                // of applying other optimizations
                Arc::new(SimplifyExpressions::new()),
                Arc::new(EliminateFilter::new()),
                Arc::new(CommonSubexprEliminate::new()),
                Arc::new(EliminateLimit::new()),
                Arc::new(ProjectionPushDown::new()),
                Arc::new(FilterPushDown::new()),
                Arc::new(LimitPushDown::new()),
                Arc::new(SingleDistinctToGroupBy::new()),
                // ToApproxPerc must be applied last because
                // it rewrites only the function and may interfere with
                // other rules
                Arc::new(ToApproxPerc::new()),
            ],
            physical_optimizers: vec![
                Arc::new(AggregateStatistics::new()),
                Arc::new(HashBuildProbeOrder::new()),
                Arc::new(CoalesceBatches::new()),
                Arc::new(Repartition::new()),
                Arc::new(AddCoalescePartitionsExec::new()),
            ],
            query_planner: Arc::new(DefaultQueryPlanner {}),
            catalog_list,
            scalar_functions: HashMap::new(),
            table_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            config,
            execution_props: ExecutionProps::new(),
            runtime_env: runtime,
        }
    }

    fn resolve_table_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> ResolvedTableReference<'a> {
        table_ref
            .into()
            .resolve(&self.config.default_catalog, &self.config.default_schema)
    }

    fn schema_for_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.resolve_table_ref(table_ref);

        self.catalog_list
            .catalog(resolved_ref.catalog)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                ))
            })?
            .schema(resolved_ref.schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve schema: {}",
                    resolved_ref.schema
                ))
            })
    }

    /// Replace the default query planner
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = query_planner;
        self
    }

    /// Replace the optimizer rules
    pub fn with_optimizer_rules(
        mut self,
        optimizers: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizers = optimizers;
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

    /// Adds a new [`OptimizerRule`]
    pub fn add_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        self.optimizers.push(optimizer_rule);
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

    /// Optimizes the logical plan by applying optimizer rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        if let LogicalPlan::Explain(e) = plan {
            let mut stringified_plans = e.stringified_plans.clone();

            // optimize the child plan, capturing the output of each optimizer
            let plan =
                self.optimize_internal(e.plan.as_ref(), |optimized_plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(optimized_plan.to_stringified(plan_type));
                })?;

            Ok(LogicalPlan::Explain(Explain {
                verbose: e.verbose,
                plan: Arc::new(plan),
                stringified_plans,
                schema: e.schema.clone(),
            }))
        } else {
            self.optimize_internal(plan, |_, _| {})
        }
    }

    /// Optimizes the logical plan by applying optimizer rules, and
    /// invoking observer function after each call
    fn optimize_internal<F>(
        &self,
        plan: &LogicalPlan,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        let execution_props = &mut self.execution_props.clone();
        let optimizers = &self.optimizers;

        let execution_props = execution_props.start_execution();

        let mut new_plan = plan.clone();
        debug!("Input logical plan:\n{}\n", plan.display_indent());
        trace!("Full input logical plan:\n{:?}", plan);
        for optimizer in optimizers {
            new_plan = optimizer.optimize(&new_plan, execution_props)?;
            observer(&new_plan, optimizer.as_ref());
        }
        debug!("Optimized logical plan:\n{}\n", new_plan.display_indent());
        trace!("Full Optimized logical plan:\n {:?}", plan);
        Ok(new_plan)
    }

    /// Creates a physical plan from a logical plan.
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = self.query_planner.clone();
        planner.create_physical_plan(logical_plan, self).await
    }
}

impl ContextProvider for SessionState {
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
        let resolved_ref = self.resolve_table_ref(name);
        let schema = self.schema_for_ref(resolved_ref).ok()?;
        schema.table(resolved_ref.table)
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions.get(name).cloned()
    }

    fn get_table_function_meta(&self, name: &str) -> Option<Arc<TableUDF>> {
        self.table_functions
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if &variable_names[0][0..2] == "@@" {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.execution_props
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
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
                "There is no UDF named \"{}\" in the registry",
                name
            ))
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "There is no UDAF named \"{}\" in the registry",
                name
            ))
        })
    }
}

/// Task Context Properties
pub enum TaskProperties {
    ///SessionConfig
    SessionConfig(SessionConfig),
    /// Name-value pairs of task properties
    KVPairs(HashMap<String, String>),
}

/// Task Execution Context
pub struct TaskContext {
    /// Session Id
    session_id: String,
    /// Optional Task Identify
    task_id: Option<String>,
    /// Task properties
    properties: TaskProperties,
    /// Scalar functions associated with this task context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions associated with this task context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Runtime environment associated with this task context
    runtime: Arc<RuntimeEnv>,
}

impl TaskContext {
    /// Create a new task context instance
    pub fn new(
        task_id: String,
        session_id: String,
        task_props: HashMap<String, String>,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            task_id: Some(task_id),
            session_id,
            properties: TaskProperties::KVPairs(task_props),
            scalar_functions,
            aggregate_functions,
            runtime,
        }
    }

    /// Return the SessionConfig associated with the Task
    pub fn session_config(&self) -> SessionConfig {
        let task_props = &self.properties;
        match task_props {
            TaskProperties::KVPairs(props) => {
                let session_config = SessionConfig::new();
                if props.is_empty() {
                    session_config
                } else {
                    session_config
                        .with_batch_size(props.get(BATCH_SIZE).unwrap().parse().unwrap())
                        .with_target_partitions(
                            props.get(TARGET_PARTITIONS).unwrap().parse().unwrap(),
                        )
                        .with_repartition_joins(
                            props.get(REPARTITION_JOINS).unwrap().parse().unwrap(),
                        )
                        .with_repartition_aggregations(
                            props
                                .get(REPARTITION_AGGREGATIONS)
                                .unwrap()
                                .parse()
                                .unwrap(),
                        )
                        .with_repartition_windows(
                            props.get(REPARTITION_WINDOWS).unwrap().parse().unwrap(),
                        )
                        .with_parquet_pruning(
                            props.get(PARQUET_PRUNING).unwrap().parse().unwrap(),
                        )
                }
            }
            TaskProperties::SessionConfig(session_config) => session_config.clone(),
        }
    }

    /// Return the session_id of this [TaskContext]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the task_id of this [TaskContext]
    pub fn task_id(&self) -> Option<String> {
        self.task_id.clone()
    }

    /// Return the [RuntimeEnv] associated with this [TaskContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime.clone()
    }
}

/// Create a new task context instance from SessionContext
impl From<&SessionContext> for TaskContext {
    fn from(session: &SessionContext) -> Self {
        let session_id = session.session_id.clone();
        let (config, scalar_functions, aggregate_functions) = {
            let session_state = session.state.read();
            (
                session_state.config.clone(),
                session_state.scalar_functions.clone(),
                session_state.aggregate_functions.clone(),
            )
        };
        let runtime = session.runtime_env();
        Self {
            task_id: None,
            session_id,
            properties: TaskProperties::SessionConfig(config),
            scalar_functions,
            aggregate_functions,
            runtime,
        }
    }
}

/// Create a new task context instance from SessionState
impl From<&SessionState> for TaskContext {
    fn from(state: &SessionState) -> Self {
        let session_id = state.session_id.clone();
        let config = state.config.clone();
        let scalar_functions = state.scalar_functions.clone();
        let aggregate_functions = state.aggregate_functions.clone();
        let runtime = state.runtime_env.clone();
        Self {
            task_id: None,
            session_id,
            properties: TaskProperties::SessionConfig(config),
            scalar_functions,
            aggregate_functions,
            runtime,
        }
    }
}

impl FunctionRegistry for TaskContext {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDF named \"{}\" in the TaskContext",
                name
            ))
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDAF named \"{}\" in the TaskContext",
                name
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::QueryPlanner;
    use crate::logical_plan::{binary_expr, lit, Operator};
    use crate::physical_plan::functions::{make_scalar_function, make_table_function};
    use crate::test;
    use crate::variable::VarType;
    use crate::{
        assert_batches_eq, assert_batches_sorted_eq,
        logical_plan::{col, create_udf, create_udtf, sum, Expr},
    };
    use crate::{logical_plan::create_udaf, physical_plan::expressions::AvgAccumulator};
    use arrow::array::{
        ArrayBuilder, ArrayRef, Int64Array, Int64Builder, StringBuilder, StructBuilder,
    };
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion_expr::{TableFunctionImplementation, Volatility};
    use std::fs::File;
    use std::sync::Weak;
    use std::thread::{self, JoinHandle};
    use std::{io::prelude::*, sync::Mutex};
    use tempfile::TempDir;

    #[tokio::test]
    async fn shared_memory_and_disk_manager() {
        // Demonstrate the ability to share DiskManager and
        // MemoryManager between two different executions.
        let ctx1 = SessionContext::new();

        // configure with same memory / disk manager
        let memory_manager = ctx1.runtime_env().memory_manager.clone();
        let disk_manager = ctx1.runtime_env().disk_manager.clone();

        let ctx2 =
            SessionContext::with_config_rt(SessionConfig::new(), ctx1.runtime_env());

        assert!(std::ptr::eq(
            Arc::as_ptr(&memory_manager),
            Arc::as_ptr(&ctx1.runtime_env().memory_manager)
        ));
        assert!(std::ptr::eq(
            Arc::as_ptr(&memory_manager),
            Arc::as_ptr(&ctx2.runtime_env().memory_manager)
        ));

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
        let mut ctx = create_ctx(&tmp_dir, partition_count).await?;

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
            "+----------------------+------------------------+------------------------+",
            "| @@version            | @name                  | @integer Plus Int64(1) |",
            "+----------------------+------------------------+------------------------+",
            "| system-var-@@version | user-defined-var-@name | 42                     |",
            "+----------------------+------------------------+------------------------+",
        ];
        assert_batches_eq!(expected, &results);

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
    async fn unprojected_filter() {
        let ctx = SessionContext::new();
        let df = ctx
            .read_table(test::table_with_sequence(1, 3).unwrap())
            .unwrap();

        let df = df
            .select(vec![binary_expr(col("i"), Operator::Plus, col("i"))])
            .unwrap()
            .filter(col("i").gt(lit(2)))
            .unwrap();
        let results = df.collect().await.unwrap();

        let expected = vec![
            "+--------------------------+",
            "| ?table?.i Plus ?table?.i |",
            "+--------------------------+",
            "| 6                        |",
            "+--------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
    }

    #[tokio::test]
    async fn aggregate_with_alias() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let ctx = create_ctx(&tmp_dir, 1).await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
        ]));

        let plan = LogicalPlanBuilder::scan_empty(None, schema.as_ref(), None)?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .project(vec![col("c1"), sum(col("c2")).alias("total_salary")])?
            .build()?;

        let plan = ctx.optimize(&plan)?;

        let physical_plan = ctx.create_physical_plan(&Arc::new(plan)).await?;
        assert_eq!("c1", physical_plan.schema().field(0).name().as_str());
        assert_eq!(
            "total_salary",
            physical_plan.schema().field(1).name().as_str()
        );
        Ok(())
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_user_defined_functions() -> Result<()> {
        let mut ctx = SessionContext::new();
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
        let mut ctx = SessionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        // Note capitalization
        let my_avg = create_udaf(
            "MY_AVG",
            DataType::Float64,
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?))),
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
            "| 1           |",
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
        let ctx = Arc::new(Mutex::new(create_ctx(&tmp_dir, partition_count).await?));

        let threads: Vec<JoinHandle<Result<_>>> = (0..2)
            .map(|_| ctx.clone())
            .map(|ctx_clone| {
                thread::spawn(move || {
                    let ctx = ctx_clone.lock().expect("Locked context");
                    // Ensure we can create logical plan code on a separate thread.
                    ctx.create_logical_plan(
                        "SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3",
                    )
                })
            })
            .collect();

        for thread in threads {
            thread.join().expect("Failed to join thread")?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn ctx_sql_should_optimize_plan() -> Result<()> {
        let ctx = SessionContext::new();
        let plan1 = ctx
            .create_logical_plan("SELECT * FROM (SELECT 1) AS one WHERE TRUE AND TRUE")?;

        let opt_plan1 = ctx.optimize(&plan1)?;

        let plan2 = ctx
            .sql("SELECT * FROM (SELECT 1) AS one WHERE TRUE AND TRUE")
            .await?;

        assert_eq!(
            format!("{:?}", opt_plan1),
            format!("{:?}", plan2.to_logical_plan())
        );

        Ok(())
    }

    #[tokio::test]
    async fn custom_query_planner() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap());
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
            SessionConfig::new().create_default_catalog_and_schema(false),
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
            .create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    #[tokio::test]
    async fn custom_catalog_and_schema_no_default() {
        let config = SessionConfig::new()
            .create_default_catalog_and_schema(false)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    #[tokio::test]
    async fn custom_catalog_and_schema_and_information_schema() {
        let config = SessionConfig::new()
            .create_default_catalog_and_schema(true)
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
                &format!("SELECT COUNT(*) AS count FROM {}", table_ref),
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
    async fn user_defined_table_function() -> Result<()> {
        let mut ctx = SessionContext::new();

        let integer_series = integer_udtf();
        ctx.register_udtf(create_udtf(
            "integer_series",
            vec![DataType::Int64, DataType::Int64],
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            integer_series,
        ));

        let struct_func = struct_udtf();
        ctx.register_udtf(create_udtf(
            "struct_func",
            vec![DataType::Int64],
            Arc::new(DataType::Struct(
                [
                    Field::new("f1", DataType::Utf8, false),
                    Field::new("f2", DataType::Int64, false),
                ]
                .to_vec(),
            )),
            Volatility::Immutable,
            struct_func,
        ));

        let result = plan_and_collect(&ctx, "SELECT struct_func(5)").await?;

        let expected = vec![
            "+-------------------------+",
            "| struct_func(Int64(5))   |",
            "+-------------------------+",
            "| {\"f1\": \"test\", \"f2\": 5} |",
            "+-------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        let result = plan_and_collect(&ctx, "SELECT integer_series(6,5)").await?;

        let expected = vec![
            "+-----------------------------------+",
            "| integer_series(Int64(6),Int64(5)) |",
            "+-----------------------------------+",
            "+-----------------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        let result = plan_and_collect(&ctx, "SELECT integer_series(1,5)").await?;

        let expected = vec![
            "+-----------------------------------+",
            "| integer_series(Int64(1),Int64(5)) |",
            "+-----------------------------------+",
            "| 1                                 |",
            "| 2                                 |",
            "| 3                                 |",
            "| 4                                 |",
            "| 5                                 |",
            "+-----------------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        let result = plan_and_collect(
            &ctx,
            "SELECT asd, struct_func(qwe), integer_series(asd, qwe), integer_series(1, qwe) r FROM (select 1 asd, 3 qwe UNION ALL select 2 asd, 4 qwe) x",
        )
        .await?;

        let expected = vec![
            "+-----+-------------------------+-----------------------------+---+",
            "| asd | struct_func(x.qwe)      | integer_series(x.asd,x.qwe) | r |",
            "+-----+-------------------------+-----------------------------+---+",
            "| 1   | {\"f1\": \"test\", \"f2\": 3} | 1                           | 1 |",
            "| 1   |                         | 2                           | 2 |",
            "| 1   |                         | 3                           | 3 |",
            "| 2   | {\"f1\": \"test\", \"f2\": 4} | 2                           | 1 |",
            "| 2   |                         | 3                           | 2 |",
            "| 2   |                         | 4                           | 3 |",
            "| 2   |                         |                             | 4 |",
            "+-----+-------------------------+-----------------------------+---+",
        ];

        assert_batches_eq!(expected, &result);

        let result =
            plan_and_collect(&ctx, "SELECT * from integer_series(1,5) pos(n)").await?;

        let expected = vec![
            "+---+", "| n |", "+---+", "| 1 |", "| 2 |", "| 3 |", "| 4 |", "| 5 |",
            "+---+",
        ];

        assert_batches_eq!(expected, &result);

        let result = plan_and_collect(&ctx, "SELECT * from integer_series(1,5)").await?;

        let expected = vec![
            "+-----------------------------------+",
            "| integer_series(Int64(1),Int64(5)) |",
            "+-----------------------------------+",
            "| 1                                 |",
            "| 2                                 |",
            "| 3                                 |",
            "| 4                                 |",
            "| 5                                 |",
            "+-----------------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    fn integer_udtf() -> TableFunctionImplementation {
        make_table_function(move |args: &[ArrayRef]| {
            assert!(args.len() == 2);

            let start_arr = &args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cast failed");

            let end_arr = &args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cast failed");

            let mut batch_sizes: Vec<usize> = Vec::new();
            let mut builder = Int64Builder::new(1);

            for (start, end) in start_arr.iter().zip(end_arr.iter()) {
                let start_number = start.unwrap();
                let end_number = end.unwrap();
                let count: usize = if end_number < start_number {
                    0
                } else {
                    (end_number - start_number + 1).try_into().unwrap()
                };
                batch_sizes.push(count);

                for i in start_number..end_number + 1 {
                    builder.append_value(i).unwrap();
                }
            }

            Ok((Arc::new(builder.finish()) as ArrayRef, batch_sizes))
        })
    }

    fn struct_udtf() -> TableFunctionImplementation {
        make_table_function(move |args: &[ArrayRef]| {
            let start_arr = &args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cast failed");

            let mut string_builder = StringBuilder::new(1);
            let mut int_builder = Int64Builder::new(1);

            let mut batch_sizes: Vec<usize> = Vec::new();
            for start in start_arr.iter() {
                let start_number = start.unwrap();
                batch_sizes.push(1);

                string_builder.append_value("test").unwrap();
                int_builder.append_value(start_number).unwrap();
            }

            let mut fields = Vec::new();
            let mut field_builders = Vec::new();
            fields.push(Field::new("f1", DataType::Utf8, false));
            field_builders.push(Box::new(string_builder) as Box<dyn ArrayBuilder>);
            fields.push(Field::new("f2", DataType::Int64, false));
            field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);

            let mut builder = StructBuilder::new(fields, field_builders);
            for _start in start_arr.iter() {
                builder.append(true).unwrap();
            }

            Ok((Arc::new(builder.finish()) as ArrayRef, batch_sizes))
        })
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
            _input_dfschema: &crate::logical_plan::DFSchema,
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
            let filename = format!("partition-{}.{}", partition, file_extension);
            let file_path = tmp_dir.path().join(&filename);
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
        async fn call_read_csv(&self) -> Arc<DataFrame>;
        async fn call_read_avro(&self) -> Arc<DataFrame>;
        async fn call_read_parquet(&self) -> Arc<DataFrame>;
    }

    struct CallRead {}

    #[async_trait]
    impl CallReadTrait for CallRead {
        async fn call_read_csv(&self) -> Arc<DataFrame> {
            let ctx = SessionContext::new();
            ctx.read_csv("dummy", CsvReadOptions::new()).await.unwrap()
        }

        async fn call_read_avro(&self) -> Arc<DataFrame> {
            let ctx = SessionContext::new();
            ctx.read_avro("dummy", AvroReadOptions::default())
                .await
                .unwrap()
        }

        async fn call_read_parquet(&self) -> Arc<DataFrame> {
            let ctx = SessionContext::new();
            ctx.read_parquet("dummy", ParquetReadOptions::default())
                .await
                .unwrap()
        }
    }
}
