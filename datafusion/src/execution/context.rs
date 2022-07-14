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

//! ExecutionContext contains methods for registering data sources and executing queries
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use log::{debug, trace};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::string::String;
use std::sync::Arc;

use crate::catalog::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    schema::{MemorySchemaProvider, SchemaProvider},
    ResolvedTableReference, TableReference,
};
use crate::datasource::listing::ListingTableConfig;
use crate::datasource::object_store::{ObjectStore, ObjectStoreRegistry};
use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};
use crate::execution::dataframe_impl::DataFrameImpl;
use crate::logical_plan::{
    CreateExternalTable, CreateMemoryTable, DropTable, FunctionRegistry, LogicalPlan,
    LogicalPlanBuilder, UNNAMED_TABLE,
};
use crate::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use crate::optimizer::filter_push_down::FilterPushDown;
use crate::optimizer::limit_push_down::LimitPushDown;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::projection_push_down::ProjectionPushDown;
use crate::optimizer::simplify_expressions::SimplifyExpressions;
use crate::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use crate::optimizer::to_approx_perc::ToApproxPerc;
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
            parquet::{ParquetFormat, DEFAULT_PARQUET_EXTENSION},
            FileFormat,
        },
        MemTable,
    },
    logical_plan::{PlanType, ToStringifiedPlan},
    optimizer::eliminate_limit::EliminateLimit,
    physical_optimizer::{
        aggregate_statistics::AggregateStatistics,
        hash_build_probe_order::HashBuildProbeOrder, optimizer::PhysicalOptimizerRule,
    },
};

use crate::physical_optimizer::coalesce_batches::CoalesceBatches;
use crate::physical_optimizer::merge_exec::AddCoalescePartitionsExec;
use crate::physical_optimizer::repartition::Repartition;

use crate::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use crate::logical_plan::plan::Explain;
use crate::physical_plan::file_format::{plan_to_csv, plan_to_parquet};
use crate::physical_plan::planner::DefaultPhysicalPlanner;
use crate::physical_plan::udf::ScalarUDF;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::PhysicalPlanner;
use crate::sql::{
    parser::{DFParser, FileType},
    planner::{ContextProvider, SqlToRel},
};
use crate::variable::{VarProvider, VarType};
use crate::{dataframe::DataFrame, physical_plan::udaf::AggregateUDF};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::{
    disk_manager::DiskManagerConfig,
    memory_manager::MemoryManagerConfig,
    options::{AvroReadOptions, CsvReadOptions},
    DiskManager, MemoryManager,
};

/// ExecutionContext is the main interface for executing queries with DataFusion. The context
/// provides the following functionality:
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
/// let mut ctx = ExecutionContext::new();
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
/// let mut ctx = ExecutionContext::new();
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await?;
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ExecutionContext {
    /// Internal state for the context
    pub state: Arc<Mutex<ExecutionContextState>>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionContext {
    /// Creates a new execution context using a default configuration.
    pub fn new() -> Self {
        Self::with_config(ExecutionConfig::new())
    }

    /// Creates a new execution context using the provided configuration.
    pub fn with_config(config: ExecutionConfig) -> Self {
        let catalog_list = Arc::new(MemoryCatalogList::new()) as Arc<dyn CatalogList>;

        if config.create_default_catalog_and_schema {
            let default_catalog = MemoryCatalogProvider::new();

            default_catalog.register_schema(
                config.default_schema.clone(),
                Arc::new(MemorySchemaProvider::new()),
            );

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

        let runtime_env = Arc::new(RuntimeEnv::new(config.runtime.clone()).unwrap());

        Self {
            state: Arc::new(Mutex::new(ExecutionContextState {
                catalog_list,
                scalar_functions: HashMap::new(),
                aggregate_functions: HashMap::new(),
                config,
                execution_props: ExecutionProps::new(),
                object_store_registry: Arc::new(ObjectStoreRegistry::new()),
                runtime_env,
            })),
        }
    }

    /// Return the [RuntimeEnv] used to run queries with this [ExecutionContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.state.lock().runtime_env.clone()
    }

    /// Creates a dataframe that will execute a SQL query.
    ///
    /// This method is `async` because queries of type `CREATE EXTERNAL TABLE`
    /// might require the schema to be inferred.
    pub async fn sql(&mut self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        let plan = self.create_logical_plan(sql)?;
        match plan {
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref has_header,
            }) => {
                let (file_format, file_extension) = match file_type {
                    FileType::CSV => Ok((
                        Arc::new(CsvFormat::default().with_has_header(*has_header))
                            as Arc<dyn FileFormat>,
                        DEFAULT_CSV_EXTENSION,
                    )),
                    FileType::Parquet => Ok((
                        Arc::new(ParquetFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_PARQUET_EXTENSION,
                    )),
                    FileType::Avro => Ok((
                        Arc::new(AvroFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_AVRO_EXTENSION,
                    )),
                    _ => Err(DataFusionError::NotImplemented(format!(
                        "Unsupported file type {:?}.",
                        file_type
                    ))),
                }?;

                let options = ListingOptions {
                    format: file_format,
                    collect_stat: false,
                    file_extension: file_extension.to_owned(),
                    target_partitions: self.state.lock().config.target_partitions,
                    table_partition_cols: vec![],
                };

                // TODO make schema in CreateExternalTable optional instead of empty
                let provided_schema = if schema.fields().is_empty() {
                    None
                } else {
                    Some(Arc::new(schema.as_ref().to_owned().into()))
                };

                self.register_listing_table(name, location, options, provided_schema)
                    .await?;
                let plan = LogicalPlanBuilder::empty(false).build()?;
                Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan)))
            }

            LogicalPlan::CreateMemoryTable(CreateMemoryTable { name, input }) => {
                let plan = self.optimize(&input)?;
                let physical = Arc::new(DataFrameImpl::new(self.state.clone(), &plan));

                let batches: Vec<_> = physical.collect_partitioned().await?;
                let table = Arc::new(MemTable::try_new(
                    Arc::new(plan.schema().as_ref().into()),
                    batches,
                )?);
                self.register_table(name.as_str(), table)?;

                let plan = LogicalPlanBuilder::empty(false).build()?;
                Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan)))
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
                    Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan)))
                }
            }

            plan => Ok(Arc::new(DataFrameImpl::new(
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
        let state = self.state.lock().clone();
        let query_planner = SqlToRel::new(&state);
        query_planner.statement_to_plan(statements.pop().unwrap())
    }

    /// Registers a variable provider within this context.
    pub fn register_variable(
        &mut self,
        variable_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) {
        self.state
            .lock()
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
            .lock()
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
    pub fn register_udaf(&mut self, f: AggregateUDF) {
        self.state
            .lock()
            .aggregate_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Creates a DataFrame for reading an Avro data source.

    pub async fn read_avro(
        &mut self,
        uri: impl Into<String>,
        options: AvroReadOptions<'_>,
    ) -> Result<Arc<dyn DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.object_store(&uri)?;
        let target_partitions = self.state.lock().config.target_partitions;
        Ok(Arc::new(DataFrameImpl::new(
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

    /// Creates an empty DataFrame.
    pub fn read_empty(&self) -> Result<Arc<dyn DataFrame>> {
        Ok(Arc::new(DataFrameImpl::new(
            self.state.clone(),
            &LogicalPlanBuilder::empty(true).build()?,
        )))
    }

    /// Creates a DataFrame for reading a CSV data source.
    pub async fn read_csv(
        &mut self,
        uri: impl Into<String>,
        options: CsvReadOptions<'_>,
    ) -> Result<Arc<dyn DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.object_store(&uri)?;
        let target_partitions = self.state.lock().config.target_partitions;
        Ok(Arc::new(DataFrameImpl::new(
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
        &mut self,
        uri: impl Into<String>,
    ) -> Result<Arc<dyn DataFrame>> {
        let uri: String = uri.into();
        let (object_store, path) = self.object_store(&uri)?;
        let target_partitions = self.state.lock().config.target_partitions;
        let logical_plan =
            LogicalPlanBuilder::scan_parquet(object_store, path, None, target_partitions)
                .await?
                .build()?;
        Ok(Arc::new(DataFrameImpl::new(
            self.state.clone(),
            &logical_plan,
        )))
    }

    /// Creates a DataFrame for reading a custom TableProvider.
    pub fn read_table(
        &mut self,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn DataFrame>> {
        Ok(Arc::new(DataFrameImpl::new(
            self.state.clone(),
            &LogicalPlanBuilder::scan(UNNAMED_TABLE, provider, None)?.build()?,
        )))
    }

    /// Registers a table that uses the listing feature of the object store to
    /// find the files to be processed
    /// This is async because it might need to resolve the schema.
    pub async fn register_listing_table<'a>(
        &'a mut self,
        name: &'a str,
        uri: &'a str,
        options: ListingOptions,
        provided_schema: Option<SchemaRef>,
    ) -> Result<()> {
        let (object_store, path) = self.object_store(uri)?;
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
        &mut self,
        name: &str,
        uri: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.state.lock().config.target_partitions);

        self.register_listing_table(
            name,
            uri,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
        )
        .await?;

        Ok(())
    }

    /// Registers a Parquet data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub async fn register_parquet(&mut self, name: &str, uri: &str) -> Result<()> {
        let (target_partitions, enable_pruning) = {
            let m = self.state.lock();
            (m.config.target_partitions, m.config.parquet_pruning)
        };
        let file_format = ParquetFormat::default().with_enable_pruning(enable_pruning);

        let listing_options = ListingOptions {
            format: Arc::new(file_format),
            collect_stat: true,
            file_extension: DEFAULT_PARQUET_EXTENSION.to_owned(),
            target_partitions,
            table_partition_cols: vec![],
        };

        self.register_listing_table(name, uri, listing_options, None)
            .await?;
        Ok(())
    }

    /// Registers an Avro data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub async fn register_avro(
        &mut self,
        name: &str,
        uri: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.state.lock().config.target_partitions);

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

        let state = self.state.lock();
        let catalog = if state.config.information_schema {
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
        self.state.lock().catalog_list.catalog(name)
    }

    /// Registers a object store with scheme using a custom `ObjectStore` so that
    /// an external file system or object storage system could be used against this context.
    ///
    /// Returns the `ObjectStore` previously registered for this scheme, if any
    pub fn register_object_store(
        &self,
        scheme: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let scheme = scheme.into();

        self.state
            .lock()
            .object_store_registry
            .register_store(scheme, object_store)
    }

    /// Retrieves a `ObjectStore` instance by scheme
    pub fn object_store<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        self.state
            .lock()
            .object_store_registry
            .get_by_uri(uri)
            .map_err(DataFusionError::from)
    }

    /// Registers a table using a custom `TableProvider` so that
    /// it can be referenced from SQL statements executed against this
    /// context.
    ///
    /// Returns the `TableProvider` previously registered for this
    /// reference, if any
    pub fn register_table<'a>(
        &'a mut self,
        table_ref: impl Into<TableReference<'a>>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        self.state
            .lock()
            .schema_for_ref(table_ref)?
            .register_table(table_ref.table().to_owned(), provider)
    }

    /// Deregisters the given table.
    ///
    /// Returns the registered provider, if any
    pub fn deregister_table<'a>(
        &'a mut self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        self.state
            .lock()
            .schema_for_ref(table_ref)?
            .deregister_table(table_ref.table())
    }

    /// Retrieves a DataFrame representing a table previously registered by calling the
    /// register_table function.
    ///
    /// Returns an error if no table has been registered with the provided reference.
    pub fn table<'a>(
        &self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn DataFrame>> {
        let table_ref = table_ref.into();
        let schema = self.state.lock().schema_for_ref(table_ref)?;
        match schema.table(table_ref.table()) {
            Some(ref provider) => {
                let plan = LogicalPlanBuilder::scan(
                    table_ref.table(),
                    Arc::clone(provider),
                    None,
                )?
                .build()?;
                Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan)))
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
    /// [`table`]: ExecutionContext::table
    #[deprecated(
        note = "Please use the catalog provider interface (`ExecutionContext::catalog`) to examine available catalogs, schemas, and tables"
    )]
    pub fn tables(&self) -> Result<HashSet<String>> {
        Ok(self
            .state
            .lock()
            // a bare reference will always resolve to the default catalog and schema
            .schema_for_ref(TableReference::Bare { table: "" })?
            .table_names()
            .iter()
            .cloned()
            .collect())
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

    /// Creates a physical plan from a logical plan.
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (state, planner) = {
            let mut state = self.state.lock();
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
            (state.clone(), Arc::clone(&state.config.query_planner))
        };

        planner.create_physical_plan(logical_plan, &state).await
    }

    /// Executes a query and writes the results to a partitioned CSV file.
    pub async fn write_csv(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        plan_to_csv(self, plan, path).await
    }

    /// Executes a query and writes the results to a partitioned Parquet file.
    pub async fn write_parquet(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
        writer_properties: arrow::io::parquet::write::WriteOptions,
    ) -> Result<()> {
        plan_to_parquet(self, plan, path, Some(writer_properties)).await
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
        let state = &mut self.state.lock();
        let execution_props = &mut state.execution_props.clone();
        let optimizers = &state.config.optimizers;

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
}

impl From<Arc<Mutex<ExecutionContextState>>> for ExecutionContext {
    fn from(state: Arc<Mutex<ExecutionContextState>>) -> Self {
        ExecutionContext { state }
    }
}

impl FunctionRegistry for ExecutionContext {
    fn udfs(&self) -> HashSet<String> {
        self.state.lock().udfs()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.state.lock().udf(name)
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.state.lock().udaf(name)
    }
}

/// A planner used to add extensions to DataFusion logical and physical plans.
#[async_trait]
pub trait QueryPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
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
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        planner.create_physical_plan(logical_plan, ctx_state).await
    }
}

/// Configuration options for execution context
#[derive(Clone)]
pub struct ExecutionConfig {
    /// Number of partitions for query execution. Increasing partitions can increase concurrency.
    pub target_partitions: usize,
    /// Responsible for optimizing a logical plan
    optimizers: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    /// Responsible for optimizing a physical execution plan
    pub physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
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
    parquet_pruning: bool,
    /// Runtime configurations such as memory threshold and local disk for spill
    pub runtime: RuntimeConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            target_partitions: num_cpus::get(),
            optimizers: vec![
                // Simplify expressions first to maximize the chance
                // of applying other optimizations
                Arc::new(SimplifyExpressions::new()),
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
            default_catalog: "datafusion".to_owned(),
            default_schema: "public".to_owned(),
            create_default_catalog_and_schema: true,
            information_schema: false,
            repartition_joins: true,
            repartition_aggregations: true,
            repartition_windows: true,
            parquet_pruning: true,
            runtime: RuntimeConfig::default(),
        }
    }
}

impl ExecutionConfig {
    /// Create an execution config with default setting
    pub fn new() -> Self {
        Default::default()
    }

    /// Customize target_partitions
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        // partition count must be greater than zero
        assert!(n > 0);
        self.target_partitions = n;
        self
    }

    /// Customize batch size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.runtime.batch_size = n;
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

    /// Customize runtime config
    pub fn with_runtime_config(mut self, config: RuntimeConfig) -> Self {
        self.runtime = config;
        self
    }

    /// Use an an existing [MemoryManager]
    pub fn with_existing_memory_manager(mut self, existing: Arc<MemoryManager>) -> Self {
        self.runtime = self
            .runtime
            .with_memory_manager(MemoryManagerConfig::new_existing(existing));
        self
    }

    /// Specify the total memory to use while running the DataFusion
    /// plan to `max_memory * memory_fraction` in bytes.
    ///
    /// Note DataFusion does not yet respect this limit in all cases.
    pub fn with_memory_limit(
        mut self,
        max_memory: usize,
        memory_fraction: f64,
    ) -> Result<Self> {
        self.runtime =
            self.runtime
                .with_memory_manager(MemoryManagerConfig::try_new_limit(
                    max_memory,
                    memory_fraction,
                )?);
        Ok(self)
    }

    /// Use an an existing [DiskManager]
    pub fn with_existing_disk_manager(mut self, existing: Arc<DiskManager>) -> Self {
        self.runtime = self
            .runtime
            .with_disk_manager(DiskManagerConfig::new_existing(existing));
        self
    }

    /// Use the specified path to create any needed temporary files
    pub fn with_temp_file_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.runtime = self
            .runtime
            .with_disk_manager(DiskManagerConfig::new_specified(vec![path.into()]));
        self
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
pub struct ExecutionContextState {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalog_list: Arc<dyn CatalogList>,
    /// Scalar functions that are registered with the context
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Context configuration
    pub config: ExecutionConfig,
    /// Execution properties
    pub execution_props: ExecutionProps,
    /// Object Store that are registered with the context
    pub object_store_registry: Arc<ObjectStoreRegistry>,
    /// Runtime environment
    pub runtime_env: Arc<RuntimeEnv>,
}

impl Default for ExecutionContextState {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionContextState {
    /// Returns new ExecutionContextState
    pub fn new() -> Self {
        ExecutionContextState {
            catalog_list: Arc::new(MemoryCatalogList::new()),
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            config: ExecutionConfig::new(),
            execution_props: ExecutionProps::new(),
            object_store_registry: Arc::new(ObjectStoreRegistry::new()),
            runtime_env: Arc::new(RuntimeEnv::default()),
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
        let resolved_ref = self.resolve_table_ref(table_ref.into());

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
}

impl ContextProvider for ExecutionContextState {
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

impl FunctionRegistry for ExecutionContextState {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::QueryPlanner;
    use crate::logical_plan::{binary_expr, lit, Operator};
    use crate::physical_plan::functions::{make_scalar_function, Volatility};
    use crate::record_batch::RecordBatch;
    use crate::test;
    use crate::variable::VarType;
    use crate::{
        assert_batches_eq, assert_batches_sorted_eq,
        logical_plan::{col, create_udf, sum, Expr},
    };
    use crate::{
        datasource::MemTable, logical_plan::create_udaf,
        physical_plan::expressions::AvgAccumulator,
    };
    use arrow::array::*;
    use arrow::datatypes::*;
    use async_trait::async_trait;
    use datafusion_common::field_util::{FieldExt, SchemaExt};
    use std::fs::File;
    use std::sync::Weak;
    use std::thread::{self, JoinHandle};
    use std::{io::prelude::*, sync::Mutex};
    use tempfile::TempDir;

    #[tokio::test]
    async fn shared_memory_and_disk_manager() {
        // Demonstrate the ability to share DiskManager and
        // MemoryManager between two different executions.
        let ctx1 = ExecutionContext::new();

        // configure with same memory / disk manager
        let memory_manager = ctx1.runtime_env().memory_manager.clone();
        let disk_manager = ctx1.runtime_env().disk_manager.clone();
        let config = ExecutionConfig::new()
            .with_existing_memory_manager(memory_manager.clone())
            .with_existing_disk_manager(disk_manager.clone());

        let ctx2 = ExecutionContext::with_config(config);

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
            plan_and_collect(&mut ctx, "SELECT @@version, @name, @integer + 1 FROM dual")
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
        let mut ctx = create_ctx(&tmp_dir, partition_count).await?;

        let provider = test::create_table_dual();
        ctx.register_table("dual", provider)?;

        assert!(ctx.deregister_table("dual")?.is_some());
        assert!(ctx.deregister_table("dual")?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn left_join_using() -> Result<()> {
        let results = execute(
            "SELECT t1.c1, t2.c2 FROM test t1 JOIN test t2 USING (c2) ORDER BY t2.c2",
            1,
        )
        .await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "| 0  | 1  |",
            "| 0  | 2  |",
            "| 0  | 3  |",
            "| 0  | 4  |",
            "| 0  | 5  |",
            "| 0  | 6  |",
            "| 0  | 7  |",
            "| 0  | 8  |",
            "| 0  | 9  |",
            "| 0  | 10 |",
            "+----+----+",
        ];

        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn left_join_using_join_key_projection() -> Result<()> {
        let results = execute(
            "SELECT t1.c1, t1.c2, t2.c2 FROM test t1 JOIN test t2 USING (c2) ORDER BY t2.c2",
            1,
        )
            .await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----+----+----+",
            "| c1 | c2 | c2 |",
            "+----+----+----+",
            "| 0  | 1  | 1  |",
            "| 0  | 2  | 2  |",
            "| 0  | 3  | 3  |",
            "| 0  | 4  | 4  |",
            "| 0  | 5  | 5  |",
            "| 0  | 6  | 6  |",
            "| 0  | 7  | 7  |",
            "| 0  | 8  | 8  |",
            "| 0  | 9  | 9  |",
            "| 0  | 10 | 10 |",
            "+----+----+----+",
        ];

        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn left_join() -> Result<()> {
        let results = execute(
            "SELECT t1.c1, t1.c2, t2.c2 FROM test t1 JOIN test t2 ON t1.c2 = t2.c2 ORDER BY t1.c2",
            1,
        )
            .await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----+----+----+",
            "| c1 | c2 | c2 |",
            "+----+----+----+",
            "| 0  | 1  | 1  |",
            "| 0  | 2  | 2  |",
            "| 0  | 3  | 3  |",
            "| 0  | 4  | 4  |",
            "| 0  | 5  | 5  |",
            "| 0  | 6  | 6  |",
            "| 0  | 7  | 7  |",
            "| 0  | 8  | 8  |",
            "| 0  | 9  | 9  |",
            "| 0  | 10 | 10 |",
            "+----+----+----+",
        ];

        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn window() -> Result<()> {
        let results = execute(
            "SELECT \
            c1, \
            c2, \
            SUM(c2) OVER (), \
            COUNT(c2) OVER (), \
            MAX(c2) OVER (), \
            MIN(c2) OVER (), \
            AVG(c2) OVER () \
            FROM test \
            ORDER BY c1, c2 \
            LIMIT 5",
            4,
        )
        .await?;
        // result in one batch, although e.g. having 2 batches do not change
        // result semantics, having a len=1 assertion upfront keeps surprises
        // at bay
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----+----+--------------+----------------+--------------+--------------+--------------+",
            "| c1 | c2 | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
            "+----+----+--------------+----------------+--------------+--------------+--------------+",
            "| 0  | 1  | 220          | 40             | 10           | 1            | 5.5          |",
            "| 0  | 2  | 220          | 40             | 10           | 1            | 5.5          |",
            "| 0  | 3  | 220          | 40             | 10           | 1            | 5.5          |",
            "| 0  | 4  | 220          | 40             | 10           | 1            | 5.5          |",
            "| 0  | 5  | 220          | 40             | 10           | 1            | 5.5          |",
            "+----+----+--------------+----------------+--------------+--------------+--------------+",
        ];

        // window function shall respect ordering
        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn window_order_by() -> Result<()> {
        let results = execute(
            "SELECT \
            c1, \
            c2, \
            ROW_NUMBER() OVER (ORDER BY c1, c2), \
            FIRST_VALUE(c2) OVER (ORDER BY c1, c2), \
            LAST_VALUE(c2) OVER (ORDER BY c1, c2), \
            NTH_VALUE(c2, 2) OVER (ORDER BY c1, c2), \
            SUM(c2) OVER (ORDER BY c1, c2), \
            COUNT(c2) OVER (ORDER BY c1, c2), \
            MAX(c2) OVER (ORDER BY c1, c2), \
            MIN(c2) OVER (ORDER BY c1, c2), \
            AVG(c2) OVER (ORDER BY c1, c2) \
            FROM test \
            ORDER BY c1, c2 \
            LIMIT 5",
            4,
        )
        .await?;
        // result in one batch, although e.g. having 2 batches do not change
        // result semantics, having a len=1 assertion upfront keeps surprises
        // at bay
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----+----+--------------+----------------------+---------------------+-----------------------------+--------------+----------------+--------------+--------------+--------------+",
            "| c1 | c2 | ROW_NUMBER() | FIRST_VALUE(test.c2) | LAST_VALUE(test.c2) | NTH_VALUE(test.c2,Int64(2)) | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
            "+----+----+--------------+----------------------+---------------------+-----------------------------+--------------+----------------+--------------+--------------+--------------+",
            "| 0  | 1  | 1            | 1                    | 1                   |                             | 1            | 1              | 1            | 1            | 1            |",
            "| 0  | 2  | 2            | 1                    | 2                   | 2                           | 3            | 2              | 2            | 1            | 1.5          |",
            "| 0  | 3  | 3            | 1                    | 3                   | 2                           | 6            | 3              | 3            | 1            | 2            |",
            "| 0  | 4  | 4            | 1                    | 4                   | 2                           | 10           | 4              | 4            | 1            | 2.5          |",
            "| 0  | 5  | 5            | 1                    | 5                   | 2                           | 15           | 5              | 5            | 1            | 3            |",
            "+----+----+--------------+----------------------+---------------------+-----------------------------+--------------+----------------+--------------+--------------+--------------+",
        ];

        // window function shall respect ordering
        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn window_partition_by() -> Result<()> {
        let results = execute(
            "SELECT \
            c1, \
            c2, \
            SUM(c2) OVER (PARTITION BY c2), \
            COUNT(c2) OVER (PARTITION BY c2), \
            MAX(c2) OVER (PARTITION BY c2), \
            MIN(c2) OVER (PARTITION BY c2), \
            AVG(c2) OVER (PARTITION BY c2) \
            FROM test \
            ORDER BY c1, c2 \
            LIMIT 5",
            4,
        )
        .await?;

        let expected = vec![
            "+----+----+--------------+----------------+--------------+--------------+--------------+",
            "| c1 | c2 | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
            "+----+----+--------------+----------------+--------------+--------------+--------------+",
            "| 0  | 1  | 4            | 4              | 1            | 1            | 1            |",
            "| 0  | 2  | 8            | 4              | 2            | 2            | 2            |",
            "| 0  | 3  | 12           | 4              | 3            | 3            | 3            |",
            "| 0  | 4  | 16           | 4              | 4            | 4            | 4            |",
            "| 0  | 5  | 20           | 4              | 5            | 5            | 5            |",
            "+----+----+--------------+----------------+--------------+--------------+--------------+",
        ];

        // window function shall respect ordering
        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn window_partition_by_order_by() -> Result<()> {
        let results = execute(
            "SELECT \
            c1, \
            c2, \
            ROW_NUMBER() OVER (PARTITION BY c2 ORDER BY c1), \
            FIRST_VALUE(c2 + c1) OVER (PARTITION BY c2 ORDER BY c1), \
            LAST_VALUE(c2 + c1) OVER (PARTITION BY c2 ORDER BY c1), \
            NTH_VALUE(c2 + c1, 1) OVER (PARTITION BY c2 ORDER BY c1), \
            SUM(c2) OVER (PARTITION BY c2 ORDER BY c1), \
            COUNT(c2) OVER (PARTITION BY c2 ORDER BY c1), \
            MAX(c2) OVER (PARTITION BY c2 ORDER BY c1), \
            MIN(c2) OVER (PARTITION BY c2 ORDER BY c1), \
            AVG(c2) OVER (PARTITION BY c2 ORDER BY c1) \
            FROM test \
            ORDER BY c1, c2 \
            LIMIT 5",
            4,
        )
        .await?;

        let expected = vec![
            "+----+----+--------------+--------------------------------+-------------------------------+---------------------------------------+--------------+----------------+--------------+--------------+--------------+",
            "| c1 | c2 | ROW_NUMBER() | FIRST_VALUE(test.c2 + test.c1) | LAST_VALUE(test.c2 + test.c1) | NTH_VALUE(test.c2 + test.c1,Int64(1)) | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
            "+----+----+--------------+--------------------------------+-------------------------------+---------------------------------------+--------------+----------------+--------------+--------------+--------------+",
            "| 0  | 1  | 1            | 1                              | 1                             | 1                                     | 1            | 1              | 1            | 1            | 1            |",
            "| 0  | 2  | 1            | 2                              | 2                             | 2                                     | 2            | 1              | 2            | 2            | 2            |",
            "| 0  | 3  | 1            | 3                              | 3                             | 3                                     | 3            | 1              | 3            | 3            | 3            |",
            "| 0  | 4  | 1            | 4                              | 4                             | 4                                     | 4            | 1              | 4            | 4            | 4            |",
            "| 0  | 5  | 1            | 5                              | 5                             | 5                                     | 5            | 1              | 5            | 5            | 5            |",
            "+----+----+--------------+--------------------------------+-------------------------------+---------------------------------------+--------------+----------------+--------------+--------------+--------------+",
        ];

        // window function shall respect ordering
        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn aggregate_decimal_min() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        // the data type of c1 is decimal(10,3)
        ctx.register_table("d_table", test::table_with_decimal())
            .unwrap();
        let result = plan_and_collect(&mut ctx, "select min(c1) from d_table")
            .await
            .unwrap();
        let expected = vec![
            "+-----------------+",
            "| MIN(d_table.c1) |",
            "+-----------------+",
            "| -100.009        |",
            "+-----------------+",
        ];
        assert_eq!(
            &DataType::Decimal(10, 3),
            result[0].schema().field(0).data_type()
        );
        assert_batches_sorted_eq!(expected, &result);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn aggregate_decimal_max() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        // the data type of c1 is decimal(10,3)
        ctx.register_table("d_table", test::table_with_decimal())
            .unwrap();

        let result = plan_and_collect(&mut ctx, "select max(c1) from d_table")
            .await
            .unwrap();
        let expected = vec![
            "+-----------------+",
            "| MAX(d_table.c1) |",
            "+-----------------+",
            "| 110.009         |",
            "+-----------------+",
        ];
        assert_eq!(
            &DataType::Decimal(10, 3),
            result[0].schema().field(0).data_type()
        );
        assert_batches_sorted_eq!(expected, &result);
        Ok(())
    }

    #[tokio::test]
    async fn aggregate_decimal_sum() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        // the data type of c1 is decimal(10,3)
        ctx.register_table("d_table", test::table_with_decimal())
            .unwrap();
        let result = plan_and_collect(&mut ctx, "select sum(c1) from d_table")
            .await
            .unwrap();
        let expected = vec![
            "+-----------------+",
            "| SUM(d_table.c1) |",
            "+-----------------+",
            "| 100.0           |",
            "+-----------------+",
        ];
        assert_eq!(
            &DataType::Decimal(20, 3),
            result[0].schema().field(0).data_type()
        );
        assert_batches_sorted_eq!(expected, &result);
        Ok(())
    }

    #[tokio::test]
    async fn aggregate_decimal_avg() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        // the data type of c1 is decimal(10,3)
        ctx.register_table("d_table", test::table_with_decimal())
            .unwrap();
        let result = plan_and_collect(&mut ctx, "select avg(c1) from d_table")
            .await
            .unwrap();
        let expected = vec![
            "+-----------------+",
            "| AVG(d_table.c1) |",
            "+-----------------+",
            "| 5.0             |",
            "+-----------------+",
        ];
        assert_eq!(
            &DataType::Decimal(14, 7),
            result[0].schema().field(0).data_type()
        );
        assert_batches_sorted_eq!(expected, &result);
        Ok(())
    }

    #[tokio::test]
    async fn aggregate() -> Result<()> {
        let results = execute("SELECT SUM(c1), SUM(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+--------------+--------------+",
            "| SUM(test.c1) | SUM(test.c2) |",
            "+--------------+--------------+",
            "| 60           | 220          |",
            "+--------------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_empty() -> Result<()> {
        // The predicate on this query purposely generates no results
        let results = execute("SELECT SUM(c1), SUM(c2) FROM test where c1 > 100000", 4)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);

        let expected = vec![
            "+--------------+--------------+",
            "| SUM(test.c1) | SUM(test.c2) |",
            "+--------------+--------------+",
            "|              |              |",
            "+--------------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_avg() -> Result<()> {
        let results = execute("SELECT AVG(c1), AVG(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+--------------+--------------+",
            "| AVG(test.c1) | AVG(test.c2) |",
            "+--------------+--------------+",
            "| 1.5          | 5.5          |",
            "+--------------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_max() -> Result<()> {
        let results = execute("SELECT MAX(c1), MAX(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+--------------+--------------+",
            "| MAX(test.c1) | MAX(test.c2) |",
            "+--------------+--------------+",
            "| 3            | 10           |",
            "+--------------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_min() -> Result<()> {
        let results = execute("SELECT MIN(c1), MIN(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+--------------+--------------+",
            "| MIN(test.c1) | MIN(test.c2) |",
            "+--------------+--------------+",
            "| 0            | 1            |",
            "+--------------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped() -> Result<()> {
        let results = execute("SELECT c1, SUM(c2) FROM test GROUP BY c1", 4).await?;

        let expected = vec![
            "+----+--------------+",
            "| c1 | SUM(test.c2) |",
            "+----+--------------+",
            "| 0  | 55           |",
            "| 1  | 55           |",
            "| 2  | 55           |",
            "| 3  | 55           |",
            "+----+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_avg() -> Result<()> {
        let results = execute("SELECT c1, AVG(c2) FROM test GROUP BY c1", 4).await?;

        let expected = vec![
            "+----+--------------+",
            "| c1 | AVG(test.c2) |",
            "+----+--------------+",
            "| 0  | 5.5          |",
            "| 1  | 5.5          |",
            "| 2  | 5.5          |",
            "| 3  | 5.5          |",
            "+----+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn boolean_literal() -> Result<()> {
        let results =
            execute("SELECT c1, c3 FROM test WHERE c1 > 2 AND c3 = true", 4).await?;

        let expected = vec![
            "+----+------+",
            "| c1 | c3   |",
            "+----+------+",
            "| 3  | true |",
            "| 3  | true |",
            "| 3  | true |",
            "| 3  | true |",
            "| 3  | true |",
            "+----+------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_empty() -> Result<()> {
        let results =
            execute("SELECT c1, AVG(c2) FROM test WHERE c1 = 123 GROUP BY c1", 4).await?;

        let expected = vec![
            "+----+--------------+",
            "| c1 | AVG(test.c2) |",
            "+----+--------------+",
            "+----+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_max() -> Result<()> {
        let results = execute("SELECT c1, MAX(c2) FROM test GROUP BY c1", 4).await?;

        let expected = vec![
            "+----+--------------+",
            "| c1 | MAX(test.c2) |",
            "+----+--------------+",
            "| 0  | 10           |",
            "| 1  | 10           |",
            "| 2  | 10           |",
            "| 3  | 10           |",
            "+----+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_min() -> Result<()> {
        let results = execute("SELECT c1, MIN(c2) FROM test GROUP BY c1", 4).await?;

        let expected = vec![
            "+----+--------------+",
            "| c1 | MIN(test.c2) |",
            "+----+--------------+",
            "| 0  | 1            |",
            "| 1  | 1            |",
            "| 2  | 1            |",
            "| 3  | 1            |",
            "+----+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_avg_add() -> Result<()> {
        let results = execute(
            "SELECT AVG(c1), AVG(c1) + 1, AVG(c1) + 2, 1 + AVG(c1) FROM test",
            4,
        )
        .await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+--------------+----------------------------+----------------------------+----------------------------+",
            "| AVG(test.c1) | AVG(test.c1) Plus Int64(1) | AVG(test.c1) Plus Int64(2) | Int64(1) Plus AVG(test.c1) |",
            "+--------------+----------------------------+----------------------------+----------------------------+",
            "| 1.5          | 2.5                        | 3.5                        | 2.5                        |",
            "+--------------+----------------------------+----------------------------+----------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn join_partitioned() -> Result<()> {
        // self join on partition id (workaround for duplicate column name)
        let results = execute(
            "SELECT 1 FROM test JOIN (SELECT c1 AS id1 FROM test) AS a ON c1=id1",
            4,
        )
        .await?;

        assert_eq!(
            results.iter().map(|b| b.num_rows()).sum::<usize>(),
            4 * 10 * 10
        );

        Ok(())
    }

    #[tokio::test]
    async fn count_basic() -> Result<()> {
        let results = execute("SELECT COUNT(c1), COUNT(c2) FROM test", 1).await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----------------+----------------+",
            "| COUNT(test.c1) | COUNT(test.c2) |",
            "+----------------+----------------+",
            "| 10             | 10             |",
            "+----------------+----------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn count_partitioned() -> Result<()> {
        let results = execute("SELECT COUNT(c1), COUNT(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let expected = vec![
            "+----------------+----------------+",
            "| COUNT(test.c1) | COUNT(test.c2) |",
            "+----------------+----------------+",
            "| 40             | 40             |",
            "+----------------+----------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn count_aggregated() -> Result<()> {
        let results = execute("SELECT c1, COUNT(c2) FROM test GROUP BY c1", 4).await?;

        let expected = vec![
            "+----+----------------+",
            "| c1 | COUNT(test.c2) |",
            "+----+----------------+",
            "| 0  | 10             |",
            "| 1  | 10             |",
            "| 2  | 10             |",
            "| 3  | 10             |",
            "+----+----------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn group_by_date_trunc() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let mut ctx = ExecutionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("c2", DataType::UInt64, false),
            Field::new(
                "t1",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        // generate a partitioned file
        for partition in 0..4 {
            let filename = format!("partition-{}.{}", partition, "csv");
            let file_path = tmp_dir.path().join(&filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..10 {
                let data = format!("{},2020-12-{}T00:00:00.000\n", i, i + 10);
                file.write_all(data.as_bytes())?;
            }
        }

        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema).has_header(false),
        )
        .await?;

        let results = plan_and_collect(
            &mut ctx,
            "SELECT date_trunc('week', t1) as week, SUM(c2) FROM test GROUP BY date_trunc('week', t1)",
        ).await?;

        let expected = vec![
            "+---------------------+--------------+",
            "| week                | SUM(test.c2) |",
            "+---------------------+--------------+",
            "| 2020-12-07 00:00:00 | 24           |",
            "| 2020-12-14 00:00:00 | 156          |",
            "+---------------------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn group_by_largeutf8() {
        {
            let mut ctx = ExecutionContext::new();

            // input data looks like:
            // A, 1
            // B, 2
            // A, 2
            // A, 4
            // C, 1
            // A, 1

            let str_array = Utf8Array::<i64>::from_slice(&["A", "B", "A", "A", "C", "A"]);
            let str_array = Arc::new(str_array);

            let val_array = Int64Array::from_slice(&[1, 2, 2, 4, 1, 1]);
            let val_array = Arc::new(val_array);

            let schema = Arc::new(Schema::new(vec![
                Field::new("str", str_array.data_type().clone(), false),
                Field::new("val", val_array.data_type().clone(), false),
            ]));

            let batch =
                RecordBatch::try_new(schema.clone(), vec![str_array, val_array]).unwrap();

            let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
            ctx.register_table("t", Arc::new(provider)).unwrap();

            let results =
                plan_and_collect(&mut ctx, "SELECT str, count(val) FROM t GROUP BY str")
                    .await
                    .expect("ran plan correctly");

            let expected = vec![
                "+-----+--------------+",
                "| str | COUNT(t.val) |",
                "+-----+--------------+",
                "| A   | 4            |",
                "| B   | 1            |",
                "| C   | 1            |",
                "+-----+--------------+",
            ];
            assert_batches_sorted_eq!(expected, &results);
        }
    }

    #[tokio::test]
    async fn unprojected_filter() {
        let mut ctx = ExecutionContext::new();
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
    async fn group_by_dictionary() {
        async fn run_test_case<K: DictionaryKey>() {
            let mut ctx = ExecutionContext::new();

            // input data looks like:
            // A, 1
            // B, 2
            // A, 2
            // A, 4
            // C, 1
            // A, 1

            let data = vec!["A", "B", "A", "A", "C", "A"];

            let data = data.into_iter().map(Some);

            let mut dict_array =
                MutableDictionaryArray::<K, MutableUtf8Array<i32>>::new();
            dict_array.try_extend(data).unwrap();
            let dict_array = dict_array.into_arc();

            let val_array = Int64Array::from_slice(&[1, 2, 2, 4, 1, 1]);
            let val_array = Arc::new(val_array);

            let schema = Arc::new(Schema::new(vec![
                Field::new("dict", dict_array.data_type().clone(), false),
                Field::new("val", val_array.data_type().clone(), false),
            ]));

            let batch = RecordBatch::try_new(schema.clone(), vec![dict_array, val_array])
                .unwrap();

            let provider = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
            ctx.register_table("t", Arc::new(provider)).unwrap();

            let results = plan_and_collect(
                &mut ctx,
                "SELECT dict, count(val) FROM t GROUP BY dict",
            )
            .await
            .expect("ran plan correctly");

            let expected = vec![
                "+------+--------------+",
                "| dict | COUNT(t.val) |",
                "+------+--------------+",
                "| A    | 4            |",
                "| B    | 1            |",
                "| C    | 1            |",
                "+------+--------------+",
            ];
            assert_batches_sorted_eq!(expected, &results);

            // Now, use dict as an aggregate
            let results =
                plan_and_collect(&mut ctx, "SELECT val, count(dict) FROM t GROUP BY val")
                    .await
                    .expect("ran plan correctly");

            let expected = vec![
                "+-----+---------------+",
                "| val | COUNT(t.dict) |",
                "+-----+---------------+",
                "| 1   | 3             |",
                "| 2   | 2             |",
                "| 4   | 1             |",
                "+-----+---------------+",
            ];
            assert_batches_sorted_eq!(expected, &results);

            // Now, use dict as an aggregate
            let results = plan_and_collect(
                &mut ctx,
                "SELECT val, count(distinct dict) FROM t GROUP BY val",
            )
            .await
            .expect("ran plan correctly");

            let expected = vec![
                "+-----+------------------------+",
                "| val | COUNT(DISTINCT t.dict) |",
                "+-----+------------------------+",
                "| 1   | 2                      |",
                "| 2   | 2                      |",
                "| 4   | 1                      |",
                "+-----+------------------------+",
            ];
            assert_batches_sorted_eq!(expected, &results);
        }

        run_test_case::<i8>().await;
        run_test_case::<i16>().await;
        run_test_case::<i32>().await;
        run_test_case::<i64>().await;
        run_test_case::<u8>().await;
        run_test_case::<u16>().await;
        run_test_case::<u32>().await;
        run_test_case::<u64>().await;
    }

    async fn run_count_distinct_integers_aggregated_scenario(
        partitions: Vec<Vec<(&str, u64)>>,
    ) -> Result<Vec<RecordBatch>> {
        let tmp_dir = TempDir::new()?;
        let mut ctx = ExecutionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_group", DataType::Utf8, false),
            Field::new("c_int8", DataType::Int8, false),
            Field::new("c_int16", DataType::Int16, false),
            Field::new("c_int32", DataType::Int32, false),
            Field::new("c_int64", DataType::Int64, false),
            Field::new("c_uint8", DataType::UInt8, false),
            Field::new("c_uint16", DataType::UInt16, false),
            Field::new("c_uint32", DataType::UInt32, false),
            Field::new("c_uint64", DataType::UInt64, false),
        ]));

        for (i, partition) in partitions.iter().enumerate() {
            let filename = format!("partition-{}.csv", i);
            let file_path = tmp_dir.path().join(&filename);
            let mut file = File::create(file_path)?;
            for row in partition {
                let row_str = format!(
                    "{},{}\n",
                    row.0,
                    // Populate values for each of the integer fields in the
                    // schema.
                    (0..8)
                        .map(|_| { row.1.to_string() })
                        .collect::<Vec<_>>()
                        .join(","),
                );
                file.write_all(row_str.as_bytes())?;
            }
        }
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema).has_header(false),
        )
        .await?;

        let results = plan_and_collect(
            &mut ctx,
            "
              SELECT
                c_group,
                COUNT(c_uint64),
                COUNT(DISTINCT c_int8),
                COUNT(DISTINCT c_int16),
                COUNT(DISTINCT c_int32),
                COUNT(DISTINCT c_int64),
                COUNT(DISTINCT c_uint8),
                COUNT(DISTINCT c_uint16),
                COUNT(DISTINCT c_uint32),
                COUNT(DISTINCT c_uint64)
              FROM test
              GROUP BY c_group
            ",
        )
        .await?;

        Ok(results)
    }

    #[tokio::test]
    async fn count_distinct_integers_aggregated_single_partition() -> Result<()> {
        let partitions = vec![
            // The first member of each tuple will be the value for the
            // `c_group` column, and the second member will be the value for
            // each of the int/uint fields.
            vec![
                ("a", 1),
                ("a", 1),
                ("a", 2),
                ("b", 9),
                ("c", 9),
                ("c", 10),
                ("c", 9),
            ],
        ];

        let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

        let expected = vec![
            "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
            "| c_group | COUNT(test.c_uint64) | COUNT(DISTINCT test.c_int8) | COUNT(DISTINCT test.c_int16) | COUNT(DISTINCT test.c_int32) | COUNT(DISTINCT test.c_int64) | COUNT(DISTINCT test.c_uint8) | COUNT(DISTINCT test.c_uint16) | COUNT(DISTINCT test.c_uint32) | COUNT(DISTINCT test.c_uint64) |",
            "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
            "| a       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |",
            "| b       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |",
            "| c       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |",
            "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn count_distinct_integers_aggregated_multiple_partitions() -> Result<()> {
        let partitions = vec![
            // The first member of each tuple will be the value for the
            // `c_group` column, and the second member will be the value for
            // each of the int/uint fields.
            vec![("a", 1), ("a", 1), ("a", 2), ("b", 9), ("c", 9)],
            vec![("a", 1), ("a", 3), ("b", 8), ("b", 9), ("b", 10), ("b", 11)],
        ];

        let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

        let expected = vec![
            "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
            "| c_group | COUNT(test.c_uint64) | COUNT(DISTINCT test.c_int8) | COUNT(DISTINCT test.c_int16) | COUNT(DISTINCT test.c_int32) | COUNT(DISTINCT test.c_int64) | COUNT(DISTINCT test.c_uint8) | COUNT(DISTINCT test.c_uint16) | COUNT(DISTINCT test.c_uint32) | COUNT(DISTINCT test.c_uint64) |",
            "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
            "| a       | 5                    | 3                           | 3                            | 3                            | 3                            | 3                            | 3                             | 3                             | 3                             |",
            "| b       | 5                    | 4                           | 4                            | 4                            | 4                            | 4                            | 4                             | 4                             | 4                             |",
            "| c       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |",
            "+---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        Ok(())
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
        assert_eq!("c1", physical_plan.schema().field(0).name());
        assert_eq!("total_salary", physical_plan.schema().field(1).name());
        Ok(())
    }

    #[tokio::test]
    async fn limit() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let mut ctx = create_ctx(&tmp_dir, 1).await?;
        ctx.register_table("t", test::table_with_sequence(1, 1000).unwrap())
            .unwrap();

        let results =
            plan_and_collect(&mut ctx, "SELECT i FROM t ORDER BY i DESC limit 3")
                .await
                .unwrap();

        let expected = vec![
            "+------+", "| i    |", "+------+", "| 1000 |", "| 999  |", "| 998  |",
            "+------+",
        ];

        assert_batches_eq!(expected, &results);

        let results = plan_and_collect(&mut ctx, "SELECT i FROM t ORDER BY i limit 3")
            .await
            .unwrap();

        let expected = vec![
            "+---+", "| i |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
        ];

        assert_batches_eq!(expected, &results);

        let results = plan_and_collect(&mut ctx, "SELECT i FROM t limit 3")
            .await
            .unwrap();

        // the actual rows are not guaranteed, so only check the count (should be 3)
        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        assert_eq!(num_rows, 3);

        Ok(())
    }

    #[tokio::test]
    async fn limit_multi_partitions() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let mut ctx = create_ctx(&tmp_dir, 1).await?;

        let partitions = vec![
            vec![test::make_partition(0)],
            vec![test::make_partition(1)],
            vec![test::make_partition(2)],
            vec![test::make_partition(3)],
            vec![test::make_partition(4)],
            vec![test::make_partition(5)],
        ];
        let schema = partitions[0][0].schema().clone();
        let provider = Arc::new(MemTable::try_new(schema, partitions).unwrap());

        ctx.register_table("t", provider).unwrap();

        // select all rows
        let results = plan_and_collect(&mut ctx, "SELECT i FROM t").await.unwrap();

        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        assert_eq!(num_rows, 15);

        for limit in 1..10 {
            let query = format!("SELECT i FROM t limit {}", limit);
            let results = plan_and_collect(&mut ctx, &query).await.unwrap();

            let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
            assert_eq!(num_rows, limit, "mismatch with query {}", query);
        }

        Ok(())
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_functions() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        let expected = vec![
            "+-----------+",
            "| sqrt(t.i) |",
            "+-----------+",
            "| 1         |",
            "+-----------+",
        ];

        let results = plan_and_collect(&mut ctx, "SELECT sqrt(i) FROM t")
            .await
            .unwrap();

        assert_batches_sorted_eq!(expected, &results);

        let results = plan_and_collect(&mut ctx, "SELECT SQRT(i) FROM t")
            .await
            .unwrap();
        assert_batches_sorted_eq!(expected, &results);

        // Using double quotes allows specifying the function name with capitalization
        let err = plan_and_collect(&mut ctx, "SELECT \"SQRT\"(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function 'SQRT'"
        );

        let results = plan_and_collect(&mut ctx, "SELECT \"sqrt\"(i) FROM t")
            .await
            .unwrap();
        assert_batches_sorted_eq!(expected, &results);
    }

    #[tokio::test]
    async fn case_builtin_math_expression() {
        let mut ctx = ExecutionContext::new();

        let type_values = vec![
            (
                DataType::Int8,
                Arc::new(Int8Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::Int16,
                Arc::new(Int16Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::Int32,
                Arc::new(Int32Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::Int64,
                Arc::new(Int64Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::UInt8,
                Arc::new(UInt8Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::UInt16,
                Arc::new(UInt16Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::UInt32,
                Arc::new(UInt32Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::UInt64,
                Arc::new(UInt64Array::from_values(vec![1])) as ArrayRef,
            ),
            (
                DataType::Float32,
                Arc::new(Float32Array::from_values(vec![1.0_f32])) as ArrayRef,
            ),
            (
                DataType::Float64,
                Arc::new(Float64Array::from_values(vec![1.0_f64])) as ArrayRef,
            ),
        ];

        for (data_type, array) in type_values.iter() {
            let schema =
                Arc::new(Schema::new(vec![Field::new("v", data_type.clone(), false)]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
            let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            ctx.deregister_table("t").unwrap();
            ctx.register_table("t", Arc::new(provider)).unwrap();
            let expected = vec![
                "+-----------+",
                "| sqrt(t.v) |",
                "+-----------+",
                "| 1         |",
                "+-----------+",
            ];
            let results = plan_and_collect(&mut ctx, "SELECT sqrt(v) FROM t")
                .await
                .unwrap();

            assert_batches_sorted_eq!(expected, &results);
        }
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_user_defined_functions() -> Result<()> {
        let mut ctx = ExecutionContext::new();
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
        let err = plan_and_collect(&mut ctx, "SELECT MY_FUNC(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function \'my_func\'"
        );

        // Can call it if you put quotes
        let result = plan_and_collect(&mut ctx, "SELECT \"MY_FUNC\"(i) FROM t").await?;

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
    async fn case_sensitive_identifiers_aggregates() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        let expected = vec![
            "+----------+",
            "| MAX(t.i) |",
            "+----------+",
            "| 1        |",
            "+----------+",
        ];

        let results = plan_and_collect(&mut ctx, "SELECT max(i) FROM t")
            .await
            .unwrap();

        assert_batches_sorted_eq!(expected, &results);

        let results = plan_and_collect(&mut ctx, "SELECT MAX(i) FROM t")
            .await
            .unwrap();
        assert_batches_sorted_eq!(expected, &results);

        // Using double quotes allows specifying the function name with capitalization
        let err = plan_and_collect(&mut ctx, "SELECT \"MAX\"(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function 'MAX'"
        );

        let results = plan_and_collect(&mut ctx, "SELECT \"max\"(i) FROM t")
            .await
            .unwrap();
        assert_batches_sorted_eq!(expected, &results);
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_user_defined_aggregates() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        // Note capitalizaton
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
        let err = plan_and_collect(&mut ctx, "SELECT MY_AVG(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function \'my_avg\'"
        );

        // Can call it if you put quotes
        let result = plan_and_collect(&mut ctx, "SELECT \"MY_AVG\"(i) FROM t").await?;

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

        let mut ctx = ExecutionContext::new();
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
            plan_and_collect(&mut ctx, "SELECT SUM(c1), SUM(c2), COUNT(*) FROM test")
                .await?;

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
        // ensure ExecutionContexts can be used in a multi-threaded
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
        let mut ctx = ExecutionContext::new();
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
    async fn simple_avg() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from_slice(&[1, 2, 3]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from_slice(&[4, 5]))],
        )?;

        let mut ctx = ExecutionContext::new();

        let provider =
            MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("t", Arc::new(provider))?;

        let result = plan_and_collect(&mut ctx, "SELECT AVG(a) FROM t").await?;

        let batch = &result[0];
        assert_eq!(1, batch.num_columns());
        assert_eq!(1, batch.num_rows());

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to cast version");
        assert_eq!(values.len(), 1);
        // avg(1,2,3,4,5) = 3.0
        assert_eq!(values.value(0), 3.0_f64);
        Ok(())
    }

    #[tokio::test]
    async fn custom_query_planner() -> Result<()> {
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().with_query_planner(Arc::new(MyQueryPlanner {})),
        );

        let df = ctx.sql("SELECT 1").await?;
        df.collect().await.expect_err("query not supported");
        Ok(())
    }

    #[tokio::test]
    async fn disabled_default_catalog_and_schema() -> Result<()> {
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().create_default_catalog_and_schema(false),
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
    async fn custom_catalog_and_schema() -> Result<()> {
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new()
                .create_default_catalog_and_schema(false)
                .with_default_catalog_and_schema("my_catalog", "my_schema"),
        );

        let catalog = MemoryCatalogProvider::new();
        let schema = MemorySchemaProvider::new();
        schema.register_table("test".to_owned(), test::table_with_sequence(1, 1)?)?;
        catalog.register_schema("my_schema", Arc::new(schema));
        ctx.register_catalog("my_catalog", Arc::new(catalog));

        for table_ref in &["my_catalog.my_schema.test", "my_schema.test", "test"] {
            let result = plan_and_collect(
                &mut ctx,
                &format!("SELECT COUNT(*) AS count FROM {}", table_ref),
            )
            .await?;

            let expected = vec![
                "+-------+",
                "| count |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ];
            assert_batches_eq!(expected, &result);
        }

        Ok(())
    }

    #[tokio::test]
    async fn cross_catalog_access() -> Result<()> {
        let mut ctx = ExecutionContext::new();

        let catalog_a = MemoryCatalogProvider::new();
        let schema_a = MemorySchemaProvider::new();
        schema_a
            .register_table("table_a".to_owned(), test::table_with_sequence(1, 1)?)?;
        catalog_a.register_schema("schema_a", Arc::new(schema_a));
        ctx.register_catalog("catalog_a", Arc::new(catalog_a));

        let catalog_b = MemoryCatalogProvider::new();
        let schema_b = MemorySchemaProvider::new();
        schema_b
            .register_table("table_b".to_owned(), test::table_with_sequence(1, 2)?)?;
        catalog_b.register_schema("schema_b", Arc::new(schema_b));
        ctx.register_catalog("catalog_b", Arc::new(catalog_b));

        let result = plan_and_collect(
            &mut ctx,
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
        let ctx = ExecutionContext::with_config(
            ExecutionConfig::new().with_information_schema(true),
        );

        // register a single catalog
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let catalog_weak = Arc::downgrade(&catalog);
        ctx.register_catalog("my_catalog", catalog);

        let catalog_list_weak = {
            let state = ctx.state.lock();
            Arc::downgrade(&state.catalog_list)
        };

        drop(ctx);

        assert_eq!(Weak::strong_count(&catalog_list_weak), 0);
        assert_eq!(Weak::strong_count(&catalog_weak), 0);
    }

    #[tokio::test]
    async fn normalized_column_identifiers() {
        // create local execution context
        let mut ctx = ExecutionContext::new();

        // register csv file with the execution context
        ctx.register_csv(
            "case_insensitive_test",
            "tests/example.csv",
            CsvReadOptions::new(),
        )
        .await
        .unwrap();

        let sql = "SELECT A, b FROM case_insensitive_test";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let sql = "SELECT t.A, b FROM case_insensitive_test AS t";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        // Aliases

        let sql = "SELECT t.A as x, b FROM case_insensitive_test AS t";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| x | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let sql = "SELECT t.A AS X, b FROM case_insensitive_test AS t";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| x | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let sql = r#"SELECT t.A AS "X", b FROM case_insensitive_test AS t"#;
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| X | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        // Order by

        let sql = "SELECT t.A AS x, b FROM case_insensitive_test AS t ORDER BY x";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| x | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let sql = "SELECT t.A AS x, b FROM case_insensitive_test AS t ORDER BY X";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| x | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let sql = r#"SELECT t.A AS "X", b FROM case_insensitive_test AS t ORDER BY "X""#;
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| X | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        // Where

        let sql = "SELECT a, b FROM case_insensitive_test where A IS NOT null";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        // Group by

        let sql = "SELECT a as x, count(*) as c FROM case_insensitive_test GROUP BY X";
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| x | c |",
            "+---+---+",
            "| 1 | 1 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let sql =
            r#"SELECT a as "X", count(*) as c FROM case_insensitive_test GROUP BY "X""#;
        let result = plan_and_collect(&mut ctx, sql)
            .await
            .expect("ran plan correctly");
        let expected = vec![
            "+---+---+",
            "| X | c |",
            "+---+---+",
            "| 1 | 1 |",
            "+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &result);
    }

    struct MyPhysicalPlanner {}

    #[async_trait]
    impl PhysicalPlanner for MyPhysicalPlanner {
        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _ctx_state: &ExecutionContextState,
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
            _ctx_state: &ExecutionContextState,
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
            ctx_state: &ExecutionContextState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let physical_planner = MyPhysicalPlanner {};
            physical_planner
                .create_physical_plan(logical_plan, ctx_state)
                .await
        }
    }

    /// Execute SQL and return results
    async fn plan_and_collect(
        ctx: &mut ExecutionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        ctx.sql(sql).await?.collect().await
    }

    /// Execute SQL and return results
    async fn execute(sql: &str, partition_count: usize) -> Result<Vec<RecordBatch>> {
        let tmp_dir = TempDir::new()?;
        let mut ctx = create_ctx(&tmp_dir, partition_count).await?;
        plan_and_collect(&mut ctx, sql).await
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
    ) -> Result<ExecutionContext> {
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().with_target_partitions(8),
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

    // Test for compilation error when calling read_* functions from an #[async_trait] function.
    // See https://github.com/apache/arrow-datafusion/issues/1154
    #[async_trait]
    trait CallReadTrait {
        async fn call_read_csv(&self) -> Arc<dyn DataFrame>;
        async fn call_read_avro(&self) -> Arc<dyn DataFrame>;
        async fn call_read_parquet(&self) -> Arc<dyn DataFrame>;
    }

    struct CallRead {}

    #[async_trait]
    impl CallReadTrait for CallRead {
        async fn call_read_csv(&self) -> Arc<dyn DataFrame> {
            let mut ctx = ExecutionContext::new();
            ctx.read_csv("dummy", CsvReadOptions::new()).await.unwrap()
        }

        async fn call_read_avro(&self) -> Arc<dyn DataFrame> {
            let mut ctx = ExecutionContext::new();
            ctx.read_avro("dummy", AvroReadOptions::default())
                .await
                .unwrap()
        }

        async fn call_read_parquet(&self) -> Arc<dyn DataFrame> {
            let mut ctx = ExecutionContext::new();
            ctx.read_parquet("dummy").await.unwrap()
        }
    }
}
