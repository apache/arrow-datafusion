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

//! [`SessionState`]: information required to run queries in a session

use crate::catalog::information_schema::{InformationSchemaProvider, INFORMATION_SCHEMA};
use crate::catalog::listing_schema::ListingSchemaProvider;
use crate::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use crate::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider,
    MemoryCatalogProviderList,
};
use crate::datasource::cte_worktable::CteWorkTable;
use crate::datasource::file_format::arrow::ArrowFormatFactory;
use crate::datasource::file_format::avro::AvroFormatFactory;
use crate::datasource::file_format::csv::CsvFormatFactory;
use crate::datasource::file_format::json::JsonFormatFactory;
#[cfg(feature = "parquet")]
use crate::datasource::file_format::parquet::ParquetFormatFactory;
use crate::datasource::file_format::{format_as_file_type, FileFormatFactory};
use crate::datasource::function::{TableFunction, TableFunctionImpl};
use crate::datasource::provider::{DefaultTableFactory, TableProviderFactory};
use crate::datasource::provider_as_source;
use crate::execution::context::{EmptySerializerRegistry, FunctionFactory, QueryPlanner};
#[cfg(feature = "array_expressions")]
use crate::functions_array;
use crate::physical_optimizer::optimizer::PhysicalOptimizer;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use crate::{functions, functions_aggregate};
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::{ConfigExtension, ConfigOptions, TableOptions};
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{
    config_err, not_impl_err, plan_datafusion_err, DFSchema, DataFusionError,
    ResolvedTableReference, TableReference,
};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::registry::{FunctionRegistry, SerializerRegistry};
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::var_provider::{is_system_variables, VarType};
use datafusion_expr::{
    AggregateUDF, Explain, Expr, ExprSchemable, LogicalPlan, ScalarUDF, TableSource,
    WindowUDF,
};
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_optimizer::{
    Analyzer, AnalyzerRule, Optimizer, OptimizerConfig, OptimizerRule,
};
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::{ContextProvider, ParserOptions, PlannerContext, SqlToRel};
use sqlparser::ast::Expr as SQLExpr;
use sqlparser::dialect::dialect_from_str;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

/// Execution context for registering data sources and executing queries.
/// See [`SessionContext`] for a higher level API.
///
/// Note that there is no `Default` or `new()` for SessionState,
/// to avoid accidentally running queries or other operations without passing through
/// the [`SessionConfig`] or [`RuntimeEnv`]. See [`SessionContext`].
///
/// [`SessionContext`]: crate::execution::context::SessionContext
#[derive(Clone)]
pub struct SessionState {
    /// A unique UUID that identifies the session
    session_id: String,
    /// Responsible for analyzing and rewrite a logical plan before optimization
    analyzer: Analyzer,
    /// Responsible for optimizing a logical plan
    optimizer: Optimizer,
    /// Responsible for optimizing a physical execution plan
    physical_optimizers: PhysicalOptimizer,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    /// Collection of catalogs containing schemas and ultimately TableProviders
    catalog_list: Arc<dyn CatalogProviderList>,
    /// Table Functions
    table_functions: HashMap<String, Arc<TableFunction>>,
    /// Scalar functions that are registered with the context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window functions registered in the context
    window_functions: HashMap<String, Arc<WindowUDF>>,
    /// Deserializer registry for extensions.
    serializer_registry: Arc<dyn SerializerRegistry>,
    /// Holds registered external FileFormat implementations
    file_formats: HashMap<String, Arc<dyn FileFormatFactory>>,
    /// Session configuration
    config: SessionConfig,
    /// Table options
    table_options: TableOptions,
    /// Execution properties
    execution_props: ExecutionProps,
    /// TableProviderFactories for different file formats.
    ///
    /// Maps strings like "JSON" to an instance of  [`TableProviderFactory`]
    ///
    /// This is used to create [`TableProvider`] instances for the
    /// `CREATE EXTERNAL TABLE ... STORED AS <FORMAT>` for custom file
    /// formats other than those built into DataFusion
    ///
    /// [`TableProvider`]: crate::datasource::provider::TableProvider
    table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
    /// Runtime environment
    runtime_env: Arc<RuntimeEnv>,

    /// [FunctionFactory] to support pluggable user defined function handler.
    ///
    /// It will be invoked on `CREATE FUNCTION` statements.
    /// thus, changing dialect o PostgreSql is required
    function_factory: Option<Arc<dyn FunctionFactory>>,
}

impl Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("session_id", &self.session_id)
            .field("analyzer", &"...")
            .field("optimizer", &"...")
            .field("physical_optimizers", &"...")
            .field("query_planner", &"...")
            .field("catalog_list", &"...")
            .field("table_functions", &"...")
            .field("scalar_functions", &self.scalar_functions)
            .field("aggregate_functions", &self.aggregate_functions)
            .field("window_functions", &self.window_functions)
            .field("serializer_registry", &"...")
            .field("config", &self.config)
            .field("table_options", &self.table_options)
            .field("execution_props", &self.execution_props)
            .field("table_factories", &"...")
            .field("runtime_env", &self.runtime_env)
            .field("function_factory", &"...")
            .finish_non_exhaustive()
    }
}

impl SessionState {
    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`] and [`RuntimeEnv`].
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let catalog_list =
            Arc::new(MemoryCatalogProviderList::new()) as Arc<dyn CatalogProviderList>;
        Self::new_with_config_rt_and_catalog_list(config, runtime, catalog_list)
    }

    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`] and [`RuntimeEnv`].
    #[deprecated(since = "32.0.0", note = "Use SessionState::new_with_config_rt")]
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self::new_with_config_rt(config, runtime)
    }

    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`],  [`RuntimeEnv`], and [`CatalogProviderList`]
    pub fn new_with_config_rt_and_catalog_list(
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        let session_id = Uuid::new_v4().to_string();

        // Create table_factories for all default formats
        let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
            HashMap::new();
        #[cfg(feature = "parquet")]
        table_factories.insert("PARQUET".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("CSV".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("JSON".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("NDJSON".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("AVRO".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("ARROW".into(), Arc::new(DefaultTableFactory::new()));

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

        let mut new_self = SessionState {
            session_id,
            analyzer: Analyzer::new(),
            optimizer: Optimizer::new(),
            physical_optimizers: PhysicalOptimizer::new(),
            query_planner: Arc::new(DefaultQueryPlanner {}),
            catalog_list,
            table_functions: HashMap::new(),
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            window_functions: HashMap::new(),
            serializer_registry: Arc::new(EmptySerializerRegistry),
            file_formats: HashMap::new(),
            table_options: TableOptions::default_from_session_config(config.options()),
            config,
            execution_props: ExecutionProps::new(),
            runtime_env: runtime,
            table_factories,
            function_factory: None,
        };

        #[cfg(feature = "parquet")]
        if let Err(e) =
            new_self.register_file_format(Arc::new(ParquetFormatFactory::new()), false)
        {
            log::info!("Unable to register default ParquetFormat: {e}")
        };

        if let Err(e) =
            new_self.register_file_format(Arc::new(JsonFormatFactory::new()), false)
        {
            log::info!("Unable to register default JsonFormat: {e}")
        };

        if let Err(e) =
            new_self.register_file_format(Arc::new(CsvFormatFactory::new()), false)
        {
            log::info!("Unable to register default CsvFormat: {e}")
        };

        if let Err(e) =
            new_self.register_file_format(Arc::new(ArrowFormatFactory::new()), false)
        {
            log::info!("Unable to register default ArrowFormat: {e}")
        };

        if let Err(e) =
            new_self.register_file_format(Arc::new(AvroFormatFactory::new()), false)
        {
            log::info!("Unable to register default AvroFormat: {e}")
        };

        // register built in functions
        functions::register_all(&mut new_self)
            .expect("can not register built in functions");

        // register crate of array expressions (if enabled)
        #[cfg(feature = "array_expressions")]
        functions_array::register_all(&mut new_self)
            .expect("can not register array expressions");

        functions_aggregate::register_all(&mut new_self)
            .expect("can not register aggregate functions");

        new_self
    }
    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`] and [`RuntimeEnv`].
    #[deprecated(
        since = "32.0.0",
        note = "Use SessionState::new_with_config_rt_and_catalog_list"
    )]
    pub fn with_config_rt_and_catalog_list(
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        Self::new_with_config_rt_and_catalog_list(config, runtime, catalog_list)
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
            Arc::clone(factory),
            store,
            format,
        );
        let _ = default_catalog
            .register_schema("default", Arc::new(schema))
            .expect("Failed to register default schema");
    }

    pub(crate) fn resolve_table_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> ResolvedTableReference {
        let catalog = &self.config_options().catalog;
        table_ref
            .into()
            .resolve(&catalog.default_catalog, &catalog.default_schema)
    }

    pub(crate) fn schema_for_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> datafusion_common::Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        if self.config.information_schema() && *resolved_ref.schema == *INFORMATION_SCHEMA
        {
            return Ok(Arc::new(InformationSchemaProvider::new(Arc::clone(
                &self.catalog_list,
            ))));
        }

        self.catalog_list
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                )
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| {
                plan_datafusion_err!("failed to resolve schema: {}", resolved_ref.schema)
            })
    }

    /// Replace the random session id.
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = session_id;
        self
    }

    /// override default query planner with `query_planner`
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = query_planner;
        self
    }

    /// Override the [`AnalyzerRule`]s optimizer plan rules.
    pub fn with_analyzer_rules(
        mut self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.analyzer = Analyzer::with_rules(rules);
        self
    }

    /// Replace the entire list of [`OptimizerRule`]s used to optimize plans
    pub fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizer = Optimizer::with_rules(rules);
        self
    }

    /// Replace the entire list of [`PhysicalOptimizerRule`]s used to optimize plans
    pub fn with_physical_optimizer_rules(
        mut self,
        physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.physical_optimizers = PhysicalOptimizer::with_rules(physical_optimizers);
        self
    }

    /// Add `analyzer_rule` to the end of the list of
    /// [`AnalyzerRule`]s used to rewrite queries.
    pub fn add_analyzer_rule(
        &mut self,
        analyzer_rule: Arc<dyn AnalyzerRule + Send + Sync>,
    ) -> &Self {
        self.analyzer.rules.push(analyzer_rule);
        self
    }

    /// Add `optimizer_rule` to the end of the list of
    /// [`OptimizerRule`]s used to rewrite queries.
    pub fn add_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        self.optimizer.rules.push(optimizer_rule);
        self
    }

    // the add_optimizer_rule takes an owned reference
    // it should probably be renamed to `with_optimizer_rule` to follow builder style
    // and `add_optimizer_rule` that takes &mut self added instead of this
    pub(crate) fn append_optimizer_rule(
        &mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) {
        self.optimizer.rules.push(optimizer_rule);
    }

    /// Add `physical_optimizer_rule` to the end of the list of
    /// [`PhysicalOptimizerRule`]s used to rewrite queries.
    pub fn add_physical_optimizer_rule(
        mut self,
        physical_optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizers.rules.push(physical_optimizer_rule);
        self
    }

    /// Adds a new [`ConfigExtension`] to TableOptions
    pub fn add_table_options_extension<T: ConfigExtension>(
        mut self,
        extension: T,
    ) -> Self {
        self.table_options.extensions.insert(extension);
        self
    }

    /// Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
    pub fn with_function_factory(
        mut self,
        function_factory: Arc<dyn FunctionFactory>,
    ) -> Self {
        self.function_factory = Some(function_factory);
        self
    }

    /// Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
    pub fn set_function_factory(&mut self, function_factory: Arc<dyn FunctionFactory>) {
        self.function_factory = Some(function_factory);
    }

    /// Replace the extension [`SerializerRegistry`]
    pub fn with_serializer_registry(
        mut self,
        registry: Arc<dyn SerializerRegistry>,
    ) -> Self {
        self.serializer_registry = registry;
        self
    }

    /// Get the function factory
    pub fn function_factory(&self) -> Option<&Arc<dyn FunctionFactory>> {
        self.function_factory.as_ref()
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

    /// Parse an SQL string into an DataFusion specific AST
    /// [`Statement`]. See [`SessionContext::sql`] for running queries.
    ///
    /// [`SessionContext::sql`]: crate::execution::context::SessionContext::sql
    pub fn sql_to_statement(
        &self,
        sql: &str,
        dialect: &str,
    ) -> datafusion_common::Result<Statement> {
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                     Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                     MsSQL, ClickHouse, BigQuery, Ansi."
            )
        })?;
        let mut statements = DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?;
        if statements.len() > 1 {
            return not_impl_err!(
                "The context currently only supports a single SQL statement"
            );
        }
        let statement = statements.pop_front().ok_or_else(|| {
            DataFusionError::NotImplemented(
                "The context requires a statement!".to_string(),
            )
        })?;
        Ok(statement)
    }

    /// parse a sql string into a sqlparser-rs AST [`SQLExpr`].
    ///
    /// See [`Self::create_logical_expr`] for parsing sql to [`Expr`].
    pub fn sql_to_expr(
        &self,
        sql: &str,
        dialect: &str,
    ) -> datafusion_common::Result<SQLExpr> {
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                     Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                     MsSQL, ClickHouse, BigQuery, Ansi."
            )
        })?;

        let expr = DFParser::parse_sql_into_expr_with_dialect(sql, dialect.as_ref())?;

        Ok(expr)
    }

    /// Resolve all table references in the SQL statement. Does not include CTE references.
    ///
    /// See [`catalog::resolve_table_references`] for more information.
    ///
    /// [`catalog::resolve_table_references`]: crate::catalog::resolve_table_references
    pub fn resolve_table_references(
        &self,
        statement: &datafusion_sql::parser::Statement,
    ) -> datafusion_common::Result<Vec<TableReference>> {
        let enable_ident_normalization =
            self.config.options().sql_parser.enable_ident_normalization;
        let (table_refs, _) = crate::catalog::resolve_table_references(
            statement,
            enable_ident_normalization,
        )?;
        Ok(table_refs)
    }

    /// Convert an AST Statement into a LogicalPlan
    pub async fn statement_to_plan(
        &self,
        statement: datafusion_sql::parser::Statement,
    ) -> datafusion_common::Result<LogicalPlan> {
        let references = self.resolve_table_references(&statement)?;

        let mut provider = SessionContextProvider {
            state: self,
            tables: HashMap::with_capacity(references.len()),
        };

        for reference in references {
            let resolved = &self.resolve_table_ref(reference);
            if let Entry::Vacant(v) = provider.tables.entry(resolved.to_string()) {
                if let Ok(schema) = self.schema_for_ref(resolved.clone()) {
                    if let Some(table) = schema.table(&resolved.table).await? {
                        v.insert(provider_as_source(table));
                    }
                }
            }
        }

        let query = self.build_sql_query_planner(&provider);
        query.statement_to_plan(statement)
    }

    fn get_parser_options(&self) -> ParserOptions {
        let sql_parser_options = &self.config.options().sql_parser;

        ParserOptions {
            parse_float_as_decimal: sql_parser_options.parse_float_as_decimal,
            enable_ident_normalization: sql_parser_options.enable_ident_normalization,
            support_varchar_with_length: sql_parser_options.support_varchar_with_length,
        }
    }

    /// Creates a [`LogicalPlan`] from the provided SQL string. This
    /// interface will plan any SQL DataFusion supports, including DML
    /// like `CREATE TABLE`, and `COPY` (which can write to local
    /// files.
    ///
    /// See [`SessionContext::sql`] and
    /// [`SessionContext::sql_with_options`] for a higher-level
    /// interface that handles DDL and verification of allowed
    /// statements.
    ///
    /// [`SessionContext::sql`]: crate::execution::context::SessionContext::sql
    /// [`SessionContext::sql_with_options`]: crate::execution::context::SessionContext::sql_with_options
    pub async fn create_logical_plan(
        &self,
        sql: &str,
    ) -> datafusion_common::Result<LogicalPlan> {
        let dialect = self.config.options().sql_parser.dialect.as_str();
        let statement = self.sql_to_statement(sql, dialect)?;
        let plan = self.statement_to_plan(statement).await?;
        Ok(plan)
    }

    /// Creates a datafusion style AST [`Expr`] from a SQL string.
    ///
    /// See example on  [SessionContext::parse_sql_expr](crate::execution::context::SessionContext::parse_sql_expr)
    pub fn create_logical_expr(
        &self,
        sql: &str,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Expr> {
        let dialect = self.config.options().sql_parser.dialect.as_str();

        let sql_expr = self.sql_to_expr(sql, dialect)?;

        let provider = SessionContextProvider {
            state: self,
            tables: HashMap::new(),
        };

        let query = self.build_sql_query_planner(&provider);
        query.sql_to_expr(sql_expr, df_schema, &mut PlannerContext::new())
    }

    /// Optimizes the logical plan by applying optimizer rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
        if let LogicalPlan::Explain(e) = plan {
            let mut stringified_plans = e.stringified_plans.clone();

            // analyze & capture output of each rule
            let analyzer_result = self.analyzer.execute_and_check(
                e.plan.as_ref().clone(),
                self.options(),
                |analyzed_plan, analyzer| {
                    let analyzer_name = analyzer.name().to_string();
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans.push(analyzed_plan.to_stringified(plan_type));
                },
            );
            let analyzed_plan = match analyzer_result {
                Ok(plan) => plan,
                Err(DataFusionError::Context(analyzer_name, err)) => {
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans
                        .push(StringifiedPlan::new(plan_type, err.to_string()));

                    return Ok(LogicalPlan::Explain(Explain {
                        verbose: e.verbose,
                        plan: Arc::clone(&e.plan),
                        stringified_plans,
                        schema: Arc::clone(&e.schema),
                        logical_optimization_succeeded: false,
                    }));
                }
                Err(e) => return Err(e),
            };

            // to delineate the analyzer & optimizer phases in explain output
            stringified_plans
                .push(analyzed_plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan));

            // optimize the child plan, capturing the output of each optimizer
            let optimized_plan = self.optimizer.optimize(
                analyzed_plan,
                self,
                |optimized_plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(optimized_plan.to_stringified(plan_type));
                },
            );
            let (plan, logical_optimization_succeeded) = match optimized_plan {
                Ok(plan) => (Arc::new(plan), true),
                Err(DataFusionError::Context(optimizer_name, err)) => {
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans
                        .push(StringifiedPlan::new(plan_type, err.to_string()));
                    (Arc::clone(&e.plan), false)
                }
                Err(e) => return Err(e),
            };

            Ok(LogicalPlan::Explain(Explain {
                verbose: e.verbose,
                plan,
                stringified_plans,
                schema: Arc::clone(&e.schema),
                logical_optimization_succeeded,
            }))
        } else {
            let analyzed_plan = self.analyzer.execute_and_check(
                plan.clone(),
                self.options(),
                |_, _| {},
            )?;
            self.optimizer.optimize(analyzed_plan, self, |_, _| {})
        }
    }

    /// Creates a physical [`ExecutionPlan`] plan from a [`LogicalPlan`].
    ///
    /// Note: this first calls [`Self::optimize`] on the provided
    /// plan.
    ///
    /// This function will error for [`LogicalPlan`]s such as catalog DDL like
    /// `CREATE TABLE`, which do not have corresponding physical plans and must
    /// be handled by another layer, typically [`SessionContext`].
    ///
    /// [`SessionContext`]: crate::execution::context::SessionContext
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.optimize(logical_plan)?;
        self.query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }

    /// Create a [`PhysicalExpr`] from an [`Expr`] after applying type
    /// coercion, and function rewrites.
    ///
    /// Note: The expression is not [simplified] or otherwise optimized:  `a = 1
    /// + 2` will not be simplified to `a = 3` as this is a more involved process.
    /// See the [expr_api] example for how to simplify expressions.
    ///
    /// # See Also:
    /// * [`SessionContext::create_physical_expr`] for a higher-level API
    /// * [`create_physical_expr`] for a lower-level API
    ///
    /// [simplified]: datafusion_optimizer::simplify_expressions
    /// [expr_api]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs
    /// [`SessionContext::create_physical_expr`]: crate::execution::context::SessionContext::create_physical_expr
    pub fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        let simplifier =
            ExprSimplifier::new(SessionSimplifyProvider::new(self, df_schema));
        // apply type coercion here to ensure types match
        let mut expr = simplifier.coerce(expr, df_schema)?;

        // rewrite Exprs to functions if necessary
        let config_options = self.config_options();
        for rewrite in self.analyzer.function_rewrites() {
            expr = expr
                .transform_up(|expr| rewrite.rewrite(expr, df_schema, config_options))?
                .data;
        }
        create_physical_expr(&expr, df_schema, self.execution_props())
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

    /// Return mutable execution properties
    pub fn execution_props_mut(&mut self) -> &mut ExecutionProps {
        &mut self.execution_props
    }

    /// Return the [`SessionConfig`]
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Return the mutable [`SessionConfig`].
    pub fn config_mut(&mut self) -> &mut SessionConfig {
        &mut self.config
    }

    /// Return the physical optimizers
    pub fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_optimizers.rules
    }

    /// return the configuration options
    pub fn config_options(&self) -> &ConfigOptions {
        self.config.options()
    }

    /// return the TableOptions options with its extensions
    pub fn default_table_options(&self) -> TableOptions {
        self.table_options
            .combine_with_session_config(self.config_options())
    }

    /// Return the table options
    pub fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    /// Return mutable table opptions
    pub fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }

    /// Registers a [`ConfigExtension`] as a table option extention that can be
    /// referenced from SQL statements executed against this context.
    pub fn register_table_options_extension<T: ConfigExtension>(&mut self, extension: T) {
        self.table_options.extensions.insert(extension)
    }

    /// Adds or updates a [FileFormatFactory] which can be used with COPY TO or CREATE EXTERNAL TABLE statements for reading
    /// and writing files of custom formats.
    pub fn register_file_format(
        &mut self,
        file_format: Arc<dyn FileFormatFactory>,
        overwrite: bool,
    ) -> Result<(), DataFusionError> {
        let ext = file_format.get_ext().to_lowercase();
        match (self.file_formats.entry(ext.clone()), overwrite){
            (Entry::Vacant(e), _) => {e.insert(file_format);},
            (Entry::Occupied(mut e), true)  => {e.insert(file_format);},
            (Entry::Occupied(_), false) => return config_err!("File type already registered for extension {ext}. Set overwrite to true to replace this extension."),
        };
        Ok(())
    }

    /// Retrieves a [FileFormatFactory] based on file extension which has been registered
    /// via SessionContext::register_file_format. Extensions are not case sensitive.
    pub fn get_file_format_factory(
        &self,
        ext: &str,
    ) -> Option<Arc<dyn FileFormatFactory>> {
        self.file_formats.get(&ext.to_lowercase()).cloned()
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }

    /// Return catalog list
    pub fn catalog_list(&self) -> &Arc<dyn CatalogProviderList> {
        &self.catalog_list
    }

    /// set the catalog list
    pub(crate) fn register_catalog_list(
        &mut self,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) {
        self.catalog_list = catalog_list;
    }

    /// Return reference to scalar_functions
    pub fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    /// Return reference to aggregate_functions
    pub fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    /// Return reference to window functions
    pub fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window_functions
    }

    /// Return reference to table_functions
    pub fn table_functions(&self) -> &HashMap<String, Arc<TableFunction>> {
        &self.table_functions
    }

    /// Return [SerializerRegistry] for extensions
    pub fn serializer_registry(&self) -> &Arc<dyn SerializerRegistry> {
        &self.serializer_registry
    }

    /// Return version of the cargo package that produced this query
    pub fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    /// Register a user defined table function
    pub fn register_udtf(&mut self, name: &str, fun: Arc<dyn TableFunctionImpl>) {
        self.table_functions.insert(
            name.to_owned(),
            Arc::new(TableFunction::new(name.to_owned(), fun)),
        );
    }

    /// Deregsiter a user defined table function
    pub fn deregister_udtf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableFunctionImpl>>> {
        let udtf = self.table_functions.remove(name);
        Ok(udtf.map(|x| Arc::clone(x.function())))
    }

    fn build_sql_query_planner<'a, S>(&self, provider: &'a S) -> SqlToRel<'a, S>
    where
        S: ContextProvider,
    {
        let query = SqlToRel::new_with_options(provider, self.get_parser_options());

        // register crate of array expressions (if enabled)
        #[cfg(feature = "array_expressions")]
        {
            let array_planner =
                Arc::new(functions_array::planner::ArrayFunctionPlanner::default()) as _;

            let field_access_planner =
                Arc::new(functions_array::planner::FieldAccessPlanner::default()) as _;

            query
                .with_user_defined_planner(array_planner)
                .with_user_defined_planner(field_access_planner)
        }
        #[cfg(not(feature = "array_expressions"))]
        {
            query
        }
    }
}

struct SessionContextProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl<'a> ContextProvider for SessionContextProvider<'a> {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let name = self.state.resolve_table_ref(name).to_string();
        self.tables
            .get(&name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table '{name}' not found"))
    }

    fn get_table_function_source(
        &self,
        name: &str,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let tbl_func = self
            .state
            .table_functions
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table function '{name}' not found"))?;
        let provider = tbl_func.create_table_provider(&args)?;

        Ok(provider_as_source(provider))
    }

    /// Create a new CTE work table for a recursive CTE logical plan
    /// This table will be used in conjunction with a Worktable physical plan
    /// to read and write each iteration of a recursive CTE
    fn create_cte_work_table(
        &self,
        name: &str,
        schema: SchemaRef,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table = Arc::new(CteWorkTable::new(name, schema));
        Ok(provider_as_source(table))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
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

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }

    fn get_file_type(&self, ext: &str) -> datafusion_common::Result<Arc<dyn FileType>> {
        self.state
            .file_formats
            .get(&ext.to_lowercase())
            .ok_or(plan_datafusion_err!(
                "There is no registered file format with ext {ext}"
            ))
            .map(|file_type| format_as_file_type(Arc::clone(file_type)))
    }
}

impl FunctionRegistry for SessionState {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDF named \"{name}\" in the registry")
        })
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDAF named \"{name}\" in the registry")
        })
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        let result = self.window_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDWF named \"{name}\" in the registry")
        })
    }

    fn register_udf(
        &mut self,
        udf: Arc<ScalarUDF>,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        udf.aliases().iter().for_each(|alias| {
            self.scalar_functions
                .insert(alias.clone(), Arc::clone(&udf));
        });
        Ok(self.scalar_functions.insert(udf.name().into(), udf))
    }

    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        udaf.aliases().iter().for_each(|alias| {
            self.aggregate_functions
                .insert(alias.clone(), Arc::clone(&udaf));
        });
        Ok(self.aggregate_functions.insert(udaf.name().into(), udaf))
    }

    fn register_udwf(
        &mut self,
        udwf: Arc<WindowUDF>,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        udwf.aliases().iter().for_each(|alias| {
            self.window_functions
                .insert(alias.clone(), Arc::clone(&udwf));
        });
        Ok(self.window_functions.insert(udwf.name().into(), udwf))
    }

    fn deregister_udf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        let udf = self.scalar_functions.remove(name);
        if let Some(udf) = &udf {
            for alias in udf.aliases() {
                self.scalar_functions.remove(alias);
            }
        }
        Ok(udf)
    }

    fn deregister_udaf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        let udaf = self.aggregate_functions.remove(name);
        if let Some(udaf) = &udaf {
            for alias in udaf.aliases() {
                self.aggregate_functions.remove(alias);
            }
        }
        Ok(udaf)
    }

    fn deregister_udwf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        let udwf = self.window_functions.remove(name);
        if let Some(udwf) = &udwf {
            for alias in udwf.aliases() {
                self.window_functions.remove(alias);
            }
        }
        Ok(udwf)
    }

    fn register_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> datafusion_common::Result<()> {
        self.analyzer.add_function_rewrite(rewrite);
        Ok(())
    }
}

impl OptimizerConfig for SessionState {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.execution_props.query_execution_start_time
    }

    fn alias_generator(&self) -> Arc<AliasGenerator> {
        Arc::clone(&self.execution_props.alias_generator)
    }

    fn options(&self) -> &ConfigOptions {
        self.config_options()
    }

    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        Some(self)
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
            state.window_functions.clone(),
            Arc::clone(&state.runtime_env),
        )
    }
}

/// The query planner used if no user defined planner is provided
struct DefaultQueryPlanner {}

#[async_trait]
impl QueryPlanner for DefaultQueryPlanner {
    /// Given a `LogicalPlan`, create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

struct SessionSimplifyProvider<'a> {
    state: &'a SessionState,
    df_schema: &'a DFSchema,
}

impl<'a> SessionSimplifyProvider<'a> {
    fn new(state: &'a SessionState, df_schema: &'a DFSchema) -> Self {
        Self { state, df_schema }
    }
}

impl<'a> SimplifyInfo for SessionSimplifyProvider<'a> {
    fn is_boolean_type(&self, expr: &Expr) -> datafusion_common::Result<bool> {
        Ok(expr.get_type(self.df_schema)? == DataType::Boolean)
    }

    fn nullable(&self, expr: &Expr) -> datafusion_common::Result<bool> {
        expr.nullable(self.df_schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.state.execution_props()
    }

    fn get_data_type(&self, expr: &Expr) -> datafusion_common::Result<DataType> {
        expr.get_type(self.df_schema)
    }
}
