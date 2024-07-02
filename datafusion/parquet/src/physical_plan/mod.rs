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

//! [`ParquetExec`] Execution plan for reading Parquet files

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::datasource::physical_plan::FileGroupPartitioner;
use datafusion::datasource::{listing::PartitionedFile, physical_plan::FileScanConfig};
use datafusion::datasource::physical_plan::file_stream::FileStream;
use datafusion_physical_plan::DisplayAs;
use opener::ParquetOpener;
use reader::{DefaultParquetFileReaderFactory, ParquetFileReaderFactory};
use self::page_filter::PagePruningPredicate;

use datafusion::{
    config::{ConfigOptions, TableParquetOptions},
    error::Result,
    execution::context::TaskContext,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};

use itertools::Itertools;
use log::debug;
use parquet::basic::{ConvertedType, LogicalType};
use parquet::schema::types::ColumnDescriptor;

mod access_plan;
mod metrics;
mod opener;
mod page_filter;
mod reader;
mod row_filter;
mod row_groups;
mod statistics;
mod writer;

use datafusion::datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapterFactory,
};
/// Execution plan for reading one or more Parquet files.
///
/// ```text
///             ▲
///             │
///             │  Produce a stream of
///             │  RecordBatches
///             │
/// ┌───────────────────────┐
/// │                       │
/// │      ParquetExec      │
/// │                       │
/// └───────────────────────┘
///             ▲
///             │  Asynchronously read from one
///             │  or more parquet files via
///             │  ObjectStore interface
///             │
///             │
///   .───────────────────.
///  │                     )
///  │`───────────────────'│
///  │    ObjectStore      │
///  │.───────────────────.│
///  │                     )
///   `───────────────────'
///
/// ```
///
/// # Example: Create a `ParquetExec`
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
/// # use datafusion::datasource::listing::PartitionedFile;
/// # let file_schema = Arc::new(Schema::empty());
/// # let object_store_url = ObjectStoreUrl::local_filesystem();
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_physical_expr::expressions::lit;
/// # let predicate = lit(true);
/// // Create a ParquetExec for reading `file1.parquet` with a file size of 100MB
/// let file_scan_config = FileScanConfig::new(object_store_url, file_schema)
///    .with_file(PartitionedFile::new("file1.parquet", 100*1024*1024));
/// let exec = ParquetExec::builder(file_scan_config)
///   // Provide a predicate for filtering row groups/pages
///   .with_predicate(predicate)
///   .build();
/// ```
///
/// # Features
///
/// Supports the following optimizations:
///
/// * Concurrent reads: Can read from one or more files in parallel as multiple
/// partitions, including concurrently reading multiple row groups from a single
/// file.
///
/// * Predicate push down: skips row groups and pages based on
/// min/max/null_counts in the row group metadata, the page index and bloom
/// filters.
///
/// * Projection pushdown: reads and decodes only the columns required.
///
/// * Limit pushdown: stop execution early after some number of rows are read.
///
/// * Custom readers: customize reading  parquet files, e.g. to cache metadata,
/// coalesce I/O operations, etc. See [`ParquetFileReaderFactory`] for more
/// details.
///
/// * Schema adapters: read parquet files with different schemas into a unified
/// table schema. This can be used to implement "schema evolution". See
/// [`SchemaAdapterFactory`] for more details.
///
/// * metadata_size_hint: controls the number of bytes read from the end of the
/// file in the initial I/O when the default [`ParquetFileReaderFactory`]. If a
/// custom reader is used, it supplies the metadata directly and this parameter
/// is ignored. [`ParquetExecBuilder::with_metadata_size_hint`] for more details.
///
/// * User provided  [`ParquetAccessPlan`]s to skip row groups and/or pages
/// based on external information. See "Implementing External Indexes" below
///
/// # Implementing External Indexes
///
/// It is possible to restrict the row groups and selections within those row
/// groups that the ParquetExec will consider by providing an initial
/// [`ParquetAccessPlan`] as `extensions` on [`PartitionedFile`]. This can be
/// used to implement external indexes on top of parquet files and select only
/// portions of the files.
///
/// The `ParquetExec` will try and reduce any provided `ParquetAccessPlan`
/// further based on the contents of `ParquetMetadata` and other settings.
///
/// ## Example of providing a ParquetAccessPlan
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{Schema, SchemaRef};
/// # use datafusion::datasource::listing::PartitionedFile;
/// # use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
/// # use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # fn schema() -> SchemaRef {
/// #   Arc::new(Schema::empty())
/// # }
/// // create an access plan to scan row group 0, 1 and 3 and skip row groups 2 and 4
/// let mut access_plan = ParquetAccessPlan::new_all(5);
/// access_plan.skip(2);
/// access_plan.skip(4);
/// // provide the plan as extension to the FileScanConfig
/// let partitioned_file = PartitionedFile::new("my_file.parquet", 1234)
///   .with_extensions(Arc::new(access_plan));
/// // create a ParquetExec to scan this file
/// let file_scan_config = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema())
///     .with_file(partitioned_file);
/// // this parquet exec will not even try to read row groups 2 and 4. Additional
/// // pruning based on predicates may also happen
/// let exec = ParquetExec::builder(file_scan_config).build();
/// ```
///
/// For a complete example, see the [`advanced_parquet_index` example]).
///
/// [`parquet_index_advanced` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_parquet_index.rs
///
/// # Execution Overview
///
/// * Step 1: [`ParquetExec::execute`] is called, returning a [`FileStream`]
/// configured to open parquet files with a [`ParquetOpener`].
///
/// * Step 2: When the stream is polled, the [`ParquetOpener`] is called to open
/// the file.
///
/// * Step 3: The `ParquetOpener` gets the [`ParquetMetaData`] (file metadata)
/// via [`ParquetFileReaderFactory`], creating a [`ParquetAccessPlan`] by
/// applying predicates to metadata. The plan and projections are used to
/// determine what pages must be read.
///
/// * Step 4: The stream begins reading data, fetching the required pages
/// and incrementally decoding them.
///
/// * Step 5: As each [`RecordBatch]` is read, it may be adapted by a
/// [`SchemaAdapter`] to match the table schema. By default missing columns are
/// filled with nulls, but this can be customized via [`SchemaAdapterFactory`].
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
/// [`SchemaAdapter`]: crate::datasource::schema_adapter::SchemaAdapter
/// [`ParquetMetadata`]: parquet::file::metadata::ParquetMetaData
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Base configuration for this scan
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional predicate for pruning row groups (derived from `predicate`)
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional predicate for pruning pages (derived from `predicate`)
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    /// Optional hint for the size of the parquet metadata
    metadata_size_hint: Option<usize>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    /// Cached plan properties such as equivalence properties, ordering, partitioning, etc.
    cache: PlanProperties,
    /// Options for reading Parquet files
    table_parquet_options: TableParquetOptions,
    /// Optional user defined schema adapter
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

/// [`ParquetExecBuilder`], builder for [`ParquetExec`].
///
/// See example on [`ParquetExec`].
pub struct ParquetExecBuilder {
    file_scan_config: FileScanConfig,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    metadata_size_hint: Option<usize>,
    table_parquet_options: TableParquetOptions,
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl ParquetExecBuilder {
    /// Create a new builder to read the provided file scan configuration
    pub fn new(file_scan_config: FileScanConfig) -> Self {
        Self::new_with_options(file_scan_config, TableParquetOptions::default())
    }

    /// Create a new builder to read the data specified in the file scan
    /// configuration with the provided `TableParquetOptions`.
    pub fn new_with_options(
        file_scan_config: FileScanConfig,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        Self {
            file_scan_config,
            predicate: None,
            metadata_size_hint: None,
            table_parquet_options,
            parquet_file_reader_factory: None,
            schema_adapter_factory: None,
        }
    }

    /// Set the predicate for the scan.
    ///
    /// The ParquetExec uses this predicate to filter row groups and data pages
    /// using the Parquet statistics and bloom filters.
    ///
    /// If the predicate can not be used to prune the scan, it is ignored (no
    /// error is raised).
    pub fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the metadata size hint
    ///
    /// This value determines how many bytes at the end of the file the default
    /// [`ParquetFileReaderFactory`] will request in the initial IO. If this is
    /// too small, the ParquetExec will need to make additional IO requests to
    /// read the footer.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Set the table parquet options that control how the ParquetExec reads.
    ///
    /// See also [`Self::new_with_options`]
    pub fn with_table_parquet_options(
        mut self,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        self.table_parquet_options = table_parquet_options;
        self
    }

    /// Set optional user defined parquet file reader factory.
    ///
    /// You can use [`ParquetFileReaderFactory`] to more precisely control how
    /// data is read from parquet files (e.g. skip re-reading metadata, coalesce
    /// I/O operations, etc).
    ///
    /// The default reader factory reads directly from an [`ObjectStore`]
    /// instance using individual I/O operations for the footer and each page.
    ///
    /// If a custom `ParquetFileReaderFactory` is provided, then data access
    /// operations will be routed to this factory instead of [`ObjectStore`].
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// Set optional schema adapter factory.
    ///
    /// [`SchemaAdapterFactory`] allows user to specify how fields from the
    /// parquet file get mapped to that of the table schema.  The default schema
    /// adapter uses arrow's cast library to map the parquet fields to the table
    /// schema.
    pub fn with_schema_adapter_factory(
        mut self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        self.schema_adapter_factory = Some(schema_adapter_factory);
        self
    }

    /// Convenience: build an `Arc`d `ParquetExec` from this builder
    pub fn build_arc(self) -> Arc<ParquetExec> {
        Arc::new(self.build())
    }

    /// Build a [`ParquetExec`]
    #[must_use]
    pub fn build(self) -> ParquetExec {
        let Self {
            file_scan_config,
            predicate,
            metadata_size_hint,
            table_parquet_options,
            parquet_file_reader_factory,
            schema_adapter_factory,
        } = self;

        let base_config = file_scan_config;
        debug!("Creating ParquetExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
        base_config.file_groups, base_config.projection, predicate, base_config.limit);

        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let file_schema = &base_config.file_schema;
        let pruning_predicate = predicate
            .clone()
            .and_then(|predicate_expr| {
                match PruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        debug!("Could not create pruning predicate for: {e}");
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .filter(|p| !p.always_true());

        let page_pruning_predicate = predicate.as_ref().and_then(|predicate_expr| {
            match PagePruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                Err(e) => {
                    debug!(
                        "Could not create page pruning predicate for '{:?}': {}",
                        pruning_predicate, e
                    );
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();
        let cache = ParquetExec::compute_properties(
            projected_schema,
            &projected_output_ordering,
            &base_config,
        );
        ParquetExec {
            base_config,
            projected_statistics,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
            metadata_size_hint,
            parquet_file_reader_factory,
            cache,
            table_parquet_options,
            schema_adapter_factory,
        }
    }
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    #[deprecated(
        since = "39.0.0",
        note = "use `ParquetExec::builder` or `ParquetExecBuilder`"
    )]
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        let mut builder =
            ParquetExecBuilder::new_with_options(base_config, table_parquet_options);
        if let Some(predicate) = predicate {
            builder = builder.with_predicate(predicate);
        }
        if let Some(metadata_size_hint) = metadata_size_hint {
            builder = builder.with_metadata_size_hint(metadata_size_hint);
        }
        builder.build()
    }

    /// Return a [`ParquetExecBuilder`].
    ///
    /// See example on [`ParquetExec`] and [`ParquetExecBuilder`] for specifying
    /// parquet table options.
    pub fn builder(file_scan_config: FileScanConfig) -> ParquetExecBuilder {
        ParquetExecBuilder::new(file_scan_config)
    }

    /// [`FileScanConfig`] that controls this scan (such as which files to read)
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Options passed to the parquet reader for this scan
    pub fn table_parquet_options(&self) -> &TableParquetOptions {
        &self.table_parquet_options
    }

    /// Optional predicate.
    pub fn predicate(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.predicate.as_ref()
    }

    /// Optional reference to this parquet scan's pruning predicate
    pub fn pruning_predicate(&self) -> Option<&Arc<PruningPredicate>> {
        self.pruning_predicate.as_ref()
    }

    /// Optional user defined parquet file reader factory.
    ///
    /// See documentation on [`ParquetExecBuilder::with_parquet_file_reader_factory`]
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// Optional schema adapter factory.
    ///
    /// See documentation on [`ParquetExecBuilder::with_schema_adapter_factory`]
    pub fn with_schema_adapter_factory(
        mut self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        self.schema_adapter_factory = Some(schema_adapter_factory);
        self
    }

    /// If true, any filter [`Expr`]s on the scan will converted to a
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) in the
    /// `ParquetRecordBatchStream`. These filters are applied by the
    /// parquet decoder to skip unecessairly decoding other columns
    /// which would not pass the predicate. Defaults to false
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_pushdown_filters(mut self, pushdown_filters: bool) -> Self {
        self.table_parquet_options.global.pushdown_filters = pushdown_filters;
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    fn pushdown_filters(&self) -> bool {
        self.table_parquet_options.global.pushdown_filters
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        self.table_parquet_options.global.reorder_filters = reorder_filters;
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    fn reorder_filters(&self) -> bool {
        self.table_parquet_options.global.reorder_filters
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimise filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(mut self, enable_page_index: bool) -> Self {
        self.table_parquet_options.global.enable_page_index = enable_page_index;
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    fn enable_page_index(&self) -> bool {
        self.table_parquet_options.global.enable_page_index
    }

    /// If enabled, the reader will read by the bloom filter
    pub fn with_bloom_filter_on_read(mut self, bloom_filter_on_read: bool) -> Self {
        self.table_parquet_options.global.bloom_filter_on_read = bloom_filter_on_read;
        self
    }

    /// If enabled, the writer will write by the bloom filter
    pub fn with_bloom_filter_on_write(
        mut self,
        enable_bloom_filter_on_write: bool,
    ) -> Self {
        self.table_parquet_options.global.bloom_filter_on_write =
            enable_bloom_filter_on_write;
        self
    }

    /// Return the value described in [`Self::with_bloom_filter_on_read`]
    fn bloom_filter_on_read(&self) -> bool {
        self.table_parquet_options.global.bloom_filter_on_read
    }

    fn output_partitioning_helper(file_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        file_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_config), // Output Partitioning
            ExecutionMode::Bounded,                        // Execution Mode
        )
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups;
        // Changing file groups may invalidate output partitioning. Update it also
        let output_partitioning = Self::output_partitioning_helper(&self.base_config);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
    }
}

impl DisplayAs for ParquetExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let predicate_string = self
                    .predicate
                    .as_ref()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                let pruning_predicate_string = self
                    .pruning_predicate
                    .as_ref()
                    .map(|pre| {
                        format!(
                            ", pruning_predicate={}, required_guarantees=[{}]",
                            pre.predicate_expr(),
                            pre.literal_guarantees()
                                .iter()
                                .map(|item| format!("{}", item))
                                .collect_vec()
                                .join(", ")
                        )
                    })
                    .unwrap_or_default();

                write!(f, "ParquetExec: ")?;
                self.base_config.fmt_as(t, f)?;
                write!(f, "{}{}", predicate_string, pruning_predicate_string,)
            }
        }
    }
}

impl ExecutionPlan for ParquetExec {
    fn name(&self) -> &'static str {
        "ParquetExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on [`FileGroupPartitioner`] for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(
                self.properties().output_ordering().is_some(),
            )
            .repartition_file_groups(&self.base_config.file_groups);

        let mut new_plan = self.clone();
        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            new_plan = new_plan.with_file_groups(repartitioned_file_groups);
        }
        Ok(Some(Arc::new(new_plan)))
    }

    fn execute(
        &self,
        partition_index: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let parquet_file_reader_factory = self
            .parquet_file_reader_factory
            .as_ref()
            .map(|f| Ok(Arc::clone(f)))
            .unwrap_or_else(|| {
                ctx.runtime_env()
                    .object_store(&self.base_config.object_store_url)
                    .map(|store| {
                        Arc::new(DefaultParquetFileReaderFactory::new(store))
                            as Arc<dyn ParquetFileReaderFactory>
                    })
            })?;

        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory::default()));

        let opener = ParquetOpener {
            partition_index,
            projection: Arc::from(projection),
            batch_size: ctx.session_config().batch_size(),
            limit: self.base_config.limit,
            predicate: self.predicate.clone(),
            pruning_predicate: self.pruning_predicate.clone(),
            page_pruning_predicate: self.page_pruning_predicate.clone(),
            table_schema: self.base_config.file_schema.clone(),
            metadata_size_hint: self.metadata_size_hint,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory,
            pushdown_filters: self.pushdown_filters(),
            reorder_filters: self.reorder_filters(),
            enable_page_index: self.enable_page_index(),
            enable_bloom_filter: self.bloom_filter_on_read(),
            schema_adapter_factory,
        };

        let stream =
            FileStream::new(&self.base_config, partition_index, opener, &self.metrics)?;

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

fn should_enable_page_index(
    enable_page_index: bool,
    page_pruning_predicate: &Option<Arc<PagePruningPredicate>>,
) -> bool {
    enable_page_index
        && page_pruning_predicate.is_some()
        && page_pruning_predicate
            .as_ref()
            .map(|p| p.filter_number() > 0)
            .unwrap_or(false)
}

// Convert parquet column schema to arrow data type, and just consider the
// decimal data type.
pub(crate) fn parquet_to_arrow_decimal_type(
    parquet_column: &ColumnDescriptor,
) -> Option<DataType> {
    let type_ptr = parquet_column.self_type_ptr();
    match type_ptr.get_basic_info().logical_type() {
        Some(LogicalType::Decimal { scale, precision }) => {
            Some(DataType::Decimal128(precision as u8, scale as i8))
        }
        _ => match type_ptr.get_basic_info().converted_type() {
            ConvertedType::DECIMAL => Some(DataType::Decimal128(
                type_ptr.get_precision() as u8,
                type_ptr.get_scale() as i8,
            )),
            _ => None,
        },
    }
}
