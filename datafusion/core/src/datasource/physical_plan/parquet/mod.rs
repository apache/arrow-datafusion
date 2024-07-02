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

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::file_stream::FileStream;
use crate::datasource::physical_plan::{
    parquet::page_filter::PagePruningPredicate, DisplayAs, FileGroupPartitioner,
    FileScanConfig,
};
use crate::{
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

use crate::datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapterFactory,
};
pub use access_plan::{ParquetAccessPlan, RowGroupAccess};
pub use metrics::ParquetFileMetrics;
use opener::ParquetOpener;
pub use reader::{DefaultParquetFileReaderFactory, ParquetFileReaderFactory};
pub use statistics::StatisticsConverter;
pub use writer::plan_to_parquet;

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
                match PruningPredicate::try_new(predicate_expr, Arc::clone(file_schema)) {
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
            match PagePruningPredicate::try_new(predicate_expr, Arc::clone(file_schema)) {
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
            table_schema: Arc::clone(&self.base_config.file_schema),
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

#[cfg(test)]
mod tests {
    // See also `parquet_exec` integration test
    use std::fs::{self, File};
    use std::io::Write;

    use super::*;
    use crate::dataframe::DataFrameWriteOptions;
    use crate::datasource::file_format::options::CsvReadOptions;
    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::datasource::file_format::test_util::scan_format;
    use crate::datasource::listing::{FileRange, ListingOptions};
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::execution::context::SessionState;
    use crate::physical_plan::displayable;
    use crate::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;
    use crate::{
        assert_batches_sorted_eq,
        datasource::file_format::{parquet::ParquetFormat, FileFormat},
        physical_plan::collect,
    };

    use arrow::array::{
        ArrayRef, Date64Array, Int32Array, Int64Array, Int8Array, StringArray,
        StructArray,
    };
    use arrow::datatypes::{Field, Schema, SchemaBuilder};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Fields;
    use datafusion_common::{assert_contains, ScalarValue};
    use datafusion_expr::{col, lit, when, Expr};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::ExecutionPlanProperties;

    use chrono::{TimeZone, Utc};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::ObjectMeta;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use url::Url;

    struct RoundTripResult {
        /// Data that was read back from ParquetFiles
        batches: Result<Vec<RecordBatch>>,
        /// The physical plan that was created (that has statistics, etc)
        parquet_exec: Arc<ParquetExec>,
    }

    /// round-trip record batches by writing each individual RecordBatch to
    /// a parquet file and then reading that parquet file with the specified
    /// options.
    #[derive(Debug, Default)]
    struct RoundTrip {
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
        predicate: Option<Expr>,
        pushdown_predicate: bool,
        page_index_predicate: bool,
    }

    impl RoundTrip {
        fn new() -> Self {
            Default::default()
        }

        fn with_projection(mut self, projection: Vec<usize>) -> Self {
            self.projection = Some(projection);
            self
        }

        fn with_schema(mut self, schema: SchemaRef) -> Self {
            self.schema = Some(schema);
            self
        }

        fn with_predicate(mut self, predicate: Expr) -> Self {
            self.predicate = Some(predicate);
            self
        }

        fn with_pushdown_predicate(mut self) -> Self {
            self.pushdown_predicate = true;
            self
        }

        fn with_page_index_predicate(mut self) -> Self {
            self.page_index_predicate = true;
            self
        }

        /// run the test, returning only the resulting RecordBatches
        async fn round_trip_to_batches(
            self,
            batches: Vec<RecordBatch>,
        ) -> Result<Vec<RecordBatch>> {
            self.round_trip(batches).await.batches
        }

        /// run the test, returning the `RoundTripResult`
        async fn round_trip(self, batches: Vec<RecordBatch>) -> RoundTripResult {
            let Self {
                projection,
                schema,
                predicate,
                pushdown_predicate,
                page_index_predicate,
            } = self;

            let file_schema = match schema {
                Some(schema) => schema,
                None => Arc::new(
                    Schema::try_merge(
                        batches.iter().map(|b| b.schema().as_ref().clone()),
                    )
                    .unwrap(),
                ),
            };
            // If testing with page_index_predicate, write parquet
            // files with multiple pages
            let multi_page = page_index_predicate;
            let (meta, _files) = store_parquet(batches, multi_page).await.unwrap();
            let file_group = meta.into_iter().map(Into::into).collect();

            // set up predicate (this is normally done by a layer higher up)
            let predicate = predicate.map(|p| logical2physical(&p, &file_schema));

            // prepare the scan
            let mut builder = ParquetExec::builder(
                FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
                    .with_file_group(file_group)
                    .with_projection(projection),
            );

            if let Some(predicate) = predicate {
                builder = builder.with_predicate(predicate);
            }
            let mut parquet_exec = builder.build();

            if pushdown_predicate {
                parquet_exec = parquet_exec
                    .with_pushdown_filters(true)
                    .with_reorder_filters(true);
            }

            if page_index_predicate {
                parquet_exec = parquet_exec.with_enable_page_index(true);
            }

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();
            let parquet_exec = Arc::new(parquet_exec);
            RoundTripResult {
                batches: collect(parquet_exec.clone(), task_ctx).await,
                parquet_exec,
            }
        }
    }

    // Add a new column with the specified field name to the RecordBatch
    fn add_to_batch(
        batch: &RecordBatch,
        field_name: &str,
        array: ArrayRef,
    ) -> RecordBatch {
        let mut fields = SchemaBuilder::from(batch.schema().fields());
        fields.push(Field::new(field_name, array.data_type().clone(), true));
        let schema = Arc::new(fields.finish());

        let mut columns = batch.columns().to_vec();
        columns.push(array);
        RecordBatch::try_new(schema, columns).expect("error; creating record batch")
    }

    fn create_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        columns.into_iter().fold(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            |batch, (field_name, arr)| add_to_batch(&batch, field_name, arr.clone()),
        )
    }

    #[tokio::test]
    async fn write_parquet_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        // register a local file system object store for /tmp directory
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);

        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;
        let out_dir_url = "file://local/out";
        let e = df
            .write_parquet(out_dir_url, DataFrameWriteOptions::new(), None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value d for column 0 at line 4");
        Ok(())
    }

    #[tokio::test]
    async fn evolved_schema() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        // batch1: c1(string)
        let batch1 =
            add_to_batch(&RecordBatch::new_empty(Arc::new(Schema::empty())), "c1", c1);

        // batch2: c1(string) and c2(int64)
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch2 = add_to_batch(&batch1, "c2", c2);

        // batch3: c1(string) and c3(int8)
        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));
        let batch3 = add_to_batch(&batch1, "c3", c3);

        // read/write them files:
        let read = RoundTrip::new()
            .round_trip_to_batches(vec![batch1, batch2, batch3])
            .await
            .unwrap();
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "|     |    |    |",
            "|     |    | 20 |",
            "|     | 2  |    |",
            "| Foo |    |    |",
            "| Foo |    | 10 |",
            "| Foo | 1  |    |",
            "| bar |    |    |",
            "| bar |    |    |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_inconsistent_order() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        // read/write them files:
        let read = RoundTrip::new()
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();
        let expected = [
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        // read/write them files:
        let read = RoundTrip::new()
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();
        let expected = [
            "+-----+----+----+",
            "| c1  | c3 | c2 |",
            "+-----+----+----+",
            "| Foo | 10 |    |",
            "|     | 20 |    |",
            "| bar |    |    |",
            "|     | 10 | 1  |",
            "|     | 20 | 2  |",
            "|     |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        let filter = col("c2").eq(lit(2_i64));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();
        let expected = [
            "+-----+----+----+",
            "| c1  | c3 | c2 |",
            "+-----+----+----+",
            "|     |    |    |",
            "|     | 10 | 1  |",
            "|     | 20 |    |",
            "|     | 20 | 2  |",
            "| Foo | 10 |    |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection_filter_with_filter_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        let filter = col("c2").eq(lit(2_i64)).or(col("c2").eq(lit(1_i64)));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1, batch2])
            .await;

        let expected = [
            "+----+----+----+",
            "| c1 | c3 | c2 |",
            "+----+----+----+",
            "|    | 10 | 1  |",
            "|    | 20 | 2  |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        let metrics = rt.parquet_exec.metrics().unwrap();
        // Note there are were 6 rows in total (across three batches)
        assert_eq!(get_value(&metrics, "pushdown_rows_filtered"), 4);
    }

    #[tokio::test]
    async fn evolved_schema_projection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef =
            Arc::new(StringArray::from(vec![Some("baz"), Some("boo"), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1), ("c4", c4)]);

        // read/write them files:
        let read = RoundTrip::new()
            .with_projection(vec![0, 3])
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();
        let expected = [
            "+-----+-----+",
            "| c1  | c4  |",
            "+-----+-----+",
            "| Foo | baz |",
            "|     | boo |",
            "| bar |     |",
            "| Foo |     |",
            "|     |     |",
            "| bar |     |",
            "+-----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        let filter = col("c3").eq(lit(0_i8));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        // Predicate should prune all row groups
        assert_eq!(read.len(), 0);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        // This does not look correct since the "c2" values in the result do not in fact match the predicate `c2 == 0`
        // but parquet pruning is not exact. If the min/max values are not defined (which they are not in this case since the it is
        // a null array, then the pruning predicate (currently) can not be applied.
        // In a real query where this predicate was pushed down from a filter stage instead of created directly in the `ParquetExec`,
        // the filter stage would be preserved as a separate execution plan stage so the actual query results would be as expected.
        let expected = [
            "+-----+----+",
            "| c1  | c2 |",
            "+-----+----+",
            "|     |    |",
            "|     |    |",
            "|     | 1  |",
            "|     | 2  |",
            "| Foo |    |",
            "| bar |    |",
            "+-----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_with_filter_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1, batch2])
            .await;

        let expected = [
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "|    | 1  |",
            "+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        let metrics = rt.parquet_exec.metrics().unwrap();
        // Note there are were 6 rows in total (across three batches)
        assert_eq!(get_value(&metrics, "pushdown_rows_filtered"), 5);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_with_page_index_pushdown() {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            // Page 1
            Some("Foo"),
            Some("Bar"),
            // Page 2
            Some("Foo2"),
            Some("Bar2"),
            // Page 3
            Some("Foo3"),
            Some("Bar3"),
        ]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![
            // Page 1:
            Some(1),
            Some(2),
            // Page 2: (pruned)
            Some(3),
            Some(4),
            // Page 3: (pruned)
            Some(5),
            None,
        ]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2.clone())]);

        // batch3 (has c2, c1) -- both columns, should still prune
        let batch3 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // batch4 (has c2, c1) -- different column order, should still prune
        let batch4 = create_batch(vec![("c2", c2), ("c1", c1)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_page_index_predicate()
            .round_trip(vec![batch1, batch2, batch3, batch4])
            .await;

        let expected = vec![
            "+------+----+",
            "| c1   | c2 |",
            "+------+----+",
            "|      | 1  |",
            "|      | 2  |",
            "| Bar  |    |",
            "| Bar  | 2  |",
            "| Bar  | 2  |",
            "| Bar2 |    |",
            "| Bar3 |    |",
            "| Foo  |    |",
            "| Foo  | 1  |",
            "| Foo  | 1  |",
            "| Foo2 |    |",
            "| Foo3 |    |",
            "+------+----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        let metrics = rt.parquet_exec.metrics().unwrap();

        // There are 4 rows pruned in each of batch2, batch3, and
        // batch4 for a total of 12. batch1 had no pruning as c2 was
        // filled in as null
        assert_eq!(get_value(&metrics, "page_index_rows_filtered"), 12);
    }

    #[tokio::test]
    async fn multi_column_predicate_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // Columns in different order to schema
        let filter = col("c2").eq(lit(1_i64)).or(col("c1").eq(lit("bar")));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip_to_batches(vec![batch1])
            .await
            .unwrap();

        let expected = [
            "+-----+----+",
            "| c1  | c2 |",
            "+-----+----+",
            "| Foo | 1  |",
            "| bar |    |",
            "+-----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn multi_column_predicate_pushdown_page_index_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // Columns in different order to schema
        let filter = col("c2").eq(lit(1_i64)).or(col("c1").eq(lit("bar")));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .with_page_index_predicate()
            .round_trip_to_batches(vec![batch1])
            .await
            .unwrap();

        let expected = [
            "+-----+----+",
            "| c1  | c2 |",
            "+-----+----+",
            "|     | 2  |",
            "| Foo | 1  |",
            "| bar |    |",
            "+-----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_incompatible_types() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef = Arc::new(Date64Array::from(vec![
            Some(86400000),
            None,
            Some(259200000),
        ]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c4), ("c2", c2), ("c1", c1)]);

        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int8, true),
        ]);

        // read/write them files:
        let read = RoundTrip::new()
            .with_schema(Arc::new(schema))
            .round_trip_to_batches(vec![batch1, batch2])
            .await;
        assert_contains!(read.unwrap_err().to_string(),
            "Cannot cast file schema field c3 of type Date64 to table schema field of type Int8");
    }

    #[tokio::test]
    async fn parquet_exec_with_projection() -> Result<()> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = "alltypes_plain.parquet";
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let parquet_exec = scan_format(
            &state,
            &ParquetFormat::default(),
            &testdata,
            filename,
            Some(vec![0, 1, 2]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_range() -> Result<()> {
        fn file_range(meta: &ObjectMeta, start: i64, end: i64) -> PartitionedFile {
            PartitionedFile {
                object_meta: meta.clone(),
                partition_values: vec![],
                range: Some(FileRange { start, end }),
                statistics: None,
                extensions: None,
            }
        }

        async fn assert_parquet_read(
            state: &SessionState,
            file_groups: Vec<Vec<PartitionedFile>>,
            expected_row_num: Option<usize>,
            file_schema: SchemaRef,
        ) -> Result<()> {
            let parquet_exec = ParquetExec::builder(
                FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
                    .with_file_groups(file_groups),
            )
            .build();
            assert_eq!(
                parquet_exec
                    .properties()
                    .output_partitioning()
                    .partition_count(),
                1
            );
            let results = parquet_exec.execute(0, state.task_ctx())?.next().await;

            if let Some(expected_row_num) = expected_row_num {
                let batch = results.unwrap()?;
                assert_eq!(expected_row_num, batch.num_rows());
            } else {
                assert!(results.is_none());
            }

            Ok(())
        }

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{testdata}/alltypes_plain.parquet");

        let meta = local_unpartitioned_file(filename);

        let store = Arc::new(LocalFileSystem::new()) as _;
        let file_schema = ParquetFormat::default()
            .infer_schema(&state, &store, &[meta.clone()])
            .await?;

        let group_empty = vec![vec![file_range(&meta, 0, 2)]];
        let group_contain = vec![vec![file_range(&meta, 2, i64::MAX)]];
        let group_all = vec![vec![
            file_range(&meta, 0, 2),
            file_range(&meta, 2, i64::MAX),
        ]];

        assert_parquet_read(&state, group_empty, None, file_schema.clone()).await?;
        assert_parquet_read(&state, group_contain, Some(8), file_schema.clone()).await?;
        assert_parquet_read(&state, group_all, Some(8), file_schema).await?;

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&object_store_url).unwrap();

        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{testdata}/alltypes_plain.parquet");

        let meta = local_unpartitioned_file(filename);

        let schema = ParquetFormat::default()
            .infer_schema(&state, &store, &[meta.clone()])
            .await
            .unwrap();

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![
                ScalarValue::from("2021"),
                ScalarValue::UInt8(Some(10)),
                ScalarValue::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(ScalarValue::from("26")),
                ),
            ],
            range: None,
            statistics: None,
            extensions: None,
        };

        let expected_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("tinyint_col", DataType::Int32, true),
            Field::new("month", DataType::UInt8, false),
            Field::new(
                "day",
                DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                ),
                false,
            ),
        ]);

        let parquet_exec = ParquetExec::builder(
            FileScanConfig::new(object_store_url, schema.clone())
                .with_file(partitioned_file)
                // file has 10 cols so index 12 should be month and 13 should be day
                .with_projection(Some(vec![0, 1, 2, 12, 13]))
                .with_table_partition_cols(vec![
                    Field::new("year", DataType::Utf8, false),
                    Field::new("month", DataType::UInt8, false),
                    Field::new(
                        "day",
                        DataType::Dictionary(
                            Box::new(DataType::UInt16),
                            Box::new(DataType::Utf8),
                        ),
                        false,
                    ),
                ]),
        )
        .build();
        assert_eq!(
            parquet_exec.cache.output_partitioning().partition_count(),
            1
        );
        assert_eq!(parquet_exec.schema().as_ref(), &expected_schema);

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap()?;
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        let expected = [
            "+----+----------+-------------+-------+-----+",
            "| id | bool_col | tinyint_col | month | day |",
            "+----+----------+-------------+-------+-----+",
            "| 4  | true     | 0           | 10    | 26  |",
            "| 5  | false    | 1           | 10    | 26  |",
            "| 6  | true     | 0           | 10    | 26  |",
            "| 7  | false    | 1           | 10    | 26  |",
            "| 2  | true     | 0           | 10    | 26  |",
            "| 3  | false    | 1           | 10    | 26  |",
            "| 0  | true     | 0           | 10    | 26  |",
            "| 1  | false    | 1           | 10    | 26  |",
            "+----+----------+-------------+-------+-----+",
        ];
        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_error() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let location = Path::from_filesystem_path(".")
            .unwrap()
            .child("invalid.parquet");

        let partitioned_file = PartitionedFile {
            object_meta: ObjectMeta {
                location,
                last_modified: Utc.timestamp_nanos(0),
                size: 1337,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
        };

        let file_schema = Arc::new(Schema::empty());
        let parquet_exec = ParquetExec::builder(
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
                .with_file(partitioned_file),
        )
        .build();

        let mut results = parquet_exec.execute(0, state.task_ctx())?;
        let batch = results.next().await.unwrap();
        // invalid file should produce an error to that effect
        assert_contains!(batch.unwrap_err().to_string(), "invalid.parquet not found");
        assert!(results.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_page_index_exec_metrics() {
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));
        let batch1 = create_batch(vec![("int", c1.clone())]);

        let filter = col("int").eq(lit(4_i32));

        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_page_index_predicate()
            .round_trip(vec![batch1])
            .await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // assert the batches and some metrics
        #[rustfmt::skip]
        let expected = [
            "+-----+",
            "| int |",
            "+-----+",
            "| 4   |",
            "| 5   |",
            "+-----+"
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        assert_eq!(get_value(&metrics, "page_index_rows_filtered"), 4);
        assert!(
            get_value(&metrics, "page_index_eval_time") > 0,
            "no eval time in metrics: {metrics:#?}"
        );
    }

    /// Returns a string array with contents:
    /// "[Foo, null, bar, bar, bar, bar, zzz]"
    fn string_batch() -> RecordBatch {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Foo"),
            None,
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("zzz"),
        ]));

        // batch1: c1(string)
        create_batch(vec![("c1", c1.clone())])
    }

    #[tokio::test]
    async fn parquet_exec_metrics() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // c1 != 'bar'
        let filter = col("c1").not_eq(lit("bar"));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // assert the batches and some metrics
        let expected = [
            "+-----+", "| c1  |", "+-----+", "| Foo |", "| zzz |", "+-----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());

        // pushdown predicates have eliminated all 4 bar rows and the
        // null row for 5 rows total
        assert_eq!(get_value(&metrics, "pushdown_rows_filtered"), 5);
        assert!(
            get_value(&metrics, "pushdown_eval_time") > 0,
            "no eval time in metrics: {metrics:#?}"
        );
    }

    #[tokio::test]
    async fn parquet_exec_display() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // c1 != 'bar'
        let filter = col("c1").not_eq(lit("bar"));

        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        // should have a pruning predicate
        let pruning_predicate = &rt.parquet_exec.pruning_predicate;
        assert!(pruning_predicate.is_some());

        // convert to explain plan form
        let display = displayable(rt.parquet_exec.as_ref())
            .indent(true)
            .to_string();

        assert_contains!(
            &display,
            "pruning_predicate=CASE WHEN c1_null_count@2 = c1_row_count@3 THEN false ELSE c1_min@0 != bar OR bar != c1_max@1 END"
        );

        assert_contains!(&display, r#"predicate=c1@0 != bar"#);

        assert_contains!(&display, "projection=[c1]");
    }

    #[tokio::test]
    async fn parquet_exec_has_no_pruning_predicate_if_can_not_prune() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // filter is too complicated for pruning (PruningPredicate code does not
        // handle case expressions), so the pruning predicate will always be
        // "true"

        // WHEN c1 != bar THEN true ELSE false END
        let filter = when(col("c1").not_eq(lit("bar")), lit(true))
            .otherwise(lit(false))
            .unwrap();

        let rt = RoundTrip::new()
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        // Should not contain a pruning predicate (since nothing can be pruned)
        let pruning_predicate = &rt.parquet_exec.pruning_predicate;
        assert!(
            pruning_predicate.is_none(),
            "Still had pruning predicate: {pruning_predicate:?}"
        );

        // but does still has a pushdown down predicate
        let predicate = rt.parquet_exec.predicate.as_ref();
        let filter_phys = logical2physical(&filter, rt.parquet_exec.schema().as_ref());
        assert_eq!(predicate.unwrap().to_string(), filter_phys.to_string());
    }

    #[tokio::test]
    async fn parquet_exec_has_pruning_predicate_for_guarantees() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // part of the filter is too complicated for pruning (PruningPredicate code does not
        // handle case expressions), but part (c1 = 'foo') can be used for bloom filtering, so
        // should still have the pruning predicate.

        // c1 = 'foo' AND (WHEN c1 != bar THEN true ELSE false END)
        let filter = col("c1").eq(lit("foo")).and(
            when(col("c1").not_eq(lit("bar")), lit(true))
                .otherwise(lit(false))
                .unwrap(),
        );

        let rt = RoundTrip::new()
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        // Should have a pruning predicate
        let pruning_predicate = &rt.parquet_exec.pruning_predicate;
        assert!(pruning_predicate.is_some());
    }

    /// returns the sum of all the metrics with the specified name
    /// the returned set.
    ///
    /// Count: returns value
    /// Time: returns elapsed nanoseconds
    ///
    /// Panics if no such metric.
    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }

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

    #[tokio::test]
    async fn write_table_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        // let mut ctx = create_ctx(&tmp_dir, 4).await?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(8),
        );
        let schema = populate_csv_partitions(&tmp_dir, 4, ".csv")?;
        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // register a local file system object store for /tmp directory
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);

        // Configure listing options
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(ParquetFormat::default().get_ext());

        // execute a simple query and write the results to parquet
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        std::fs::create_dir(&out_dir).unwrap();
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        let schema: Schema = df.schema().into();
        // Register a listing table - this will use all files in the directory as data sources
        // for the query
        ctx.register_listing_table(
            "my_table",
            &out_dir,
            listing_options,
            Some(Arc::new(schema)),
            None,
        )
        .await
        .unwrap();
        df.write_table("my_table", DataFrameWriteOptions::new())
            .await?;

        // create a new context and verify that the results were saved to a partitioned parquet file
        let ctx = SessionContext::new();

        // get write_id
        let mut paths = fs::read_dir(&out_dir).unwrap();
        let path = paths.next();
        let name = path
            .unwrap()?
            .path()
            .file_name()
            .expect("Should be a file name")
            .to_str()
            .expect("Should be a str")
            .to_owned();
        let (parsed_id, _) = name.split_once('_').expect("File should contain _ !");
        let write_id = parsed_id.to_owned();

        // register each partition as well as the top level dir
        ctx.register_parquet(
            "part0",
            &format!("{out_dir}/{write_id}_0.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_parquet("allparts", &out_dir, ParquetReadOptions::default())
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 40);

        Ok(())
    }

    #[tokio::test]
    async fn test_struct_filter_parquet() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let path = tmp_dir.path().to_str().unwrap().to_string() + "/test.parquet";
        write_file(&path);
        let ctx = SessionContext::new();
        let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));
        ctx.register_listing_table("base_table", path, opt, None, None)
            .await
            .unwrap();
        let sql = "select * from base_table where name='test02'";
        let batch = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batch.len(), 1);
        let expected = [
            "+---------------------+----+--------+",
            "| struct              | id | name   |",
            "+---------------------+----+--------+",
            "| {id: 4, name: aaa2} | 2  | test02 |",
            "+---------------------+----+--------+",
        ];
        crate::assert_batches_eq!(expected, &batch);
        Ok(())
    }

    fn write_file(file: &String) {
        let struct_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![
            Field::new("struct", DataType::Struct(struct_fields.clone()), false),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let id_array = Int64Array::from(vec![Some(1), Some(2)]);
        let columns = vec![
            Arc::new(Int64Array::from(vec![3, 4])) as _,
            Arc::new(StringArray::from(vec!["aaa1", "aaa2"])) as _,
        ];
        let struct_array = StructArray::new(struct_fields, columns, None);

        let name_array = StringArray::from(vec![Some("test01"), Some("test02")]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(struct_array),
                Arc::new(id_array),
                Arc::new(name_array),
            ],
        )
        .unwrap();
        let file = File::create(file).unwrap();
        let w_opt = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(w_opt)).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();
    }
}
