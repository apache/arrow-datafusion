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

//! Execution plan for reading Parquet files

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_physical_expr::PhysicalExpr;
use fmt::Debug;
use std::any::Any;
use std::cmp::min;
use std::fmt;
use std::fs;
use std::ops::Range;
use std::sync::Arc;
use uuid::Uuid;

use crate::config::ConfigOptions;
use crate::datasource::file_format::parquet::fetch_parquet_metadata;
use crate::physical_plan::file_format::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::physical_plan::file_format::FileMeta;
use crate::{
    datasource::listing::FileRange,
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        expressions::PhysicalSortExpr,
        file_format::{FileScanConfig, SchemaAdapter},
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use arrow::error::ArrowError;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use log::debug;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::errors::ParquetError;
use parquet::file::{metadata::ParquetMetaData, properties::WriterProperties};
use parquet::schema::types::ColumnDescriptor;

mod metrics;
mod page_filter;
mod row_filter;
mod row_groups;

use crate::physical_plan::common::AbortOnDropSingle;
use crate::physical_plan::file_format::parquet::page_filter::PagePruningPredicate;
pub use metrics::ParquetFileMetrics;

use super::get_output_ordering;
use super::FileWriterSaveMode;

#[derive(Default)]
struct RepartitionState {
    current_partition_index: usize,
    current_partition_size: usize,
}

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Override for `Self::with_pushdown_filters`. If None, uses
    /// values from base_config
    pushdown_filters: Option<bool>,
    /// Override for `Self::with_reorder_filters`. If None, uses
    /// values from base_config
    reorder_filters: Option<bool>,
    /// Override for `Self::with_enable_page_index`. If None, uses
    /// values from base_config
    enable_page_index: Option<bool>,
    /// Base configuraton for this scan
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional predicate for pruning pages
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    /// Optional hint for the size of the parquet metadata
    metadata_size_hint: Option<usize>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
    ) -> Self {
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
            .filter(|p| !p.allways_true());

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

        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            pushdown_filters: None,
            reorder_filters: None,
            enable_page_index: None,
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
            metadata_size_hint,
            parquet_file_reader_factory: None,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
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
    /// `ParquetFileReaderFactory` complements `TableProvider`, It enables users to provide custom
    /// implementation for data access operations.
    ///
    /// If custom `ParquetFileReaderFactory` is provided, then data access operations will be routed
    /// to this factory instead of `ObjectStore`.
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
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
        self.pushdown_filters = Some(pushdown_filters);
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    fn pushdown_filters(&self, config_options: &ConfigOptions) -> bool {
        self.pushdown_filters
            .unwrap_or(config_options.execution.parquet.pushdown_filters)
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        self.reorder_filters = Some(reorder_filters);
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    fn reorder_filters(&self, config_options: &ConfigOptions) -> bool {
        self.reorder_filters
            .unwrap_or(config_options.execution.parquet.reorder_filters)
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimise filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(mut self, enable_page_index: bool) -> Self {
        self.enable_page_index = Some(enable_page_index);
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    fn enable_page_index(&self, config_options: &ConfigOptions) -> bool {
        self.enable_page_index
            .unwrap_or(config_options.execution.parquet.enable_page_index)
    }

    /// Redistribute files across partitions according to their size
    pub fn get_repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
    ) -> Self {
        let flattened_files = self
            .base_config()
            .file_groups
            .iter()
            .flatten()
            .collect::<Vec<_>>();

        // Perform redistribution only in case all files should be read from beginning to end
        let has_ranges = flattened_files.iter().any(|f| f.range.is_some());
        if has_ranges {
            return self.clone();
        }

        let total_size = flattened_files
            .iter()
            .map(|f| f.object_meta.size as i64)
            .sum::<i64>();
        if total_size < (repartition_file_min_size as i64) {
            return self.clone();
        }

        let target_partition_size =
            (total_size as usize + (target_partitions) - 1) / (target_partitions);

        let repartitioned_files = flattened_files
            .into_iter()
            .scan(RepartitionState::default(), |state, source_file| {
                let mut produced_files = vec![];
                let mut range_start = 0;
                while range_start < source_file.object_meta.size {
                    let range_end = min(
                        range_start
                            + (target_partition_size - state.current_partition_size),
                        source_file.object_meta.size,
                    );

                    let mut produced_file = source_file.clone();
                    produced_file.range = Some(FileRange {
                        start: range_start as i64,
                        end: range_end as i64,
                    });
                    produced_files.push((state.current_partition_index, produced_file));

                    if state.current_partition_size + (range_end - range_start)
                        >= target_partition_size
                    {
                        state.current_partition_index += 1;
                        state.current_partition_size = 0;
                    } else {
                        state.current_partition_size += range_end - range_start;
                    }
                    range_start = range_end;
                }
                Some(produced_files)
            })
            .flatten()
            .group_by(|(partition_idx, _)| *partition_idx)
            .into_iter()
            .map(|(_, group)| group.map(|(_, vals)| vals).collect_vec())
            .collect_vec();

        let mut new_parquet_exec = self.clone();
        new_parquet_exec.base_config.file_groups = repartitioned_files;
        new_parquet_exec
    }
}

impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        get_output_ordering(&self.base_config)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
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

        let config_options = ctx.session_config().options();

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
            pushdown_filters: self.pushdown_filters(config_options),
            reorder_filters: self.reorder_filters(config_options),
            enable_page_index: self.enable_page_index(config_options),
        };

        let stream =
            FileStream::new(&self.base_config, partition_index, opener, &self.metrics)?;

        Ok(Box::pin(stream))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let predicate_string = self
                    .predicate
                    .as_ref()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                let pruning_predicate_string = self
                    .pruning_predicate
                    .as_ref()
                    .map(|pre| format!(", pruning_predicate={}", pre.predicate_expr()))
                    .unwrap_or_default();

                let output_ordering_string = self
                    .output_ordering()
                    .map(make_output_ordering_string)
                    .unwrap_or_default();

                write!(
                    f,
                    "ParquetExec: limit={:?}, partitions={}{}{}{}, projection={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                    predicate_string,
                    pruning_predicate_string,
                    output_ordering_string,
                    super::ProjectSchemaDisplay(&self.projected_schema),
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

fn make_output_ordering_string(ordering: &[PhysicalSortExpr]) -> String {
    use std::fmt::Write;
    let mut w: String = ", output_ordering=[".into();

    for (i, e) in ordering.iter().enumerate() {
        if i > 0 {
            write!(&mut w, ", ").unwrap()
        }
        write!(&mut w, "{e}").unwrap()
    }
    write!(&mut w, "]").unwrap();
    w
}

/// Implements [`FileOpener`] for a parquet file
struct ParquetOpener {
    partition_index: usize,
    projection: Arc<[usize]>,
    batch_size: usize,
    limit: Option<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    table_schema: SchemaRef,
    metadata_size_hint: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    pushdown_filters: bool,
    reorder_filters: bool,
    enable_page_index: bool,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();

        let file_metrics = ParquetFileMetrics::new(
            self.partition_index,
            file_meta.location().as_ref(),
            &self.metrics,
        );

        let reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                file_meta,
                self.metadata_size_hint,
                &self.metrics,
            )?;

        let schema_adapter = SchemaAdapter::new(self.table_schema.clone());
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let predicate = self.predicate.clone();
        let pruning_predicate = self.pruning_predicate.clone();
        let page_pruning_predicate = self.page_pruning_predicate.clone();
        let table_schema = self.table_schema.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let enable_page_index = self.enable_page_index;
        let limit = self.limit;

        Ok(Box::pin(async move {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                    .await?;
            let adapted_projections =
                schema_adapter.map_projections(builder.schema(), &projection)?;
            // let predicate = predicate.map(|p| reassign_predicate_columns(p, builder.schema(), true)).transpose()?;

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    builder.schema().as_ref(),
                    table_schema.as_ref(),
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                );

                match row_filter {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(
                            "Ignoring error building row filter for '{:?}': {}",
                            predicate, e
                        );
                    }
                };
            };

            // Row group pruning: attempt to skip entire row_groups
            // using metadata on the row groups
            let file_metadata = builder.metadata();
            let row_groups = row_groups::prune_row_groups(
                file_metadata.row_groups(),
                file_range,
                pruning_predicate.as_ref().map(|p| p.as_ref()),
                &file_metrics,
            );

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if enable_page_index && !row_groups.is_empty() {
                if let Some(p) = page_pruning_predicate {
                    let pruned =
                        p.prune(&row_groups, file_metadata.as_ref(), &file_metrics)?;
                    if let Some(row_selection) = pruned {
                        builder = builder.with_row_selection(row_selection);
                    }
                }
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_groups)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| {
                        schema_adapter
                            .adapt_batch(b, &projection)
                            .map_err(Into::into)
                    })
                });

            Ok(adapted.boxed())
        }))
    }
}

/// Factory of parquet file readers.
///
/// Provides means to implement custom data access interface.
pub trait ParquetFileReaderFactory: Debug + Send + Sync + 'static {
    /// Provides `AsyncFileReader` over parquet file specified in `FileMeta`
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>>;
}

#[derive(Debug)]
pub struct DefaultParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl DefaultParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage
struct ParquetFileReader {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    file_metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics.bytes_scanned.add(range.end - range.start);

        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| {
                ParquetError::General(format!("AsyncChunkReader::get_bytes error: {e}"))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total);

        async move {
            self.store
                .get_ranges(&self.meta.location, &ranges)
                .await
                .map_err(|e| {
                    ParquetError::General(format!(
                        "AsyncChunkReader::get_byte_ranges error: {e}"
                    ))
                })
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = fetch_parquet_metadata(
                self.store.as_ref(),
                &self.meta,
                self.metadata_size_hint,
            )
            .await
            .map_err(|e| {
                ParquetError::General(format!(
                    "AsyncChunkReader::get_metadata error: {e}"
                ))
            })?;
            Ok(Arc::new(metadata))
        })
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );

        Ok(Box::new(ParquetFileReader {
            meta: file_meta.object_meta,
            store: Arc::clone(&self.store),
            metadata_size_hint,
            file_metrics,
        }))
    }
}

/// create target folder using different save mode strategy, refer to [`FileWriterSaveMode`] for more details
fn create_target_folder(
    path: impl AsRef<str>,
    save_mode: FileWriterSaveMode,
) -> Result<Option<std::path::PathBuf>> {
    let path = path.as_ref();
    let fs_path = std::path::Path::new(path);

    if fs_path.exists() {
        match save_mode {
            FileWriterSaveMode::Overwrite => {
                std::fs::remove_dir_all(fs_path)?;
            }
            FileWriterSaveMode::Ignore => {
                return Ok(None);
            }
            FileWriterSaveMode::Append => {
                return Ok(Some(fs_path.to_path_buf()));
            }
            _ => {}
        };
    }

    if let Err(e) = fs::create_dir(fs_path) {
        return Err(DataFusionError::Execution(format!(
            "Could not create directory {path}: {e:?}"
        )));
    }

    Ok(Some(fs_path.to_path_buf()))
}

/// Executes a query and writes the results to a partitioned Parquet file.
pub async fn plan_to_parquet(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
    writer_properties: Option<WriterProperties>,
    save_mode: FileWriterSaveMode,
) -> Result<()> {
    // create directory to contain the Parquet files (one per partition)
    let fs_path = &create_target_folder(path, save_mode)?;
    if fs_path.is_none() {
        return Ok(());
    }
    let fs_path = fs_path.as_ref().unwrap().as_path();
    // TODO: its likely better to task_ctx.task_id but now it comes None always
    let write_id = Uuid::new_v4().to_string();
    let mut tasks = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let plan = plan.clone();
        let filename = format!("part-{write_id}-{i}.parquet");
        let path = fs_path.join(filename);
        let file = fs::File::create(path)?;
        let mut writer =
            ArrowWriter::try_new(file, plan.schema(), writer_properties.clone())?;
        let stream = plan.execute(i, task_ctx.clone())?;
        let handle: tokio::task::JoinHandle<Result<()>> =
            tokio::task::spawn(async move {
                stream
                    .map(|batch| {
                        writer.write(&batch?).map_err(DataFusionError::ParquetError)
                    })
                    .try_collect()
                    .await
                    .map_err(DataFusionError::from)?;

                writer.close().map_err(DataFusionError::from).map(|_| ())
            });
        tasks.push(AbortOnDropSingle::new(handle));
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .try_for_each(|result| {
            result.map_err(|e| DataFusionError::Execution(format!("{e}")))?
        })?;
    Ok(())
}

// Copy from the arrow-rs
// https://github.com/apache/arrow-rs/blob/733b7e7fd1e8c43a404c3ce40ecf741d493c21b4/parquet/src/arrow/buffer/bit_util.rs#L55
// Convert the byte slice to fixed length byte array with the length of 16
fn sign_extend_be(b: &[u8]) -> [u8; 16] {
    assert!(b.len() <= 16, "Array too large, expected less than 16");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; 16] } else { [0u8; 16] };
    for (d, s) in result.iter_mut().skip(16 - b.len()).zip(b) {
        *d = *s;
    }
    result
}

// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i128(b: &[u8]) -> i128 {
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(sign_extend_be(b))
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

    use super::*;
    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::datasource::file_format::test_util::scan_format;
    use crate::datasource::listing::{FileRange, PartitionedFile};
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::execution::context::SessionState;
    use crate::execution::options::CsvReadOptions;
    use crate::physical_plan::displayable;
    use crate::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;
    use crate::{
        assert_batches_sorted_eq,
        datasource::file_format::{parquet::ParquetFormat, FileFormat},
        physical_plan::collect,
    };
    use arrow::array::{ArrayRef, Float32Array, Int32Array};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::{
        array::{Int64Array, Int8Array, StringArray},
        datatypes::{DataType, Field, SchemaBuilder},
    };
    use chrono::{TimeZone, Utc};
    use datafusion_common::ScalarValue;
    use datafusion_common::{assert_contains, ToDFSchema};
    use datafusion_expr::{col, lit, when, Expr};
    use datafusion_physical_expr::create_physical_expr;
    use datafusion_physical_expr::execution_props::ExecutionProps;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::ObjectMeta;
    use std::fs::{DirEntry, File};
    use std::io::Write;
    use tempfile::TempDir;

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
            let file_groups = meta.into_iter().map(Into::into).collect();

            // set up predicate (this is normally done by a layer higher up)
            let predicate = predicate.map(|p| logical2physical(&p, &file_schema));

            // prepare the scan
            let mut parquet_exec = ParquetExec::new(
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::local_filesystem(),
                    file_groups: vec![file_groups],
                    file_schema,
                    statistics: Statistics::default(),
                    projection,
                    limit: None,
                    table_partition_cols: vec![],
                    output_ordering: None,
                    infinite_source: false,
                },
                predicate,
                None,
            );

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
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/csv/corrupt.csv", options).await?;
        let tmp_dir = TempDir::new()?;
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let e = df
            .write_parquet(&out_dir, None, FileWriterSaveMode::Overwrite)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!("Arrow error: Parser error: Error while parsing value d for column 0 at line 4", format!("{e}"));
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
        let expected = vec![
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
        let expected = vec![
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
        let expected = vec![
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

        let expected = vec![
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
        let expected = vec![
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
        let expected = vec![
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

        let expected = vec![
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

        let expected = vec![
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

        let expected = vec![
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

        let c4: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(1.0_f32), Some(2.0_f32), None]));

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
                         "Execution error: Failed to map column projection for field c3. Incompatible data types Float32 and Int8");
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
                extensions: None,
            }
        }

        async fn assert_parquet_read(
            state: &SessionState,
            file_groups: Vec<Vec<PartitionedFile>>,
            expected_row_num: Option<usize>,
            file_schema: SchemaRef,
        ) -> Result<()> {
            let parquet_exec = ParquetExec::new(
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::local_filesystem(),
                    file_groups,
                    file_schema,
                    statistics: Statistics::default(),
                    projection: None,
                    limit: None,
                    table_partition_cols: vec![],
                    output_ordering: None,
                    infinite_source: false,
                },
                None,
                None,
            );
            assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);
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
                ScalarValue::Utf8(Some("2021".to_owned())),
                ScalarValue::UInt8(Some(10)),
                ScalarValue::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(ScalarValue::Utf8(Some("26".to_owned()))),
                ),
            ],
            range: None,
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

        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url,
                file_groups: vec![vec![partitioned_file]],
                file_schema: schema,
                statistics: Statistics::default(),
                // file has 10 cols so index 12 should be month and 13 should be day
                projection: Some(vec![0, 1, 2, 12, 13]),
                limit: None,
                table_partition_cols: vec![
                    ("year".to_owned(), DataType::Utf8),
                    ("month".to_owned(), DataType::UInt8),
                    (
                        "day".to_owned(),
                        DataType::Dictionary(
                            Box::new(DataType::UInt16),
                            Box::new(DataType::Utf8),
                        ),
                    ),
                ],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);
        assert_eq!(parquet_exec.schema().as_ref(), &expected_schema);

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap()?;
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        let expected = vec![
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
            },
            partition_values: vec![],
            range: None,
            extensions: None,
        };

        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: vec![vec![partitioned_file]],
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

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
        let expected = vec![
            "+-----+",
            "| int |",
            "+-----+",
            "| 4   |",
            "| 5   |",
            "+-----+",
        ];
        assert_batches_sorted_eq!(expected, &rt.batches.unwrap());
        assert_eq!(get_value(&metrics, "page_index_rows_filtered"), 4);
        assert!(
            get_value(&metrics, "page_index_eval_time") > 0,
            "no eval time in metrics: {metrics:#?}"
        );
    }

    #[tokio::test]
    async fn parquet_exec_metrics() {
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
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // on
        let filter = col("c1").not_eq(lit("bar"));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // assert the batches and some metrics
        let expected = vec![
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
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // on
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
        let display = displayable(rt.parquet_exec.as_ref()).indent().to_string();

        assert_contains!(
            &display,
            "pruning_predicate=c1_min@0 != bar OR bar != c1_max@1"
        );

        assert_contains!(&display, r#"predicate=c1@0 != bar"#);

        assert_contains!(&display, "projection=[c1]");
    }

    #[tokio::test]
    async fn parquet_exec_skip_empty_pruning() {
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
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // filter is too complicated for pruning
        let filter = when(col("c1").not_eq(lit("bar")), lit(true))
            .otherwise(lit(false))
            .unwrap();

        let rt = RoundTrip::new()
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        // Should not contain a pruning predicate
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
    async fn parquet_exec_repartition_single_file() {
        // Single file, single partition into multiple partitions
        let partitioned_file = PartitionedFile::new("a".to_string(), 123);
        let single_partition = vec![vec![partitioned_file]];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: single_partition,
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

        let actual = file_groups_to_vec(
            parquet_exec
                .get_repartitioned(4, 10)
                .base_config()
                .file_groups
                .clone(),
        );
        let expected = vec![
            (0, "a".to_string(), 0, 31),
            (1, "a".to_string(), 31, 62),
            (2, "a".to_string(), 62, 93),
            (3, "a".to_string(), 93, 123),
        ];
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn parquet_exec_repartition_too_much_partitions() {
        // Single file, single parittion into 96 partitions
        let partitioned_file = PartitionedFile::new("a".to_string(), 8);
        let single_partition = vec![vec![partitioned_file]];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: single_partition,
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

        let actual = file_groups_to_vec(
            parquet_exec
                .get_repartitioned(96, 5)
                .base_config()
                .file_groups
                .clone(),
        );
        let expected = vec![
            (0, "a".to_string(), 0, 1),
            (1, "a".to_string(), 1, 2),
            (2, "a".to_string(), 2, 3),
            (3, "a".to_string(), 3, 4),
            (4, "a".to_string(), 4, 5),
            (5, "a".to_string(), 5, 6),
            (6, "a".to_string(), 6, 7),
            (7, "a".to_string(), 7, 8),
        ];
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn parquet_exec_repartition_multiple_partitions() {
        // Multiple files in single partition after redistribution
        let partitioned_file_1 = PartitionedFile::new("a".to_string(), 40);
        let partitioned_file_2 = PartitionedFile::new("b".to_string(), 60);
        let source_partitions = vec![vec![partitioned_file_1], vec![partitioned_file_2]];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: source_partitions,
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

        let actual = file_groups_to_vec(
            parquet_exec
                .get_repartitioned(3, 10)
                .base_config()
                .file_groups
                .clone(),
        );
        let expected = vec![
            (0, "a".to_string(), 0, 34),
            (1, "a".to_string(), 34, 40),
            (1, "b".to_string(), 0, 28),
            (2, "b".to_string(), 28, 60),
        ];
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn parquet_exec_repartition_same_num_partitions() {
        // "Rebalance" files across partitions
        let partitioned_file_1 = PartitionedFile::new("a".to_string(), 40);
        let partitioned_file_2 = PartitionedFile::new("b".to_string(), 60);
        let source_partitions = vec![vec![partitioned_file_1], vec![partitioned_file_2]];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: source_partitions,
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

        let actual = file_groups_to_vec(
            parquet_exec
                .get_repartitioned(2, 10)
                .base_config()
                .file_groups
                .clone(),
        );
        let expected = vec![
            (0, "a".to_string(), 0, 40),
            (0, "b".to_string(), 0, 10),
            (1, "b".to_string(), 10, 60),
        ];
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn parquet_exec_repartition_no_action_ranges() {
        // No action due to Some(range) in second file
        let partitioned_file_1 = PartitionedFile::new("a".to_string(), 123);
        let mut partitioned_file_2 = PartitionedFile::new("b".to_string(), 144);
        partitioned_file_2.range = Some(FileRange { start: 1, end: 50 });

        let source_partitions = vec![vec![partitioned_file_1], vec![partitioned_file_2]];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: source_partitions,
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

        let actual = parquet_exec
            .get_repartitioned(65, 10)
            .base_config()
            .file_groups
            .clone();
        assert_eq!(2, actual.len());
    }

    #[tokio::test]
    async fn parquet_exec_repartition_no_action_min_size() {
        // No action due to target_partition_size
        let partitioned_file = PartitionedFile::new("a".to_string(), 123);
        let single_partition = vec![vec![partitioned_file]];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::local_filesystem(),
                file_groups: single_partition,
                file_schema: Arc::new(Schema::empty()),
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        );

        let actual = parquet_exec
            .get_repartitioned(65, 500)
            .base_config()
            .file_groups
            .clone();
        assert_eq!(1, actual.len());
    }

    fn file_groups_to_vec(
        file_groups: Vec<Vec<PartitionedFile>>,
    ) -> Vec<(usize, String, i64, i64)> {
        file_groups
            .iter()
            .enumerate()
            .flat_map(|(part_idx, files)| {
                files
                    .iter()
                    .map(|f| {
                        (
                            part_idx,
                            f.object_meta.location.to_string(),
                            f.range.as_ref().unwrap().start,
                            f.range.as_ref().unwrap().end,
                        )
                    })
                    .collect_vec()
            })
            .collect_vec()
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
    async fn write_parquet_results() -> Result<()> {
        async fn register_parquet(
            ctx: &SessionContext,
            files: &[DirEntry],
            num: u8,
        ) -> Result<()> {
            let filename = files
                .iter()
                .find(|f| {
                    f.file_name()
                        .to_str()
                        .unwrap()
                        .ends_with(&format!("-{num}.parquet"))
                })
                .unwrap()
                .path();

            let filename = filename.to_str().unwrap();
            ctx.register_parquet(
                &format!("part{num}"),
                filename,
                ParquetReadOptions::default(),
            )
            .await?;

            Ok(())
        }
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        // let mut ctx = create_ctx(&tmp_dir, 4).await?;
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(8));
        let schema = populate_csv_partitions(&tmp_dir, 4, ".csv")?;
        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // execute a simple query and write the results to parquet
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        df.write_parquet(&out_dir, None, FileWriterSaveMode::Overwrite)
            .await?;
        // write_parquet(&mut ctx, "SELECT c1, c2 FROM test", &out_dir, None).await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new();

        // register each partition as well as the top level dir
        let files = list_folder(&out_dir)?;
        register_parquet(&ctx, &files, 0).await?;
        register_parquet(&ctx, &files, 1).await?;
        register_parquet(&ctx, &files, 2).await?;
        register_parquet(&ctx, &files, 3).await?;

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
        let x = &format!("{out_dir}/part-2.parquet");
        dbg!(&x);

        Ok(())
    }

    #[tokio::test]
    async fn write_parquet_savemode() -> Result<()> {
        async fn write_parquet(
            ctx: &SessionContext,
            out_dir: &str,
            save_mode: FileWriterSaveMode,
        ) -> Result<()> {
            ctx.sql("SELECT 1 a")
                .await?
                .repartition(datafusion_expr::Partitioning::RoundRobinBatch(1))?
                .write_parquet(out_dir, None, save_mode)
                .await
        }

        let ctx = SessionContext::new();
        let tmp_dir = TempDir::new()?;
        let out_dir =
            tmp_dir.as_ref().to_str().unwrap().to_string() + "/write_parquet_savemode";

        // Overwrite
        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Overwrite).await?;
        let run0: Vec<DirEntry> = list_folder(&out_dir)?;
        assert_eq!(
            run0.iter()
                .filter(|f| f.file_name().to_str().unwrap().ends_with(".parquet"))
                .count(),
            1
        );
        let file0 = run0.first().unwrap().file_name();
        let file0 = file0.to_str().unwrap();

        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Overwrite).await?;
        let run1: Vec<DirEntry> = list_folder(&out_dir)?;
        assert_eq!(
            run1.iter()
                .filter(|f| f.file_name().to_str().unwrap().ends_with(".parquet"))
                .count(),
            1
        );
        let file1 = run1.first().unwrap().file_name();
        let file1 = file1.to_str().unwrap();
        // Overwrite saves 1 new file from second run, files from first run removed
        assert!(file0 != file1);
        std::fs::remove_dir_all(&out_dir)?;

        // Append
        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Append).await?;
        let run0: Vec<DirEntry> = list_folder(&out_dir)?;
        assert_eq!(
            run0.iter()
                .filter(|f| f.file_name().to_str().unwrap().ends_with(".parquet"))
                .count(),
            1
        );
        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Append).await?;
        let run1: Vec<DirEntry> = list_folder(&out_dir)?;
        // Append saves 1 new file from second run, files from first run preserved
        assert_eq!(
            run1.iter()
                .filter(|f| f.file_name().to_str().unwrap().ends_with(".parquet"))
                .count(),
            2
        );
        std::fs::remove_dir_all(&out_dir)?;

        // Ignore
        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Ignore).await?;
        let run0: Vec<DirEntry> = list_folder(&out_dir)?;
        assert_eq!(
            run0.iter()
                .filter(|f| f.file_name().to_str().unwrap().ends_with(".parquet"))
                .count(),
            1
        );
        let file0 = run0.first().unwrap().file_name();
        let file0 = file0.to_str().unwrap();
        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Ignore).await?;
        let run1: Vec<DirEntry> = list_folder(&out_dir)?;
        assert_eq!(
            run1.iter()
                .filter(|f| f.file_name().to_str().unwrap().ends_with(".parquet"))
                .count(),
            1
        );
        let file1 = run1.first().unwrap().file_name();
        let file1 = file1.to_str().unwrap();
        // Ignore preserves 1 new file from first run, no files from second run written
        assert_eq!(file0, file1);
        std::fs::remove_dir_all(&out_dir)?;

        // ErrorIfExists throws an error if folder already exists
        write_parquet(&ctx, &out_dir, FileWriterSaveMode::Append).await?;
        let res = write_parquet(&ctx, &out_dir, FileWriterSaveMode::ErrorIfExists).await;
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(&err.contains("Could not create directory"));

        Ok(())
    }

    fn list_folder(dir: &String) -> Result<Vec<DirEntry>> {
        let files = fs::read_dir(dir)?
            .map(|f| f.unwrap())
            .collect::<Vec<DirEntry>>();
        Ok(files)
    }

    fn logical2physical(expr: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let df_schema = schema.clone().to_dfschema().unwrap();
        let execution_props = ExecutionProps::new();
        create_physical_expr(expr, &df_schema, schema, &execution_props).unwrap()
    }
}
