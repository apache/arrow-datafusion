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

//! [`ParquetFormat`]: Parquet [`FileFormat`] abstractions

use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use super::write::demux::start_demuxer_task;
use super::write::{create_writer, SharedBuffer};
use super::{FileFormat, FileFormatFactory, FileScanConfig};
use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::{Fields, Schema, SchemaRef};
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::physical_plan::{FileGroupDisplay, FileSinkConfig};
use crate::datasource::statistics::{create_max_min_accs, get_col_stats};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::insert::{DataSink, DataSinkExec};
use crate::physical_plan::{
    Accumulator, DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    Statistics,
};

use arrow::compute::sum;
use datafusion_common::config::{ConfigField, ConfigFileType, TableParquetOptions};
use datafusion_common::file_options::parquet_writer::ParquetWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{
    exec_err, internal_datafusion_err, not_impl_err, DataFusionError, GetExt,
    DEFAULT_PARQUET_EXTENSION,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortRequirement};
use datafusion_physical_plan::metrics::MetricsSet;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use hashbrown::HashMap;
use log::debug;
use object_store::buffered::BufWriter;
use parquet::arrow::arrow_writer::{
    compute_leaves, get_column_writers, ArrowColumnChunk, ArrowColumnWriter,
    ArrowLeafColumn,
};
use parquet::arrow::{
    arrow_to_parquet_schema, parquet_to_arrow_schema, AsyncArrowWriter,
};
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::FileMetaData;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;

use crate::datasource::physical_plan::parquet::{
    ParquetExecBuilder, StatisticsConverter,
};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

/// Initial writing buffer size. Note this is just a size hint for efficiency. It
/// will grow beyond the set value if needed.
const INITIAL_BUFFER_BYTES: usize = 1048576;

/// When writing parquet files in parallel, if the buffered Parquet data exceeds
/// this size, it is flushed to object store
const BUFFER_FLUSH_BYTES: usize = 1024000;

#[derive(Default)]
/// Factory struct used to create [ParquetFormat]
pub struct ParquetFormatFactory {
    options: Option<TableParquetOptions>,
}

impl ParquetFormatFactory {
    /// Creates an instance of [ParquetFormatFactory]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [ParquetFormatFactory] with customized default options
    pub fn new_with_options(options: TableParquetOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl FileFormatFactory for ParquetFormatFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let parquet_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::PARQUET);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.parquet
            }
            Some(parquet_options) => {
                let mut parquet_options = parquet_options.clone();
                for (k, v) in format_options {
                    parquet_options.set(k, v)?;
                }
                parquet_options
            }
        };

        Ok(Arc::new(
            ParquetFormat::default().with_options(parquet_options),
        ))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ParquetFormat::default())
    }
}

impl GetExt for ParquetFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".parquet" -> "parquet"
        DEFAULT_PARQUET_EXTENSION[1..].to_string()
    }
}

/// The Apache Parquet `FileFormat` implementation
#[derive(Debug, Default)]
pub struct ParquetFormat {
    options: TableParquetOptions,
}

impl ParquetFormat {
    /// Construct a new Format with no local overrides
    pub fn new() -> Self {
        Self::default()
    }

    /// Activate statistics based row group level pruning
    /// - If `None`, defaults to value on `config_options`
    pub fn with_enable_pruning(mut self, enable: bool) -> Self {
        self.options.global.pruning = enable;
        self
    }

    /// Return `true` if pruning is enabled
    pub fn enable_pruning(&self) -> bool {
        self.options.global.pruning
    }

    /// Provide a hint to the size of the file metadata. If a hint is provided
    /// the reader will try and fetch the last `size_hint` bytes of the parquet file optimistically.
    /// Without a hint, two read are required. One read to fetch the 8-byte parquet footer and then
    /// another read to fetch the metadata length encoded in the footer.
    ///
    /// - If `None`, defaults to value on `config_options`
    pub fn with_metadata_size_hint(mut self, size_hint: Option<usize>) -> Self {
        self.options.global.metadata_size_hint = size_hint;
        self
    }

    /// Return the metadata size hint if set
    pub fn metadata_size_hint(&self) -> Option<usize> {
        self.options.global.metadata_size_hint
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.
    ///
    /// - If `None`, defaults to value on `config_options`
    pub fn with_skip_metadata(mut self, skip_metadata: bool) -> Self {
        self.options.global.skip_metadata = skip_metadata;
        self
    }

    /// Returns `true` if schema metadata will be cleared prior to
    /// schema merging.
    pub fn skip_metadata(&self) -> bool {
        self.options.global.skip_metadata
    }

    /// Set Parquet options for the ParquetFormat
    pub fn with_options(mut self, options: TableParquetOptions) -> Self {
        self.options = options;
        self
    }

    /// Parquet options
    pub fn options(&self) -> &TableParquetOptions {
        &self.options
    }
}

/// Clears all metadata (Schema level and field level) on an iterator
/// of Schemas
fn clear_metadata(
    schemas: impl IntoIterator<Item = Schema>,
) -> impl Iterator<Item = Schema> {
    schemas.into_iter().map(|schema| {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                field.as_ref().clone().with_metadata(Default::default()) // clear meta
            })
            .collect::<Fields>();
        Schema::new(fields)
    })
}

async fn fetch_schema_with_location(
    store: &dyn ObjectStore,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<(Path, Schema)> {
    let loc_path = file.location.clone();
    let schema = fetch_schema(store, file, metadata_size_hint).await?;
    Ok((loc_path, schema))
}

#[async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        ParquetFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => Err(DataFusionError::Internal(
                "Parquet FileFormat does not support compression.".into(),
            )),
        }
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| {
                fetch_schema_with_location(
                    store.as_ref(),
                    object,
                    self.metadata_size_hint(),
                )
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        // Schema inference adds fields based the order they are seen
        // which depends on the order the files are processed. For some
        // object stores (like local file systems) the order returned from list
        // is not deterministic. Thus, to ensure deterministic schema inference
        // sort the files first.
        // https://github.com/apache/datafusion/pull/6629
        schemas.sort_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas
            .into_iter()
            .map(|(_, schema)| schema)
            .collect::<Vec<_>>();

        let schema = if self.skip_metadata() {
            Schema::try_merge(clear_metadata(schemas))
        } else {
            Schema::try_merge(schemas)
        }?;

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let stats = fetch_statistics(
            store.as_ref(),
            table_schema,
            object,
            self.metadata_size_hint(),
        )
        .await?;
        Ok(stats)
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut builder =
            ParquetExecBuilder::new_with_options(conf, self.options.clone());

        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        if self.enable_pruning() {
            if let Some(predicate) = filters.cloned() {
                builder = builder.with_predicate(predicate);
            }
        }
        if let Some(metadata_size_hint) = self.metadata_size_hint() {
            builder = builder.with_metadata_size_hint(metadata_size_hint);
        }

        Ok(builder.build_arc())
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return not_impl_err!("Overwrites are not implemented yet for Parquet");
        }

        let sink_schema = Arc::clone(conf.output_schema());
        let sink = Arc::new(ParquetSink::new(conf, self.options.clone()));

        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            sink_schema,
            order_requirements,
        )) as _)
    }
}

/// Fetches parquet metadata from ObjectStore for given object
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [`ParquetFileReaderFactory`].
///
/// [`ParquetFileReaderFactory`]: crate::datasource::physical_plan::ParquetFileReaderFactory
pub async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
    size_hint: Option<usize>,
) -> Result<ParquetMetaData> {
    if meta.size < 8 {
        return exec_err!("file size of {} is less than footer", meta.size);
    }

    // If a size hint is provided, read more than the minimum size
    // to try and avoid a second fetch.
    let footer_start = if let Some(size_hint) = size_hint {
        meta.size.saturating_sub(size_hint)
    } else {
        meta.size - 8
    };

    let suffix = store
        .get_range(&meta.location, footer_start..meta.size)
        .await?;

    let suffix_len = suffix.len();

    let mut footer = [0; 8];
    footer.copy_from_slice(&suffix[suffix_len - 8..suffix_len]);

    let length = decode_footer(&footer)?;

    if meta.size < length + 8 {
        return exec_err!(
            "file size of {} is less than footer + metadata {}",
            meta.size,
            length + 8
        );
    }

    // Did not fetch the entire file metadata in the initial read, need to make a second request
    if length > suffix_len - 8 {
        let metadata_start = meta.size - length - 8;
        let remaining_metadata = store
            .get_range(&meta.location, metadata_start..footer_start)
            .await?;

        let mut metadata = BytesMut::with_capacity(length);

        metadata.put(remaining_metadata.as_ref());
        metadata.put(&suffix[..suffix_len - 8]);

        Ok(decode_metadata(metadata.as_ref())?)
    } else {
        let metadata_start = meta.size - length - 8;

        Ok(decode_metadata(
            &suffix[metadata_start - footer_start..suffix_len - 8],
        )?)
    }
}

/// Read and parse the schema of the Parquet file at location `path`
async fn fetch_schema(
    store: &dyn ObjectStore,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Schema> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    let file_metadata = metadata.file_metadata();
    let schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    Ok(schema)
}

/// Read and parse the statistics of the Parquet file at location `path`
///
/// See [`statistics_from_parquet_meta`] for more details
async fn fetch_statistics(
    store: &dyn ObjectStore,
    table_schema: SchemaRef,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Statistics> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    statistics_from_parquet_meta(&metadata, table_schema).await
}

/// Convert statistics in  [`ParquetMetaData`] into [`Statistics`] using ['StatisticsConverter`]
///
/// The statistics are calculated for each column in the table schema
/// using the row group statistics in the parquet metadata.
pub async fn statistics_from_parquet_meta(
    metadata: &ParquetMetaData,
    table_schema: SchemaRef,
) -> Result<Statistics> {
    let row_groups_metadata = metadata.row_groups();

    let mut statistics = Statistics::new_unknown(&table_schema);
    let mut has_statistics = false;
    let mut num_rows = 0_usize;
    let mut total_byte_size = 0_usize;
    for row_group_meta in row_groups_metadata {
        num_rows += row_group_meta.num_rows() as usize;
        total_byte_size += row_group_meta.total_byte_size() as usize;

        if !has_statistics {
            row_group_meta.columns().iter().for_each(|column| {
                has_statistics = column.statistics().is_some();
            });
        }
    }
    statistics.num_rows = Precision::Exact(num_rows);
    statistics.total_byte_size = Precision::Exact(total_byte_size);

    let file_metadata = metadata.file_metadata();
    let file_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    statistics.column_statistics = if has_statistics {
        let (mut max_accs, mut min_accs) = create_max_min_accs(&table_schema);
        let mut null_counts_array =
            vec![Precision::Exact(0); table_schema.fields().len()];

        table_schema
            .fields()
            .iter()
            .enumerate()
            .for_each(|(idx, field)| {
                match StatisticsConverter::try_new(
                    field.name(),
                    &file_schema,
                    file_metadata.schema_descr(),
                ) {
                    Ok(stats_converter) => {
                        summarize_min_max_null_counts(
                            &mut min_accs,
                            &mut max_accs,
                            &mut null_counts_array,
                            idx,
                            num_rows,
                            &stats_converter,
                            row_groups_metadata,
                        )
                        .ok();
                    }
                    Err(e) => {
                        debug!("Failed to create statistics converter: {}", e);
                        null_counts_array[idx] = Precision::Exact(num_rows);
                    }
                }
            });

        get_col_stats(
            &table_schema,
            null_counts_array,
            &mut max_accs,
            &mut min_accs,
        )
    } else {
        Statistics::unknown_column(&table_schema)
    };

    Ok(statistics)
}

fn summarize_min_max_null_counts(
    min_accs: &mut [Option<MinAccumulator>],
    max_accs: &mut [Option<MaxAccumulator>],
    null_counts_array: &mut [Precision<usize>],
    arrow_schema_index: usize,
    num_rows: usize,
    stats_converter: &StatisticsConverter,
    row_groups_metadata: &[RowGroupMetaData],
) -> Result<()> {
    let max_values = stats_converter.row_group_maxes(row_groups_metadata)?;
    let min_values = stats_converter.row_group_mins(row_groups_metadata)?;
    let null_counts = stats_converter.row_group_null_counts(row_groups_metadata)?;

    if let Some(max_acc) = &mut max_accs[arrow_schema_index] {
        max_acc.update_batch(&[max_values])?;
    }

    if let Some(min_acc) = &mut min_accs[arrow_schema_index] {
        min_acc.update_batch(&[min_values])?;
    }

    null_counts_array[arrow_schema_index] = Precision::Exact(match sum(&null_counts) {
        Some(null_count) => null_count as usize,
        None => num_rows,
    });

    Ok(())
}

/// Implements [`DataSink`] for writing to a parquet file.
pub struct ParquetSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Underlying parquet options
    parquet_options: TableParquetOptions,
    /// File metadata from successfully produced parquet files. The Mutex is only used
    /// to allow inserting to HashMap from behind borrowed reference in DataSink::write_all.
    written: Arc<parking_lot::Mutex<HashMap<Path, FileMetaData>>>,
}

impl Debug for ParquetSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetSink").finish()
    }
}

impl DisplayAs for ParquetSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ParquetSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_groups).fmt_as(t, f)?;
                write!(f, ")")
            }
        }
    }
}

impl ParquetSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig, parquet_options: TableParquetOptions) -> Self {
        Self {
            config,
            parquet_options,
            written: Default::default(),
        }
    }

    /// Retrieve the inner [`FileSinkConfig`].
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    /// Retrieve the file metadata for the written files, keyed to the path
    /// which may be partitioned (in the case of hive style partitioning).
    pub fn written(&self) -> HashMap<Path, FileMetaData> {
        self.written.lock().clone()
    }

    /// Converts table schema to writer schema, which may differ in the case
    /// of hive style partitioning where some columns are removed from the
    /// underlying files.
    fn get_writer_schema(&self) -> Arc<Schema> {
        if !self.config.table_partition_cols.is_empty()
            && !self.config.keep_partition_by_columns
        {
            let schema = self.config.output_schema();
            let partition_names: Vec<_> = self
                .config
                .table_partition_cols
                .iter()
                .map(|(s, _)| s)
                .collect();
            Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|f| !partition_names.contains(&f.name()))
                    .map(|f| (**f).clone())
                    .collect::<Vec<_>>(),
            ))
        } else {
            Arc::clone(self.config.output_schema())
        }
    }

    /// Creates an AsyncArrowWriter which serializes a parquet file to an ObjectStore
    /// AsyncArrowWriters are used when individual parquet file serialization is not parallelized
    async fn create_async_arrow_writer(
        &self,
        location: &Path,
        object_store: Arc<dyn ObjectStore>,
        parquet_props: WriterProperties,
    ) -> Result<AsyncArrowWriter<BufWriter>> {
        let buf_writer = BufWriter::new(object_store, location.clone());
        let writer = AsyncArrowWriter::try_new(
            buf_writer,
            self.get_writer_schema(),
            Some(parquet_props),
        )?;
        Ok(writer)
    }

    /// Parquet options
    pub fn parquet_options(&self) -> &TableParquetOptions {
        &self.parquet_options
    }
}

#[async_trait]
impl DataSink for ParquetSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let parquet_props = ParquetWriterOptions::try_from(&self.parquet_options)?;

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        let parquet_opts = &self.parquet_options;
        let allow_single_file_parallelism =
            parquet_opts.global.allow_single_file_parallelism;

        let part_col = if !self.config.table_partition_cols.is_empty() {
            Some(self.config.table_partition_cols.clone())
        } else {
            None
        };

        let parallel_options = ParallelParquetWriterOptions {
            max_parallel_row_groups: parquet_opts
                .global
                .maximum_parallel_row_group_writers,
            max_buffered_record_batches_per_stream: parquet_opts
                .global
                .maximum_buffered_record_batches_per_stream,
        };

        let (demux_task, mut file_stream_rx) = start_demuxer_task(
            data,
            context,
            part_col,
            self.config.table_paths[0].clone(),
            "parquet".into(),
            self.config.keep_partition_by_columns,
        );

        let mut file_write_tasks: JoinSet<
            std::result::Result<(Path, FileMetaData), DataFusionError>,
        > = JoinSet::new();

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            if !allow_single_file_parallelism {
                let mut writer = self
                    .create_async_arrow_writer(
                        &path,
                        Arc::clone(&object_store),
                        parquet_props.writer_options().clone(),
                    )
                    .await?;
                file_write_tasks.spawn(async move {
                    while let Some(batch) = rx.recv().await {
                        writer.write(&batch).await?;
                    }
                    let file_metadata = writer
                        .close()
                        .await
                        .map_err(DataFusionError::ParquetError)?;
                    Ok((path, file_metadata))
                });
            } else {
                let writer = create_writer(
                    // Parquet files as a whole are never compressed, since they
                    // manage compressed blocks themselves.
                    FileCompressionType::UNCOMPRESSED,
                    &path,
                    Arc::clone(&object_store),
                )
                .await?;
                let schema = self.get_writer_schema();
                let props = parquet_props.clone();
                let parallel_options_clone = parallel_options.clone();
                file_write_tasks.spawn(async move {
                    let file_metadata = output_single_parquet_file_parallelized(
                        writer,
                        rx,
                        schema,
                        props.writer_options(),
                        parallel_options_clone,
                    )
                    .await?;
                    Ok((path, file_metadata))
                });
            }
        }

        let mut row_count = 0;
        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    let (path, file_metadata) = r?;
                    row_count += file_metadata.num_rows;
                    let mut written_files = self.written.lock();
                    written_files
                        .try_insert(path.clone(), file_metadata)
                        .map_err(|e| internal_datafusion_err!("duplicate entry detected for partitioned file {path}: {e}"))?;
                    drop(written_files);
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        demux_task.join_unwind().await?;

        Ok(row_count as u64)
    }
}

/// Consumes a stream of [ArrowLeafColumn] via a channel and serializes them using an [ArrowColumnWriter]
/// Once the channel is exhausted, returns the ArrowColumnWriter.
async fn column_serializer_task(
    mut rx: Receiver<ArrowLeafColumn>,
    mut writer: ArrowColumnWriter,
) -> Result<ArrowColumnWriter> {
    while let Some(col) = rx.recv().await {
        writer.write(&col)?;
    }
    Ok(writer)
}

type ColumnWriterTask = SpawnedTask<Result<ArrowColumnWriter>>;
type ColSender = Sender<ArrowLeafColumn>;

/// Spawns a parallel serialization task for each column
/// Returns join handles for each columns serialization task along with a send channel
/// to send arrow arrays to each serialization task.
fn spawn_column_parallel_row_group_writer(
    schema: Arc<Schema>,
    parquet_props: Arc<WriterProperties>,
    max_buffer_size: usize,
) -> Result<(Vec<ColumnWriterTask>, Vec<ColSender>)> {
    let schema_desc = arrow_to_parquet_schema(&schema)?;
    let col_writers = get_column_writers(&schema_desc, &parquet_props, &schema)?;
    let num_columns = col_writers.len();

    let mut col_writer_tasks = Vec::with_capacity(num_columns);
    let mut col_array_channels = Vec::with_capacity(num_columns);
    for writer in col_writers.into_iter() {
        // Buffer size of this channel limits the number of arrays queued up for column level serialization
        let (send_array, recieve_array) =
            mpsc::channel::<ArrowLeafColumn>(max_buffer_size);
        col_array_channels.push(send_array);

        let task = SpawnedTask::spawn(column_serializer_task(recieve_array, writer));
        col_writer_tasks.push(task);
    }

    Ok((col_writer_tasks, col_array_channels))
}

/// Settings related to writing parquet files in parallel
#[derive(Clone)]
struct ParallelParquetWriterOptions {
    max_parallel_row_groups: usize,
    max_buffered_record_batches_per_stream: usize,
}

/// This is the return type of calling [ArrowColumnWriter].close() on each column
/// i.e. the Vec of encoded columns which can be appended to a row group
type RBStreamSerializeResult = Result<(Vec<ArrowColumnChunk>, usize)>;

/// Sends the ArrowArrays in passed [RecordBatch] through the channels to their respective
/// parallel column serializers.
async fn send_arrays_to_col_writers(
    col_array_channels: &[ColSender],
    rb: &RecordBatch,
    schema: Arc<Schema>,
) -> Result<()> {
    // Each leaf column has its own channel, increment next_channel for each leaf column sent.
    let mut next_channel = 0;
    for (array, field) in rb.columns().iter().zip(schema.fields()) {
        for c in compute_leaves(field, array)? {
            col_array_channels[next_channel]
                .send(c)
                .await
                .map_err(|_| {
                    DataFusionError::Internal("Unable to send array to writer!".into())
                })?;
            next_channel += 1;
        }
    }

    Ok(())
}

/// Spawns a tokio task which joins the parallel column writer tasks,
/// and finalizes the row group
fn spawn_rg_join_and_finalize_task(
    column_writer_tasks: Vec<ColumnWriterTask>,
    rg_rows: usize,
) -> SpawnedTask<RBStreamSerializeResult> {
    SpawnedTask::spawn(async move {
        let num_cols = column_writer_tasks.len();
        let mut finalized_rg = Vec::with_capacity(num_cols);
        for task in column_writer_tasks.into_iter() {
            let writer = task.join_unwind().await?;
            finalized_rg.push(writer.close()?);
        }

        Ok((finalized_rg, rg_rows))
    })
}

/// This task coordinates the serialization of a parquet file in parallel.
/// As the query produces RecordBatches, these are written to a RowGroup
/// via parallel [ArrowColumnWriter] tasks. Once the desired max rows per
/// row group is reached, the parallel tasks are joined on another separate task
/// and sent to a concatenation task. This task immediately continues to work
/// on the next row group in parallel. So, parquet serialization is parallelized
/// accross both columns and row_groups, with a theoretical max number of parallel tasks
/// given by n_columns * num_row_groups.
fn spawn_parquet_parallel_serialization_task(
    mut data: Receiver<RecordBatch>,
    serialize_tx: Sender<SpawnedTask<RBStreamSerializeResult>>,
    schema: Arc<Schema>,
    writer_props: Arc<WriterProperties>,
    parallel_options: ParallelParquetWriterOptions,
) -> SpawnedTask<Result<(), DataFusionError>> {
    SpawnedTask::spawn(async move {
        let max_buffer_rb = parallel_options.max_buffered_record_batches_per_stream;
        let max_row_group_rows = writer_props.max_row_group_size();
        let (mut column_writer_handles, mut col_array_channels) =
            spawn_column_parallel_row_group_writer(
                Arc::clone(&schema),
                Arc::clone(&writer_props),
                max_buffer_rb,
            )?;
        let mut current_rg_rows = 0;

        while let Some(mut rb) = data.recv().await {
            // This loop allows the "else" block to repeatedly split the RecordBatch to handle the case
            // when max_row_group_rows < execution.batch_size as an alternative to a recursive async
            // function.
            loop {
                if current_rg_rows + rb.num_rows() < max_row_group_rows {
                    send_arrays_to_col_writers(
                        &col_array_channels,
                        &rb,
                        Arc::clone(&schema),
                    )
                    .await?;
                    current_rg_rows += rb.num_rows();
                    break;
                } else {
                    let rows_left = max_row_group_rows - current_rg_rows;
                    let a = rb.slice(0, rows_left);
                    send_arrays_to_col_writers(
                        &col_array_channels,
                        &a,
                        Arc::clone(&schema),
                    )
                    .await?;

                    // Signal the parallel column writers that the RowGroup is done, join and finalize RowGroup
                    // on a separate task, so that we can immediately start on the next RG before waiting
                    // for the current one to finish.
                    drop(col_array_channels);
                    let finalize_rg_task = spawn_rg_join_and_finalize_task(
                        column_writer_handles,
                        max_row_group_rows,
                    );

                    serialize_tx.send(finalize_rg_task).await.map_err(|_| {
                        DataFusionError::Internal(
                            "Unable to send closed RG to concat task!".into(),
                        )
                    })?;

                    current_rg_rows = 0;
                    rb = rb.slice(rows_left, rb.num_rows() - rows_left);

                    (column_writer_handles, col_array_channels) =
                        spawn_column_parallel_row_group_writer(
                            Arc::clone(&schema),
                            Arc::clone(&writer_props),
                            max_buffer_rb,
                        )?;
                }
            }
        }

        drop(col_array_channels);
        // Handle leftover rows as final rowgroup, which may be smaller than max_row_group_rows
        if current_rg_rows > 0 {
            let finalize_rg_task =
                spawn_rg_join_and_finalize_task(column_writer_handles, current_rg_rows);

            serialize_tx.send(finalize_rg_task).await.map_err(|_| {
                DataFusionError::Internal(
                    "Unable to send closed RG to concat task!".into(),
                )
            })?;
        }

        Ok(())
    })
}

/// Consume RowGroups serialized by other parallel tasks and concatenate them in
/// to the final parquet file, while flushing finalized bytes to an [ObjectStore]
async fn concatenate_parallel_row_groups(
    mut serialize_rx: Receiver<SpawnedTask<RBStreamSerializeResult>>,
    schema: Arc<Schema>,
    writer_props: Arc<WriterProperties>,
    mut object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
) -> Result<FileMetaData> {
    let merged_buff = SharedBuffer::new(INITIAL_BUFFER_BYTES);

    let schema_desc = arrow_to_parquet_schema(schema.as_ref())?;
    let mut parquet_writer = SerializedFileWriter::new(
        merged_buff.clone(),
        schema_desc.root_schema_ptr(),
        writer_props,
    )?;

    while let Some(task) = serialize_rx.recv().await {
        let result = task.join_unwind().await;
        let mut rg_out = parquet_writer.next_row_group()?;
        let (serialized_columns, _cnt) = result?;
        for chunk in serialized_columns {
            chunk.append_to_row_group(&mut rg_out)?;
            let mut buff_to_flush = merged_buff.buffer.try_lock().unwrap();
            if buff_to_flush.len() > BUFFER_FLUSH_BYTES {
                object_store_writer
                    .write_all(buff_to_flush.as_slice())
                    .await?;
                buff_to_flush.clear();
            }
        }
        rg_out.close()?;
    }

    let file_metadata = parquet_writer.close()?;
    let final_buff = merged_buff.buffer.try_lock().unwrap();

    object_store_writer.write_all(final_buff.as_slice()).await?;
    object_store_writer.shutdown().await?;

    Ok(file_metadata)
}

/// Parallelizes the serialization of a single parquet file, by first serializing N
/// independent RecordBatch streams in parallel to RowGroups in memory. Another
/// task then stitches these independent RowGroups together and streams this large
/// single parquet file to an ObjectStore in multiple parts.
async fn output_single_parquet_file_parallelized(
    object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
    data: Receiver<RecordBatch>,
    output_schema: Arc<Schema>,
    parquet_props: &WriterProperties,
    parallel_options: ParallelParquetWriterOptions,
) -> Result<FileMetaData> {
    let max_rowgroups = parallel_options.max_parallel_row_groups;
    // Buffer size of this channel limits maximum number of RowGroups being worked on in parallel
    let (serialize_tx, serialize_rx) =
        mpsc::channel::<SpawnedTask<RBStreamSerializeResult>>(max_rowgroups);

    let arc_props = Arc::new(parquet_props.clone());
    let launch_serialization_task = spawn_parquet_parallel_serialization_task(
        data,
        serialize_tx,
        Arc::clone(&output_schema),
        Arc::clone(&arc_props),
        parallel_options,
    );
    let file_metadata = concatenate_parallel_row_groups(
        serialize_rx,
        output_schema,
        arc_props,
        object_store_writer,
    )
    .await?;

    launch_serialization_task.join_unwind().await?;
    Ok(file_metadata)
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::test::object_store::local_unpartitioned_file;

    use parquet::arrow::ArrowWriter;
    use tempfile::NamedTempFile;

    /// How many rows per page should be written
    const ROWS_PER_PAGE: usize = 2;

    /// Writes `batches` to a temporary parquet file
    ///
    /// If multi_page is set to `true`, the parquet file(s) are written
    /// with 2 rows per data page (used to test page filtering and
    /// boundaries).
    pub async fn store_parquet(
        batches: Vec<RecordBatch>,
        multi_page: bool,
    ) -> Result<(Vec<ObjectMeta>, Vec<NamedTempFile>)> {
        // we need the tmp files to be sorted as some tests rely on the how the returning files are ordered
        // https://github.com/apache/datafusion/pull/6629
        let tmp_files = {
            let mut tmp_files: Vec<_> = (0..batches.len())
                .map(|_| NamedTempFile::new().expect("creating temp file"))
                .collect();
            tmp_files.sort_by(|a, b| a.path().cmp(b.path()));
            tmp_files
        };

        // Each batch writes to their own file
        let files: Vec<_> = batches
            .into_iter()
            .zip(tmp_files.into_iter())
            .map(|(batch, mut output)| {
                let builder = WriterProperties::builder();
                let props = if multi_page {
                    builder.set_data_page_row_count_limit(ROWS_PER_PAGE)
                } else {
                    builder
                }
                .build();

                let mut writer =
                    ArrowWriter::try_new(&mut output, batch.schema(), Some(props))
                        .expect("creating writer");

                if multi_page {
                    // write in smaller batches as the parquet writer
                    // only checks datapage size limits on the boundaries of each batch
                    write_in_chunks(&mut writer, &batch, ROWS_PER_PAGE);
                } else {
                    writer.write(&batch).expect("Writing batch");
                };
                writer.close().unwrap();
                output
            })
            .collect();

        let meta: Vec<_> = files.iter().map(local_unpartitioned_file).collect();

        Ok((meta, files))
    }

    /// write batches chunk_size rows at a time
    fn write_in_chunks<W: std::io::Write + Send>(
        writer: &mut ArrowWriter<W>,
        batch: &RecordBatch,
        chunk_size: usize,
    ) {
        let mut i = 0;
        while i < batch.num_rows() {
            let num = chunk_size.min(batch.num_rows() - i);
            writer.write(&batch.slice(i, num)).unwrap();
            i += num;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use crate::datasource::listing::{ListingTableUrl, PartitionedFile};
    use crate::physical_plan::collect;
    use std::fmt::{Display, Formatter};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::physical_plan::metrics::MetricValue;
    use crate::prelude::{SessionConfig, SessionContext};
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use async_trait::async_trait;
    use datafusion_common::cast::{
        as_binary_array, as_boolean_array, as_float32_array, as_float64_array,
        as_int32_array, as_timestamp_nanosecond_array,
    };
    use datafusion_common::config::ParquetOptions;
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Utf8;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream::BoxStream;
    use log::error;
    use object_store::local::LocalFileSystem;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOpts, PutOptions,
        PutPayload, PutResult,
    };
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::file::metadata::{KeyValue, ParquetColumnIndex, ParquetOffsetIndex};
    use parquet::file::page_index::index::Index;
    use tokio::fs::File;

    #[tokio::test]
    async fn read_merged_batches() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)]).unwrap();

        let store = Arc::new(LocalFileSystem::new()) as _;
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        let session = SessionContext::new();
        let ctx = session.state();
        let format = ParquetFormat::default();
        let schema = format.infer_schema(&ctx, &store, &meta).await.unwrap();

        let stats =
            fetch_statistics(store.as_ref(), schema.clone(), &meta[0], None).await?;

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.null_count, Precision::Exact(3));

        let stats = fetch_statistics(store.as_ref(), schema, &meta[1], None).await?;
        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(3));
        assert_eq!(c2_stats.null_count, Precision::Exact(1));
        assert_eq!(
            c2_stats.max_value,
            Precision::Exact(ScalarValue::Int64(Some(2)))
        );
        assert_eq!(
            c2_stats.min_value,
            Precision::Exact(ScalarValue::Int64(Some(1)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn is_schema_stable() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 =
            RecordBatch::try_from_iter(vec![("a", c1.clone()), ("b", c1.clone())])
                .unwrap();
        let batch2 =
            RecordBatch::try_from_iter(vec![("c", c2.clone()), ("d", c2.clone())])
                .unwrap();

        let store = Arc::new(LocalFileSystem::new()) as _;
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        let session = SessionContext::new();
        let ctx = session.state();
        let format = ParquetFormat::default();
        let schema = format.infer_schema(&ctx, &store, &meta).await.unwrap();

        let order: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|i| i.to_string())
            .collect();
        let coll: Vec<_> = schema
            .all_fields()
            .into_iter()
            .map(|i| i.name().to_string())
            .collect();
        assert_eq!(coll, order);

        Ok(())
    }

    #[derive(Debug)]
    struct RequestCountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        request_count: AtomicUsize,
    }

    impl Display for RequestCountingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RequestCounting({})", self.inner)
        }
    }

    impl RequestCountingObjectStore {
        pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                request_count: Default::default(),
            }
        }

        pub fn request_count(&self) -> usize {
            self.request_count.load(Ordering::SeqCst)
        }

        pub fn upcast(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
            self.clone()
        }
    }

    #[async_trait]
    impl ObjectStore for RequestCountingObjectStore {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Err(object_store::Error::NotImplemented)
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            Err(object_store::Error::NotImplemented)
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            Box::pin(futures::stream::once(async {
                Err(object_store::Error::NotImplemented)
            }))
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy_if_not_exists(
            &self,
            _from: &Path,
            _to: &Path,
        ) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }
    }

    #[tokio::test]
    async fn fetch_metadata_with_size_hint() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)]).unwrap();

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        // Use a size hint larger than the parquet footer but smaller than the actual metadata, requiring a second fetch
        // for the remaining metadata
        fetch_parquet_metadata(store.as_ref() as &dyn ObjectStore, &meta[0], Some(9))
            .await
            .expect("error reading metadata with hint");

        assert_eq!(store.request_count(), 2);

        let session = SessionContext::new();
        let ctx = session.state();
        let format = ParquetFormat::default().with_metadata_size_hint(Some(9));
        let schema = format
            .infer_schema(&ctx, &store.upcast(), &meta)
            .await
            .unwrap();

        let stats =
            fetch_statistics(store.upcast().as_ref(), schema.clone(), &meta[0], Some(9))
                .await?;

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.null_count, Precision::Exact(3));

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));

        // Use the file size as the hint so we can get the full metadata from the first fetch
        let size_hint = meta[0].size;

        fetch_parquet_metadata(store.upcast().as_ref(), &meta[0], Some(size_hint))
            .await
            .expect("error reading metadata with hint");

        // ensure the requests were coalesced into a single request
        assert_eq!(store.request_count(), 1);

        let format = ParquetFormat::default().with_metadata_size_hint(Some(size_hint));
        let schema = format
            .infer_schema(&ctx, &store.upcast(), &meta)
            .await
            .unwrap();
        let stats = fetch_statistics(
            store.upcast().as_ref(),
            schema.clone(),
            &meta[0],
            Some(size_hint),
        )
        .await?;

        assert_eq!(stats.num_rows, Precision::Exact(3));
        let c1_stats = &stats.column_statistics[0];
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.null_count, Precision::Exact(3));

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));

        // Use the a size hint larger than the file size to make sure we don't panic
        let size_hint = meta[0].size + 100;

        fetch_parquet_metadata(store.upcast().as_ref(), &meta[0], Some(size_hint))
            .await
            .expect("error reading metadata with hint");

        assert_eq!(store.request_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_from_parquet_metadata_dictionary() -> Result<()> {
        // Data for column c_dic: ["a", "b", "c", "d"]
        let values = StringArray::from_iter_values(["a", "b", "c", "d"]);
        let keys = Int32Array::from_iter_values([0, 1, 2, 3]);
        let dic_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let c_dic: ArrayRef = Arc::new(dic_array);

        let batch1 = RecordBatch::try_from_iter(vec![("c_dic", c_dic)]).unwrap();

        // Use store_parquet to write each batch to its own file
        // . batch1 written into first file and includes:
        //    - column c_dic that has 4 rows with no null. Stats min and max of dictionary column is available.
        let store = Arc::new(LocalFileSystem::new()) as _;
        let (files, _file_names) = store_parquet(vec![batch1], false).await?;

        let state = SessionContext::new().state();
        let format = ParquetFormat::default();
        let schema = format.infer_schema(&state, &store, &files).await.unwrap();

        // Fetch statistics for first file
        let pq_meta = fetch_parquet_metadata(store.as_ref(), &files[0], None).await?;
        let stats = statistics_from_parquet_meta(&pq_meta, schema.clone()).await?;
        assert_eq!(stats.num_rows, Precision::Exact(4));

        // column c_dic
        let c_dic_stats = &stats.column_statistics[0];

        assert_eq!(c_dic_stats.null_count, Precision::Exact(0));
        assert_eq!(
            c_dic_stats.max_value,
            Precision::Exact(Utf8(Some("d".into())))
        );
        assert_eq!(
            c_dic_stats.min_value,
            Precision::Exact(Utf8(Some("a".into())))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_from_parquet_metadata() -> Result<()> {
        // Data for column c1: ["Foo", null, "bar"]
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();

        // Data for column c2: [1, 2, null]
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)]).unwrap();

        // Use store_parquet to write each batch to its own file
        // . batch1 written into first file and includes:
        //    - column c1 that has 3 rows with one null. Stats min and max of string column is missing for this test even the column has values
        // . batch2 written into second file and includes:
        //    - column c2 that has 3 rows with one null. Stats min and max of int are avaialble and 1 and 2 respectively
        let store = Arc::new(LocalFileSystem::new()) as _;
        let (files, _file_names) = store_parquet(vec![batch1, batch2], false).await?;

        let state = SessionContext::new().state();
        let format = ParquetFormat::default();
        let schema = format.infer_schema(&state, &store, &files).await.unwrap();

        let null_i64 = ScalarValue::Int64(None);
        let null_utf8 = ScalarValue::Utf8(None);

        // Fetch statistics for first file
        let pq_meta = fetch_parquet_metadata(store.as_ref(), &files[0], None).await?;
        let stats = statistics_from_parquet_meta(&pq_meta, schema.clone()).await?;
        //
        assert_eq!(stats.num_rows, Precision::Exact(3));
        // column c1
        let c1_stats = &stats.column_statistics[0];
        assert_eq!(c1_stats.null_count, Precision::Exact(1));
        assert_eq!(
            c1_stats.max_value,
            Precision::Exact(ScalarValue::Utf8(Some("bar".to_string())))
        );
        assert_eq!(
            c1_stats.min_value,
            Precision::Exact(ScalarValue::Utf8(Some("Foo".to_string())))
        );
        // column c2: missing from the file so the table treats all 3 rows as null
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c2_stats.null_count, Precision::Exact(3));
        assert_eq!(c2_stats.max_value, Precision::Exact(null_i64.clone()));
        assert_eq!(c2_stats.min_value, Precision::Exact(null_i64.clone()));

        // Fetch statistics for second file
        let pq_meta = fetch_parquet_metadata(store.as_ref(), &files[1], None).await?;
        let stats = statistics_from_parquet_meta(&pq_meta, schema.clone()).await?;
        assert_eq!(stats.num_rows, Precision::Exact(3));
        // column c1: missing from the file so the table treats all 3 rows as null
        let c1_stats = &stats.column_statistics[0];
        assert_eq!(c1_stats.null_count, Precision::Exact(3));
        assert_eq!(c1_stats.max_value, Precision::Exact(null_utf8.clone()));
        assert_eq!(c1_stats.min_value, Precision::Exact(null_utf8.clone()));
        // column c2
        let c2_stats = &stats.column_statistics[1];
        assert_eq!(c2_stats.null_count, Precision::Exact(1));
        assert_eq!(c2_stats.max_value, Precision::Exact(2i64.into()));
        assert_eq!(c2_stats.min_value, Precision::Exact(1i64.into()));

        Ok(())
    }

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        // test metadata
        assert_eq!(exec.statistics()?.num_rows, Precision::Exact(8));
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Exact(671));

        Ok(())
    }

    #[tokio::test]
    async fn capture_bytes_scanned_metric() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session = SessionContext::new_with_config(config);
        let ctx = session.state();

        // Read the full file
        let projection = None;
        let exec = get_exec(&ctx, "alltypes_plain.parquet", projection, None).await?;

        // Read only one column. This should scan less data.
        let projection = Some(vec![0]);
        let exec_projected =
            get_exec(&ctx, "alltypes_plain.parquet", projection, None).await?;

        let task_ctx = ctx.task_ctx();

        let _ = collect(exec.clone(), task_ctx.clone()).await?;
        let _ = collect(exec_projected.clone(), task_ctx).await?;

        assert_bytes_scanned(exec, 671);
        assert_bytes_scanned(exec_projected, 73);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec =
            get_exec(&state, "alltypes_plain.parquet", projection, Some(1)).await?;

        // note: even if the limit is set, the executor rounds up to the batch size
        assert_eq!(exec.statistics()?.num_rows, Precision::Exact(8));
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Exact(671));
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(Nanosecond, None)",
            y
        );

        let batches = collect(exec, task_ctx).await?;

        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![10]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_timestamp_nanosecond_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float32_array(batches[0].column(0))?;
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![7]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float64_array(batches[0].column(0))?;
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_decimal_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();

        // parquet use the int32 as the physical type to store decimal
        let exec = get_exec(&state, "int32_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        // parquet use the int64 as the physical type to store decimal
        let exec = get_exec(&state, "int64_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(10, 2), column.data_type());

        // parquet use the fixed length binary as the physical type to store decimal
        let exec = get_exec(&state, "fixed_length_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(25, 2), column.data_type());

        let exec =
            get_exec(&state, "fixed_length_decimal_legacy.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(13, 2), column.data_type());

        // parquet use the byte array as the physical type to store decimal
        let exec = get_exec(&state, "byte_array_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        Ok(())
    }
    #[tokio::test]
    async fn test_read_parquet_page_index() -> Result<()> {
        let testdata = crate::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages.parquet");
        let file = File::open(path).await.unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(file, options.clone())
                .await
                .unwrap()
                .metadata()
                .clone();
        check_page_index_validation(builder.column_index(), builder.offset_index());

        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let file = File::open(path).await.unwrap();

        let builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
            .await
            .unwrap()
            .metadata()
            .clone();
        check_page_index_validation(builder.column_index(), builder.offset_index());

        Ok(())
    }

    fn check_page_index_validation(
        page_index: Option<&ParquetColumnIndex>,
        offset_index: Option<&ParquetOffsetIndex>,
    ) {
        assert!(page_index.is_some());
        assert!(offset_index.is_some());

        let page_index = page_index.unwrap();
        let offset_index = offset_index.unwrap();

        // there is only one row group in one file.
        assert_eq!(page_index.len(), 1);
        assert_eq!(offset_index.len(), 1);
        let page_index = page_index.first().unwrap();
        let offset_index = offset_index.first().unwrap();

        // 13 col in one row group
        assert_eq!(page_index.len(), 13);
        assert_eq!(offset_index.len(), 13);

        // test result in int_col
        let int_col_index = page_index.get(4).unwrap();
        let int_col_offset = offset_index.get(4).unwrap();

        // 325 pages in int_col
        assert_eq!(int_col_offset.len(), 325);
        match int_col_index {
            Index::INT32(index) => {
                assert_eq!(index.indexes.len(), 325);
                for min_max in index.clone().indexes {
                    assert!(min_max.min.is_some());
                    assert!(min_max.max.is_some());
                    assert!(min_max.null_count.is_some());
                }
            }
            _ => {
                error!("fail to read page index.")
            }
        }
    }

    fn assert_bytes_scanned(exec: Arc<dyn ExecutionPlan>, expected: usize) {
        let actual = exec
            .metrics()
            .expect("Metrics not recorded")
            .sum(|metric| matches!(metric.value(), MetricValue::Count { name, .. } if name == "bytes_scanned"))
            .map(|t| t.as_usize())
            .expect("bytes_scanned metric not recorded");

        assert_eq!(actual, expected);
    }

    async fn get_exec(
        state: &SessionState,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::parquet_test_data();
        let format = ParquetFormat::default();
        scan_format(state, &format, &testdata, file_name, projection, limit).await
    }

    fn build_ctx(store_url: &url::Url) -> Arc<TaskContext> {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let local = Arc::new(
            LocalFileSystem::new_with_prefix(&tmp_dir)
                .expect("should create object store"),
        );

        let mut session = SessionConfig::default();
        let mut parquet_opts = ParquetOptions {
            allow_single_file_parallelism: true,
            ..Default::default()
        };
        parquet_opts.allow_single_file_parallelism = true;
        session.options_mut().execution.parquet = parquet_opts;

        let runtime = RuntimeEnv::default();
        runtime
            .object_store_registry
            .register_store(store_url, local);

        Arc::new(
            TaskContext::default()
                .with_session_config(session)
                .with_runtime(Arc::new(runtime)),
        )
    }

    #[tokio::test]
    async fn parquet_sink_write() -> Result<()> {
        let field_a = Field::new("a", DataType::Utf8, false);
        let field_b = Field::new("b", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let object_store_url = ObjectStoreUrl::local_filesystem();

        let file_sink_config = FileSinkConfig {
            object_store_url: object_store_url.clone(),
            file_groups: vec![PartitionedFile::new("/tmp".to_string(), 1)],
            table_paths: vec![ListingTableUrl::parse("file:///")?],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            overwrite: true,
            keep_partition_by_columns: false,
        };
        let parquet_sink = Arc::new(ParquetSink::new(
            file_sink_config,
            TableParquetOptions {
                key_value_metadata: std::collections::HashMap::from([
                    ("my-data".to_string(), Some("stuff".to_string())),
                    ("my-data-bool-key".to_string(), None),
                ]),
                ..Default::default()
            },
        ));

        // create data
        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["baz", "baz"]));
        let batch = RecordBatch::try_from_iter(vec![("a", col_a), ("b", col_b)]).unwrap();

        // write stream
        parquet_sink
            .write_all(
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(vec![Ok(batch)]),
                )),
                &build_ctx(object_store_url.as_ref()),
            )
            .await
            .unwrap();

        // assert written
        let mut written = parquet_sink.written();
        let written = written.drain();
        assert_eq!(
            written.len(),
            1,
            "expected a single parquet files to be written, instead found {}",
            written.len()
        );

        // check the file metadata
        let (
            path,
            FileMetaData {
                num_rows,
                schema,
                key_value_metadata,
                ..
            },
        ) = written.take(1).next().unwrap();
        let path_parts = path.parts().collect::<Vec<_>>();
        assert_eq!(path_parts.len(), 1, "should not have path prefix");

        assert_eq!(num_rows, 2, "file metdata to have 2 rows");
        assert!(
            schema.iter().any(|col_schema| col_schema.name == "a"),
            "output file metadata should contain col a"
        );
        assert!(
            schema.iter().any(|col_schema| col_schema.name == "b"),
            "output file metadata should contain col b"
        );

        let mut key_value_metadata = key_value_metadata.unwrap();
        key_value_metadata.sort_by(|a, b| a.key.cmp(&b.key));
        let expected_metadata = vec![
            KeyValue {
                key: "my-data".to_string(),
                value: Some("stuff".to_string()),
            },
            KeyValue {
                key: "my-data-bool-key".to_string(),
                value: None,
            },
        ];
        assert_eq!(key_value_metadata, expected_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_sink_write_partitions() -> Result<()> {
        let field_a = Field::new("a", DataType::Utf8, false);
        let field_b = Field::new("b", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let object_store_url = ObjectStoreUrl::local_filesystem();

        // set file config to include partitioning on field_a
        let file_sink_config = FileSinkConfig {
            object_store_url: object_store_url.clone(),
            file_groups: vec![PartitionedFile::new("/tmp".to_string(), 1)],
            table_paths: vec![ListingTableUrl::parse("file:///")?],
            output_schema: schema.clone(),
            table_partition_cols: vec![("a".to_string(), DataType::Utf8)], // add partitioning
            overwrite: true,
            keep_partition_by_columns: false,
        };
        let parquet_sink = Arc::new(ParquetSink::new(
            file_sink_config,
            TableParquetOptions::default(),
        ));

        // create data with 2 partitions
        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["baz", "baz"]));
        let batch = RecordBatch::try_from_iter(vec![("a", col_a), ("b", col_b)]).unwrap();

        // write stream
        parquet_sink
            .write_all(
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(vec![Ok(batch)]),
                )),
                &build_ctx(object_store_url.as_ref()),
            )
            .await
            .unwrap();

        // assert written
        let mut written = parquet_sink.written();
        let written = written.drain();
        assert_eq!(
            written.len(),
            2,
            "expected two parquet files to be written, instead found {}",
            written.len()
        );

        // check the file metadata includes partitions
        let mut expected_partitions = std::collections::HashSet::from(["a=foo", "a=bar"]);
        for (
            path,
            FileMetaData {
                num_rows, schema, ..
            },
        ) in written.take(2)
        {
            let path_parts = path.parts().collect::<Vec<_>>();
            assert_eq!(path_parts.len(), 2, "should have path prefix");

            let prefix = path_parts[0].as_ref();
            assert!(
                expected_partitions.contains(prefix),
                "expected path prefix to match partition, instead found {:?}",
                prefix
            );
            expected_partitions.remove(prefix);

            assert_eq!(num_rows, 1, "file metdata to have 1 row");
            assert!(
                !schema.iter().any(|col_schema| col_schema.name == "a"),
                "output file metadata will not contain partitioned col a"
            );
            assert!(
                schema.iter().any(|col_schema| col_schema.name == "b"),
                "output file metadata should contain col b"
            );
        }

        Ok(())
    }
}
