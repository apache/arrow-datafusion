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

//! Parquet format abstractions

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use datafusion_common::DataFusionError;
use hashbrown::HashMap;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics as ParquetStatistics;

use super::FileFormat;
use super::FileScanConfig;
use crate::arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use crate::arrow::datatypes::{DataType, Field};
use crate::datasource::{create_max_min_accs, get_col_stats};
use crate::error::Result;
use crate::logical_plan::combine_filters;
use crate::logical_plan::Expr;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::file_format::{ParquetExec, SchemaAdapter};
use crate::physical_plan::{Accumulator, ExecutionPlan, Statistics};

/// The default file extension of parquet files
pub const DEFAULT_PARQUET_EXTENSION: &str = ".parquet";

/// The Apache Parquet `FileFormat` implementation
#[derive(Debug)]
pub struct ParquetFormat {
    enable_pruning: bool,
    metadata_size_hint: Option<usize>,
    skip_metadata: bool,
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self {
            enable_pruning: true,
            metadata_size_hint: None,
            skip_metadata: true,
        }
    }
}

impl ParquetFormat {
    /// Activate statistics based row group level pruning
    /// - defaults to true
    pub fn with_enable_pruning(mut self, enable: bool) -> Self {
        self.enable_pruning = enable;
        self
    }

    /// Provide a hint to the size of the file metadata. If a hint is provided
    /// the reader will try and fetch the last `size_hint` bytes of the parquet file optimistically.
    /// With out a hint, two read are required. One read to fetch the 8-byte parquet footer and then
    /// another read to fetch the metadata length encoded in the footer.
    pub fn with_metadata_size_hint(mut self, size_hint: usize) -> Self {
        self.metadata_size_hint = Some(size_hint);
        self
    }
    /// Return true if pruning is enabled
    pub fn enable_pruning(&self) -> bool {
        self.enable_pruning
    }

    /// Return the metadata size hint if set
    pub fn metadata_size_hint(&self) -> Option<usize> {
        self.metadata_size_hint
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.  Defaults to true.
    pub fn with_skip_metadata(mut self, skip_metadata: bool) -> Self {
        self.skip_metadata = skip_metadata;
        self
    }

    /// returns true if schema metadata will be cleared prior to
    /// schema merging.
    pub fn skip_metadata(&self) -> bool {
        self.skip_metadata
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
                field.clone().with_metadata(None) // clear meta
            })
            .collect::<Vec<_>>();
        Schema::new(fields)
    })
}

#[async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::with_capacity(objects.len());
        for object in objects {
            let schema =
                fetch_schema(store.as_ref(), object, self.metadata_size_hint).await?;
            schemas.push(schema)
        }

        let schema = if self.skip_metadata {
            Schema::try_merge(clear_metadata(schemas))
        } else {
            Schema::try_merge(schemas)
        }?;

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let stats = fetch_statistics(
            store.as_ref(),
            table_schema,
            object,
            self.metadata_size_hint,
        )
        .await?;
        Ok(stats)
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = if self.enable_pruning {
            combine_filters(filters)
        } else {
            None
        };

        Ok(Arc::new(ParquetExec::new(
            conf,
            predicate,
            self.metadata_size_hint(),
        )))
    }
}

fn summarize_min_max(
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
    fields: &[Field],
    i: usize,
    stat: &ParquetStatistics,
) {
    match stat {
        ParquetStatistics::Boolean(s) => {
            if let DataType::Boolean = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(BooleanArray::from(
                            vec![Some(*s.max())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(BooleanArray::from(
                            vec![Some(*s.min())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Int32(s) => {
            if let DataType::Int32 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Int32Array::from_value(
                            *s.max(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Int32Array::from_value(
                            *s.min(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Int64(s) => {
            if let DataType::Int64 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Int64Array::from_value(
                            *s.max(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Int64Array::from_value(
                            *s.min(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Float(s) => {
            if let DataType::Float32 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Float32Array::from(
                            vec![Some(*s.max())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Float32Array::from(
                            vec![Some(*s.min())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Double(s) => {
            if let DataType::Float64 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Float64Array::from(
                            vec![Some(*s.max())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Float64Array::from(
                            vec![Some(*s.min())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        _ => {
            max_values[i] = None;
            min_values[i] = None;
        }
    }
}

pub(crate) async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
    size_hint: Option<usize>,
) -> Result<ParquetMetaData> {
    if meta.size < 8 {
        return Err(DataFusionError::Execution(format!(
            "file size of {} is less than footer",
            meta.size
        )));
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
        return Err(DataFusionError::Execution(format!(
            "file size of {} is less than footer + metadata {}",
            meta.size,
            length + 8
        )));
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
async fn fetch_statistics(
    store: &dyn ObjectStore,
    table_schema: SchemaRef,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Statistics> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    let file_metadata = metadata.file_metadata();

    let file_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    let num_fields = table_schema.fields().len();
    let fields = table_schema.fields().to_vec();

    let mut num_rows = 0;
    let mut total_byte_size = 0;
    let mut null_counts = vec![0; num_fields];
    let mut has_statistics = false;

    let schema_adapter = SchemaAdapter::new(table_schema.clone());

    let (mut max_values, mut min_values) = create_max_min_accs(&table_schema);

    for row_group_meta in metadata.row_groups() {
        num_rows += row_group_meta.num_rows();
        total_byte_size += row_group_meta.total_byte_size();

        let mut column_stats: HashMap<usize, (u64, &ParquetStatistics)> = HashMap::new();

        for (i, column) in row_group_meta.columns().iter().enumerate() {
            if let Some(stat) = column.statistics() {
                has_statistics = true;
                column_stats.insert(i, (stat.null_count(), stat));
            }
        }

        if has_statistics {
            for (table_idx, null_cnt) in null_counts.iter_mut().enumerate() {
                if let Some(file_idx) =
                    schema_adapter.map_column_index(table_idx, &file_schema)
                {
                    if let Some((null_count, stats)) = column_stats.get(&file_idx) {
                        *null_cnt += *null_count as usize;
                        summarize_min_max(
                            &mut max_values,
                            &mut min_values,
                            &fields,
                            table_idx,
                            stats,
                        )
                    } else {
                        // If none statistics of current column exists, set the Max/Min Accumulator to None.
                        max_values[table_idx] = None;
                        min_values[table_idx] = None;
                    }
                } else {
                    *null_cnt += num_rows as usize;
                }
            }
        }
    }

    let column_stats = if has_statistics {
        Some(get_col_stats(
            &table_schema,
            null_counts,
            &mut max_values,
            &mut min_values,
        ))
    } else {
        None
    };

    let statistics = Statistics {
        num_rows: Some(num_rows as usize),
        total_byte_size: Some(total_byte_size as usize),
        column_statistics: column_stats,
        is_exact: true,
    };

    Ok(statistics)
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::test::object_store::local_unpartitioned_file;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::NamedTempFile;

    pub async fn store_parquet(
        batches: Vec<RecordBatch>,
    ) -> Result<(Vec<ObjectMeta>, Vec<NamedTempFile>)> {
        let files: Vec<_> = batches
            .into_iter()
            .map(|batch| {
                let mut output = NamedTempFile::new().expect("creating temp file");

                let props = WriterProperties::builder().build();
                let mut writer =
                    ArrowWriter::try_new(&mut output, batch.schema(), Some(props))
                        .expect("creating writer");

                writer.write(&batch).expect("Writing batch");
                writer.close().unwrap();
                output
            })
            .collect();

        let meta: Vec<_> = files.iter().map(local_unpartitioned_file).collect();
        Ok((meta, files))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use crate::physical_plan::collect;
    use std::fmt::{Display, Formatter};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::physical_plan::metrics::MetricValue;
    use crate::prelude::{SessionConfig, SessionContext};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array,
        Int32Array, StringArray, TimestampNanosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion_common::ScalarValue;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{GetResult, ListResult, MultipartId};
    use tokio::io::AsyncWrite;

    #[tokio::test]
    async fn read_merged_batches() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)]).unwrap();

        let store = Arc::new(LocalFileSystem::new()) as _;
        let (meta, _files) = store_parquet(vec![batch1, batch2]).await?;

        let format = ParquetFormat::default();
        let schema = format.infer_schema(&store, &meta).await.unwrap();

        let stats =
            fetch_statistics(store.as_ref(), schema.clone(), &meta[0], None).await?;

        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(1));
        assert_eq!(c2_stats.null_count, Some(3));

        let stats = fetch_statistics(store.as_ref(), schema, &meta[1], None).await?;
        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(3));
        assert_eq!(c2_stats.null_count, Some(1));
        assert_eq!(c2_stats.max_value, Some(ScalarValue::Int64(Some(2))));
        assert_eq!(c2_stats.min_value, Some(ScalarValue::Int64(Some(1))));

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
        async fn put(&self, _location: &Path, _bytes: Bytes) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)>
        {
            Err(object_store::Error::NotImplemented)
        }

        async fn abort_multipart(
            &self,
            _location: &Path,
            _multipart_id: &MultipartId,
        ) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn get(&self, _location: &Path) -> object_store::Result<GetResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn get_range(
            &self,
            location: &Path,
            range: Range<usize>,
        ) -> object_store::Result<Bytes> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(location, range).await
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            Err(object_store::Error::NotImplemented)
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>>
        {
            Err(object_store::Error::NotImplemented)
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
        let (meta, _files) = store_parquet(vec![batch1, batch2]).await?;

        // Use a size hint larger than the parquet footer but smaller than the actual metadata, requiring a second fetch
        // for the remaining metadata
        fetch_parquet_metadata(store.as_ref() as &dyn ObjectStore, &meta[0], Some(9))
            .await
            .expect("error reading metadata with hint");

        assert_eq!(store.request_count(), 2);

        let format = ParquetFormat::default().with_metadata_size_hint(9);
        let schema = format.infer_schema(&store.upcast(), &meta).await.unwrap();

        let stats =
            fetch_statistics(store.upcast().as_ref(), schema.clone(), &meta[0], Some(9))
                .await?;

        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(1));
        assert_eq!(c2_stats.null_count, Some(3));

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

        let format = ParquetFormat::default().with_metadata_size_hint(size_hint);
        let schema = format.infer_schema(&store.upcast(), &meta).await.unwrap();
        let stats = fetch_statistics(
            store.upcast().as_ref(),
            schema.clone(),
            &meta[0],
            Some(size_hint),
        )
        .await?;

        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(1));
        assert_eq!(c2_stats.null_count, Some(3));

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
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let ctx = SessionContext::with_config(config);
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;
        let task_ctx = ctx.task_ctx();
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
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn capture_bytes_scanned_metric() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let ctx = SessionContext::with_config(config);

        // Read the full file
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        // Read only one column. This should scan less data.
        let projection = Some(vec![0]);
        let exec_projected = get_exec("alltypes_plain.parquet", projection, None).await?;

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
        let task_ctx = session_ctx.task_ctx();
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", projection, Some(1)).await?;

        // note: even if the limit is set, the executor rounds up to the batch size
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));
        assert!(exec.statistics().is_exact);
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

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
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![1]);
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![10]);
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![6]);
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![7]);
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec("alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_decimal_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // parquet use the int32 as the physical type to store decimal
        let exec = get_exec("int32_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        // parquet use the int64 as the physical type to store decimal
        let exec = get_exec("int64_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(10, 2), column.data_type());

        // parquet use the fixed length binary as the physical type to store decimal
        let exec = get_exec("fixed_length_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(25, 2), column.data_type());

        let exec = get_exec("fixed_length_decimal_legacy.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(13, 2), column.data_type());

        // parquet use the fixed length binary as the physical type to store decimal
        // TODO: arrow-rs don't support convert the physical type of binary to decimal
        // https://github.com/apache/arrow-rs/pull/2160
        // let exec = get_exec("byte_array_decimal.parquet", None, None).await?;

        Ok(())
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
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::parquet_test_data();
        let format = ParquetFormat::default();
        scan_format(&format, &testdata, file_name, projection, limit).await
    }
}
