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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, ParquetExecBuilder,
};
use datafusion::datasource::physical_plan::{
    parquet::ParquetFileReaderFactory, FileMeta, FileScanConfig,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::{
    internal_datafusion_err, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::utils::{Guarantee, LiteralGuarantee};
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::ObjectStore;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;
use datafusion_common::logical_type::schema::LogicalSchemaRef;

/// This example demonstrates using low level DataFusion APIs to read only
/// certain row groups and ranges from parquet files, based on external
/// information.
///
/// Using these APIs, you can instruct DataFusion's parquet reader to skip
/// ("prune") portions of files that do not contain relevant data. These APIs
/// can be useful for doing low latency queries over a large number of Parquet
/// files on remote storage (e.g. S3) where the cost of reading the metadata for
/// each file is high (e.g. because it requires a network round trip to the
/// storage service).
///
/// Depending on the information from the index, DataFusion can make a request
/// to the storage service (e.g. S3)  to read only the necessary data.
///
/// Note that this example uses a hard coded index implementation. For a more
/// realistic example of creating an index to prune files, see the
/// `parquet_index.rs` example.
///
/// Specifically, this example illustrates how to:
/// 1. Use [`ParquetFileReaderFactory`] to avoid re-reading parquet metadata on each query
/// 2. Use [`PruningPredicate`] for predicate analysis
/// 3. Pass a row group selection to [`ParuetExec`]
/// 4. Pass a row selection (within a row group) to [`ParquetExec`]
///
/// Note this is a *VERY* low level example for people who want to build their
/// own custom indexes (e.g. for low latency queries). Most users should use
/// higher level APIs for reading parquet files:
/// [`SessionContext::read_parquet`] or [`ListingTable`], which also do file
/// pruning based on parquet statistics (using the same underlying APIs)
///
/// # Diagram
///
/// This diagram shows how the `ParquetExec` is configured to do only a single
/// (range) read from a parquet file, for the data that is needed. It does
/// not read the file footer or any of the row groups that are not needed.
///
/// ```text
///         ┌───────────────────────┐ The TableProvider configures the
///         │ ┌───────────────────┐ │ ParquetExec:
///         │ │                   │ │
///         │ └───────────────────┘ │
///         │ ┌───────────────────┐ │
/// Row     │ │                   │ │  1. To read only specific Row
/// Groups  │ └───────────────────┘ │  Groups (the ParquetExec tries
///         │ ┌───────────────────┐ │  to reduce this further based
///         │ │                   │ │  on metadata)
///         │ └───────────────────┘ │              ┌────────────────────┐
///         │ ┌───────────────────┐ │              │                    │
///         │ │                   │◀┼ ─ ─ ┐        │    ParquetExec     │
///         │ └───────────────────┘ │              │  (Parquet Reader)  │
///         │          ...          │     └ ─ ─ ─ ─│                    │
///         │ ┌───────────────────┐ │              │ ╔═══════════════╗  │
///         │ │                   │ │              │ ║ParquetMetadata║  │
///         │ └───────────────────┘ │              │ ╚═══════════════╝  │
///         │ ╔═══════════════════╗ │              └────────────────────┘
///         │ ║  Thrift metadata  ║ │
///         │ ╚═══════════════════╝ │      1. With cached ParquetMetadata, so
///         └───────────────────────┘      the ParquetExec does not re-read /
///          Parquet File                  decode the thrift footer
///
/// ```
///
/// Within a Row Group, Column Chunks store data in DataPages. This example also
/// shows how to configure the ParquetExec to read a `RowSelection` (row ranges)
/// which will skip unneeded data pages. This requires that the Parquet file has
/// a [Page Index].
///
/// ```text
///         ┌───────────────────────┐   If the RowSelection does not include any
///         │          ...          │   rows from a particular Data Page, that
///         │                       │   Data Page is not fetched or decoded.
///         │ ┌───────────────────┐ │   Note this requires a PageIndex
///         │ │     ┌──────────┐  │ │
/// Row     │ │     │DataPage 0│  │ │                 ┌────────────────────┐
/// Groups  │ │     └──────────┘  │ │                 │                    │
///         │ │     ┌──────────┐  │ │                 │    ParquetExec     │
///         │ │ ... │DataPage 1│ ◀┼ ┼ ─ ─ ─           │  (Parquet Reader)  │
///         │ │     └──────────┘  │ │      └ ─ ─ ─ ─ ─│                    │
///         │ │     ┌──────────┐  │ │                 │ ╔═══════════════╗  │
///         │ │     │DataPage 2│  │ │ If only rows    │ ║ParquetMetadata║  │
///         │ │     └──────────┘  │ │ from DataPage 1 │ ╚═══════════════╝  │
///         │ └───────────────────┘ │ are selected,   └────────────────────┘
///         │                       │ only DataPage 1
///         │          ...          │ is fetched and
///         │                       │ decoded
///         │ ╔═══════════════════╗ │
///         │ ║  Thrift metadata  ║ │
///         │ ╚═══════════════════╝ │
///         └───────────────────────┘
///          Parquet File
/// ```
///
/// [`ListingTable`]: datafusion::datasource::listing::ListingTable
/// [Page Index](https://github.com/apache/parquet-format/blob/master/PageIndex.md)
#[tokio::main]
async fn main() -> Result<()> {
    // the object store is used to read the parquet files (in this case, it is
    // a local file system, but in a real system it could be S3, GCS, etc)
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());

    // Create a custom table provider with our special index.
    let provider = Arc::new(IndexTableProvider::try_new(Arc::clone(&object_store))?);

    // SessionContext for running queries that has the table provider
    // registered as "index_table"
    let ctx = SessionContext::new();
    ctx.register_table("index_table", Arc::clone(&provider) as _)?;

    // register object store provider for urls like `file://` work
    let url = Url::try_from("file://").unwrap();
    ctx.register_object_store(&url, object_store);

    // Select data from the table without any predicates (and thus no pruning)
    println!("** Select data, no predicates:");
    ctx.sql("SELECT avg(id), max(text) FROM index_table")
        .await?
        .show()
        .await?;
    // the underlying parquet reader makes 10 IO requests, one for each row group

    // Now, run a query that has a predicate that our index can handle
    //
    // For this query, the access plan specifies skipping 8 row groups
    // and scanning 2 of them. The skipped row groups are not read at all:
    //
    // [Skip, Skip, Scan, Skip, Skip, Skip, Skip, Scan, Skip, Skip]
    //
    // Note that the parquet reader makes 2 IO requests - one for the data from
    // each row group.
    println!("** Select data, predicate `id IN (250, 750)`");
    ctx.sql("SELECT text FROM index_table WHERE id IN (250, 750)")
        .await?
        .show()
        .await?;

    // Finally, demonstrate scanning sub ranges within the row groups.
    // Parquet's minimum decode unit is a page, so specifying ranges
    // within a row group can be used to skip pages within a row group.
    //
    // For this query, the access plan specifies skipping all but the last row
    // group and within the last row group, reading only the row with id 950
    //
    // [Skip, Skip, Skip, Skip, Skip, Skip, Skip, Skip, Skip, Selection(skip 49, select 1, skip 50)]
    //
    // Note that the parquet reader makes a single IO request - for the data
    // pages that must be decoded
    //
    // Note: in order to prune pages, the Page Index must be loaded and the
    // ParquetExec will load it on demand if not present. To avoid a second IO
    // during query, this example loaded the Page Index pre-emptively by setting
    // `ArrowReader::with_page_index` in `IndexedFile::try_new`
    provider.set_use_row_selection(true);
    println!("** Select data, predicate `id = 950`");
    ctx.sql("SELECT text  FROM index_table WHERE id = 950")
        .await?
        .show()
        .await?;

    Ok(())
}

/// DataFusion `TableProvider` that uses knowledge of how data is distributed in
/// a file to prune row groups and rows from the file.
///
/// `file1.parquet` contains values `0..1000`
#[derive(Debug)]
pub struct IndexTableProvider {
    /// Where the file is stored (cleanup on drop)
    #[allow(dead_code)]
    tmpdir: TempDir,
    /// The file that is being read.
    indexed_file: IndexedFile,
    /// The underlying object store
    object_store: Arc<dyn ObjectStore>,
    /// if true, use row selections in addition to row group selections
    use_row_selections: AtomicBool,
}
impl IndexTableProvider {
    /// Create a new IndexTableProvider
    /// * `object_store` - the object store implementation to use for reading files
    pub fn try_new(object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let tmpdir = TempDir::new().expect("Can't make temporary directory");

        let indexed_file =
            IndexedFile::try_new(tmpdir.path().join("indexed_file.parquet"), 0..1000)?;

        Ok(Self {
            indexed_file,
            tmpdir,
            object_store,
            use_row_selections: AtomicBool::new(false),
        })
    }

    /// set the value of use row selections
    pub fn set_use_row_selection(&self, use_row_selections: bool) {
        self.use_row_selections
            .store(use_row_selections, Ordering::SeqCst);
    }

    /// return the value of use row selections
    pub fn use_row_selections(&self) -> bool {
        self.use_row_selections.load(Ordering::SeqCst)
    }

    /// convert filters like `a = 1`, `b = 2`
    /// to a single predicate like `a = 1 AND b = 2` suitable for execution
    fn filters_to_predicate(
        &self,
        state: &SessionState,
        filters: &[Expr],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let df_schema = DFSchema::try_from(self.schema())?;

        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &df_schema))
            .transpose()?
            // if there are no filters, use a literal true to have a predicate
            // that always evaluates to true we can pass to the index
            .unwrap_or_else(|| datafusion_physical_expr::expressions::lit(true));

        Ok(predicate)
    }

    /// Returns a [`ParquetAccessPlan`] that specifies how to scan the
    /// parquet file.
    ///
    /// A `ParquetAccessPlan` specifies which row groups  and which rows within
    /// those row groups to scan.
    fn create_plan(
        &self,
        predicate: &Arc<dyn PhysicalExpr>,
    ) -> Result<ParquetAccessPlan> {
        // In this example, we use the PruningPredicate's literal guarantees to
        // analyze the predicate. In a real system, using
        // `PruningPredicate::prune` would likely be easier to do.
        let schema = SchemaRef::new(self.schema().as_ref().clone().into());
        let pruning_predicate =
            PruningPredicate::try_new(Arc::clone(predicate), schema)?;

        // The PruningPredicate's guarantees must all be satisfied in order for
        // the predicate to possibly evaluate to true.
        let guarantees = pruning_predicate.literal_guarantees();
        let Some(constants) = self.value_constants(guarantees) else {
            return Ok(self.indexed_file.scan_all_plan());
        };

        // Begin with a plan that skips all row groups.
        let mut plan = self.indexed_file.scan_none_plan();

        // determine which row groups have the values in the guarantees
        for value in constants {
            let ScalarValue::Int32(Some(val)) = value else {
                // if we have unexpected type of constant, no pruning is possible
                return Ok(self.indexed_file.scan_all_plan());
            };

            // Since we know the values in the files are between 0..1000 and
            // evenly distributed between  in row groups, calculate in what row
            // group this value appears and tell the parquet reader to read it
            let val = *val as usize;
            let num_rows_in_row_group = 1000 / plan.len();
            let row_group_index = val / num_rows_in_row_group;
            plan.scan(row_group_index);

            // If we want to use row selections, which the parquet reader can
            // use to skip data pages when the parquet file has a "page index"
            // and the reader is configured to read it, add a row selection
            if self.use_row_selections() {
                let offset_in_row_group = val - row_group_index * num_rows_in_row_group;
                let selection = RowSelection::from(vec![
                    // skip rows before the desired row
                    RowSelector::skip(offset_in_row_group.saturating_sub(1)),
                    // select the actual row
                    RowSelector::select(1),
                    // skip any remaining rows in the group
                    RowSelector::skip(num_rows_in_row_group - offset_in_row_group),
                ]);

                plan.scan_selection(row_group_index, selection);
            }
        }

        Ok(plan)
    }

    /// Returns the set of constants that the `"id"` column must take in order
    /// for the predicate to be true.
    ///
    /// If `None` is returned, we can't extract the necessary information from
    /// the guarantees.
    fn value_constants<'a>(
        &self,
        guarantees: &'a [LiteralGuarantee],
    ) -> Option<&'a HashSet<ScalarValue>> {
        // only handle a single guarantee for column in this example
        if guarantees.len() != 1 {
            return None;
        }
        let guarantee = guarantees.first()?;

        // Only handle IN guarantees for the "in" column
        if guarantee.guarantee != Guarantee::In || guarantee.column.name() != "id" {
            return None;
        }
        Some(&guarantee.literals)
    }
}

/// Stores information needed to scan a file
#[derive(Debug)]
struct IndexedFile {
    /// File name
    file_name: String,
    /// The path of the file
    path: PathBuf,
    /// The size of the file
    file_size: u64,
    /// The pre-parsed parquet metadata for the file
    metadata: Arc<ParquetMetaData>,
    /// The arrow schema of the file
    schema: SchemaRef,
}

impl IndexedFile {
    fn try_new(path: impl AsRef<Path>, value_range: Range<i32>) -> Result<Self> {
        let path = path.as_ref();
        // write the actual file
        make_demo_file(path, value_range)?;

        // Now, open the file and read its size and metadata
        let file_name = path
            .file_name()
            .ok_or_else(|| internal_datafusion_err!("Invalid path"))?
            .to_str()
            .ok_or_else(|| internal_datafusion_err!("Invalid filename"))?
            .to_string();
        let file_size = path.metadata()?.len();

        let file = File::open(path).map_err(|e| {
            DataFusionError::from(e).context(format!("Error opening file {path:?}"))
        })?;

        let options = ArrowReaderOptions::new()
            // Load the page index when reading metadata to cache
            // so it is available to interpret row selections
            .with_page_index(true);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
        let metadata = reader.metadata().clone();
        let schema = reader.schema().clone();

        // canonicalize after writing the file
        let path = std::fs::canonicalize(path)?;

        Ok(Self {
            file_name,
            path,
            file_size,
            metadata,
            schema,
        })
    }

    /// Return a `PartitionedFile` to scan the underlying file
    ///
    /// The returned value does not have any  `ParquetAccessPlan` specified in
    /// its extensions.
    fn partitioned_file(&self) -> PartitionedFile {
        PartitionedFile::new(self.path.display().to_string(), self.file_size)
    }

    /// Return a `ParquetAccessPlan` that scans all row groups in the file
    fn scan_all_plan(&self) -> ParquetAccessPlan {
        ParquetAccessPlan::new_all(self.metadata.num_row_groups())
    }

    /// Return a `ParquetAccessPlan` that scans no row groups in the file
    fn scan_none_plan(&self) -> ParquetAccessPlan {
        ParquetAccessPlan::new_none(self.metadata.num_row_groups())
    }
}

/// Implement the TableProvider trait for IndexTableProvider
/// so that we can query it as a table.
#[async_trait]
impl TableProvider for IndexTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.indexed_file.schema.as_ref().clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let indexed_file = &self.indexed_file;
        let predicate = self.filters_to_predicate(state, filters)?;

        // Figure out which row groups to scan based on the predicate
        let access_plan = self.create_plan(&predicate)?;
        println!("{access_plan:?}");

        let partitioned_file = indexed_file
            .partitioned_file()
            // provide the starting access plan to the ParquetExec by
            // storing it as  "extensions" on PartitionedFile
            .with_extensions(Arc::new(access_plan) as _);

        // Prepare for scanning
        let schema = SchemaRef::new(self.schema().as_ref().clone().into());
        let object_store_url = ObjectStoreUrl::parse("file://")?;
        let file_scan_config = FileScanConfig::new(object_store_url, schema)
            .with_limit(limit)
            .with_projection(projection.cloned())
            .with_file(partitioned_file);

        // Configure a factory interface to avoid re-reading the metadata for each file
        let reader_factory =
            CachedParquetFileReaderFactory::new(Arc::clone(&self.object_store))
                .with_file(indexed_file);

        // Finally, put it all together into a ParquetExec
        Ok(ParquetExecBuilder::new(file_scan_config)
            // provide the predicate so the ParquetExec can try and prune
            // row groups internally
            .with_predicate(predicate)
            // provide the factory to create parquet reader without re-reading metadata
            .with_parquet_file_reader_factory(Arc::new(reader_factory))
            .build_arc())
    }

    /// Tell DataFusion to push filters down to the scan method
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Inexact because the pruning can't handle all expressions and pruning
        // is not done at the row level -- there may be rows in returned files
        // that do not pass the filter
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// A custom [`ParquetFileReaderFactory`] that handles opening parquet files
/// from object storage, and uses pre-loaded metadata.

#[derive(Debug)]
struct CachedParquetFileReaderFactory {
    /// The underlying object store implementation for reading file data
    object_store: Arc<dyn ObjectStore>,
    /// The parquet metadata for each file in the index, keyed by the file name
    /// (e.g. `file1.parquet`)
    metadata: HashMap<String, Arc<ParquetMetaData>>,
}

impl CachedParquetFileReaderFactory {
    fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            metadata: HashMap::new(),
        }
    }
    /// Add the pre-parsed information about the file to the factor
    fn with_file(mut self, indexed_file: &IndexedFile) -> Self {
        self.metadata.insert(
            indexed_file.file_name.clone(),
            Arc::clone(&indexed_file.metadata),
        );
        self
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        // for this example we ignore the partition index and metrics
        // but in a real system you would likely use them to report details on
        // the performance of the reader.
        let filename = file_meta
            .location()
            .parts()
            .last()
            .expect("No path in location")
            .as_ref()
            .to_string();

        let object_store = Arc::clone(&self.object_store);
        let mut inner = ParquetObjectReader::new(object_store, file_meta.object_meta);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        let metadata = self
            .metadata
            .get(&filename)
            .expect("metadata for file not found: {filename}");
        Ok(Box::new(ParquetReaderWithCache {
            filename,
            metadata: Arc::clone(metadata),
            inner,
        }))
    }
}

/// wrapper around a ParquetObjectReader that caches metadata
struct ParquetReaderWithCache {
    filename: String,
    metadata: Arc<ParquetMetaData>,
    inner: ParquetObjectReader,
}

impl AsyncFileReader for ParquetReaderWithCache {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        println!("get_bytes: {} Reading range {:?}", self.filename, range);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        println!(
            "get_byte_ranges: {} Reading ranges {:?}",
            self.filename, ranges
        );
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        println!("get_metadata: {} returning cached metadata", self.filename);

        // return the cached metadata so the parquet reader does not read it
        let metadata = self.metadata.clone();
        async move { Ok(metadata) }.boxed()
    }
}

/// Creates a new parquet file at the specified path.
///
/// * id: Int32
/// * text: Utf8
///
/// The `id` column  increases sequentially from `min_value` to `max_value`
/// The `text` column is a repeating sequence of `TheTextValue{i}`
///
/// Each row group has 100 rows
fn make_demo_file(path: impl AsRef<Path>, value_range: Range<i32>) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path)?;

    let id = Int32Array::from_iter_values(value_range.clone());
    let text =
        StringArray::from_iter_values(value_range.map(|i| format!("TheTextValue{i}")));

    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(id) as ArrayRef),
        ("text", Arc::new(text) as ArrayRef),
    ])?;

    let schema = batch.schema();

    // enable page statistics for the tag column,
    // for everything else.
    let props = WriterProperties::builder()
        .set_max_row_group_size(100)
        // compute column chunk (per row group) statistics by default
        .set_statistics_enabled(EnabledStatistics::Chunk)
        // compute column page statistics for the tag column
        .set_column_statistics_enabled(ColumnPath::from("tag"), EnabledStatistics::Page)
        .build();

    // write the actual values to the file
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
