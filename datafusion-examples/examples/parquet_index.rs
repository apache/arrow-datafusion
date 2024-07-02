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

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int32Array, RecordBatch, StringArray,
    UInt64Array,
};
use arrow::datatypes::Int32Type;
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    parquet::StatisticsConverter,
    {FileScanConfig, ParquetExec},
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::{
    arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter,
};
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::{
    internal_datafusion_err, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{utils::conjunction, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::PhysicalExpr;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Display;
use std::fs::{self, DirEntry, File};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tempfile::TempDir;
use url::Url;
use datafusion_common::logical_type::schema::LogicalSchemaRef;

/// This example demonstrates building a secondary index over multiple Parquet
/// files and using that index during query to skip ("prune") files that do not
/// contain relevant data.
///
/// This example rules out relevant data using min/max values of a column
/// extracted from the Parquet metadata. In a real system, the index could be
/// more sophisticated, e.g. using inverted indices, bloom filters or other
/// techniques.
///
/// Note this is a low level example for people who want to build their own
/// custom indexes. To read a directory of parquet files as a table, you can use
/// a higher level API such as [`SessionContext::read_parquet`] or
/// [`ListingTable`], which also do file pruning based on parquet statistics
/// (using the same underlying APIs)
///
/// For a more advanced example of using an index to prune row groups within a
/// file, see the (forthcoming) `advanced_parquet_index` example.
///
/// # Diagram
///
/// ```text
///                                   ┏━━━━━━━━━━━━━━━━━━━━━━━━┓
///                                   ┃     Index              ┃
///                                   ┃                        ┃
///  step 1: predicate is   ┌ ─ ─ ─ ─▶┃ (sometimes referred to ┃
///  evaluated against                ┃ as a "catalog" or      ┃
///  data in the index      │         ┃ "metastore")           ┃
///  (using                           ┗━━━━━━━━━━━━━━━━━━━━━━━━┛
///  PruningPredicate)      │                      │
///
///                         │                      │
/// ┌──────────────┐
/// │  value = 150 │─ ─ ─ ─ ┘                      │
/// └──────────────┘                                   ┌─────────────┐
///  Predicate from query                          │   │             │
///                                                    └─────────────┘
///                                                │   ┌─────────────┐
///                   step 2: Index returns only    ─ ▶│             │
///                   parquet files that might have    └─────────────┘
///                   matching data.                         ...
///                                                    ┌─────────────┐
///                   Thus some parquet files are      │             │
///                   "pruned" and thus are not        └─────────────┘
///                   scanned at all                   Parquet Files
///
/// ```
///
/// [`ListingTable`]: datafusion::datasource::listing::ListingTable
#[tokio::main]
async fn main() -> Result<()> {
    // Demo data has three files, each with schema
    // * file_name (string)
    // * value (int32)
    //
    // The files are as follows:
    // * file1.parquet (value: 0..100)
    // * file2.parquet (value: 100..200)
    // * file3.parquet (value: 200..3000)
    let data = DemoData::try_new()?;

    // Create a table provider with and  our special index.
    let provider = Arc::new(IndexTableProvider::try_new(data.path())?);
    println!("** Table Provider:");
    println!("{provider}\n");

    // Create a SessionContext for running queries that has the table provider
    // registered as "index_table"
    let ctx = SessionContext::new();
    ctx.register_table("index_table", Arc::clone(&provider) as _)?;

    // register object store provider for urls like `file://` work
    let url = Url::try_from("file://").unwrap();
    let object_store = object_store::local::LocalFileSystem::new();
    ctx.register_object_store(&url, Arc::new(object_store));

    // Select data from the table without any predicates (and thus no pruning)
    println!("** Select data, no predicates:");
    ctx.sql("SELECT file_name, value FROM index_table LIMIT 10")
        .await?
        .show()
        .await?;
    println!("Files pruned: {}\n", provider.index().last_num_pruned());

    // Run a query that uses the index to prune files.
    //
    // Using the predicate "value = 150", the IndexTable can skip reading file 1
    // (max value 100) and file 3 (min value of 200)
    println!("** Select data, predicate `value = 150`");
    ctx.sql("SELECT file_name, value FROM index_table WHERE value = 150")
        .await?
        .show()
        .await?;
    println!("Files pruned: {}\n", provider.index().last_num_pruned());

    // likewise, we can use a more complicated predicate like
    // "value < 20 OR value > 500" to read only file 1 and file 3
    println!("** Select data, predicate `value < 20 OR value > 500`");
    ctx.sql(
        "SELECT file_name, count(value) FROM index_table \
            WHERE value < 20 OR value > 500 GROUP BY file_name",
    )
    .await?
    .show()
    .await?;
    println!("Files pruned: {}\n", provider.index().last_num_pruned());

    Ok(())
}

/// DataFusion `TableProvider` that uses [`IndexTableProvider`], a secondary
/// index to decide which Parquet files to read.
#[derive(Debug)]
pub struct IndexTableProvider {
    /// The index of the parquet files in the directory
    index: ParquetMetadataIndex,
    /// the directory in which the files are stored
    dir: PathBuf,
}

impl Display for IndexTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IndexTableProvider")?;
        writeln!(f, "---- Index ----")?;
        write!(f, "{}", self.index)
    }
}

impl IndexTableProvider {
    /// Create a new IndexTableProvider
    pub fn try_new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();

        // Create an index of the parquet files in the directory as we see them.
        let mut index_builder = ParquetMetadataIndexBuilder::new();

        let files = read_dir(&dir)?;
        for file in &files {
            index_builder.add_file(&file.path())?;
        }

        let index = index_builder.build()?;

        Ok(Self { index, dir })
    }

    /// return a reference to the underlying index
    fn index(&self) -> &ParquetMetadataIndex {
        &self.index
    }
}

#[async_trait]
impl TableProvider for IndexTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.index.schema().into())
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
        let df_schema = DFSchema::try_from(self.schema())?;
        // convert filters like [`a = 1`, `b = 2`] to a single filter like `a = 1 AND b = 2`
        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &df_schema))
            .transpose()?
            // if there are no filters, use a literal true to have a predicate
            // that always evaluates to true we can pass to the index
            .unwrap_or_else(|| datafusion_physical_expr::expressions::lit(true));

        // Use the index to find the files that might have data that matches the
        // predicate. Any file that can not have data that matches the predicate
        // will not be returned.
        let files = self.index.get_files(predicate.clone())?;

        let object_store_url = ObjectStoreUrl::parse("file://")?;
        let schema = SchemaRef::new(self.schema().as_ref().clone().into());
        let mut file_scan_config = FileScanConfig::new(object_store_url, schema)
            .with_projection(projection.cloned())
            .with_limit(limit);

        // Transform to the format needed to pass to ParquetExec
        // Create one file group per file (default to scanning them all in parallel)
        for (file_name, file_size) in files {
            let path = self.dir.join(file_name);
            let canonical_path = fs::canonicalize(path)?;
            file_scan_config = file_scan_config.with_file(PartitionedFile::new(
                canonical_path.display().to_string(),
                file_size,
            ));
        }
        let exec = ParquetExec::builder(file_scan_config)
            .with_predicate(predicate)
            .build_arc();

        Ok(exec)
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

/// Simple in memory secondary index for a set of parquet files
///
/// The index is represented as an arrow [`RecordBatch`] that can be passed
/// directly by the DataFusion [`PruningPredicate`] API
///
/// The `RecordBatch` looks as follows.
///
/// ```text
/// +---------------+-----------+-----------+------------------+------------------+
/// | file_name     | file_size | row_count | value_column_min | value_column_max |
/// +---------------+-----------+-----------+------------------+------------------+
/// | file1.parquet | 6062      | 100       | 0                | 99               |
/// | file2.parquet | 6062      | 100       | 100              | 199              |
/// | file3.parquet | 163310    | 2800      | 200              | 2999             |
/// +---------------+-----------+-----------+------------------+------------------+
/// ```
///
/// It must store file_name and file_size to construct `PartitionedFile`.
///
/// Note a more advanced index might store finer grained information, such as information
/// about each row group within a file
#[derive(Debug)]
struct ParquetMetadataIndex {
    file_schema: SchemaRef,
    /// The index of the parquet files. See the struct level documentation for
    /// the schema of this index.
    index: RecordBatch,
    /// The number of files that were pruned in the last query
    last_num_pruned: AtomicUsize,
}

impl Display for ParquetMetadataIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "ParquetMetadataIndex(last_num_pruned: {})",
            self.last_num_pruned()
        )?;
        let batches = pretty_format_batches(&[self.index.clone()]).unwrap();
        write!(f, "{batches}",)
    }
}

impl ParquetMetadataIndex {
    ///  the schema of the *files* in the index (not the index's schema)
    fn schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    ///  number of files in the index
    fn len(&self) -> usize {
        self.index.num_rows()
    }

    /// Return a [`PartitionedFile`] for the specified file offset
    ///
    /// For example, if the index batch contained data like
    ///
    /// ```text
    /// fileA
    /// fileB
    /// fileC
    /// ```
    ///
    /// `get_file(1)` would return `(fileB, size)`
    fn get_file(&self, file_offset: usize) -> (&str, u64) {
        // Filenames and sizes are always non null, so we don't have to check is_valid
        let file_name = self.file_names().value(file_offset);
        let file_size = self.file_size().value(file_offset);
        (file_name, file_size)
    }

    /// Return the number of files that were pruned in the last query
    pub fn last_num_pruned(&self) -> usize {
        self.last_num_pruned.load(Ordering::SeqCst)
    }

    /// Set the number of files that were pruned in the last query
    fn set_last_num_pruned(&self, num_pruned: usize) {
        self.last_num_pruned.store(num_pruned, Ordering::SeqCst);
    }

    /// Return all the files matching the predicate
    ///
    /// Returns a tuple `(file_name, file_size)`
    pub fn get_files(
        &self,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Result<Vec<(&str, u64)>> {
        // Use the PruningPredicate API to determine which files can not
        // possibly have any relevant data.
        let pruning_predicate =
            PruningPredicate::try_new(predicate, self.schema().clone())?;

        // Now evaluate the pruning predicate into a boolean mask, one element per
        // file in the index. If the mask is true, the file may have rows that
        // match the predicate. If the mask is false, we know the file can not have *any*
        // rows that match the predicate and thus can be skipped.
        let file_mask = pruning_predicate.prune(self)?;

        let num_left = file_mask.iter().filter(|x| **x).count();
        self.set_last_num_pruned(self.len() - num_left);

        // Return only files that match the predicate from the index
        let files_and_sizes: Vec<_> = file_mask
            .into_iter()
            .enumerate()
            .filter_map(|(file, keep)| {
                if keep {
                    Some(self.get_file(file))
                } else {
                    None
                }
            })
            .collect();
        Ok(files_and_sizes)
    }

    /// Return the file_names column of this index
    fn file_names(&self) -> &StringArray {
        self.index
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    /// Return the file_size column of this index
    fn file_size(&self) -> &UInt64Array {
        self.index
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
    }

    /// Reference to the row count column
    fn row_counts_ref(&self) -> &ArrayRef {
        self.index.column(2)
    }

    /// Reference to the column minimum values
    fn value_column_mins(&self) -> &ArrayRef {
        self.index.column(3)
    }

    /// Reference to the column maximum values
    fn value_column_maxes(&self) -> &ArrayRef {
        self.index.column(4)
    }
}

/// In order to use the PruningPredicate API, we need to provide DataFusion
/// the required statistics via the [`PruningStatistics`] trait
impl PruningStatistics for ParquetMetadataIndex {
    /// return the minimum values for the value column
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        if column.name.eq("value") {
            Some(self.value_column_mins().clone())
        } else {
            None
        }
    }

    /// return the maximum values for the value column
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        if column.name.eq("value") {
            Some(self.value_column_maxes().clone())
        } else {
            None
        }
    }

    /// return the number of "containers". In this example, each "container" is
    /// a file (aka a row in the index)
    fn num_containers(&self) -> usize {
        self.len()
    }

    /// Return `None` to signal we don't have any information about null
    /// counts in the index,
    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    /// return the row counts for each file
    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        Some(self.row_counts_ref().clone())
    }

    /// The `contained` API can be used with structures such as Bloom filters,
    /// but is not used in this example, so return `None`
    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Builds a [`ParquetMetadataIndex`] from a set of parquet files
#[derive(Debug, Default)]
struct ParquetMetadataIndexBuilder {
    file_schema: Option<SchemaRef>,
    filenames: Vec<String>,
    file_sizes: Vec<u64>,
    row_counts: Vec<u64>,
    /// Holds the min/max value of the value column for each file
    value_column_mins: Vec<i32>,
    value_column_maxs: Vec<i32>,
}

impl ParquetMetadataIndexBuilder {
    fn new() -> Self {
        Self::default()
    }

    /// Add a new file to the index
    fn add_file(&mut self, file: &Path) -> Result<()> {
        let file_name = file
            .file_name()
            .ok_or_else(|| internal_datafusion_err!("No filename"))?
            .to_str()
            .ok_or_else(|| internal_datafusion_err!("Invalid filename"))?;
        let file_size = file.metadata()?.len();

        let file = File::open(file).map_err(|e| {
            DataFusionError::from(e).context(format!("Error opening file {file:?}"))
        })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Get the schema of the file. A real system might have to handle the
        // case where the schema of the file is not the same as the schema of
        // the other files e.g. using SchemaAdapter.
        if self.file_schema.is_none() {
            self.file_schema = Some(reader.schema().clone());
        }

        // extract the parquet statistics from the file's footer
        let metadata = reader.metadata();
        let row_groups = metadata.row_groups();

        // Extract the min/max values for each row group from the statistics
        let converter = StatisticsConverter::try_new(
            "value",
            reader.schema(),
            reader.parquet_schema(),
        )?;
        let row_counts = converter
            .row_group_row_counts(row_groups.iter())?
            .ok_or_else(|| {
                internal_datafusion_err!("Row group row counts are missing")
            })?;
        let value_column_mins = converter.row_group_mins(row_groups.iter())?;
        let value_column_maxes = converter.row_group_maxes(row_groups.iter())?;

        // In a real system you would have to handle nulls, which represent
        // unknown statistics. All statistics are known in this example
        assert_eq!(row_counts.null_count(), 0);
        assert_eq!(value_column_mins.null_count(), 0);
        assert_eq!(value_column_maxes.null_count(), 0);

        // The statistics gathered above are for each row group. We need to
        // aggregate them together to compute the overall file row count,
        // min and max.
        let row_count = row_counts
            .iter()
            .flatten() // skip nulls (should be none)
            .sum::<u64>();
        let value_column_min = value_column_mins
            .as_primitive::<Int32Type>()
            .iter()
            .flatten() // skip nulls (i.e. min is unknown)
            .min()
            .unwrap_or_default();
        let value_column_max = value_column_maxes
            .as_primitive::<Int32Type>()
            .iter()
            .flatten() // skip nulls (i.e. max is unknown)
            .max()
            .unwrap_or_default();

        // sanity check the statistics
        assert_eq!(row_count, metadata.file_metadata().num_rows() as u64);

        self.add_row(
            file_name,
            file_size,
            row_count,
            value_column_min,
            value_column_max,
        );
        Ok(())
    }

    /// Add an entry for a single new file to the in progress index
    fn add_row(
        &mut self,
        file_name: impl Into<String>,
        file_size: u64,
        row_count: u64,
        value_column_min: i32,
        value_column_max: i32,
    ) {
        self.filenames.push(file_name.into());
        self.file_sizes.push(file_size);
        self.row_counts.push(row_count);
        self.value_column_mins.push(value_column_min);
        self.value_column_maxs.push(value_column_max);
    }

    /// Build the index from the files added
    fn build(self) -> Result<ParquetMetadataIndex> {
        let Some(file_schema) = self.file_schema else {
            return Err(internal_datafusion_err!("No files added to index"));
        };

        let file_name: ArrayRef = Arc::new(StringArray::from(self.filenames));
        let file_size: ArrayRef = Arc::new(UInt64Array::from(self.file_sizes));
        let row_count: ArrayRef = Arc::new(UInt64Array::from(self.row_counts));
        let value_column_min: ArrayRef =
            Arc::new(Int32Array::from(self.value_column_mins));
        let value_column_max: ArrayRef =
            Arc::new(Int32Array::from(self.value_column_maxs));

        let index = RecordBatch::try_from_iter(vec![
            ("file_name", file_name),
            ("file_size", file_size),
            ("row_count", row_count),
            ("value_column_min", value_column_min),
            ("value_column_max", value_column_max),
        ])?;

        Ok(ParquetMetadataIndex {
            file_schema,
            index,
            last_num_pruned: AtomicUsize::new(0),
        })
    }
}

/// Return a list of the directory entries in the given directory, sorted by name
fn read_dir(dir: &Path) -> Result<Vec<DirEntry>> {
    let mut files = dir
        .read_dir()
        .map_err(|e| {
            DataFusionError::from(e).context(format!("Error reading directory {dir:?}"))
        })?
        .map(|entry| {
            entry.map_err(|e| {
                DataFusionError::from(e)
                    .context(format!("Error reading directory entry in {dir:?}"))
            })
        })
        .collect::<Result<Vec<DirEntry>>>()?;
    files.sort_by_key(|entry| entry.file_name());
    Ok(files)
}

/// Demonstration Data
///
/// Makes a directory with three parquet files
///
/// The schema of the files is
/// * file_name (string)
/// * value (int32)
///
/// The files are as follows:
/// * file1.parquet (values 0..100)
/// * file2.parquet (values 100..200)
/// * file3.parquet (values 200..3000)
struct DemoData {
    tmpdir: TempDir,
}

impl DemoData {
    fn try_new() -> Result<Self> {
        let tmpdir = TempDir::new()?;
        make_demo_file(tmpdir.path().join("file1.parquet"), 0..100)?;
        make_demo_file(tmpdir.path().join("file2.parquet"), 100..200)?;
        make_demo_file(tmpdir.path().join("file3.parquet"), 200..3000)?;

        Ok(Self { tmpdir })
    }

    fn path(&self) -> PathBuf {
        self.tmpdir.path().into()
    }
}

/// Creates a new parquet file at the specified path.
///
/// The `value` column  increases sequentially from `min_value` to `max_value`
/// with the following schema:
///
/// * file_name: Utf8
/// * value: Int32
fn make_demo_file(path: impl AsRef<Path>, value_range: Range<i32>) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path)?;
    let filename = path
        .file_name()
        .ok_or_else(|| internal_datafusion_err!("No filename"))?
        .to_str()
        .ok_or_else(|| internal_datafusion_err!("Invalid filename"))?;

    let num_values = value_range.len();
    let file_names =
        StringArray::from_iter_values(std::iter::repeat(&filename).take(num_values));
    let values = Int32Array::from_iter_values(value_range);
    let batch = RecordBatch::try_from_iter(vec![
        ("file_name", Arc::new(file_names) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
    ])?;

    let schema = batch.schema();

    // write the actual values to the file
    let props = None;
    let mut writer = ArrowWriter::try_new(file, schema, props)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}
