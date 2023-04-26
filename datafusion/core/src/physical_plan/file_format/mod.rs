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

//! Execution plans that read file formats

mod avro;
#[cfg(test)]
mod chunked_store;
mod csv;
mod file_stream;
mod json;
mod parquet;

pub(crate) use self::csv::plan_to_csv;
pub use self::csv::{CsvConfig, CsvExec, CsvOpener};
pub(crate) use self::parquet::plan_to_parquet;
pub use self::parquet::{ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory};
use arrow::{
    array::{ArrayData, ArrayRef, BufferBuilder, DictionaryArray},
    buffer::Buffer,
    datatypes::{ArrowNativeType, DataType, Field, Schema, SchemaRef, UInt16Type},
    record_batch::RecordBatch,
};
pub use avro::AvroExec;
use datafusion_physical_expr::PhysicalSortExpr;
pub use file_stream::{FileOpenFuture, FileOpener, FileStream};
pub(crate) use json::plan_to_json;
pub use json::{JsonOpener, NdJsonExec};

use crate::datasource::{
    listing::{FileRange, PartitionedFile},
    object_store::ObjectStoreUrl,
};
use crate::physical_plan::ExecutionPlan;
use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::array::new_null_array;
use arrow::record_batch::RecordBatchOptions;
use datafusion_common::tree_node::{TreeNode, VisitRecursion};
use log::{debug, info, warn};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    sync::Arc,
    vec,
};

use super::{ColumnStatistics, Statistics};

/// Convert type to a type suitable for use as a [`ListingTable`]
/// partition column. Returns `Dictionary(UInt16, val_type)`, which is
/// a reasonable trade off between a reasonable number of partition
/// values and space efficiency.
///
/// This use this to specify types for partition columns. However
/// you MAY also choose not to dictionary-encode the data or to use a
/// different dictionary type.
///
/// Use [`wrap_partition_value_in_dict`] to wrap a [`ScalarValue`] in the same say.
///
/// [`ListingTable`]: crate::datasource::listing::ListingTable
pub fn wrap_partition_type_in_dict(val_type: DataType) -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(val_type))
}

/// Convert a [`ScalarValue`] of partition columns to a type, as
/// decribed in the documentation of [`wrap_partition_type_in_dict`],
/// which can wrap the types.
pub fn wrap_partition_value_in_dict(val: ScalarValue) -> ScalarValue {
    ScalarValue::Dictionary(Box::new(DataType::UInt16), Box::new(val))
}

/// Get all of the [`PartitionedFile`] to be scanned for an [`ExecutionPlan`]
pub fn get_scan_files(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<Vec<Vec<PartitionedFile>>>> {
    let mut collector: Vec<Vec<Vec<PartitionedFile>>> = vec![];
    plan.apply(&mut |plan| {
        let plan_any = plan.as_any();
        let file_groups =
            if let Some(parquet_exec) = plan_any.downcast_ref::<ParquetExec>() {
                parquet_exec.base_config().file_groups.clone()
            } else if let Some(avro_exec) = plan_any.downcast_ref::<AvroExec>() {
                avro_exec.base_config().file_groups.clone()
            } else if let Some(json_exec) = plan_any.downcast_ref::<NdJsonExec>() {
                json_exec.base_config().file_groups.clone()
            } else if let Some(csv_exec) = plan_any.downcast_ref::<CsvExec>() {
                csv_exec.base_config().file_groups.clone()
            } else {
                return Ok(VisitRecursion::Continue);
            };

        collector.push(file_groups);
        Ok(VisitRecursion::Skip)
    })?;
    Ok(collector)
}

/// The base configurations to provide when creating a physical plan for
/// any given file format.
#[derive(Debug, Clone)]
pub struct FileScanConfig {
    /// Object store URL, used to get an [`ObjectStore`] instance from
    /// [`RuntimeEnv::object_store`]
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    /// [`RuntimeEnv::object_store`]: crate::execution::runtime_env::RuntimeEnv::object_store
    pub object_store_url: ObjectStoreUrl,
    /// Schema before `projection` is applied. It contains the all columns that may
    /// appear in the files. It does not include table partition columns
    /// that may be added.
    pub file_schema: SchemaRef,
    /// List of files to be processed, grouped into partitions
    ///
    /// Each file must have a schema of `file_schema` or a subset. If
    /// a particular file has a subset, the missing columns are
    /// padded with with NULLs.
    ///
    /// DataFusion may attempt to read each partition of files
    /// concurrently, however files *within* a partition will be read
    /// sequentially, one after the next.
    pub file_groups: Vec<Vec<PartitionedFile>>,
    /// Estimated overall statistics of the files, taking `filters` into account.
    pub statistics: Statistics,
    /// Columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    pub projection: Option<Vec<usize>>,
    /// The maximum number of records to read from this plan. If `None`,
    /// all records after filtering are returned.
    pub limit: Option<usize>,
    /// The partitioning columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// The order in which the data is sorted, if known.
    pub output_ordering: Option<Vec<PhysicalSortExpr>>,
    /// Indicates whether this plan may produce an infinite stream of records.
    pub infinite_source: bool,
}

impl FileScanConfig {
    /// Project the schema and the statistics on the given column indices
    fn project(&self) -> (SchemaRef, Statistics) {
        if self.projection.is_none() && self.table_partition_cols.is_empty() {
            return (Arc::clone(&self.file_schema), self.statistics.clone());
        }

        let proj_iter: Box<dyn Iterator<Item = usize>> = match &self.projection {
            Some(proj) => Box::new(proj.iter().copied()),
            None => Box::new(
                0..(self.file_schema.fields().len() + self.table_partition_cols.len()),
            ),
        };

        let mut table_fields = vec![];
        let mut table_cols_stats = vec![];
        for idx in proj_iter {
            if idx < self.file_schema.fields().len() {
                table_fields.push(self.file_schema.field(idx).clone());
                if let Some(file_cols_stats) = &self.statistics.column_statistics {
                    table_cols_stats.push(file_cols_stats[idx].clone())
                } else {
                    table_cols_stats.push(ColumnStatistics::default())
                }
            } else {
                let partition_idx = idx - self.file_schema.fields().len();
                table_fields.push(Field::new(
                    &self.table_partition_cols[partition_idx].0,
                    self.table_partition_cols[partition_idx].1.to_owned(),
                    false,
                ));
                // TODO provide accurate stat for partition column (#1186)
                table_cols_stats.push(ColumnStatistics::default())
            }
        }

        let table_stats = Statistics {
            num_rows: self.statistics.num_rows,
            is_exact: self.statistics.is_exact,
            // TODO correct byte size?
            total_byte_size: None,
            column_statistics: Some(table_cols_stats),
        };

        let table_schema = Arc::new(
            Schema::new(table_fields).with_metadata(self.file_schema.metadata().clone()),
        );

        (table_schema, table_stats)
    }

    #[allow(unused)] // Only used by avro
    fn projected_file_column_names(&self) -> Option<Vec<String>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .map(|col_idx| self.file_schema.field(*col_idx).name())
                .cloned()
                .collect()
        })
    }

    fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .copied()
                .collect()
        })
    }
}

/// A wrapper to customize partitioned file display
///
/// Prints in the format:
/// ```text
/// {NUM_GROUPS groups: [[file1, file2,...], [fileN, fileM, ...], ...]}
/// ```
#[derive(Debug)]
struct FileGroupsDisplay<'a>(&'a [Vec<PartitionedFile>]);

impl<'a> Display for FileGroupsDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let mut first_group = true;
        let groups = if self.0.len() == 1 { "group" } else { "groups" };
        write!(f, "{{{} {}: [", self.0.len(), groups)?;
        for group in self.0 {
            if !first_group {
                write!(f, ", ")?;
            }
            first_group = false;
            write!(f, "[")?;

            let mut first_file = true;
            for pf in group {
                if !first_file {
                    write!(f, ", ")?;
                }
                first_file = false;

                write!(f, "{}", pf.object_meta.location.as_ref())?;

                if let Some(range) = pf.range.as_ref() {
                    write!(f, ":{}..{}", range.start, range.end)?;
                }
            }
            write!(f, "]")?;
        }
        write!(f, "]}}")?;
        Ok(())
    }
}

/// A wrapper to customize partitioned file display
#[derive(Debug)]
struct ProjectSchemaDisplay<'a>(&'a SchemaRef);

impl<'a> Display for ProjectSchemaDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let parts: Vec<_> = self
            .0
            .fields()
            .iter()
            .map(|x| x.name().to_owned())
            .collect::<Vec<String>>();
        write!(f, "[{}]", parts.join(", "))
    }
}

/// A utility which can adapt file-level record batches to a table schema which may have a schema
/// obtained from merging multiple file-level schemas.
///
/// This is useful for enabling schema evolution in partitioned datasets.
///
/// This has to be done in two stages.
///
/// 1. Before reading the file, we have to map projected column indexes from the table schema to
///    the file schema.
///
/// 2. After reading a record batch we need to map the read columns back to the expected columns
///    indexes and insert null-valued columns wherever the file schema was missing a colum present
///    in the table schema.
#[derive(Clone, Debug)]
pub(crate) struct SchemaAdapter {
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter {
    pub(crate) fn new(table_schema: SchemaRef) -> SchemaAdapter {
        Self { table_schema }
    }

    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    pub(crate) fn map_column_index(
        &self,
        index: usize,
        file_schema: &Schema,
    ) -> Option<usize> {
        let field = self.table_schema.field(index);
        file_schema.index_of(field.name()).ok()
    }

    /// Map projected column indexes to the file schema. This will fail if the table schema
    /// and the file schema contain a field with the same name and different types.
    pub fn map_projections(
        &self,
        file_schema: &Schema,
        projections: &[usize],
    ) -> Result<Vec<usize>> {
        let mut mapped: Vec<usize> = vec![];
        for idx in projections {
            let field = self.table_schema.field(*idx);
            if let Ok(mapped_idx) = file_schema.index_of(field.name().as_str()) {
                if file_schema.field(mapped_idx).data_type() == field.data_type() {
                    mapped.push(mapped_idx)
                } else {
                    let msg = format!("Failed to map column projection for field {}. Incompatible data types {:?} and {:?}", field.name(), file_schema.field(mapped_idx).data_type(), field.data_type());
                    info!("{}", msg);
                    return Err(DataFusionError::Execution(msg));
                }
            }
        }
        Ok(mapped)
    }

    /// Re-order projected columns by index in record batch to match table schema column ordering. If the record
    /// batch does not contain a column for an expected field, insert a null-valued column at the
    /// required column index.
    pub fn adapt_batch(
        &self,
        batch: RecordBatch,
        projections: &[usize],
    ) -> Result<RecordBatch> {
        let batch_rows = batch.num_rows();

        let batch_schema = batch.schema();

        let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.columns().len());
        let batch_cols = batch.columns().to_vec();

        for field_idx in projections {
            let table_field = &self.table_schema.fields()[*field_idx];
            if let Some((batch_idx, _name)) =
                batch_schema.column_with_name(table_field.name().as_str())
            {
                cols.push(batch_cols[batch_idx].clone());
            } else {
                cols.push(new_null_array(table_field.data_type(), batch_rows))
            }
        }

        let projected_schema = Arc::new(self.table_schema.clone().project(projections)?);

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        Ok(RecordBatch::try_new_with_options(
            projected_schema,
            cols,
            &options,
        )?)
    }
}

/// A helper that projects partition columns into the file record batches.
///
/// One interesting trick is the usage of a cache for the key buffers of the partition column
/// dictionaries. Indeed, the partition columns are constant, so the dictionaries that represent them
/// have all their keys equal to 0. This enables us to re-use the same "all-zero" buffer across batches,
/// which makes the space consumption of the partition columns O(batch_size) instead of O(record_count).
struct PartitionColumnProjector {
    /// An Arrow buffer initialized to zeros that represents the key array of all partition
    /// columns (partition columns are materialized by dictionary arrays with only one
    /// value in the dictionary, thus all the keys are equal to zero).
    key_buffer_cache: ZeroBufferGenerators,
    /// Mapping between the indexes in the list of partition columns and the target
    /// schema. Sorted by index in the target schema so that we can iterate on it to
    /// insert the partition columns in the target record batch.
    projected_partition_indexes: Vec<(usize, usize)>,
    /// The schema of the table once the projection was applied.
    projected_schema: SchemaRef,
}

impl PartitionColumnProjector {
    // Create a projector to insert the partitioning columns into batches read from files
    // - `projected_schema`: the target schema with both file and partitioning columns
    // - `table_partition_cols`: all the partitioning column names
    fn new(projected_schema: SchemaRef, table_partition_cols: &[String]) -> Self {
        let mut idx_map = HashMap::new();
        for (partition_idx, partition_name) in table_partition_cols.iter().enumerate() {
            if let Ok(schema_idx) = projected_schema.index_of(partition_name) {
                idx_map.insert(partition_idx, schema_idx);
            }
        }

        let mut projected_partition_indexes: Vec<_> = idx_map.into_iter().collect();
        projected_partition_indexes.sort_by(|(_, a), (_, b)| a.cmp(b));

        Self {
            projected_partition_indexes,
            key_buffer_cache: Default::default(),
            projected_schema,
        }
    }

    // Transform the batch read from the file by inserting the partitioning columns
    // to the right positions as deduced from `projected_schema`
    // - `file_batch`: batch read from the file, with internal projection applied
    // - `partition_values`: the list of partition values, one for each partition column
    fn project(
        &mut self,
        file_batch: RecordBatch,
        partition_values: &[ScalarValue],
    ) -> Result<RecordBatch> {
        let expected_cols =
            self.projected_schema.fields().len() - self.projected_partition_indexes.len();

        if file_batch.columns().len() != expected_cols {
            return Err(DataFusionError::Execution(format!(
                "Unexpected batch schema from file, expected {} cols but got {}",
                expected_cols,
                file_batch.columns().len()
            )));
        }
        let mut cols = file_batch.columns().to_vec();
        for &(pidx, sidx) in &self.projected_partition_indexes {
            let mut partition_value = Cow::Borrowed(&partition_values[pidx]);

            // check if user forgot to dict-encode the partition value
            let field = self.projected_schema.field(sidx);
            let expected_data_type = field.data_type();
            let actual_data_type = partition_value.get_datatype();
            if let DataType::Dictionary(key_type, _) = expected_data_type {
                if !matches!(actual_data_type, DataType::Dictionary(_, _)) {
                    warn!("Partition value for column {} was not dictionary-encoded, applied auto-fix.", field.name());
                    partition_value = Cow::Owned(ScalarValue::Dictionary(
                        key_type.clone(),
                        Box::new(partition_value.as_ref().clone()),
                    ));
                }
            }

            cols.insert(
                sidx,
                create_output_array(
                    &mut self.key_buffer_cache,
                    partition_value.as_ref(),
                    file_batch.num_rows(),
                ),
            )
        }
        RecordBatch::try_new(Arc::clone(&self.projected_schema), cols).map_err(Into::into)
    }
}

#[derive(Debug, Default)]
struct ZeroBufferGenerators {
    gen_i8: ZeroBufferGenerator<i8>,
    gen_i16: ZeroBufferGenerator<i16>,
    gen_i32: ZeroBufferGenerator<i32>,
    gen_i64: ZeroBufferGenerator<i64>,
    gen_u8: ZeroBufferGenerator<u8>,
    gen_u16: ZeroBufferGenerator<u16>,
    gen_u32: ZeroBufferGenerator<u32>,
    gen_u64: ZeroBufferGenerator<u64>,
}

/// Generate a arrow [`Buffer`] that contains zero values.
#[derive(Debug, Default)]
struct ZeroBufferGenerator<T>
where
    T: ArrowNativeType,
{
    cache: Option<Buffer>,
    _t: PhantomData<T>,
}

impl<T> ZeroBufferGenerator<T>
where
    T: ArrowNativeType,
{
    const SIZE: usize = std::mem::size_of::<T>();

    fn get_buffer(&mut self, n_vals: usize) -> Buffer {
        match &mut self.cache {
            Some(buf) if buf.len() >= n_vals * Self::SIZE => {
                buf.slice_with_length(0, n_vals * Self::SIZE)
            }
            _ => {
                let mut key_buffer_builder = BufferBuilder::<T>::new(n_vals);
                key_buffer_builder.advance(n_vals); // keys are all 0
                self.cache.insert(key_buffer_builder.finish()).clone()
            }
        }
    }
}

fn create_dict_array<T>(
    buffer_gen: &mut ZeroBufferGenerator<T>,
    dict_val: &ScalarValue,
    len: usize,
    data_type: DataType,
) -> ArrayRef
where
    T: ArrowNativeType,
{
    let dict_vals = dict_val.to_array();

    let sliced_key_buffer = buffer_gen.get_buffer(len);

    // assemble pieces together
    let mut builder = ArrayData::builder(data_type)
        .len(len)
        .add_buffer(sliced_key_buffer);
    builder = builder.add_child_data(dict_vals.to_data());
    Arc::new(DictionaryArray::<UInt16Type>::from(
        builder.build().unwrap(),
    ))
}

fn create_output_array(
    key_buffer_cache: &mut ZeroBufferGenerators,
    val: &ScalarValue,
    len: usize,
) -> ArrayRef {
    if let ScalarValue::Dictionary(key_type, dict_val) = &val {
        match key_type.as_ref() {
            DataType::Int8 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i8,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::Int16 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i16,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::Int32 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i32,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::Int64 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i64,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::UInt8 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u8,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::UInt16 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u16,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::UInt32 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u32,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            DataType::UInt64 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u64,
                    dict_val,
                    len,
                    val.get_datatype(),
                );
            }
            _ => {}
        }
    }

    val.to_array_of_size(len)
}

/// A single file or part of a file that should be read, along with its schema, statistics
pub struct FileMeta {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub object_meta: ObjectMeta,
    /// An optional file range for a more fine-grained parallel execution
    pub range: Option<FileRange>,
    /// An optional field for user defined per object metadata
    pub extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl FileMeta {
    /// The full path to the object
    pub fn location(&self) -> &Path {
        &self.object_meta.location
    }
}

impl From<ObjectMeta> for FileMeta {
    fn from(object_meta: ObjectMeta) -> Self {
        Self {
            object_meta,
            range: None,
            extensions: None,
        }
    }
}

/// The various listing tables does not attempt to read all files
/// concurrently, instead they will read files in sequence within a
/// partition.  This is an important property as it allows plans to
/// run against 1000s of files and not try to open them all
/// concurrently.
///
/// However, it means if we assign more than one file to a partitition
/// the output sort order will not be preserved as illustrated in the
/// following diagrams:
///
/// When only 1 file is assigned to each partition, each partition is
/// correctly sorted on `(A, B, C)`
///
/// ```text
///┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┓
///  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///┃   ┌───────────────┐     ┌──────────────┐ │   ┌──────────────┐ │   ┌─────────────┐   ┃
///  │ │   1.parquet   │ │ │ │  2.parquet   │   │ │  3.parquet   │   │ │  4.parquet  │ │
///┃   │ Sort: A, B, C │     │Sort: A, B, C │ │   │Sort: A, B, C │ │   │Sort: A, B, C│   ┃
///  │ └───────────────┘ │ │ └──────────────┘   │ └──────────────┘   │ └─────────────┘ │
///┃                                          │                    │                     ┃
///  │                   │ │                    │                    │                 │
///┃                                          │                    │                     ┃
///  │                   │ │                    │                    │                 │
///┃                                          │                    │                     ┃
///  │                   │ │                    │                    │                 │
///┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
///     DataFusion           DataFusion           DataFusion           DataFusion
///┃    Partition 1          Partition 2          Partition 3          Partition 4       ┃
/// ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///
///                                      ParquetExec
///```
///
/// However, when more than 1 file is assigned to each partition, each
/// partition is NOT correctly sorted on `(A, B, C)`. Once the second
/// file is scanned, the same values for A, B and C can be repeated in
/// the same sorted stream
///
///```text
///┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
///┃   ┌───────────────┐     ┌──────────────┐ │
///  │ │   1.parquet   │ │ │ │  2.parquet   │   ┃
///┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///  │ └───────────────┘ │ │ └──────────────┘   ┃
///┃   ┌───────────────┐     ┌──────────────┐ │
///  │ │   3.parquet   │ │ │ │  4.parquet   │   ┃
///┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///  │ └───────────────┘ │ │ └──────────────┘   ┃
///┃                                          │
///  │                   │ │                    ┃
///┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///     DataFusion           DataFusion         ┃
///┃    Partition 1          Partition 2
/// ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┛
///
///              ParquetExec
///```
pub(crate) fn get_output_ordering(
    base_config: &FileScanConfig,
) -> Option<&[PhysicalSortExpr]> {
    base_config.output_ordering.as_ref()
        .map(|output_ordering| if base_config.file_groups.iter().any(|group| group.len() > 1) {
            debug!("Skipping specified output ordering {:?}. Some file group had more than one file: {:?}",
                   output_ordering, base_config.file_groups);
            None
        } else {
            Some(output_ordering.as_slice())
        }).unwrap_or_else(|| None)
}

/// Defines strategies for the saving behavior in the case when the saved folder already exists
#[derive(Debug, Clone, Copy)]
pub enum FileWriterSaveMode {
    /// Overwrite mode means that when saving a parquet file, if folder already exists, existing data is expected to be overwritten.
    /// If folder does not exist, the folder will be created. This is default value.
    Overwrite,
    /// Append mode means that when saving a parquet file, if folder already exists, new data is expected to be appended on top of the existing.
    /// If folder does not exist, the folder will be created
    Append,
    /// Ignore mode means that when saving a parquet file, if folder already exists, new data will be ignored.
    /// If folder does not exist, the folder will be created
    Ignore,
    /// ErrorIfExists mode means that when saving a parquet file, if folder already exists, the error will be raised.
    /// If folder does not exist, the folder will be created
    ErrorIfExists,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        test::{build_table_i32, columns},
        test_util::aggr_test_schema,
    };

    use super::*;

    #[test]
    fn physical_plan_config_no_projection() {
        let file_schema = aggr_test_schema();
        let conf = config_for_projection(
            Arc::clone(&file_schema),
            None,
            Statistics::default(),
            vec![(
                "date".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            )],
        );

        let (proj_schema, proj_statistics) = conf.project();
        assert_eq!(proj_schema.fields().len(), file_schema.fields().len() + 1);
        assert_eq!(
            proj_schema.field(file_schema.fields().len()).name(),
            "date",
            "partition columns are the last columns"
        );
        assert_eq!(
            proj_statistics
                .column_statistics
                .expect("projection creates column statistics")
                .len(),
            file_schema.fields().len() + 1
        );
        // TODO implement tests for partition column statistics once implemented

        let col_names = conf.projected_file_column_names();
        assert_eq!(col_names, None);

        let col_indices = conf.file_column_projection_indices();
        assert_eq!(col_indices, None);
    }

    #[test]
    fn physical_plan_config_with_projection() {
        let file_schema = aggr_test_schema();
        let conf = config_for_projection(
            Arc::clone(&file_schema),
            Some(vec![file_schema.fields().len(), 0]),
            Statistics {
                num_rows: Some(10),
                // assign the column index to distinct_count to help assert
                // the source statistic after the projection
                column_statistics: Some(
                    (0..file_schema.fields().len())
                        .map(|i| ColumnStatistics {
                            distinct_count: Some(i),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            },
            vec![(
                "date".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            )],
        );

        let (proj_schema, proj_statistics) = conf.project();
        assert_eq!(
            columns(&proj_schema),
            vec!["date".to_owned(), "c1".to_owned()]
        );
        let proj_stat_cols = proj_statistics
            .column_statistics
            .expect("projection creates column statistics");
        assert_eq!(proj_stat_cols.len(), 2);
        // TODO implement tests for proj_stat_cols[0] once partition column
        // statistics are implemented
        assert_eq!(proj_stat_cols[1].distinct_count, Some(0));

        let col_names = conf.projected_file_column_names();
        assert_eq!(col_names, Some(vec!["c1".to_owned()]));

        let col_indices = conf.file_column_projection_indices();
        assert_eq!(col_indices, Some(vec![0]));
    }

    #[test]
    fn partition_column_projector() {
        let file_batch = build_table_i32(
            ("a", &vec![0, 1, 2]),
            ("b", &vec![-2, -1, 0]),
            ("c", &vec![10, 11, 12]),
        );
        let partition_cols = vec![
            (
                "year".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
            (
                "month".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
            (
                "day".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
        ];
        // create a projected schema
        let conf = config_for_projection(
            file_batch.schema(),
            // keep all cols from file and 2 from partitioning
            Some(vec![
                0,
                1,
                2,
                file_batch.schema().fields().len(),
                file_batch.schema().fields().len() + 2,
            ]),
            Statistics::default(),
            partition_cols.clone(),
        );
        let (proj_schema, _) = conf.project();
        // created a projector for that projected schema
        let mut proj = PartitionColumnProjector::new(
            proj_schema,
            &partition_cols
                .iter()
                .map(|x| x.0.clone())
                .collect::<Vec<_>>(),
        );

        // project first batch
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "2021".to_owned(),
                    ))),
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "10".to_owned(),
                    ))),
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "26".to_owned(),
                    ))),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = vec![
            "+---+----+----+------+-----+",
            "| a | b  | c  | year | day |",
            "+---+----+----+------+-----+",
            "| 0 | -2 | 10 | 2021 | 26  |",
            "| 1 | -1 | 11 | 2021 | 26  |",
            "| 2 | 0  | 12 | 2021 | 26  |",
            "+---+----+----+------+-----+",
        ];
        crate::assert_batches_eq!(expected, &[projected_batch]);

        // project another batch that is larger than the previous one
        let file_batch = build_table_i32(
            ("a", &vec![5, 6, 7, 8, 9]),
            ("b", &vec![-10, -9, -8, -7, -6]),
            ("c", &vec![12, 13, 14, 15, 16]),
        );
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "2021".to_owned(),
                    ))),
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "10".to_owned(),
                    ))),
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "27".to_owned(),
                    ))),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = vec![
            "+---+-----+----+------+-----+",
            "| a | b   | c  | year | day |",
            "+---+-----+----+------+-----+",
            "| 5 | -10 | 12 | 2021 | 27  |",
            "| 6 | -9  | 13 | 2021 | 27  |",
            "| 7 | -8  | 14 | 2021 | 27  |",
            "| 8 | -7  | 15 | 2021 | 27  |",
            "| 9 | -6  | 16 | 2021 | 27  |",
            "+---+-----+----+------+-----+",
        ];
        crate::assert_batches_eq!(expected, &[projected_batch]);

        // project another batch that is smaller than the previous one
        let file_batch = build_table_i32(
            ("a", &vec![0, 1, 3]),
            ("b", &vec![2, 3, 4]),
            ("c", &vec![4, 5, 6]),
        );
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "2021".to_owned(),
                    ))),
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "10".to_owned(),
                    ))),
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        "28".to_owned(),
                    ))),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = vec![
            "+---+---+---+------+-----+",
            "| a | b | c | year | day |",
            "+---+---+---+------+-----+",
            "| 0 | 2 | 4 | 2021 | 28  |",
            "| 1 | 3 | 5 | 2021 | 28  |",
            "| 3 | 4 | 6 | 2021 | 28  |",
            "+---+---+---+------+-----+",
        ];
        crate::assert_batches_eq!(expected, &[projected_batch]);

        // forgot to dictionary-wrap the scalar value
        let file_batch = build_table_i32(
            ("a", &vec![0, 1, 2]),
            ("b", &vec![-2, -1, 0]),
            ("c", &vec![10, 11, 12]),
        );
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    ScalarValue::Utf8(Some("2021".to_owned())),
                    ScalarValue::Utf8(Some("10".to_owned())),
                    ScalarValue::Utf8(Some("26".to_owned())),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = vec![
            "+---+----+----+------+-----+",
            "| a | b  | c  | year | day |",
            "+---+----+----+------+-----+",
            "| 0 | -2 | 10 | 2021 | 26  |",
            "| 1 | -1 | 11 | 2021 | 26  |",
            "| 2 | 0  | 12 | 2021 | 26  |",
            "+---+----+----+------+-----+",
        ];
        crate::assert_batches_eq!(expected, &[projected_batch]);
    }

    #[test]
    fn schema_adapter_adapt_projections() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int8, true),
        ]));

        let file_schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
        ]);

        let file_schema_2 = Arc::new(Schema::new(vec![
            Field::new("c3", DataType::Int8, true),
            Field::new("c2", DataType::Int64, true),
        ]));

        let file_schema_3 =
            Arc::new(Schema::new(vec![Field::new("c3", DataType::Float32, true)]));

        let adapter = SchemaAdapter::new(table_schema);

        let projections1: Vec<usize> = vec![0, 1, 2];
        let projections2: Vec<usize> = vec![2];

        let mapped = adapter
            .map_projections(&file_schema, projections1.as_slice())
            .expect("mapping projections");

        assert_eq!(mapped, vec![0, 1]);

        let mapped = adapter
            .map_projections(&file_schema, projections2.as_slice())
            .expect("mapping projections");

        assert!(mapped.is_empty());

        let mapped = adapter
            .map_projections(&file_schema_2, projections1.as_slice())
            .expect("mapping projections");

        assert_eq!(mapped, vec![1, 0]);

        let mapped = adapter
            .map_projections(&file_schema_2, projections2.as_slice())
            .expect("mapping projections");

        assert_eq!(mapped, vec![0]);

        let mapped = adapter.map_projections(&file_schema_3, projections1.as_slice());

        assert!(mapped.is_err());
    }

    // sets default for configs that play no role in projections
    fn config_for_projection(
        file_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        statistics: Statistics,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> FileScanConfig {
        FileScanConfig {
            file_schema,
            file_groups: vec![vec![]],
            limit: None,
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            projection,
            statistics,
            table_partition_cols,
            output_ordering: None,
            infinite_source: false,
        }
    }

    #[test]
    fn file_groups_display_empty() {
        let expected = "{0 groups: []}";
        assert_eq!(&FileGroupsDisplay(&[]).to_string(), expected);
    }

    #[test]
    fn file_groups_display_one() {
        let files = [vec![partitioned_file("foo"), partitioned_file("bar")]];

        let expected = "{1 group: [[foo, bar]]}";
        assert_eq!(&FileGroupsDisplay(&files).to_string(), expected);
    }

    #[test]
    fn file_groups_display_many() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![],
        ];

        let expected = "{3 groups: [[foo, bar], [baz], []]}";
        assert_eq!(&FileGroupsDisplay(&files).to_string(), expected);
    }

    /// create a PartitionedFile for testing
    fn partitioned_file(path: &str) -> PartitionedFile {
        let object_meta = ObjectMeta {
            location: object_store::path::Path::parse(path).unwrap(),
            last_modified: Utc::now(),
            size: 42,
        };

        PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: None,
            extensions: None,
        }
    }
}
