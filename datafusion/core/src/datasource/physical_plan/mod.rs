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

mod arrow_file;
mod avro;
mod csv;
mod file_groups;
mod file_scan_config;
mod file_stream;
mod json;
#[cfg(feature = "parquet")]
pub mod parquet;

pub(crate) use self::csv::plan_to_csv;
pub(crate) use self::json::plan_to_json;
#[cfg(feature = "parquet")]
pub use self::parquet::{ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory};

pub use arrow_file::ArrowExec;
pub use avro::AvroExec;
pub use csv::{CsvConfig, CsvExec, CsvOpener};
pub use file_groups::FileGroupPartitioner;
pub use file_scan_config::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileScanConfig,
};
pub use file_stream::{FileOpenFuture, FileOpener, FileStream, OnError};
pub use json::{JsonOpener, NdJsonExec};

use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    ops::Range,
    sync::Arc,
    vec,
};

use super::listing::ListingTableUrl;
use crate::error::Result;
use crate::physical_plan::{DisplayAs, DisplayFormatType};
use crate::{
    datasource::{
        listing::{FileRange, PartitionedFile},
        object_store::ObjectStoreUrl,
    },
    physical_plan::display::{display_orderings, ProjectSchemaDisplay},
};

use arrow::{
    array::new_null_array,
    compute::{can_cast_types, cast, SortColumn},
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion_common::{plan_err, DataFusionError};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalSortExpr;

use futures::StreamExt;
use log::debug;
use object_store::{path::Path, GetOptions, GetRange, ObjectMeta, ObjectStore};

/// The base configurations to provide when creating a physical plan for
/// writing to any given file format.
pub struct FileSinkConfig {
    /// Object store URL, used to get an ObjectStore instance
    pub object_store_url: ObjectStoreUrl,
    /// A vector of [`PartitionedFile`] structs, each representing a file partition
    pub file_groups: Vec<PartitionedFile>,
    /// Vector of partition paths
    pub table_paths: Vec<ListingTableUrl>,
    /// The schema of the output file
    pub output_schema: SchemaRef,
    /// A vector of column names and their corresponding data types,
    /// representing the partitioning columns for the file
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Controls whether existing data should be overwritten by this sink
    pub overwrite: bool,
}

impl FileSinkConfig {
    /// Get output schema
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

impl Debug for FileScanConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "object_store_url={:?}, ", self.object_store_url)?;

        write!(f, "statistics={:?}, ", self.statistics)?;

        DisplayAs::fmt_as(self, DisplayFormatType::Verbose, f)
    }
}

impl DisplayAs for FileScanConfig {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        let (schema, _, orderings) = self.project();

        write!(f, "file_groups=")?;
        FileGroupsDisplay(&self.file_groups).fmt_as(t, f)?;

        if !schema.fields().is_empty() {
            write!(f, ", projection={}", ProjectSchemaDisplay(&schema))?;
        }

        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }

        display_orderings(f, &orderings)?;

        Ok(())
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

impl<'a> DisplayAs for FileGroupsDisplay<'a> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        let n_groups = self.0.len();
        let groups = if n_groups == 1 { "group" } else { "groups" };
        write!(f, "{{{n_groups} {groups}: [")?;
        match t {
            DisplayFormatType::Default => {
                // To avoid showing too many partitions
                let max_groups = 5;
                fmt_up_to_n_elements(self.0, max_groups, f, |group, f| {
                    FileGroupDisplay(group).fmt_as(t, f)
                })?;
            }
            DisplayFormatType::Verbose => {
                fmt_elements_split_by_commas(self.0.iter(), f, |group, f| {
                    FileGroupDisplay(group).fmt_as(t, f)
                })?
            }
        }
        write!(f, "]}}")
    }
}

/// A wrapper to customize partitioned group of files display
///
/// Prints in the format:
/// ```text
/// [file1, file2,...]
/// ```
#[derive(Debug)]
pub(crate) struct FileGroupDisplay<'a>(pub &'a [PartitionedFile]);

impl<'a> DisplayAs for FileGroupDisplay<'a> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        write!(f, "[")?;
        match t {
            DisplayFormatType::Default => {
                // To avoid showing too many files
                let max_files = 5;
                fmt_up_to_n_elements(self.0, max_files, f, |pf, f| {
                    write!(f, "{}", pf.object_meta.location.as_ref())?;
                    if let Some(range) = pf.range.as_ref() {
                        write!(f, ":{}..{}", range.start, range.end)?;
                    }
                    Ok(())
                })?
            }
            DisplayFormatType::Verbose => {
                fmt_elements_split_by_commas(self.0.iter(), f, |pf, f| {
                    write!(f, "{}", pf.object_meta.location.as_ref())?;
                    if let Some(range) = pf.range.as_ref() {
                        write!(f, ":{}..{}", range.start, range.end)?;
                    }
                    Ok(())
                })?
            }
        }
        write!(f, "]")
    }
}

/// helper to format an array of up to N elements
fn fmt_up_to_n_elements<E, F>(
    elements: &[E],
    n: usize,
    f: &mut Formatter,
    format_element: F,
) -> FmtResult
where
    F: Fn(&E, &mut Formatter) -> FmtResult,
{
    let len = elements.len();
    fmt_elements_split_by_commas(elements.iter().take(n), f, |element, f| {
        format_element(element, f)
    })?;
    // Remaining elements are showed as `...` (to indicate there is more)
    if len > n {
        write!(f, ", ...")?;
    }
    Ok(())
}

/// helper formatting array elements with a comma and a space between them
fn fmt_elements_split_by_commas<E, I, F>(
    iter: I,
    f: &mut Formatter,
    format_element: F,
) -> FmtResult
where
    I: Iterator<Item = E>,
    F: Fn(E, &mut Formatter) -> FmtResult,
{
    for (idx, element) in iter.enumerate() {
        if idx > 0 {
            write!(f, ", ")?;
        }
        format_element(element, f)?;
    }
    Ok(())
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
        Some(file_schema.fields.find(field.name())?.0)
    }

    /// Creates a `SchemaMapping` that can be used to cast or map the columns from the file schema to the table schema.
    ///
    /// If the provided `file_schema` contains columns of a different type to the expected
    /// `table_schema`, the method will attempt to cast the array data from the file schema
    /// to the table schema where possible.
    ///
    /// Returns a [`SchemaMapping`] that can be applied to the output batch
    /// along with an ordered list of columns to project from the file
    pub fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(SchemaMapping, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, table_field)) =
                self.table_schema.fields().find(file_field.name())
            {
                match can_cast_types(file_field.data_type(), table_field.data_type()) {
                    true => {
                        field_mappings[table_idx] = Some(projection.len());
                        projection.push(file_idx);
                    }
                    false => {
                        return plan_err!(
                            "Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                            file_field.name(),
                            file_field.data_type(),
                            table_field.data_type()
                        )
                    }
                }
            }
        }

        Ok((
            SchemaMapping {
                table_schema: self.table_schema.clone(),
                field_mappings,
            },
            projection,
        ))
    }
}

/// The SchemaMapping struct holds a mapping from the file schema to the table schema
/// and any necessary type conversions that need to be applied.
#[derive(Debug)]
pub struct SchemaMapping {
    /// The schema of the table. This is the expected schema after conversion and it should match the schema of the query result.
    table_schema: SchemaRef,
    /// Mapping from field index in `table_schema` to index in projected file_schema
    field_mappings: Vec<Option<usize>>,
}

impl SchemaMapping {
    /// Adapts a `RecordBatch` to match the `table_schema` using the stored mapping and conversions.
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        let cols = self
            .table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
            .map(|(field, file_idx)| match file_idx {
                Some(batch_idx) => cast(&batch_cols[*batch_idx], field.data_type()),
                None => Ok(new_null_array(field.data_type(), batch_rows)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = self.table_schema.clone();
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
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
/// However, it means if we assign more than one file to a partition
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
fn get_projected_output_ordering(
    base_config: &FileScanConfig,
    projected_schema: &SchemaRef,
    table_schema: &SchemaRef,
) -> Vec<Vec<PhysicalSortExpr>> {
    let mut all_orderings = vec![];
    for output_ordering in &base_config.output_ordering {
        // Check if any file groups are not sorted
        if base_config.file_groups.iter().any(|group| {
            if group.len() <= 1 {
                // File groups with <= 1 files are always sorted
                return false;
            }

            let statistics = match MinMaxStatistics::new_from_files(
                output_ordering,
                table_schema,
                projected_schema,
                group,
            ) {
                Ok(statistics) => statistics,
                Err(e) => {
                    log::trace!("Error fetching statistics for file group: {e}");
                    return false;
                }
            };

            !statistics.is_sorted()
        }) {
            debug!(
                "Skipping specified output ordering {:?}. \
                Some file groups couldn't be determined to be sorted: {:?}",
                base_config.output_ordering[0], base_config.file_groups
            );
            return vec![];
        }

        let mut new_ordering = vec![];
        for PhysicalSortExpr { expr, options } in output_ordering {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                let name = col.name();
                if let Some((idx, _)) = projected_schema.column_with_name(name) {
                    // Compute the new sort expression (with correct index) after projection:
                    new_ordering.push(PhysicalSortExpr {
                        expr: Arc::new(Column::new(name, idx)),
                        options: *options,
                    });
                    continue;
                }
            }
            // Cannot find expression in the projected_schema, stop iterating
            // since rest of the orderings are violated
            break;
        }
        // do not push empty entries
        // otherwise we may have `Some(vec![])` at the output ordering.
        if !new_ordering.is_empty() {
            all_orderings.push(new_ordering);
        }
    }
    all_orderings
}

// A normalized representation of file min/max statistics that allows for efficient sorting & comparison.
pub(crate) struct MinMaxStatistics {
    min: arrow::row::Rows,
    max: arrow::row::Rows,
}

impl MinMaxStatistics {
    fn new_from_files<'a>(
        sort_order: &[PhysicalSortExpr],
        table_schema: &SchemaRef,
        projected_schema: &SchemaRef,
        files: impl IntoIterator<Item = &'a PartitionedFile>,
    ) -> Result<Self> {
        use datafusion_common::ScalarValue;

        let statistics_and_partition_values = files
            .into_iter()
            .map(|file| {
                file.statistics
                    .as_ref()
                    .zip(Some(file.partition_values.as_slice()))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Plan("Parquet file missing statistics".to_string())
            })?;

        let get_min_max = |i: usize| -> (Vec<ScalarValue>, Vec<ScalarValue>) {
            statistics_and_partition_values
                .iter()
                .map(|(s, pv)| {
                    if i < s.column_statistics.len() {
                        (
                            s.column_statistics[i]
                                .min_value
                                .get_value()
                                .cloned()
                                .unwrap_or(ScalarValue::Null),
                            s.column_statistics[i]
                                .max_value
                                .get_value()
                                .cloned()
                                .unwrap_or(ScalarValue::Null),
                        )
                    } else {
                        let partition_value = &pv[i - s.column_statistics.len()];
                        (partition_value.clone(), partition_value.clone())
                    }
                })
                .unzip()
        };

        let (min_values, max_values): (Vec<_>, Vec<_>) = projected_schema
            .fields()
            .iter()
            .map(|field| {
                let (min, max) =
                    get_min_max(table_schema.index_of(field.name()).map_err(|e| {
                        DataFusionError::ArrowError(
                            e,
                            Some(format!("get min/max for field: '{}'", field.name())),
                        )
                    })?);
                Ok((
                    ScalarValue::iter_to_array(min)?,
                    ScalarValue::iter_to_array(max)?,
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.context("collect min/max values"))?
            .into_iter()
            .unzip();

        Self::new(
            sort_order,
            table_schema,
            RecordBatch::try_new(Arc::clone(projected_schema), min_values).map_err(
                |e| {
                    DataFusionError::ArrowError(e, Some("\ncreate min batch".to_string()))
                },
            )?,
            RecordBatch::try_new(Arc::clone(projected_schema), max_values).map_err(
                |e| {
                    DataFusionError::ArrowError(e, Some("\ncreate max batch".to_string()))
                },
            )?,
        )
    }

    fn new(
        sort_order: &[PhysicalSortExpr],
        table_schema: &SchemaRef,
        min_values: RecordBatch,
        max_values: RecordBatch,
    ) -> Result<Self> {
        use arrow::row::*;

        let sort_fields = sort_order
            .iter()
            .map(|expr| {
                expr.expr
                    .data_type(table_schema)
                    .map(|data_type| SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.context("create sort fields"))?;
        let converter = RowConverter::new(sort_fields)?;

        let [min, max] = [min_values, max_values].map(|values| {
            let sorting_columns = sort_order
                .iter()
                .map(|sort_expr| {
                    let column = sort_expr
                        .expr
                        .as_any()
                        .downcast_ref::<datafusion_physical_expr::expressions::Column>()
                        .ok_or(DataFusionError::Plan(
                            "sort expression must be on column".to_string(),
                        ))?;

                    let schema = values.schema();

                    let idx = schema.index_of(column.name())?;
                    let field = schema.field(idx);

                    // check that sort columns are non-nullable
                    if field.is_nullable() {
                        return Err(DataFusionError::Plan(
                            "cannot sort by nullable column".to_string(),
                        ));
                    }

                    Ok(SortColumn {
                        values: Arc::clone(values.column(idx)),
                        options: Some(sort_expr.options),
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|e| e.context("create sorting columns"))?;
            converter
                .convert_columns(
                    &sorting_columns
                        .into_iter()
                        .map(|c| c.values)
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| {
                    DataFusionError::ArrowError(e, Some("convert columns".to_string()))
                })
        });

        Ok(Self {
            min: min.map_err(|e| e.context("build min rows"))?,
            max: max.map_err(|e| e.context("build max rows"))?,
        })
    }

    fn is_sorted(&self) -> bool {
        self.max
            .iter()
            .zip(self.min.iter().skip(1))
            .all(|(max, next_min)| max < next_min)
    }
}

/// Represents the possible outcomes of a range calculation.
///
/// This enum is used to encapsulate the result of calculating the range of
/// bytes to read from an object (like a file) in an object store.
///
/// Variants:
/// - `Range(Option<Range<usize>>)`:
///   Represents a range of bytes to be read. It contains an `Option` wrapping a
///   `Range<usize>`. `None` signifies that the entire object should be read,
///   while `Some(range)` specifies the exact byte range to read.
/// - `TerminateEarly`:
///   Indicates that the range calculation determined no further action is
///   necessary, possibly because the calculated range is empty or invalid.
enum RangeCalculation {
    Range(Option<Range<usize>>),
    TerminateEarly,
}

/// Calculates an appropriate byte range for reading from an object based on the
/// provided metadata.
///
/// This asynchronous function examines the `FileMeta` of an object in an object store
/// and determines the range of bytes to be read. The range calculation may adjust
/// the start and end points to align with meaningful data boundaries (like newlines).
///
/// Returns a `Result` wrapping a `RangeCalculation`, which is either a calculated byte range or an indication to terminate early.
///
/// Returns an `Error` if any part of the range calculation fails, such as issues in reading from the object store or invalid range boundaries.
async fn calculate_range(
    file_meta: &FileMeta,
    store: &Arc<dyn ObjectStore>,
) -> Result<RangeCalculation> {
    let location = file_meta.location();
    let file_size = file_meta.object_meta.size;

    match file_meta.range {
        None => Ok(RangeCalculation::Range(None)),
        Some(FileRange { start, end }) => {
            let (start, end) = (start as usize, end as usize);

            let start_delta = if start != 0 {
                find_first_newline(store, location, start - 1, file_size).await?
            } else {
                0
            };

            let end_delta = if end != file_size {
                find_first_newline(store, location, end - 1, file_size).await?
            } else {
                0
            };

            let range = start + start_delta..end + end_delta;

            if range.start == range.end {
                return Ok(RangeCalculation::TerminateEarly);
            }

            Ok(RangeCalculation::Range(Some(range)))
        }
    }
}

/// Asynchronously finds the position of the first newline character in a specified byte range
/// within an object, such as a file, in an object store.
///
/// This function scans the contents of the object starting from the specified `start` position
/// up to the `end` position, looking for the first occurrence of a newline (`'\n'`) character.
/// It returns the position of the first newline relative to the start of the range.
///
/// Returns a `Result` wrapping a `usize` that represents the position of the first newline character found within the specified range. If no newline is found, it returns the length of the scanned data, effectively indicating the end of the range.
///
/// The function returns an `Error` if any issues arise while reading from the object store or processing the data stream.
///
async fn find_first_newline(
    object_store: &Arc<dyn ObjectStore>,
    location: &Path,
    start: usize,
    end: usize,
) -> Result<usize> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };

    let result = object_store.get_opts(location, options).await?;
    let mut result_stream = result.into_stream();

    let mut index = 0;

    while let Some(chunk) = result_stream.next().await.transpose()? {
        if let Some(position) = chunk.iter().position(|&byte| byte == b'\n') {
            return Ok(index + position);
        }

        index += chunk.len();
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::{DefaultDisplay, VerboseDisplay};

    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Float64Type, UInt32Type};
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Int32Array, Int64Array, StringArray,
        UInt64Array,
    };
    use arrow_schema::Field;

    use chrono::Utc;

    #[test]
    fn schema_mapping_map_batch() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::UInt32, true),
            Field::new("c3", DataType::Float64, true),
        ]));

        let adapter = SchemaAdapter::new(table_schema.clone());

        let file_schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::UInt64, true),
            Field::new("c3", DataType::Float32, true),
        ]);

        let (mapping, _) = adapter.map_schema(&file_schema).expect("map schema failed");

        let c1 = StringArray::from(vec!["hello", "world"]);
        let c2 = UInt64Array::from(vec![9_u64, 5_u64]);
        let c3 = Float32Array::from(vec![2.0_f32, 7.0_f32]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3)],
        )
        .unwrap();

        let mapped_batch = mapping.map_batch(batch).unwrap();

        assert_eq!(mapped_batch.schema(), table_schema);
        assert_eq!(mapped_batch.num_columns(), 3);
        assert_eq!(mapped_batch.num_rows(), 2);

        let c1 = mapped_batch.column(0).as_string::<i32>();
        let c2 = mapped_batch.column(1).as_primitive::<UInt32Type>();
        let c3 = mapped_batch.column(2).as_primitive::<Float64Type>();

        assert_eq!(c1.value(0), "hello");
        assert_eq!(c1.value(1), "world");
        assert_eq!(c2.value(0), 9_u32);
        assert_eq!(c2.value(1), 5_u32);
        assert_eq!(c3.value(0), 2.0_f64);
        assert_eq!(c3.value(1), 7.0_f64);
    }

    #[test]
    fn schema_adapter_map_schema_with_projection() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Utf8, true),
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Int32, true),
            Field::new("c4", DataType::Float32, true),
        ]));

        let file_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("c1", DataType::Boolean, true),
            Field::new("c2", DataType::Float32, true),
            Field::new("c3", DataType::Binary, true),
            Field::new("c4", DataType::Int64, true),
        ]);

        let indices = vec![1, 2, 4];
        let schema = SchemaRef::from(table_schema.project(&indices).unwrap());
        let adapter = SchemaAdapter::new(schema);
        let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();

        let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let c1 = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let c2 = Float32Array::from(vec![Some(2.0_f32), Some(7.0_f32), Some(3.0_f32)]);
        let c3 = BinaryArray::from_opt_vec(vec![
            Some(b"hallo"),
            Some(b"danke"),
            Some(b"super"),
        ]);
        let c4 = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![
                Arc::new(id),
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
            ],
        )
        .unwrap();
        let rows_num = batch.num_rows();
        let projected = batch.project(&projection).unwrap();
        let mapped_batch = mapping.map_batch(projected).unwrap();

        assert_eq!(
            mapped_batch.schema(),
            Arc::new(table_schema.project(&indices).unwrap())
        );
        assert_eq!(mapped_batch.num_columns(), indices.len());
        assert_eq!(mapped_batch.num_rows(), rows_num);

        let c1 = mapped_batch.column(0).as_string::<i32>();
        let c2 = mapped_batch.column(1).as_primitive::<Float64Type>();
        let c4 = mapped_batch.column(2).as_primitive::<Float32Type>();

        assert_eq!(c1.value(0), "true");
        assert_eq!(c1.value(1), "false");
        assert_eq!(c1.value(2), "true");

        assert_eq!(c2.value(0), 2.0_f64);
        assert_eq!(c2.value(1), 7.0_f64);
        assert_eq!(c2.value(2), 3.0_f64);

        assert_eq!(c4.value(0), 1.0_f32);
        assert_eq!(c4.value(1), 2.0_f32);
        assert_eq!(c4.value(2), 3.0_f32);
    }

    #[test]
    fn file_groups_display_empty() {
        let expected = "{0 groups: []}";
        assert_eq!(DefaultDisplay(FileGroupsDisplay(&[])).to_string(), expected);
    }

    #[test]
    fn file_groups_display_one() {
        let files = [vec![partitioned_file("foo"), partitioned_file("bar")]];

        let expected = "{1 group: [[foo, bar]]}";
        assert_eq!(
            DefaultDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_many_default() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![],
        ];

        let expected = "{3 groups: [[foo, bar], [baz], []]}";
        assert_eq!(
            DefaultDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_many_verbose() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![],
        ];

        let expected = "{3 groups: [[foo, bar], [baz], []]}";
        assert_eq!(
            VerboseDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_too_many_default() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![partitioned_file("qux")],
            vec![partitioned_file("quux")],
            vec![partitioned_file("quuux")],
            vec![partitioned_file("quuuux")],
            vec![],
        ];

        let expected = "{7 groups: [[foo, bar], [baz], [qux], [quux], [quuux], ...]}";
        assert_eq!(
            DefaultDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_too_many_verbose() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![partitioned_file("qux")],
            vec![partitioned_file("quux")],
            vec![partitioned_file("quuux")],
            vec![partitioned_file("quuuux")],
            vec![],
        ];

        let expected =
            "{7 groups: [[foo, bar], [baz], [qux], [quux], [quuux], [quuuux], []]}";
        assert_eq!(
            VerboseDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_group_display_many_default() {
        let files = vec![partitioned_file("foo"), partitioned_file("bar")];

        let expected = "[foo, bar]";
        assert_eq!(
            DefaultDisplay(FileGroupDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_group_display_too_many_default() {
        let files = vec![
            partitioned_file("foo"),
            partitioned_file("bar"),
            partitioned_file("baz"),
            partitioned_file("qux"),
            partitioned_file("quux"),
            partitioned_file("quuux"),
        ];

        let expected = "[foo, bar, baz, qux, quux, ...]";
        assert_eq!(
            DefaultDisplay(FileGroupDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_group_display_too_many_verbose() {
        let files = vec![
            partitioned_file("foo"),
            partitioned_file("bar"),
            partitioned_file("baz"),
            partitioned_file("qux"),
            partitioned_file("quux"),
            partitioned_file("quuux"),
        ];

        let expected = "[foo, bar, baz, qux, quux, quuux]";
        assert_eq!(
            VerboseDisplay(FileGroupDisplay(&files)).to_string(),
            expected
        );
    }

    /// create a PartitionedFile for testing
    fn partitioned_file(path: &str) -> PartitionedFile {
        let object_meta = ObjectMeta {
            location: object_store::path::Path::parse(path).unwrap(),
            last_modified: Utc::now(),
            size: 42,
            e_tag: None,
            version: None,
        };

        PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
        }
    }
}
