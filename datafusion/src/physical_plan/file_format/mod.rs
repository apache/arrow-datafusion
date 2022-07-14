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
mod csv;
mod file_stream;
mod json;
mod parquet;

pub(crate) use self::parquet::plan_to_parquet;
pub use self::parquet::ParquetExec;
use arrow::{
    array::{ArrayRef, DictionaryArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::{Error as ArrowError, Result as ArrowResult},
};
pub use avro::AvroExec;
pub(crate) use csv::plan_to_csv;
pub use csv::CsvExec;
use datafusion_common::record_batch::RecordBatch;
pub use json::NdJsonExec;
use std::iter;

use crate::error::DataFusionError;
use crate::{
    datasource::{object_store::ObjectStore, PartitionedFile},
    error::Result,
    scalar::ScalarValue,
};
use arrow::array::{new_null_array, UInt16Array};
use arrow::datatypes::IntegerType;
use datafusion_common::field_util::{FieldExt, SchemaExt};
use lazy_static::lazy_static;
use log::info;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
    vec,
};

use super::{ColumnStatistics, Statistics};

lazy_static! {
    /// The datatype used for all partitioning columns for now
    pub static ref DEFAULT_PARTITION_COLUMN_DATATYPE: DataType =
        DataType::Dictionary(IntegerType::UInt16, Box::new(DataType::Utf8), false);
}

/// The base configurations to provide when creating a physical plan for
/// any given file format.
#[derive(Debug, Clone)]
pub struct FileScanConfig {
    /// Store from which the `files` should be fetched
    pub object_store: Arc<dyn ObjectStore>,
    /// Schema before projection. It contains the columns that are expected
    /// to be in the files without the table partition columns.
    pub file_schema: SchemaRef,
    /// List of files to be processed, grouped into partitions
    pub file_groups: Vec<Vec<PartitionedFile>>,
    /// Estimated overall statistics of the files, taking `filters` into account.
    pub statistics: Statistics,
    /// Columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    pub projection: Option<Vec<usize>>,
    /// The minimum number of records required from this source plan
    pub limit: Option<usize>,
    /// The partitioning column names
    pub table_partition_cols: Vec<String>,
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
                    &self.table_partition_cols[partition_idx],
                    DEFAULT_PARTITION_COLUMN_DATATYPE.clone(),
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

        let table_schema = Arc::new(Schema::new(table_fields));

        (table_schema, table_stats)
    }

    fn projected_file_column_names(&self) -> Option<Vec<String>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .map(|col_idx| self.file_schema.field(*col_idx).name().to_string())
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
#[derive(Debug)]
struct FileGroupsDisplay<'a>(&'a [Vec<PartitionedFile>]);

impl<'a> Display for FileGroupsDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let parts: Vec<_> = self
            .0
            .iter()
            .map(|pp| {
                pp.iter()
                    .map(|pf| pf.file_meta.path())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .collect();
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
            if let Ok(mapped_idx) = file_schema.index_of(field.name()) {
                if file_schema.field(mapped_idx).data_type() == field.data_type() {
                    mapped.push(mapped_idx)
                } else {
                    let msg = format!(
                        "Failed to map column projection for field {}. Incompatible data types {:?} and {:?}",
                        field.name(),
                        file_schema.field(mapped_idx).data_type(),
                        field.data_type()
                    );
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
                batch_schema.column_with_name(table_field.name())
            {
                cols.push(batch_cols[batch_idx].clone());
            } else {
                cols.push(
                    new_null_array(table_field.data_type().clone(), batch_rows).into(),
                )
            }
        }

        let projected_schema = Arc::new(self.table_schema.clone().project(projections)?);

        let merged_batch = RecordBatch::try_new(projected_schema, cols)?;

        Ok(merged_batch)
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
    key_array_cache: Option<UInt16Array>,
    /// Mapping between the indexes in the list of partition columns and the target
    /// schema. Sorted by index in the target schema so that we can iterate on it to
    /// insert the partition columns in the target record batch.
    projected_partition_indexes: Vec<(usize, usize)>,
    /// The schema of the table once the projection was applied.
    projected_schema: SchemaRef,
}

impl PartitionColumnProjector {
    // Create a projector to insert the partitioning columns into batches read from files
    // - projected_schema: the target schema with both file and partitioning columns
    // - table_partition_cols: all the partitioning column names
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
            key_array_cache: None,
            projected_schema,
        }
    }

    // Transform the batch read from the file by inserting the partitioning columns
    // to the right positions as deduced from `projected_schema`
    // - file_batch: batch read from the file, with internal projection applied
    // - partition_values: the list of partition values, one for each partition column
    fn project(
        &mut self,
        file_batch: RecordBatch,
        partition_values: &[ScalarValue],
    ) -> ArrowResult<RecordBatch> {
        let expected_cols =
            self.projected_schema.fields().len() - self.projected_partition_indexes.len();

        if file_batch.columns().len() != expected_cols {
            return Err(ArrowError::ExternalFormat(format!(
                "Unexpected batch schema from file, expected {} cols but got {}",
                expected_cols,
                file_batch.columns().len()
            )));
        }

        let mut cols = file_batch.columns().to_vec();
        for &(pidx, sidx) in &self.projected_partition_indexes {
            cols.insert(
                sidx,
                create_dict_array(
                    &mut self.key_array_cache,
                    &partition_values[pidx],
                    file_batch.num_rows(),
                ),
            )
        }
        RecordBatch::try_new(Arc::clone(&self.projected_schema), cols)
    }
}

fn create_dict_array(
    key_array_cache: &mut Option<UInt16Array>,
    val: &ScalarValue,
    len: usize,
) -> ArrayRef {
    // build value dictionary
    let dict_vals = val.to_array();

    // build keys array
    let sliced_keys = match key_array_cache {
        Some(buf) if buf.len() >= len => buf.slice(0, len),
        _ => key_array_cache
            .insert(UInt16Array::from_trusted_len_values_iter(
                iter::repeat(0).take(len),
            ))
            .clone(),
    };
    Arc::new(DictionaryArray::<u16>::from_data(sliced_keys, dict_vals))
}

#[cfg(test)]
mod tests {
    use crate::{
        assert_batches_eq,
        test::{build_table_i32, columns, object_store::TestObjectStore},
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
            vec!["date".to_owned()],
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
            vec!["date".to_owned()],
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
        let partition_cols =
            vec!["year".to_owned(), "month".to_owned(), "day".to_owned()];
        // create a projected schema
        let conf = config_for_projection(
            file_batch.schema().clone(),
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
        let mut proj = PartitionColumnProjector::new(proj_schema, &partition_cols);

        // project first batch
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
        assert_batches_eq!(expected, &[projected_batch]);

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
                    ScalarValue::Utf8(Some("2021".to_owned())),
                    ScalarValue::Utf8(Some("10".to_owned())),
                    ScalarValue::Utf8(Some("27".to_owned())),
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
        assert_batches_eq!(expected, &[projected_batch]);

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
                    ScalarValue::Utf8(Some("2021".to_owned())),
                    ScalarValue::Utf8(Some("10".to_owned())),
                    ScalarValue::Utf8(Some("28".to_owned())),
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
        assert_batches_eq!(expected, &[projected_batch]);
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
        table_partition_cols: Vec<String>,
    ) -> FileScanConfig {
        FileScanConfig {
            file_schema,
            file_groups: vec![vec![]],
            limit: None,
            object_store: TestObjectStore::new_arc(&[]),
            projection,
            statistics,
            table_partition_cols,
        }
    }
}
