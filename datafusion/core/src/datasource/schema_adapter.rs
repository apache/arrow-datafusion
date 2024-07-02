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

//! [`SchemaAdapter`] and [`SchemaAdapterFactory`] to adapt file-level record batches to a table schema.
//!
//! Adapter provides a method of translating the RecordBatches that come out of the
//! physical format into how they should be used by DataFusion.  For instance, a schema
//! can be stored external to a parquet file that maps parquet logical types to arrow types.

use arrow::compute::{can_cast_types, cast};
use arrow_array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::plan_err;
use std::fmt::Debug;
use std::sync::Arc;

/// Factory for creating [`SchemaAdapter`]
///
/// This interface provides a way to implement custom schema adaptation logic
/// for ParquetExec (for example, to fill missing columns with default value
/// other than null)
pub trait SchemaAdapterFactory: Debug + Send + Sync + 'static {
    /// Provides `SchemaAdapter`.
    fn create(&self, schema: SchemaRef) -> Box<dyn SchemaAdapter>;
}

/// Adapt file-level [`RecordBatch`]es to a table schema, which may have a schema
/// obtained from merging multiple file-level schemas.
///
/// This is useful for enabling schema evolution in partitioned datasets.
///
/// This has to be done in two stages.
///
/// 1. Before reading the file, we have to map projected column indexes from the
///    table schema to the file schema.
///
/// 2. After reading a record batch map the read columns back to the expected
///    columns indexes and insert null-valued columns wherever the file schema was
///    missing a column present in the table schema.
pub trait SchemaAdapter: Send + Sync {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize>;

    /// Creates a `SchemaMapping` that can be used to cast or map the columns
    /// from the file schema to the table schema.
    ///
    /// If the provided `file_schema` contains columns of a different type to the expected
    /// `table_schema`, the method will attempt to cast the array data from the file schema
    /// to the table schema where possible.
    ///
    /// Returns a [`SchemaMapper`] that can be applied to the output batch
    /// along with an ordered list of columns to project from the file
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)>;
}

/// Creates a `SchemaMapping` that can be used to cast or map the columns
/// from the file schema to the table schema.
pub trait SchemaMapper: Debug + Send + Sync {
    /// Adapts a `RecordBatch` to match the `table_schema` using the stored mapping and conversions.
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch>;

    /// Adapts a [`RecordBatch`] that does not  have all the columns from the
    /// file schema.
    ///
    /// This method is used when applying a filter to a subset of the columns during
    /// an `ArrowPredicate`.
    ///
    /// This method is slower than `map_batch` as it looks up columns by name.
    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch>;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DefaultSchemaAdapterFactory {}

impl SchemaAdapterFactory for DefaultSchemaAdapterFactory {
    fn create(&self, table_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Box::new(DefaultSchemaAdapter { table_schema })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DefaultSchemaAdapter {
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter for DefaultSchemaAdapter {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
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
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
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
            Arc::new(SchemaMapping {
                table_schema: Arc::clone(&self.table_schema),
                field_mappings,
            }),
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

impl SchemaMapper for SchemaMapping {
    /// Adapts a `RecordBatch` to match the `table_schema` using the stored mapping and conversions.
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
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
            .collect::<datafusion_common::Result<Vec<_>, _>>()?;

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = Arc::clone(&self.table_schema);
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }

    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch> {
        let batch_cols = batch.columns().to_vec();
        let schema = batch.schema();

        let mut cols = vec![];
        let mut fields = vec![];
        for (i, f) in schema.fields().iter().enumerate() {
            let table_field = self.table_schema.field_with_name(f.name());
            if let Ok(tf) = table_field {
                cols.push(cast(&batch_cols[i], tf.data_type())?);
                fields.push(tf.clone());
            }
        }

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = Arc::new(Schema::new(fields));
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use crate::assert_batches_sorted_eq;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, SchemaRef};
    use object_store::path::Path;
    use object_store::ObjectMeta;

    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use crate::physical_plan::collect;
    use crate::prelude::SessionContext;

    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::schema_adapter::{
        SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
    };
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    #[tokio::test]
    async fn can_override_schema_adapter() {
        // Test shows that SchemaAdapter can add a column that doesn't existin in the
        // record batches returned from parquet.  This can be useful for schema evolution
        // where older files may not have all columns.
        let tmp_dir = TempDir::new().unwrap();
        let table_dir = tmp_dir.path().join("parquet_test");
        fs::DirBuilder::new().create(table_dir.as_path()).unwrap();
        let f1 = Field::new("id", DataType::Int32, true);

        let file_schema = Arc::new(Schema::new(vec![f1.clone()]));
        let filename = "part.parquet".to_string();
        let path = table_dir.as_path().join(filename.clone());
        let file = fs::File::create(path.clone()).unwrap();
        let mut writer = ArrowWriter::try_new(file, file_schema.clone(), None).unwrap();

        let ids = Arc::new(Int32Array::from(vec![1i32]));
        let rec_batch = RecordBatch::try_new(file_schema.clone(), vec![ids]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();

        let location = Path::parse(path.to_str().unwrap()).unwrap();
        let metadata = std::fs::metadata(path.as_path()).expect("Local file metadata");
        let meta = ObjectMeta {
            location,
            last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
            size: metadata.len() as usize,
            e_tag: None,
            version: None,
        };

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
        };

        let f1 = Field::new("id", DataType::Int32, true);
        let f2 = Field::new("extra_column", DataType::Utf8, true);

        let schema = Arc::new(Schema::new(vec![f1.clone(), f2.clone()]));

        // prepare the scan
        let parquet_exec = ParquetExec::builder(
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema)
                .with_file(partitioned_file),
        )
        .build()
        .with_schema_adapter_factory(Arc::new(TestSchemaAdapterFactory {}));

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

        let expected = [
            "+----+--------------+",
            "| id | extra_column |",
            "+----+--------------+",
            "| 1  | foo          |",
            "+----+--------------+",
        ];

        assert_batches_sorted_eq!(expected, &read);
    }

    #[derive(Debug)]
    struct TestSchemaAdapterFactory {}

    impl SchemaAdapterFactory for TestSchemaAdapterFactory {
        fn create(&self, schema: SchemaRef) -> Box<dyn SchemaAdapter> {
            Box::new(TestSchemaAdapter {
                table_schema: schema,
            })
        }
    }

    struct TestSchemaAdapter {
        /// Schema for the table
        table_schema: SchemaRef,
    }

    impl SchemaAdapter for TestSchemaAdapter {
        fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
            let field = self.table_schema.field(index);
            Some(file_schema.fields.find(field.name())?.0)
        }

        fn map_schema(
            &self,
            file_schema: &Schema,
        ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
            let mut projection = Vec::with_capacity(file_schema.fields().len());

            for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
                if self.table_schema.fields().find(file_field.name()).is_some() {
                    projection.push(file_idx);
                }
            }

            Ok((Arc::new(TestSchemaMapping {}), projection))
        }
    }

    #[derive(Debug)]
    struct TestSchemaMapping {}

    impl SchemaMapper for TestSchemaMapping {
        fn map_batch(
            &self,
            batch: RecordBatch,
        ) -> datafusion_common::Result<RecordBatch> {
            let f1 = Field::new("id", DataType::Int32, true);
            let f2 = Field::new("extra_column", DataType::Utf8, true);

            let schema = Arc::new(Schema::new(vec![f1.clone(), f2.clone()]));

            let extra_column = Arc::new(StringArray::from(vec!["foo"]));
            let mut new_columns = batch.columns().to_vec();
            new_columns.push(extra_column);

            Ok(RecordBatch::try_new(schema, new_columns).unwrap())
        }

        fn map_partial_batch(
            &self,
            batch: RecordBatch,
        ) -> datafusion_common::Result<RecordBatch> {
            self.map_batch(batch)
        }
    }
}
