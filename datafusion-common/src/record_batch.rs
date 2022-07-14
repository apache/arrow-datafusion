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

//! Contains [`RecordBatch`].
use std::sync::Arc;

use crate::field_util::SchemaExt;
use arrow::array::*;
use arrow::chunk::Chunk;
use arrow::compute::filter::{build_filter, filter};
use arrow::datatypes::*;
use arrow::error::{Error as ArrowError, Result};

/// A two-dimensional dataset with a number of
/// columns ([`Array`]) and rows and defined [`Schema`](crate::datatypes::Schema).
/// # Implementation
/// Cloning is `O(C)` where `C` is the number of columns.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    schema: Arc<Schema>,
    columns: Vec<Arc<dyn Array>>,
}

impl RecordBatch {
    /// Creates a [`RecordBatch`] from a schema and columns.
    /// # Errors
    /// This function errors iff
    /// * `columns` is empty
    /// * the schema and column data types do not match
    /// * `columns` have a different length
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::array::PrimitiveArray;
    /// # use arrow::datatypes::{Schema, Field, DataType};
    /// # use datafusion_common::record_batch::RecordBatch;
    /// # use datafusion_common::field_util::SchemaExt;
    /// # fn main() -> arrow2::error::Result<()> {
    /// let id_array = PrimitiveArray::from_slice([1i32, 2, 3, 4, 5]);
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]));
    ///
    /// let batch = RecordBatch::try_new(
    ///     schema,
    ///     vec![Arc::new(id_array)]
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(schema: Arc<Schema>, columns: Vec<Arc<dyn Array>>) -> Result<Self> {
        let options = RecordBatchOptions::default();
        Self::validate_new_batch(&schema, columns.as_slice(), &options)?;
        Ok(RecordBatch { schema, columns })
    }

    /// Creates a [`RecordBatch`] from a schema and columns, with additional options,
    /// such as whether to strictly validate field names.
    ///
    /// See [`Self::try_new()`] for the expected conditions.
    pub fn try_new_with_options(
        schema: Arc<Schema>,
        columns: Vec<Arc<dyn Array>>,
        options: &RecordBatchOptions,
    ) -> Result<Self> {
        Self::validate_new_batch(&schema, &columns, options)?;
        Ok(RecordBatch { schema, columns })
    }

    /// Creates a new empty [`RecordBatch`].
    pub fn new_empty(schema: Arc<Schema>) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type().clone()).into())
            .collect();
        RecordBatch { schema, columns }
    }

    /// Creates a new [`RecordBatch`] from a [`arrow::chunk::Chunk`]
    pub fn new_with_chunk(schema: &Arc<Schema>, chunk: Chunk<ArrayRef>) -> Self {
        Self {
            schema: schema.clone(),
            columns: chunk.into_arrays(),
        }
    }

    /// Validate the schema and columns using [`RecordBatchOptions`]. Returns an error
    /// if any validation check fails.
    fn validate_new_batch(
        schema: &Schema,
        columns: &[Arc<dyn Array>],
        options: &RecordBatchOptions,
    ) -> Result<()> {
        // check that there are some columns
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "at least one column must be defined to create a record batch"
                    .to_string(),
            ));
        }
        // check that number of fields in schema match column length
        if schema.fields().len() != columns.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "number of columns({}) must match number of fields({}) in schema",
                columns.len(),
                schema.fields().len(),
            )));
        }
        // check that all columns have the same row count, and match the schema
        let len = columns[0].len();

        // This is a bit repetitive, but it is better to check the condition outside the loop
        if options.match_field_names {
            for (i, column) in columns.iter().enumerate() {
                if column.len() != len {
                    return Err(ArrowError::InvalidArgumentError(
                        "all columns in a record batch must have the same length"
                            .to_string(),
                    ));
                }
                if column.data_type() != schema.field(i).data_type() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "column types must match schema types, expected {:?} but found {:?} at column index {}",
                        schema.field(i).data_type(),
                        column.data_type(),
                        i)));
                }
            }
        } else {
            for (i, column) in columns.iter().enumerate() {
                if column.len() != len {
                    return Err(ArrowError::InvalidArgumentError(
                        "all columns in a record batch must have the same length"
                            .to_string(),
                    ));
                }
                if !column.data_type().eq(schema.field(i).data_type()) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "column types must match schema types, expected {:?} but found {:?} at column index {}",
                        schema.field(i).data_type(),
                        column.data_type(),
                        i)));
                }
            }
        }

        Ok(())
    }

    /// Returns the [`Schema`](crate::datatypes::Schema) of the record batch.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Returns the number of columns in the record batch.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::array::PrimitiveArray;
    /// # use arrow::datatypes::{Schema, Field, DataType};
    /// # use datafusion_common::record_batch::RecordBatch;
    /// # use datafusion_common::field_util::SchemaExt;
    /// # fn main() -> arrow2::error::Result<()> {
    /// let id_array = PrimitiveArray::from_slice([1i32, 2, 3, 4, 5]);
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]));
    ///
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)])?;
    ///
    /// assert_eq!(batch.num_columns(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of rows in each column.
    ///
    /// # Panics
    ///
    /// Panics if the `RecordBatch` contains no columns.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::array::PrimitiveArray;
    /// # use arrow::datatypes::{Schema, Field, DataType};
    /// # use datafusion_common::record_batch::RecordBatch;
    /// # use datafusion_common::field_util::SchemaExt;
    /// # fn main() -> arrow2::error::Result<()> {
    /// let id_array = PrimitiveArray::from_slice([1i32, 2, 3, 4, 5]);
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]));
    ///
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)])?;
    ///
    /// assert_eq!(batch.num_rows(), 5);
    /// # Ok(())
    /// # }
    /// ```
    pub fn num_rows(&self) -> usize {
        self.columns[0].len()
    }

    /// Get a reference to a column's array by index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside of `0..num_columns`.
    pub fn column(&self, index: usize) -> &Arc<dyn Array> {
        &self.columns[index]
    }

    /// Get a reference to all columns in the record batch.
    pub fn columns(&self) -> &[Arc<dyn Array>] {
        &self.columns[..]
    }

    /// Create a `RecordBatch` from an iterable list of pairs of the
    /// form `(field_name, array)`, with the same requirements on
    /// fields and arrays as [`RecordBatch::try_new`]. This method is
    /// often used to create a single `RecordBatch` from arrays,
    /// e.g. for testing.
    ///
    /// The resulting schema is marked as nullable for each column if
    /// the array for that column is has any nulls. To explicitly
    /// specify nullibility, use [`RecordBatch::try_from_iter_with_nullable`]
    ///
    /// Example:
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::*;
    /// use arrow::datatypes::DataType;
    /// use datafusion_common::record_batch::RecordBatch;
    ///
    /// let a: Arc<dyn Array> = Arc::new(Int32Array::from_slice(&[1, 2]));
    /// let b: Arc<dyn Array> = Arc::new(Utf8Array::<i32>::from_slice(&["a", "b"]));
    ///
    /// let record_batch = RecordBatch::try_from_iter(vec![
    ///   ("a", a),
    ///   ("b", b),
    /// ]);
    /// ```
    pub fn try_from_iter<I, F>(value: I) -> Result<Self>
    where
        I: IntoIterator<Item = (F, Arc<dyn Array>)>,
        F: AsRef<str>,
    {
        // TODO: implement `TryFrom` trait, once
        // https://github.com/rust-lang/rust/issues/50133 is no longer an
        // issue
        let iter = value.into_iter().map(|(field_name, array)| {
            let nullable = array.null_count() > 0;
            (field_name, array, nullable)
        });

        Self::try_from_iter_with_nullable(iter)
    }

    /// Create a `RecordBatch` from an iterable list of tuples of the
    /// form `(field_name, array, nullable)`, with the same requirements on
    /// fields and arrays as [`RecordBatch::try_new`]. This method is often
    /// used to create a single `RecordBatch` from arrays, e.g. for
    /// testing.
    ///
    /// Example:
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::*;
    /// use arrow::datatypes::DataType;
    /// use datafusion_common::record_batch::RecordBatch;
    ///
    /// let a: Arc<dyn Array> = Arc::new(Int32Array::from_slice(&[1, 2]));
    /// let b: Arc<dyn Array> = Arc::new(Utf8Array::<i32>::from_slice(&["a", "b"]));
    ///
    /// // Note neither `a` nor `b` has any actual nulls, but we mark
    /// // b an nullable
    /// let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
    ///   ("a", a, false),
    ///   ("b", b, true),
    /// ]);
    /// ```
    pub fn try_from_iter_with_nullable<I, F>(value: I) -> Result<Self>
    where
        I: IntoIterator<Item = (F, Arc<dyn Array>, bool)>,
        F: AsRef<str>,
    {
        // TODO: implement `TryFrom` trait, once
        // https://github.com/rust-lang/rust/issues/50133 is no longer an
        // issue
        let (fields, columns) = value
            .into_iter()
            .map(|(field_name, array, nullable)| {
                let field_name = field_name.as_ref();
                let field = Field::new(field_name, array.data_type().clone(), nullable);
                (field, array)
            })
            .unzip();

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
    }

    /// Deconstructs itself into its internal components
    pub fn into_inner(self) -> (Vec<Arc<dyn Array>>, Arc<Schema>) {
        let Self { columns, schema } = self;
        (columns, schema)
    }

    /// Projects the schema onto the specified columns
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch> {
        let projected_schema = self.schema.project(indices)?;
        let batch_fields = indices
            .iter()
            .map(|f| {
                self.columns.get(*f).cloned().ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "project index {} out of bounds, max field {}",
                        f,
                        self.columns.len()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(SchemaRef::new(projected_schema), batch_fields)
    }

    /// Return a new RecordBatch where each column is sliced
    /// according to `offset` and `length`
    ///
    /// # Panics
    ///
    /// Panics if `offset` with `length` is greater than column length.
    pub fn slice(&self, offset: usize, length: usize) -> RecordBatch {
        if self.schema.fields().is_empty() {
            assert!((offset + length) == 0);
            return RecordBatch::new_empty(self.schema.clone());
        }
        assert!((offset + length) <= self.num_rows());

        let columns = self
            .columns()
            .iter()
            .map(|column| Arc::from(column.slice(offset, length)))
            .collect();

        Self {
            schema: self.schema.clone(),
            columns,
        }
    }
}

/// Options that control the behaviour used when creating a [`RecordBatch`].
#[derive(Debug)]
pub struct RecordBatchOptions {
    /// Match field names of structs and lists. If set to `true`, the names must match.
    pub match_field_names: bool,
}

impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self {
            match_field_names: true,
        }
    }
}

impl From<StructArray> for RecordBatch {
    /// # Panics iff the null count of the array is not null.
    fn from(array: StructArray) -> Self {
        assert!(array.null_count() == 0);
        let (fields, values, _) = array.into_data();
        RecordBatch {
            schema: Arc::new(Schema::new(fields)),
            columns: values,
        }
    }
}

impl From<RecordBatch> for StructArray {
    fn from(batch: RecordBatch) -> Self {
        let (fields, values) = batch
            .schema
            .fields
            .iter()
            .zip(batch.columns.iter())
            .map(|t| (t.0.clone(), t.1.clone()))
            .unzip();
        StructArray::from_data(DataType::Struct(fields), values, None)
    }
}

impl From<RecordBatch> for Chunk<ArrayRef> {
    fn from(rb: RecordBatch) -> Self {
        Chunk::new(rb.columns)
    }
}

impl From<&RecordBatch> for Chunk<ArrayRef> {
    fn from(rb: &RecordBatch) -> Self {
        Chunk::new(rb.columns.clone())
    }
}

/// Returns a new [RecordBatch] with arrays containing only values matching the filter.
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    filter_values: &BooleanArray,
) -> Result<RecordBatch> {
    let num_colums = record_batch.columns().len();

    let filtered_arrays = match num_colums {
        1 => {
            vec![filter(record_batch.columns()[0].as_ref(), filter_values)?.into()]
        }
        _ => {
            let filter = build_filter(filter_values)?;
            record_batch
                .columns()
                .iter()
                .map(|a| filter(a.as_ref()).into())
                .collect()
        }
    };
    RecordBatch::try_new(record_batch.schema().clone(), filtered_arrays)
}
