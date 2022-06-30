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

use super::arrow_array_reader::AvroArrowArrayReader;
use crate::arrow::datatypes::SchemaRef;
use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use arrow::error::Result as ArrowResult;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// Avro file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the Avro file
    ///
    /// If the schema is not supplied, the reader will try to read the schema.
    schema: Option<SchemaRef>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<String>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            batch_size: 1024,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring Avro parsing options.
    ///
    /// To convert a builder into a reader, call `Reader::from_builder`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate apache_avro;
    ///
    /// use std::fs::File;
    ///
    /// fn example() -> crate::datafusion::avro_to_arrow::Reader<'static, File> {
    ///     let file = File::open("test/data/basic.avro").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = crate::datafusion::avro_to_arrow::ReaderBuilder::new().read_schema().with_batch_size(100);
    ///
    ///     let reader = builder.build::<File>(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Avro file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the Avro reader to infer the schema of the file
    pub fn read_schema(mut self) -> Self {
        // remove any schema that is set
        self.schema = None;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<'a, R>(self, source: R) -> Result<Reader<'a, R>>
    where
        R: Read + Seek,
    {
        let mut source = source;

        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => Arc::new(super::read_avro_schema_from_reader(&mut source)?),
        };
        source.seek(SeekFrom::Start(0))?;
        Reader::try_new(source, schema, self.batch_size, self.projection)
    }
}

/// Avro file record  reader
pub struct Reader<'a, R: Read> {
    array_reader: AvroArrowArrayReader<'a, R>,
    schema: SchemaRef,
    batch_size: usize,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Create a new Avro Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn try_new(
        reader: R,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        Ok(Self {
            array_reader: AvroArrowArrayReader::try_new(
                reader,
                schema.clone(),
                projection,
            )?,
            schema,
            batch_size,
        })
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the next batch of results (defined by `self.batch_size`), or `None` if there
    /// are no more results
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> ArrowResult<Option<RecordBatch>> {
        self.array_reader.next_batch(self.batch_size)
    }
}

impl<'a, R: Read> Iterator for Reader<'a, R> {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::*;
    use crate::arrow::datatypes::{DataType, Field};
    use arrow::datatypes::TimeUnit;
    use std::fs::File;

    fn build_reader(name: &str) -> Reader<File> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/{}", testdata, name);
        let builder = ReaderBuilder::new().read_schema().with_batch_size(64);
        builder.build(File::open(filename).unwrap()).unwrap()
    }

    fn get_col<'a, T: 'static>(
        batch: &'a RecordBatch,
        col: (usize, &Field),
    ) -> Option<&'a T> {
        batch.column(col.0).as_any().downcast_ref::<T>()
    }

    #[test]
    fn test_avro_basic() {
        let mut reader = build_reader("alltypes_dictionary.avro");
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(2, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let id = schema.column_with_name("id").unwrap();
        assert_eq!(0, id.0);
        assert_eq!(&DataType::Int32, id.1.data_type());
        let col = get_col::<Int32Array>(&batch, id).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let bool_col = schema.column_with_name("bool_col").unwrap();
        assert_eq!(1, bool_col.0);
        assert_eq!(&DataType::Boolean, bool_col.1.data_type());
        let col = get_col::<BooleanArray>(&batch, bool_col).unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
        let tinyint_col = schema.column_with_name("tinyint_col").unwrap();
        assert_eq!(2, tinyint_col.0);
        assert_eq!(&DataType::Int32, tinyint_col.1.data_type());
        let col = get_col::<Int32Array>(&batch, tinyint_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let smallint_col = schema.column_with_name("smallint_col").unwrap();
        assert_eq!(3, smallint_col.0);
        assert_eq!(&DataType::Int32, smallint_col.1.data_type());
        let col = get_col::<Int32Array>(&batch, smallint_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let int_col = schema.column_with_name("int_col").unwrap();
        assert_eq!(4, int_col.0);
        let col = get_col::<Int32Array>(&batch, int_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        assert_eq!(&DataType::Int32, int_col.1.data_type());
        let col = get_col::<Int32Array>(&batch, int_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let bigint_col = schema.column_with_name("bigint_col").unwrap();
        assert_eq!(5, bigint_col.0);
        let col = get_col::<Int64Array>(&batch, bigint_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(10, col.value(1));
        assert_eq!(&DataType::Int64, bigint_col.1.data_type());
        let float_col = schema.column_with_name("float_col").unwrap();
        assert_eq!(6, float_col.0);
        let col = get_col::<Float32Array>(&batch, float_col).unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(1.1, col.value(1));
        assert_eq!(&DataType::Float32, float_col.1.data_type());
        let col = get_col::<Float32Array>(&batch, float_col).unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(1.1, col.value(1));
        let double_col = schema.column_with_name("double_col").unwrap();
        assert_eq!(7, double_col.0);
        assert_eq!(&DataType::Float64, double_col.1.data_type());
        let col = get_col::<Float64Array>(&batch, double_col).unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(10.1, col.value(1));
        let date_string_col = schema.column_with_name("date_string_col").unwrap();
        assert_eq!(8, date_string_col.0);
        assert_eq!(&DataType::Binary, date_string_col.1.data_type());
        let col = get_col::<BinaryArray>(&batch, date_string_col).unwrap();
        assert_eq!("01/01/09".as_bytes(), col.value(0));
        assert_eq!("01/01/09".as_bytes(), col.value(1));
        let string_col = schema.column_with_name("string_col").unwrap();
        assert_eq!(9, string_col.0);
        assert_eq!(&DataType::Binary, string_col.1.data_type());
        let col = get_col::<BinaryArray>(&batch, string_col).unwrap();
        assert_eq!("0".as_bytes(), col.value(0));
        assert_eq!("1".as_bytes(), col.value(1));
        let timestamp_col = schema.column_with_name("timestamp_col").unwrap();
        assert_eq!(10, timestamp_col.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            timestamp_col.1.data_type()
        );
        let col = get_col::<TimestampMicrosecondArray>(&batch, timestamp_col).unwrap();
        assert_eq!(1230768000000000, col.value(0));
        assert_eq!(1230768060000000, col.value(1));
    }
}
