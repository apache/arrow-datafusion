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

//! CSV format abstractions

use std::any::Any;

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{self, datatypes::SchemaRef};
use async_trait::async_trait;
use bytes::{Buf, Bytes};

use datafusion_common::DataFusionError;

use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

use super::FileFormat;
use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::logical_expr::Expr;
use crate::physical_plan::file_format::{
    newline_delimited_stream, CsvExec, FileScanConfig,
};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// The default file extension of csv files
pub const DEFAULT_CSV_EXTENSION: &str = ".csv";
/// Character Separated Value `FileFormat` implementation.
#[derive(Debug)]
pub struct CsvFormat {
    has_header: bool,
    delimiter: u8,
    schema_infer_max_rec: Option<usize>,
    file_compression_type: FileCompressionType,
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_rec: Some(DEFAULT_SCHEMA_INFER_MAX_RECORD),
            has_header: true,
            delimiter: b',',
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

impl CsvFormat {
    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: Option<usize>) -> Self {
        self.schema_infer_max_rec = max_rec;
        self
    }

    /// Set true to indicate that the first line is a header.
    /// - default to true
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// True if the first line is a header.
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// The character separating values within a row.
    /// - default to ','
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set a `FileCompressionType` of CSV
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// The delimiter character.
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];

        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);

        for object in objects {
            // stream to only read as many rows as needed into memory
            let stream = self.read_to_delimited_chunks(store, object).await;
            let (schema, records_read) = self
                .infer_schema_from_stream(records_to_read, stream)
                .await?;
            records_to_read -= records_read;
            schemas.push(schema);
            if records_to_read == 0 {
                break;
            }
        }

        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = CsvExec::new(
            conf,
            self.has_header,
            self.delimiter,
            self.file_compression_type.to_owned(),
        );
        Ok(Arc::new(exec))
    }
}

impl CsvFormat {
    /// Return a newline delimited stream from the specified file on
    /// object store, decompressing if necessary
    ///
    /// Each returned `Bytes` has a whole number of newline delimited rows
    async fn read_to_delimited_chunks(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> impl Stream<Item = Result<Bytes>> {
        let file_compression_type = self.file_compression_type.to_owned();
        // stream to only read as many rows as needed into memory
        let stream = store
            .get(&object.location)
            .await
            .map_err(DataFusionError::ObjectStore);

        match stream {
            Ok(s) => {
                let file_stream = s.into_stream().map_err(DataFusionError::ObjectStore);
                let decoder = file_compression_type
                    .convert_stream(file_stream)
                    .map_err(|e| DataFusionError::External(Box::new(e)));
                match decoder {
                    Ok(decoded_stream) => newline_delimited_stream(
                        decoded_stream
                            .map_err(|e| DataFusionError::External(Box::new(e))),
                    )
                    .left_stream(),
                    Err(e) => futures::stream::iter(vec![Err(e)]).right_stream(),
                }
            }
            Err(e) => futures::stream::iter(vec![Err(e)]).right_stream(),
        }
    }

    /// Return the inferred schema reading up to records_to_read from a
    /// stream of delimited chunks returning the inferred schema and the
    /// number of lines that were read
    async fn infer_schema_from_stream(
        &self,
        mut records_to_read: usize,
        stream: impl Stream<Item = Result<Bytes>>,
    ) -> Result<(Schema, usize)> {
        let mut total_records_read = 0;
        let mut column_names = vec![];
        let mut column_type_possibilities = vec![];
        let mut first_chunk = true;

        pin_mut!(stream);

        while let Some(chunk) = stream.next().await.transpose()? {
            let (Schema { fields, .. }, records_read) =
                arrow::csv::reader::infer_reader_schema(
                    chunk.reader(),
                    self.delimiter,
                    Some(records_to_read),
                    // only consider header for first chunk
                    self.has_header && first_chunk,
                )?;
            records_to_read -= records_read;
            total_records_read += records_read;

            if first_chunk {
                // set up initial structures for recording inferred schema across chunks
                (column_names, column_type_possibilities) = fields
                    .into_iter()
                    .map(|field| {
                        let mut possibilities = HashSet::new();
                        if records_read > 0 {
                            // at least 1 data row read, record the inferred datatype
                            possibilities.insert(field.data_type().clone());
                        }
                        (field.name().clone(), possibilities)
                    })
                    .unzip();
                first_chunk = false;
            } else {
                if fields.len() != column_type_possibilities.len() {
                    return Err(DataFusionError::Execution(
                        format!(
                            "Encountered unequal lengths between records on CSV file whilst inferring schema. \
                             Expected {} records, found {} records",
                            column_type_possibilities.len(),
                            fields.len()
                        )
                    ));
                }

                column_type_possibilities.iter_mut().zip(fields).for_each(
                    |(possibilities, field)| {
                        possibilities.insert(field.data_type().clone());
                    },
                );
            }

            if records_to_read == 0 {
                break;
            }
        }

        let schema = build_schema_helper(column_names, &column_type_possibilities);
        Ok((schema, total_records_read))
    }
}

fn build_schema_helper(names: Vec<String>, types: &[HashSet<DataType>]) -> Schema {
    let fields = names
        .into_iter()
        .zip(types)
        .map(|(field_name, data_type_possibilities)| {
            // ripped from arrow::csv::reader::infer_reader_schema_with_csv_options
            // determine data type based on possible types
            // if there are incompatible types, use DataType::Utf8
            match data_type_possibilities.len() {
                1 => Field::new(
                    field_name,
                    data_type_possibilities.iter().next().unwrap().clone(),
                    true,
                ),
                2 => {
                    if data_type_possibilities.contains(&DataType::Int64)
                        && data_type_possibilities.contains(&DataType::Float64)
                    {
                        // we have an integer and double, fall down to double
                        Field::new(field_name, DataType::Float64, true)
                    } else {
                        // default to Utf8 for conflicting datatypes (e.g bool and int)
                        Field::new(field_name, DataType::Utf8, true)
                    }
                }
                _ => Field::new(field_name, DataType::Utf8, true),
            }
        })
        .collect();
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use super::*;
    use crate::datasource::file_format::test_util::VariableStream;
    use crate::physical_plan::collect;
    use crate::prelude::{SessionConfig, SessionContext};
    use bytes::Bytes;
    use chrono::DateTime;
    use datafusion_common::cast::as_string_array;
    use futures::StreamExt;
    use object_store::path::Path;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        // skip column 9 that overflows the automaticly discovered column type of i64 (u64 would work)
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]);
        let exec = get_exec(&state, "aggregate_test_100.csv", projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(12, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 50 /* 100/2 */);

        // test metadata
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0, 1, 2, 3]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let projection = None;
        let exec = get_exec(&state, "aggregate_test_100.csv", projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Int64",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8"
            ],
            x
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_char_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, "aggregate_test_100.csv", projection, None).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(100, batches[0].num_rows());

        let array = as_string_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..5 {
            values.push(array.value(i));
        }

        assert_eq!(vec!["c", "d", "b", "a", "b"], values);

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_stream() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let variable_object_store =
            Arc::new(VariableStream::new(Bytes::from("1,2,3,4,5\n"), 200));
        let object_meta = ObjectMeta {
            location: Path::parse("/")?,
            last_modified: DateTime::default(),
            size: usize::MAX,
        };

        let num_rows_to_read = 100;
        let csv_format = CsvFormat {
            has_header: false,
            schema_infer_max_rec: Some(num_rows_to_read),
            ..Default::default()
        };
        let inferred_schema = csv_format
            .infer_schema(
                &state,
                &(variable_object_store.clone() as Arc<dyn ObjectStore>),
                &[object_meta],
            )
            .await?;

        let actual_fields: Vec<_> = inferred_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "column_1: Int64",
                "column_2: Int64",
                "column_3: Int64",
                "column_4: Int64",
                "column_5: Int64"
            ],
            actual_fields
        );
        // ensuring on csv infer that it won't try to read entire file
        // should only read as many rows as was configured in the CsvFormat
        assert_eq!(
            num_rows_to_read,
            variable_object_store.get_iterations_detected()
        );

        Ok(())
    }

    async fn get_exec(
        state: &SessionState,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let root = format!("{}/csv", crate::test_util::arrow_test_data());
        let format = CsvFormat::default();
        scan_format(state, &format, &root, file_name, projection, limit).await
    }
}
