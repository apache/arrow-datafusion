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

use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::array::{ArrayRef, Int64Array, Int8Array, StringArray};
use arrow::datatypes::{Field, Schema, SchemaBuilder};
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_sorted_eq;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileMeta, FileScanConfig, ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{collect, Statistics};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;

const EXPECTED_USER_DEFINED_METADATA: &str = "some-user-defined-metadata";

#[tokio::test]
async fn route_data_access_ops_to_parquet_file_reader_factory() {
    let c1: ArrayRef = Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
    let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
    let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

    let batch = create_batch(vec![
        ("c1", c1.clone()),
        ("c2", c2.clone()),
        ("c3", c3.clone()),
    ]);

    let file_schema = batch.schema().clone();
    let (in_memory_object_store, parquet_files_meta) =
        store_parquet_in_memory(vec![batch]).await;
    let file_groups = parquet_files_meta
        .into_iter()
        .map(|meta| PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: Some(Arc::new(String::from(EXPECTED_USER_DEFINED_METADATA))),
        })
        .collect();

    // prepare the scan
    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            // just any url that doesn't point to in memory object store
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![file_groups],
            statistics: Statistics::new_unknown(&file_schema),
            file_schema,
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        },
        None,
        None,
        Default::default(),
    )
    .with_parquet_file_reader_factory(Arc::new(InMemoryParquetFileReaderFactory(
        Arc::clone(&in_memory_object_store),
    )));

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

    let expected = [
        "+-----+----+----+",
        "| c1  | c2 | c3 |",
        "+-----+----+----+",
        "| Foo | 1  | 10 |",
        "|     | 2  | 20 |",
        "| bar |    |    |",
        "+-----+----+----+",
    ];

    assert_batches_sorted_eq!(expected, &read);
}

#[derive(Debug)]
struct InMemoryParquetFileReaderFactory(Arc<dyn ObjectStore>);

impl ParquetFileReaderFactory for InMemoryParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let metadata = file_meta
            .extensions
            .as_ref()
            .expect("has user defined metadata");
        let metadata = metadata
            .downcast_ref::<String>()
            .expect("has string metadata");

        assert_eq!(EXPECTED_USER_DEFINED_METADATA, &metadata[..]);

        let parquet_file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );

        Ok(Box::new(ParquetFileReader {
            store: Arc::clone(&self.0),
            meta: file_meta.object_meta,
            metrics: parquet_file_metrics,
            metadata_size_hint,
        }))
    }
}

fn create_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    columns.into_iter().fold(
        RecordBatch::new_empty(Arc::new(Schema::empty())),
        |batch, (field_name, arr)| add_to_batch(&batch, field_name, arr.clone()),
    )
}

fn add_to_batch(batch: &RecordBatch, field_name: &str, array: ArrayRef) -> RecordBatch {
    let mut fields = SchemaBuilder::from(batch.schema().fields());
    fields.push(Field::new(field_name, array.data_type().clone(), true));
    let schema = Arc::new(fields.finish());

    let mut columns = batch.columns().to_vec();
    columns.push(array);
    RecordBatch::try_new(schema, columns).expect("error; creating record batch")
}

async fn store_parquet_in_memory(
    batches: Vec<RecordBatch>,
) -> (Arc<dyn ObjectStore>, Vec<ObjectMeta>) {
    let in_memory = InMemory::new();

    let parquet_batches: Vec<(ObjectMeta, Bytes)> = batches
        .into_iter()
        .enumerate()
        .map(|(offset, batch)| {
            let mut buf = Vec::<u8>::with_capacity(32 * 1024);
            let mut output = Cursor::new(&mut buf);

            let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), None)
                .expect("creating writer");

            writer.write(&batch).expect("Writing batch");
            writer.close().unwrap();

            let meta = ObjectMeta {
                location: Path::parse(format!("file-{offset}.parquet"))
                    .expect("creating path"),
                last_modified: chrono::DateTime::from(SystemTime::now()),
                size: buf.len(),
                e_tag: None,
                version: None,
            };

            (meta, Bytes::from(buf))
        })
        .collect();

    let mut objects = Vec::with_capacity(parquet_batches.len());
    for (meta, bytes) in parquet_batches {
        in_memory
            .put(&meta.location, bytes)
            .await
            .expect("put parquet file into in memory object store");
        objects.push(meta);
    }

    (Arc::new(in_memory), objects)
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage
struct ParquetFileReader {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add(range.end - range.start);

        self.store
            .get_range(&self.meta.location, range)
            .map_err(|e| {
                ParquetError::General(format!("AsyncChunkReader::get_bytes error: {e}"))
            })
            .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = fetch_parquet_metadata(
                self.store.as_ref(),
                &self.meta,
                self.metadata_size_hint,
            )
            .await
            .map_err(|e| {
                ParquetError::General(format!(
                    "AsyncChunkReader::get_metadata error: {e}"
                ))
            })?;
            Ok(Arc::new(metadata))
        })
    }
}
