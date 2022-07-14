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

//! A generic stream over file format readers that can be used by
//! any file format that read its files from start to end.
//!
//! Note: Most traits here need to be marked `Sync + Send` to be
//! compliant with the `SendableRecordBatchStream` trait.

use crate::datasource::object_store::ReadSeek;
use crate::{
    datasource::{object_store::ObjectStore, PartitionedFile},
    physical_plan::RecordBatchStream,
    scalar::ScalarValue,
};
use arrow::{
    datatypes::SchemaRef,
    error::{Error as ArrowError, Result as ArrowResult},
};
use datafusion_common::record_batch::RecordBatch;
use futures::Stream;
use std::{
    iter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::PartitionColumnProjector;

pub type FileIter = Box<dyn Iterator<Item = PartitionedFile> + Send + Sync>;
pub type BatchIter = Box<dyn Iterator<Item = ArrowResult<RecordBatch>> + Send + Sync>;

/// A closure that creates a file format reader (iterator over `RecordBatch`) from a `Read` object
/// and an optional number of required records.
pub trait FormatReaderOpener:
    FnMut(Box<dyn ReadSeek + Send + Sync>, &Option<usize>) -> BatchIter
    + Send
    + Unpin
    + 'static
{
}

impl<T> FormatReaderOpener for T where
    T: FnMut(Box<dyn ReadSeek + Send + Sync>, &Option<usize>) -> BatchIter
        + Send
        + Unpin
        + 'static
{
}

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream<F: FormatReaderOpener> {
    /// An iterator over record batches of the last file returned by file_iter
    batch_iter: BatchIter,
    /// Partitioning column values for the current batch_iter
    partition_values: Vec<ScalarValue>,
    /// An iterator over input files.
    file_iter: FileIter,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    projected_schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A closure that takes a reader and an optional remaining number of lines
    /// (before reaching the limit) and returns a batch iterator. If the file reader
    /// is not capable of limiting the number of records in the last batch, the file
    /// stream will take care of truncating it.
    file_reader: F,
    /// The partition column projector
    pc_projector: PartitionColumnProjector,
    /// the store from which to source the files.
    object_store: Arc<dyn ObjectStore>,
}

impl<F: FormatReaderOpener> FileStream<F> {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        files: Vec<PartitionedFile>,
        file_reader: F,
        projected_schema: SchemaRef,
        limit: Option<usize>,
        table_partition_cols: Vec<String>,
    ) -> Self {
        let pc_projector = PartitionColumnProjector::new(
            Arc::clone(&projected_schema),
            &table_partition_cols,
        );

        Self {
            file_iter: Box::new(files.into_iter()),
            batch_iter: Box::new(iter::empty()),
            partition_values: vec![],
            remain: limit,
            projected_schema,
            file_reader,
            pc_projector,
            object_store,
        }
    }

    /// Acts as a flat_map of record batches over files. Adds the partitioning
    /// Columns to the returned record batches.
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        match self.batch_iter.next() {
            Some(Ok(batch)) => {
                Some(self.pc_projector.project(batch, &self.partition_values))
            }
            Some(Err(e)) => Some(Err(e)),
            None => match self.file_iter.next() {
                Some(f) => {
                    self.partition_values = f.partition_values;
                    self.object_store
                        .file_reader(f.file_meta.sized_file)
                        .and_then(|r| r.sync_reader())
                        .map_err(|e| ArrowError::External("".to_owned(), Box::new(e)))
                        .and_then(|f| {
                            self.batch_iter = (self.file_reader)(f, &self.remain);
                            self.next_batch().transpose()
                        })
                        .transpose()
                }
                None => None,
            },
        }
    }
}

impl<F: FormatReaderOpener> Stream for FileStream<F> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // check if finished or no limit
        match self.remain {
            Some(r) if r == 0 => return Poll::Ready(None),
            None => return Poll::Ready(self.get_mut().next_batch()),
            Some(r) => r,
        };

        Poll::Ready(match self.as_mut().next_batch() {
            Some(Ok(item)) => {
                if let Some(remain) = self.remain.as_mut() {
                    if *remain >= item.num_rows() {
                        *remain -= item.num_rows();
                        Some(Ok(item))
                    } else {
                        let len = *remain;
                        *remain = 0;
                        Some(Ok(RecordBatch::try_new(
                            item.schema().clone(),
                            item.columns()
                                .iter()
                                .map(|column| column.slice(0, len).into())
                                .collect(),
                        )?))
                    }
                } else {
                    Some(Ok(item))
                }
            }
            other => other,
        })
    }
}

impl<F: FormatReaderOpener> RecordBatchStream for FileStream<F> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::{
        assert_batches_eq,
        error::Result,
        test::{make_partition, object_store::TestObjectStore},
    };

    /// helper that creates a stream of 2 files with the same pair of batches in each ([0,1,2] and [0,1])
    async fn create_and_collect(limit: Option<usize>) -> Vec<RecordBatch> {
        let records = vec![make_partition(3), make_partition(2)];

        let source_schema = records[0].schema().clone();

        let reader = move |_file, _remain: &Option<usize>| {
            // this reader returns the same batch regardless of the file
            Box::new(records.clone().into_iter().map(Ok)) as BatchIter
        };

        let file_stream = FileStream::new(
            TestObjectStore::new_arc(&[("mock_file1", 10), ("mock_file2", 20)]),
            vec![
                PartitionedFile::new("mock_file1".to_owned(), 10),
                PartitionedFile::new("mock_file2".to_owned(), 20),
            ],
            reader,
            source_schema,
            limit,
            vec![],
        );

        file_stream
            .map(|b| b.expect("No error expected in stream"))
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn without_limit() -> Result<()> {
        let batches = create_and_collect(None).await;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_between_files() -> Result<()> {
        let batches = create_and_collect(Some(5)).await;
        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_at_middle_of_batch() -> Result<()> {
        let batches = create_and_collect(Some(6)).await;
        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "+---+",
        ], &batches);

        Ok(())
    }
}
