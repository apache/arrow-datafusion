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

use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{ready, FutureExt, Stream, StreamExt};

use crate::datasource::listing::PartitionedFile;
use crate::error::Result;
use crate::physical_plan::file_format::{
    FileMeta, FileScanConfig, PartitionColumnProjector,
};
use crate::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};
use crate::physical_plan::RecordBatchStream;

/// A fallible future that resolves to a stream of [`RecordBatch`]
pub type FileOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>>;

/// Describes the behavior of the `FileStream` if file opening or scanning fails
pub enum OnError {
    /// Continue scanning, ignoring the failed file
    Fail,
    /// Fail the entire stream and return the underlying
    Skip,
}

/// Generic API for opening a file using an [`ObjectStore`] and resolving to a
/// stream of [`RecordBatch`]
///
/// [`ObjectStore`]: object_store::ObjectStore
pub trait FileOpener: Unpin {
    /// Asynchronously open the specified file and return a stream
    /// of [`RecordBatch`]
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture>;
}

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream<F: FileOpener> {
    /// An iterator over input files.
    file_iter: VecDeque<PartitionedFile>,
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
    /// The stream state
    state: FileStreamState,
    /// File stream specific metrics
    file_stream_metrics: FileStreamMetrics,
    /// runtime baseline metrics
    baseline_metrics: BaselineMetrics,
    /// Describes the behavior of the `FileStream` if file opening or scanning fails
    on_error: OnError,
}

/// Represents the state of the next `FileOpenFuture`. Since we need to poll
/// this future while scanning the current file, we need to store the result if it
/// is ready
enum NextOpen {
    Pending(FileOpenFuture),
    Ready(Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>),
}

enum FileStreamState {
    /// The idle state, no file is currently being read
    Idle,
    /// Currently performing asynchronous IO to obtain a stream of RecordBatch
    /// for a given parquet file
    Open {
        /// A [`FileOpenFuture`] returned by [`FileOpener::open`]
        future: FileOpenFuture,
        /// The partition values for this file
        partition_values: Vec<ScalarValue>,
    },
    /// Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
    /// returned by [`FileOpener::open`]
    Scan {
        /// Partitioning column values for the current batch_iter
        partition_values: Vec<ScalarValue>,
        /// The reader instance
        reader: BoxStream<'static, Result<RecordBatch, ArrowError>>,
        /// A [`FileOpenFuture`] for the next file to be processed,
        /// and its corresponding partition column values, if any.
        /// This allows the next file to be opened in parallel while the
        /// current file is read.
        next: Option<(NextOpen, Vec<ScalarValue>)>,
    },
    /// Encountered an error
    Error,
    /// Reached the row limit
    Limit,
}

/// A timer that can be started and stopped.
struct StartableTime {
    metrics: Time,
    // use for record each part cost time, will eventually add into 'metrics'.
    start: Option<Instant>,
}

impl StartableTime {
    fn start(&mut self) {
        assert!(self.start.is_none());
        self.start = Some(Instant::now());
    }

    fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.metrics.add_elapsed(start);
        }
    }
}

/// Metrics for [`FileStream`]
///
/// Note that all of these metrics are in terms of wall clock time
/// (not cpu time) so they include time spent waiting on I/O as well
/// as other operators.
struct FileStreamMetrics {
    /// Wall clock time elapsed for file opening.
    ///
    /// Time between when [`FileOpener::open`] is called and when the
    /// [`FileStream`] receives a stream for reading.
    ///
    /// If there are multiple files being scanned, the stream
    /// will open the next file in the background while scanning the
    /// current file. This metric will only capture time spent opening
    /// while not also scanning.
    pub time_opening: StartableTime,
    /// Wall clock time elapsed for file scanning + first record batch of decompression + decoding
    ///
    /// Time between when the [`FileStream`] requests data from the
    /// stream and when the first [`RecordBatch`] is produced.
    pub time_scanning_until_data: StartableTime,
    /// Total elapsed wall clock time for for scanning + record batch decompression / decoding
    ///
    /// Sum of time between when the [`FileStream`] requests data from
    /// the stream and when a [`RecordBatch`] is produced for all
    /// record batches in the stream. Note that this metric also
    /// includes the time of the parent operator's execution.
    pub time_scanning_total: StartableTime,
    /// Wall clock time elapsed for data decompression + decoding
    ///
    /// Time spent waiting for the FileStream's input.
    pub time_processing: StartableTime,
    /// Count of errors opening file.
    ///
    /// If using `OnError::Skip` this will provide a count of the number of files
    /// which were skipped and will not be included in the scan results.
    pub file_open_errors: Count,
    /// Count of errors scanning file
    ///
    /// If using `OnError::Skip` this will provide a count of the number of files
    /// which were skipped and will not be included in the scan results.
    pub file_scan_errors: Count,
}

impl FileStreamMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let time_opening = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_opening", partition),
            start: None,
        };

        let time_scanning_until_data = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_scanning_until_data", partition),
            start: None,
        };

        let time_scanning_total = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_scanning_total", partition),
            start: None,
        };

        let time_processing = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_processing", partition),
            start: None,
        };

        let file_open_errors =
            MetricBuilder::new(metrics).counter("file_open_errors", partition);

        let file_scan_errors =
            MetricBuilder::new(metrics).counter("file_scan_errors", partition);

        Self {
            time_opening,
            time_scanning_until_data,
            time_scanning_total,
            time_processing,
            file_open_errors,
            file_scan_errors,
        }
    }
}

impl<F: FileOpener> FileStream<F> {
    /// Create a new `FileStream` using the give `FileOpener` to scan underlying files
    pub fn new(
        config: &FileScanConfig,
        partition: usize,
        file_reader: F,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let (projected_schema, ..) = config.project();
        let pc_projector = PartitionColumnProjector::new(
            projected_schema.clone(),
            &config
                .table_partition_cols
                .iter()
                .map(|x| x.0.clone())
                .collect::<Vec<_>>(),
        );

        let files = config.file_groups[partition].clone();

        Ok(Self {
            file_iter: files.into(),
            projected_schema,
            remain: config.limit,
            file_reader,
            pc_projector,
            state: FileStreamState::Idle,
            file_stream_metrics: FileStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            on_error: OnError::Fail,
        })
    }

    /// Specify the behavior when an error occurs opening or scanning a file
    ///
    /// If `OnError::Skip` the stream will skip files which encounter an error and continue
    /// If `OnError:Fail` (default) the stream will fail and stop processing when an error occurs
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Begin opening the next file in parallel while decoding the current file in FileStream.
    ///
    /// Since file opening is mostly IO (and may involve a
    /// bunch of sequential IO), it can be parallelized with decoding.
    fn start_next_file(&mut self) -> Option<Result<(FileOpenFuture, Vec<ScalarValue>)>> {
        let part_file = self.file_iter.pop_front()?;

        let file_meta = FileMeta {
            object_meta: part_file.object_meta,
            range: part_file.range,
            extensions: part_file.extensions,
        };

        Some(
            self.file_reader
                .open(file_meta)
                .map(|future| (future, part_file.partition_values)),
        )
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => {
                    self.file_stream_metrics.time_opening.start();

                    match self.start_next_file().transpose() {
                        Ok(Some((future, partition_values))) => {
                            self.state = FileStreamState::Open {
                                future,
                                partition_values,
                            }
                        }
                        Ok(None) => return Poll::Ready(None),
                        Err(e) => {
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                FileStreamState::Open {
                    future,
                    partition_values,
                } => match ready!(future.poll_unpin(cx)) {
                    Ok(reader) => {
                        let partition_values = mem::take(partition_values);

                        // include time needed to start opening in `start_next_file`
                        self.file_stream_metrics.time_opening.stop();
                        let next = self.start_next_file().transpose();
                        self.file_stream_metrics.time_scanning_until_data.start();
                        self.file_stream_metrics.time_scanning_total.start();

                        match next {
                            Ok(Some((next_future, next_partition_values))) => {
                                self.state = FileStreamState::Scan {
                                    partition_values,
                                    reader,
                                    next: Some((
                                        NextOpen::Pending(next_future),
                                        next_partition_values,
                                    )),
                                };
                            }
                            Ok(None) => {
                                self.state = FileStreamState::Scan {
                                    reader,
                                    partition_values,
                                    next: None,
                                };
                            }
                            Err(e) => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Err(e) => {
                        self.file_stream_metrics.file_open_errors.add(1);
                        match self.on_error {
                            OnError::Skip => {
                                self.file_stream_metrics.time_opening.stop();
                                self.state = FileStreamState::Idle
                            }
                            OnError::Fail => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                },
                FileStreamState::Scan {
                    reader,
                    partition_values,
                    next,
                } => {
                    // We need to poll the next `FileOpenFuture` here to drive it forward
                    if let Some((next_open_future, _)) = next {
                        if let NextOpen::Pending(f) = next_open_future {
                            if let Poll::Ready(reader) = f.as_mut().poll(cx) {
                                *next_open_future = NextOpen::Ready(reader);
                            }
                        }
                    }
                    match ready!(reader.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();
                            let result = self
                                .pc_projector
                                .project(batch, partition_values)
                                .map_err(|e| ArrowError::ExternalError(e.into()))
                                .map(|batch| match &mut self.remain {
                                    Some(remain) => {
                                        if *remain > batch.num_rows() {
                                            *remain -= batch.num_rows();
                                            batch
                                        } else {
                                            let batch = batch.slice(0, *remain);
                                            self.state = FileStreamState::Limit;
                                            *remain = 0;
                                            batch
                                        }
                                    }
                                    None => batch,
                                });

                            if result.is_err() {
                                // If the partition value projection fails, this is not governed by
                                // the `OnError` behavior
                                self.state = FileStreamState::Error
                            }
                            self.file_stream_metrics.time_scanning_total.start();
                            return Poll::Ready(Some(result.map_err(Into::into)));
                        }
                        Some(Err(err)) => {
                            self.file_stream_metrics.file_scan_errors.add(1);
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match self.on_error {
                                // If `OnError::Skip` we skip the file as soon as we hit the first error
                                OnError::Skip => match mem::take(next) {
                                    Some((future, partition_values)) => {
                                        self.file_stream_metrics.time_opening.start();

                                        match future {
                                            NextOpen::Pending(future) => {
                                                self.state = FileStreamState::Open {
                                                    future,
                                                    partition_values,
                                                }
                                            }
                                            NextOpen::Ready(reader) => {
                                                self.state = FileStreamState::Open {
                                                    future: Box::pin(std::future::ready(
                                                        reader,
                                                    )),
                                                    partition_values,
                                                }
                                            }
                                        }
                                    }
                                    None => return Poll::Ready(None),
                                },
                                OnError::Fail => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(err.into())));
                                }
                            }
                        }
                        None => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match mem::take(next) {
                                Some((future, partition_values)) => {
                                    self.file_stream_metrics.time_opening.start();

                                    match future {
                                        NextOpen::Pending(future) => {
                                            self.state = FileStreamState::Open {
                                                future,
                                                partition_values,
                                            }
                                        }
                                        NextOpen::Ready(reader) => {
                                            self.state = FileStreamState::Open {
                                                future: Box::pin(std::future::ready(
                                                    reader,
                                                )),
                                                partition_values,
                                            }
                                        }
                                    }
                                }
                                None => return Poll::Ready(None),
                            }
                        }
                    }
                }
                FileStreamState::Error | FileStreamState::Limit => {
                    return Poll::Ready(None)
                }
            }
        }
    }
}

impl<F: FileOpener> Stream for FileStream<F> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.file_stream_metrics.time_processing.start();
        let result = self.poll_inner(cx);
        self.file_stream_metrics.time_processing.stop();
        self.baseline_metrics.record_poll(result)
    }
}

impl<F: FileOpener> RecordBatchStream for FileStream<F> {
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::DataFusionError;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::metrics::ExecutionPlanMetricsSet;
    use crate::prelude::SessionContext;
    use crate::{
        error::Result,
        test::{make_partition, object_store::register_test_store},
    };

    struct TestOpener {
        records: Vec<RecordBatch>,
    }

    impl FileOpener for TestOpener {
        fn open(&self, _file_meta: FileMeta) -> Result<FileOpenFuture> {
            let iterator = self.records.clone().into_iter().map(Ok);
            let stream = futures::stream::iter(iterator).boxed();
            Ok(futures::future::ready(Ok(stream)).boxed())
        }
    }

    /// helper that creates a stream of 2 files with the same pair of batches in each ([0,1,2] and [0,1])
    async fn create_and_collect(limit: Option<usize>) -> Vec<RecordBatch> {
        let records = vec![make_partition(3), make_partition(2)];
        let file_schema = records[0].schema();

        let reader = TestOpener { records };

        let ctx = SessionContext::new();
        register_test_store(&ctx, &[("mock_file1", 10), ("mock_file2", 20)]);

        let config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema,
            file_groups: vec![vec![
                PartitionedFile::new("mock_file1".to_owned(), 10),
                PartitionedFile::new("mock_file2".to_owned(), 20),
            ]],
            statistics: Default::default(),
            projection: None,
            limit,
            table_partition_cols: vec![],
            output_ordering: vec![],
            infinite_source: false,
        };
        let metrics_set = ExecutionPlanMetricsSet::new();
        let file_stream = FileStream::new(&config, 0, reader, &metrics_set).unwrap();

        file_stream
            .map(|b| b.expect("No error expected in stream"))
            .collect::<Vec<_>>()
            .await
    }

    /// Test `FileOpener` which will simulate errors during file opening or scanning
    struct TestOpenerWithErrors {
        /// Index in stream of files which should throw an error while opening
        error_opening_idx: Vec<usize>,
        /// Index in stream of files which should throw an error while scanning
        error_scanning_idx: Vec<usize>,
        /// Index of last file in stream
        current_idx: AtomicUsize,
        /// `RecordBatch` to return
        records: Vec<RecordBatch>,
    }

    impl FileOpener for TestOpenerWithErrors {
        fn open(&self, _file_meta: FileMeta) -> Result<FileOpenFuture> {
            let idx = self.current_idx.fetch_add(1, Ordering::SeqCst);

            if self.error_opening_idx.contains(&idx) {
                Ok(futures::future::ready(Err(DataFusionError::Internal(
                    "error opening".to_owned(),
                )))
                .boxed())
            } else if self.error_scanning_idx.contains(&idx) {
                let error = futures::future::ready(Err(ArrowError::IoError(
                    "error scanning".to_owned(),
                )));
                let stream = futures::stream::once(error).boxed();
                Ok(futures::future::ready(Ok(stream)).boxed())
            } else {
                let iterator = self.records.clone().into_iter().map(Ok);
                let stream = futures::stream::iter(iterator).boxed();
                Ok(futures::future::ready(Ok(stream)).boxed())
            }
        }
    }

    /// helper that creates a stream of files and simulates errors during opening and/or scanning
    async fn create_and_collect_with_errors(
        on_error: OnError,
        num_files: usize,
        scan_errors: Vec<usize>,
        open_errors: Vec<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let records = vec![make_partition(3), make_partition(2)];
        let file_schema = records[0].schema();

        let reader = TestOpenerWithErrors {
            error_opening_idx: open_errors,
            error_scanning_idx: scan_errors,
            current_idx: Default::default(),
            records,
        };

        let ctx = SessionContext::new();
        let mock_files: Vec<(String, u64)> = (0..num_files)
            .map(|idx| (format!("mock_file{idx}"), 10_u64))
            .collect();

        let mock_files_ref: Vec<(&str, u64)> = mock_files
            .iter()
            .map(|(name, size)| (name.as_str(), *size))
            .collect();

        register_test_store(&ctx, &mock_files_ref);

        let file_group = mock_files
            .into_iter()
            .map(|(name, size)| PartitionedFile::new(name, size))
            .collect();

        let config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema,
            file_groups: vec![file_group],
            statistics: Default::default(),
            projection: None,
            limit,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };
        let metrics_set = ExecutionPlanMetricsSet::new();
        let file_stream = FileStream::new(&config, 0, reader, &metrics_set)
            .unwrap()
            .with_on_error(on_error);

        file_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
    }

    #[tokio::test]
    async fn on_error_opening() -> Result<()> {
        let batches =
            create_and_collect_with_errors(OnError::Skip, 2, vec![], vec![0], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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

        let batches =
            create_and_collect_with_errors(OnError::Skip, 2, vec![], vec![1], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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

        let batches =
            create_and_collect_with_errors(OnError::Skip, 2, vec![], vec![0, 1], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn on_error_scanning() -> Result<()> {
        let batches =
            create_and_collect_with_errors(OnError::Skip, 2, vec![0], vec![], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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

        let batches =
            create_and_collect_with_errors(OnError::Skip, 2, vec![1], vec![], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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

        let batches =
            create_and_collect_with_errors(OnError::Skip, 2, vec![0, 1], vec![], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn on_error_mixed() -> Result<()> {
        let batches =
            create_and_collect_with_errors(OnError::Skip, 3, vec![0], vec![1], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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

        let batches =
            create_and_collect_with_errors(OnError::Skip, 3, vec![1], vec![0], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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

        let batches =
            create_and_collect_with_errors(OnError::Skip, 3, vec![0, 1], vec![2], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        let batches =
            create_and_collect_with_errors(OnError::Skip, 3, vec![1], vec![0, 2], None)
                .await?;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn without_limit() -> Result<()> {
        let batches = create_and_collect(None).await;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
