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

//! Simple iterator over batches for use in testing

use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};
use tokio::sync::Barrier;

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::Stream;

use crate::{
    common,
    stream::{ReceiverStream, RecordBatchStreamAdapter},
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_physical_expr::PhysicalSortExpr;

use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_execution::TaskContext;

/// Index into the data that has been returned so far
#[derive(Debug, Default, Clone)]
pub struct BatchIndex {
    inner: Arc<std::sync::Mutex<usize>>,
}

impl BatchIndex {
    /// Return the current index
    pub fn value(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        *inner
    }

    // increment the current index by one
    pub fn incr(&self) {
        let mut inner = self.inner.lock().unwrap();
        *inner += 1;
    }
}

/// Iterator over batches
#[derive(Debug, Default)]
pub(crate) struct TestStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Index into the data that has been returned so far
    index: BatchIndex,
}

impl TestStream {
    /// Create an iterator for a vector of record batches. Assumes at
    /// least one entry in data (for the schema)
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self {
            data,
            ..Default::default()
        }
    }

    /// Return a handle to the index counter for this stream
    pub fn index(&self) -> BatchIndex {
        self.index.clone()
    }
}

impl Stream for TestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_batch = self.index.value();

        Poll::Ready(if next_batch < self.data.len() {
            let next_batch = self.index.value();
            self.index.incr();
            Some(Ok(self.data[next_batch].clone()))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for TestStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.data[0].schema()
    }
}

/// A Mock ExecutionPlan that can be used for writing tests of other
/// ExecutionPlans
#[derive(Debug)]
pub struct MockExec {
    /// the results to send back
    data: Vec<Result<RecordBatch>>,
    schema: SchemaRef,
    /// if true (the default), sends data using a separate task to to ensure the
    /// batches are not available without this stream yielding first
    use_task: bool,
}

impl MockExec {
    /// Create a new `MockExec` with a single partition that returns
    /// the specified `Results`s.
    ///
    /// By default, the batches are not produced immediately (the
    /// caller has to actually yield and another task must run) to
    /// ensure any poll loops are correct. This behavior can be
    /// changed with `with_use_task`
    pub fn new(data: Vec<Result<RecordBatch>>, schema: SchemaRef) -> Self {
        Self {
            data,
            schema,
            use_task: true,
        }
    }

    /// If `use_task` is true (the default) then the batches are sent
    /// back using a separate task to ensure the underlying stream is
    /// not immediately ready
    pub fn with_use_task(mut self, use_task: bool) -> Self {
        self.use_task = use_task;
        self
    }
}

impl DisplayAs for MockExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MockExec")
            }
        }
    }
}

impl ExecutionPlan for MockExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    /// Returns a stream which yields data
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        // Result doesn't implement clone, so do it ourself
        let data: Vec<_> = self
            .data
            .iter()
            .map(|r| match r {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(clone_error(e)),
            })
            .collect();

        if self.use_task {
            let mut builder = ReceiverStream::builder(self.schema(), 2);
            // send data in order but in a separate task (to ensure
            // the batches are not available without the stream
            // yielding).
            let tx = builder.tx();
            builder.spawn(async move {
                for batch in data {
                    println!("Sending batch via delayed stream");
                    if let Err(e) = tx.send(batch).await {
                        println!("ERROR batch via delayed stream: {e}");
                    }
                }

                Ok(())
            });
            // returned stream simply reads off the rx stream
            Ok(builder.build())
        } else {
            // make an input that will error
            let stream = futures::stream::iter(data);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    // Panics if one of the batches is an error
    fn statistics(&self) -> Statistics {
        let data: Result<Vec<_>> = self
            .data
            .iter()
            .map(|r| match r {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(clone_error(e)),
            })
            .collect();

        let data = data.unwrap();

        common::compute_record_batch_statistics(&[data], &self.schema, None)
    }
}

fn clone_error(e: &DataFusionError) -> DataFusionError {
    use DataFusionError::*;
    match e {
        Execution(msg) => Execution(msg.to_string()),
        _ => unimplemented!(),
    }
}

/// A Mock ExecutionPlan that does not start producing input until a
/// barrier is called
///
#[derive(Debug)]
pub struct BarrierExec {
    /// partitions to send back
    data: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,

    /// all streams wait on this barrier to produce
    barrier: Arc<Barrier>,
}

impl BarrierExec {
    /// Create a new exec with some number of partitions.
    pub fn new(data: Vec<Vec<RecordBatch>>, schema: SchemaRef) -> Self {
        // wait for all streams and the input
        let barrier = Arc::new(Barrier::new(data.len() + 1));
        Self {
            data,
            schema,
            barrier,
        }
    }

    /// wait until all the input streams and this function is ready
    pub async fn wait(&self) {
        println!("BarrierExec::wait waiting on barrier");
        self.barrier.wait().await;
        println!("BarrierExec::wait done waiting");
    }
}

impl DisplayAs for BarrierExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BarrierExec")
            }
        }
    }
}

impl ExecutionPlan for BarrierExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.data.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    /// Returns a stream which yields data
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert!(partition < self.data.len());

        let mut builder = ReceiverStream::builder(self.schema(), 2);

        // task simply sends data in order after barrier is reached
        let data = self.data[partition].clone();
        let b = self.barrier.clone();
        let tx = builder.tx();
        builder.spawn(async move {
            println!("Partition {partition} waiting on barrier");
            b.wait().await;
            for batch in data {
                println!("Partition {partition} sending batch");
                if let Err(e) = tx.send(Ok(batch)).await {
                    println!("ERROR batch via barrier stream stream: {e}");
                }
            }

            Ok(())
        });

        // returned stream simply reads off the rx stream
        Ok(builder.build())
    }

    fn statistics(&self) -> Statistics {
        common::compute_record_batch_statistics(&self.data, &self.schema, None)
    }
}

/// A mock execution plan that errors on a call to execute
#[derive(Debug)]
pub struct ErrorExec {
    schema: SchemaRef,
}

impl Default for ErrorExec {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorExec {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dummy",
            DataType::Int64,
            true,
        )]));
        Self { schema }
    }
}

impl DisplayAs for ErrorExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ErrorExec")
            }
        }
    }
}

impl ExecutionPlan for ErrorExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    /// Returns a stream which yields data
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("ErrorExec, unsurprisingly, errored in partition {partition}")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// A mock execution plan that simply returns the provided statistics
#[derive(Debug, Clone)]
pub struct StatisticsExec {
    stats: Statistics,
    schema: Arc<Schema>,
}
impl StatisticsExec {
    pub fn new(stats: Statistics, schema: Schema) -> Self {
        assert!(
            stats
                .column_statistics
                .as_ref()
                .map(|cols| cols.len() == schema.fields().len())
                .unwrap_or(true),
            "if defined, the column statistics vector length should be the number of fields"
        );
        Self {
            stats,
            schema: Arc::new(schema),
        }
    }
}

impl DisplayAs for StatisticsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StatisticsExec: col_count={}, row_count={:?}",
                    self.schema.fields().len(),
                    self.stats.num_rows,
                )
            }
        }
    }
}

impl ExecutionPlan for StatisticsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(2)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("This plan only serves for testing statistics")
    }

    fn statistics(&self) -> Statistics {
        self.stats.clone()
    }
}

/// Execution plan that emits streams that block forever.
///
/// This is useful to test shutdown / cancelation behavior of certain execution plans.
#[derive(Debug)]
pub struct BlockingExec {
    /// Schema that is mocked by this plan.
    schema: SchemaRef,

    /// Number of output partitions.
    n_partitions: usize,

    /// Ref-counting helper to check if the plan and the produced stream are still in memory.
    refs: Arc<()>,
}

impl BlockingExec {
    /// Create new [`BlockingExec`] with a give schema and number of partitions.
    pub fn new(schema: SchemaRef, n_partitions: usize) -> Self {
        Self {
            schema,
            n_partitions,
            refs: Default::default(),
        }
    }

    /// Weak pointer that can be used for ref-counting this execution plan and its streams.
    ///
    /// Use [`Weak::strong_count`] to determine if the plan itself and its streams are dropped (should be 0 in that
    /// case). Note that tokio might take some time to cancel spawned tasks, so you need to wrap this check into a retry
    /// loop. Use [`assert_strong_count_converges_to_zero`] to archive this.
    pub fn refs(&self) -> Weak<()> {
        Arc::downgrade(&self.refs)
    }
}

impl DisplayAs for BlockingExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BlockingExec",)
            }
        }
    }
}

impl ExecutionPlan for BlockingExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.n_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("Children cannot be replaced in {self:?}")
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(BlockingStream {
            schema: Arc::clone(&self.schema),
            _refs: Arc::clone(&self.refs),
        }))
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}

/// A [`RecordBatchStream`] that is pending forever.
#[derive(Debug)]
pub struct BlockingStream {
    /// Schema mocked by this stream.
    schema: SchemaRef,

    /// Ref-counting helper to check if the stream are still in memory.
    _refs: Arc<()>,
}

impl Stream for BlockingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl RecordBatchStream for BlockingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Asserts that the strong count of the given [`Weak`] pointer converges to zero.
///
/// This might take a while but has a timeout.
pub async fn assert_strong_count_converges_to_zero<T>(refs: Weak<T>) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if dbg!(Weak::strong_count(&refs)) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

///

/// Execution plan that emits streams that panics.
///
/// This is useful to test panic handling of certain execution plans.
#[derive(Debug)]
pub struct PanicExec {
    /// Schema that is mocked by this plan.
    schema: SchemaRef,

    /// Number of output partitions. Each partition will produce this
    /// many empty output record batches prior to panicing
    batches_until_panics: Vec<usize>,
}

impl PanicExec {
    /// Create new [`PanickingExec`] with a give schema and number of
    /// partitions, which will each panic immediately.
    pub fn new(schema: SchemaRef, n_partitions: usize) -> Self {
        Self {
            schema,
            batches_until_panics: vec![0; n_partitions],
        }
    }

    /// Set the number of batches prior to panic for a partition
    pub fn with_partition_panic(mut self, partition: usize, count: usize) -> Self {
        self.batches_until_panics[partition] = count;
        self
    }
}

impl DisplayAs for PanicExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PanickingExec",)
            }
        }
    }
}

impl ExecutionPlan for PanicExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        let num_partitions = self.batches_until_panics.len();
        Partitioning::UnknownPartitioning(num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("Children cannot be replaced in {:?}", self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(PanicStream {
            partition,
            batches_until_panic: self.batches_until_panics[partition],
            schema: Arc::clone(&self.schema),
            ready: false,
        }))
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}

/// A [`RecordBatchStream`] that yields every other batch and panics
/// after `batches_until_panic` batches have been produced.
///
/// Useful for testing the behavior of streams on panic
#[derive(Debug)]
struct PanicStream {
    /// Which partition was this
    partition: usize,
    /// How may batches will be produced until panic
    batches_until_panic: usize,
    /// Schema mocked by this stream.
    schema: SchemaRef,
    /// Should we return ready ?
    ready: bool,
}

impl Stream for PanicStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.batches_until_panic > 0 {
            if self.ready {
                self.batches_until_panic -= 1;
                self.ready = false;
                let batch = RecordBatch::new_empty(self.schema.clone());
                return Poll::Ready(Some(Ok(batch)));
            } else {
                self.ready = true;
                // get called again
                cx.waker().clone().wake();
                return Poll::Pending;
            }
        }
        panic!("PanickingStream did panic: {}", self.partition)
    }
}

impl RecordBatchStream for PanicStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
