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

use crate::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use crate::{
    common, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use futures::Stream;
use tokio::sync::Barrier;

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
pub struct TestStream {
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
    /// if true (the default), sends data using a separate task to ensure the
    /// batches are not available without this stream yielding first
    use_task: bool,
    cache: PlanProperties,
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
        let cache = Self::compute_properties(Arc::clone(&schema));
        Self {
            data,
            schema,
            use_task: true,
            cache,
        }
    }

    /// If `use_task` is true (the default) then the batches are sent
    /// back using a separate task to ensure the underlying stream is
    /// not immediately ready
    pub fn with_use_task(mut self, use_task: bool) -> Self {
        self.use_task = use_task;
        self
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
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
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
            let mut builder = RecordBatchReceiverStream::builder(self.schema(), 2);
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
    fn statistics(&self) -> Result<Statistics> {
        let data: Result<Vec<_>> = self
            .data
            .iter()
            .map(|r| match r {
                Ok(batch) => Ok(batch.clone()),
                Err(e) => Err(clone_error(e)),
            })
            .collect();

        let data = data?;

        Ok(common::compute_record_batch_statistics(
            &[data],
            &self.schema,
            None,
        ))
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
    cache: PlanProperties,
}

impl BarrierExec {
    /// Create a new exec with some number of partitions.
    pub fn new(data: Vec<Vec<RecordBatch>>, schema: SchemaRef) -> Self {
        // wait for all streams and the input
        let barrier = Arc::new(Barrier::new(data.len() + 1));
        let cache = Self::compute_properties(Arc::clone(&schema), &data);
        Self {
            data,
            schema,
            barrier,
            cache,
        }
    }

    /// wait until all the input streams and this function is ready
    pub async fn wait(&self) {
        println!("BarrierExec::wait waiting on barrier");
        self.barrier.wait().await;
        println!("BarrierExec::wait done waiting");
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        data: &[Vec<RecordBatch>],
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(data.len()),
            ExecutionMode::Bounded,
        )
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
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

        let mut builder = RecordBatchReceiverStream::builder(self.schema(), 2);

        // task simply sends data in order after barrier is reached
        let data = self.data[partition].clone();
        let b = Arc::clone(&self.barrier);
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

    fn statistics(&self) -> Result<Statistics> {
        Ok(common::compute_record_batch_statistics(
            &self.data,
            &self.schema,
            None,
        ))
    }
}

/// A mock execution plan that errors on a call to execute
#[derive(Debug)]
pub struct ErrorExec {
    cache: PlanProperties,
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
        let cache = Self::compute_properties(schema);
        Self { cache }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
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
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
}

/// A mock execution plan that simply returns the provided statistics
#[derive(Debug, Clone)]
pub struct StatisticsExec {
    stats: Statistics,
    schema: Arc<Schema>,
    cache: PlanProperties,
}
impl StatisticsExec {
    pub fn new(stats: Statistics, schema: Schema) -> Self {
        assert_eq!(
            stats
                .column_statistics.len(), schema.fields().len(),
            "if defined, the column statistics vector length should be the number of fields"
        );
        let cache = Self::compute_properties(Arc::new(schema.clone()));
        Self {
            stats,
            schema: Arc::new(schema),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(2),
            ExecutionMode::Bounded,
        )
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
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.stats.clone())
    }
}

/// Execution plan that emits streams that block forever.
///
/// This is useful to test shutdown / cancelation behavior of certain execution plans.
#[derive(Debug)]
pub struct BlockingExec {
    /// Schema that is mocked by this plan.
    schema: SchemaRef,

    /// Ref-counting helper to check if the plan and the produced stream are still in memory.
    refs: Arc<()>,
    cache: PlanProperties,
}

impl BlockingExec {
    /// Create new [`BlockingExec`] with a give schema and number of partitions.
    pub fn new(schema: SchemaRef, n_partitions: usize) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema), n_partitions);
        Self {
            schema,
            refs: Default::default(),
            cache,
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

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef, n_partitions: usize) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(n_partitions),
            ExecutionMode::Bounded,
        )
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
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
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
    cache: PlanProperties,
}

impl PanicExec {
    /// Create new [`PanicExec`] with a give schema and number of
    /// partitions, which will each panic immediately.
    pub fn new(schema: SchemaRef, n_partitions: usize) -> Self {
        let batches_until_panics = vec![0; n_partitions];
        let cache = Self::compute_properties(Arc::clone(&schema), &batches_until_panics);
        Self {
            schema,
            batches_until_panics,
            cache,
        }
    }

    /// Set the number of batches prior to panic for a partition
    pub fn with_partition_panic(mut self, partition: usize, count: usize) -> Self {
        self.batches_until_panics[partition] = count;
        self
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        batches_until_panics: &[usize],
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        let num_partitions = batches_until_panics.len();

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(num_partitions),
            ExecutionMode::Bounded,
        )
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
                write!(f, "PanicExec",)
            }
        }
    }
}

impl ExecutionPlan for PanicExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
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
                let batch = RecordBatch::new_empty(Arc::clone(&self.schema));
                return Poll::Ready(Some(Ok(batch)));
            } else {
                self.ready = true;
                // get called again
                cx.waker().wake_by_ref();
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
