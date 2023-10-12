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

//! Stream wrappers for physical operators

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::displayable;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::{exec_err, internal_err};
use datafusion_execution::TaskContext;
use futures::stream::BoxStream;
use futures::{Future, Stream, StreamExt};
use log::debug;
use pin_project_lite::pin_project;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;

use super::metrics::BaselineMetrics;
use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};

/// Builder for [`ReceiverStream`] that propagates errors
/// and panic's correctly.
///
/// [`ReceiverStream`] is used to spawn one or more tasks
/// that produce `O` output, (such as a `RecordBatch`),
/// and sends them to a single `Receiver` which can improve parallelism.
///
/// This also handles propagating panic`s and canceling the tasks.
pub struct ReceiverStreamBuilder<O> {
    tx: Sender<Result<O>>,
    rx: Receiver<Result<O>>,
    schema: SchemaRef,
    join_set: JoinSet<Result<()>>,
}

impl<O: Send + Sync + 'static> ReceiverStreamBuilder<O> {
    /// create new channels with the specified buffer size
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);

        Self {
            tx,
            rx,
            schema,
            join_set: JoinSet::new(),
        }
    }

    /// Get a handle for sending `O` to the output
    pub fn tx(&self) -> Sender<Result<O>> {
        self.tx.clone()
    }

    /// Spawn task that will be aborted if this builder (or the stream
    /// built from it) are dropped
    ///
    /// this is often used to spawn tasks that write to the sender
    /// retrieved from `Self::tx`
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = Result<()>>,
        F: Send + 'static,
    {
        self.join_set.spawn(task);
    }

    /// Spawn a blocking task that will be aborted if this builder (or the stream
    /// built from it) are dropped
    ///
    /// this is often used to spawn tasks that write to the sender
    /// retrieved from `Self::tx`
    pub fn spawn_blocking<F>(&mut self, f: F)
    where
        F: FnOnce() -> Result<()>,
        F: Send + 'static,
    {
        self.join_set.spawn_blocking(f);
    }

    /// runs the input_partition of the `input` ExecutionPlan on the
    /// tokio threadpool and writes its outputs to this stream
    ///
    /// If the input partition produces an error, the error will be
    /// sent to the output stream and no further results are sent.
    pub(crate) fn run_input(
        &mut self,
        input: Arc<dyn StreamAdaptor<Item = Result<O>> + Send + Sync>,
        partition: usize,
        context: Arc<TaskContext>,
    ) {
        let output = self.tx();

        self.spawn(async move { input.call(output, partition, context).await });
    }

    /// Create a stream of all `O`es written to `tx`
    pub fn build(self) -> Pin<Box<ReceiverStream<O>>> {
        let Self {
            tx,
            rx,
            schema,
            mut join_set,
        } = self;

        // don't need tx
        drop(tx);

        // future that checks the result of the join set, and propagates panic if seen
        let check = async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(task_result) => {
                        match task_result {
                            // nothing to report
                            Ok(_) => continue,
                            // This means a blocking task error
                            Err(e) => {
                                return Some(exec_err!("Spawned Task error: {e}"));
                            }
                        }
                    }
                    // This means a tokio task error, likely a panic
                    Err(e) => {
                        if e.is_panic() {
                            // resume on the main thread
                            std::panic::resume_unwind(e.into_panic());
                        } else {
                            // This should only occur if the task is
                            // cancelled, which would only occur if
                            // the JoinSet were aborted, which in turn
                            // would imply that the receiver has been
                            // dropped and this code is not running
                            return Some(internal_err!("Non Panic Task error: {e}"));
                        }
                    }
                }
            }
            None
        };

        let check_stream = futures::stream::once(check)
            // unwrap Option / only return the error
            .filter_map(|item| async move { item });

        // Convert the receiver into a stream
        let rx_stream = futures::stream::unfold(rx, |mut rx| async move {
            let next_item = rx.recv().await;
            next_item.map(|next_item| (next_item, rx))
        });

        // Merge the streams together so whichever is ready first
        // produces the batch
        let inner = futures::stream::select(rx_stream, check_stream).boxed();

        Box::pin(ReceiverStream::<O>::new(schema, inner))
    }
}

/// A SendableStream that combines multiple inputs, on new tokio Tasks,
/// increasing the potential parallelism.
///
/// When `O` is a [`RecordBatch`], this is a [`SendableRecordBatchStream`].
///
/// This structure also handles propagating panics and cancelling the
/// underlying tasks correctly.
///
/// Use [`Self::builder`] to construct one.
pub struct ReceiverStream<O> {
    schema: SchemaRef,
    inner: BoxStream<'static, Result<O>>,
}

impl<O: Send + Sync + 'static> ReceiverStream<O> {
    /// Create a builder with an internal buffer of capacity batches.
    pub fn builder(schema: SchemaRef, capacity: usize) -> ReceiverStreamBuilder<O> {
        ReceiverStreamBuilder::<O>::new(schema, capacity)
    }
}

impl<O: Send + Sync> Stream for ReceiverStream<O> {
    type Item = Result<O>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<O: Send + Sync> ReceiverStream<O> {
    /// Create a new [`ReceiverStream`] from the provided schema and stream
    pub fn new(schema: SchemaRef, inner: BoxStream<'static, Result<O>>) -> Self {
        Self { schema, inner }
    }
}

impl RecordBatchStream for ReceiverStream<RecordBatch> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait::async_trait]
pub(crate) trait StreamAdaptor: Send + Sync {
    type Item: Send + Sync;

    async fn call(
        &self,
        output: Sender<Self::Item>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<()>;
}

pub(crate) struct RecordBatchReceiverStreamAdaptor {
    input: Arc<dyn ExecutionPlan>,
}

impl RecordBatchReceiverStreamAdaptor {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

#[async_trait::async_trait]
impl StreamAdaptor for RecordBatchReceiverStreamAdaptor {
    type Item = Result<RecordBatch>;

    async fn call(
        &self,
        output: Sender<Self::Item>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let mut stream = match self.input.execute(partition, context) {
            Err(e) => {
                // If send fails, the plan being torn down, there
                // is no place to send the error and no reason to continue.
                output.send(Err(e)).await.ok();
                debug!(
                    "Stopping execution: error executing input: {}",
                    displayable(self.input.as_ref()).one_line()
                );
                return Ok(());
            }
            Ok(stream) => stream,
        };

        // Transfer batches from inner stream to the output tx
        // immediately.
        while let Some(item) = stream.next().await {
            let is_err = item.is_err();

            // If send fails, plan being torn down, there is no
            // place to send the error and no reason to continue.
            if output.send(item).await.is_err() {
                debug!(
                    "Stopping execution: output is gone, plan cancelling: {}",
                    displayable(self.input.as_ref()).one_line()
                );
                return Ok(());
            }

            // stop after the first error is encontered (don't
            // drive all streams to completion)
            if is_err {
                debug!(
                    "Stopping execution: plan returned error: {}",
                    displayable(self.input.as_ref()).one_line()
                );
                return Ok(());
            }
        }

        Ok(())
    }
}

pin_project! {
    /// Combines a [`Stream`] with a [`SchemaRef`] implementing
    /// [`RecordBatchStream`] for the combination
    pub struct RecordBatchStreamAdapter<S> {
        schema: SchemaRef,

        #[pin]
        stream: S,
    }
}

impl<S> RecordBatchStreamAdapter<S> {
    /// Creates a new [`RecordBatchStreamAdapter`] from the provided schema and stream
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self { schema, stream }
    }
}

impl<S> std::fmt::Debug for RecordBatchStreamAdapter<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordBatchStreamAdapter")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<S> Stream for RecordBatchStreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for RecordBatchStreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Stream wrapper that records `BaselineMetrics` for a particular
/// `[SendableRecordBatchStream]` (likely a partition)
pub(crate) struct ObservedStream {
    inner: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl ObservedStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            inner,
            baseline_metrics,
        }
    }
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for ObservedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::exec_err;

    use crate::test::exec::{
        assert_strong_count_converges_to_zero, BlockingExec, MockExec, PanicExec,
    };

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]))
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic")]
    async fn record_batch_receiver_stream_propagates_panics() {
        let schema = schema();

        let num_partitions = 10;
        let input = PanicExec::new(schema.clone(), num_partitions);
        consume(input, 10).await
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic: 1")]
    async fn record_batch_receiver_stream_propagates_panics_early_shutdown() {
        let schema = schema();

        // make 2 partitions, second partition panics before the first
        let num_partitions = 2;
        let input = PanicExec::new(schema.clone(), num_partitions)
            .with_partition_panic(0, 10)
            .with_partition_panic(1, 3); // partition 1 should panic first (after 3 )

        // ensure that the panic results in an early shutdown (that
        // everything stops after the first panic).

        // Since the stream reads every other batch: (0,1,0,1,0,panic)
        // so should not exceed 5 batches prior to the panic
        let max_batches = 5;
        consume(input, max_batches).await
    }

    #[tokio::test]
    async fn record_batch_receiver_stream_drop_cancel() {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = schema();

        // Make an input that never proceeds
        let input = BlockingExec::new(schema.clone(), 1);
        let refs = input.refs();

        // Configure a RecordBatchReceiverStream to consume the input
        let mut builder = ReceiverStream::builder(schema, 2);
        let input = Arc::new(RecordBatchReceiverStreamAdaptor::new(Arc::new(input)));
        builder.run_input(input, 0, task_ctx.clone());
        let stream = builder.build();

        // input should still be present
        assert!(std::sync::Weak::strong_count(&refs) > 0);

        // drop the stream, ensure the refs go to zero
        drop(stream);
        assert_strong_count_converges_to_zero(refs).await;
    }

    #[tokio::test]
    /// Ensure that if an error is received in one stream, the
    /// `RecordBatchReceiverStream` stops early and does not drive
    /// other streams to completion.
    async fn record_batch_receiver_stream_error_does_not_drive_completion() {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = schema();

        // make an input that will error twice
        let error_stream =
            MockExec::new(vec![exec_err!("Test1"), exec_err!("Test2")], schema.clone())
                .with_use_task(false);

        let mut builder = ReceiverStream::builder(schema, 2);
        let input = Arc::new(RecordBatchReceiverStreamAdaptor::new(Arc::new(
            error_stream,
        )));
        builder.run_input(input, 0, task_ctx.clone());
        let mut stream = builder.build();

        // get the first result, which should be an error
        let first_batch = stream.next().await.unwrap();
        let first_err = first_batch.unwrap_err();
        assert_eq!(first_err.strip_backtrace(), "Execution error: Test1");

        // There should be no more batches produced (should not get the second error)
        assert!(stream.next().await.is_none());
    }

    /// Consumes all the input's partitions into a
    /// RecordBatchReceiverStream and runs it to completion
    ///
    /// panic's if more than max_batches is seen,
    async fn consume(input: PanicExec, max_batches: usize) {
        let task_ctx = Arc::new(TaskContext::default());

        let input = Arc::new(input);
        let num_partitions = input.output_partitioning().partition_count();

        // Configure a RecordBatchReceiverStream to consume all the input partitions
        let mut builder = ReceiverStream::builder(input.schema(), num_partitions);
        for partition in 0..num_partitions {
            builder.run_input(
                Arc::new(RecordBatchReceiverStreamAdaptor::new(input.clone())),
                partition,
                task_ctx.clone(),
            );
        }
        let mut stream = builder.build();

        // drain the stream until it is complete, panic'ing on error
        let mut num_batches = 0;
        while let Some(next) = stream.next().await {
            next.unwrap();
            num_batches += 1;
            assert!(
                num_batches < max_batches,
                "Got the limit of {num_batches} batches before seeing panic"
            );
        }
    }
}
