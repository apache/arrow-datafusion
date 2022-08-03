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

use crate::error::{DataFusionError, Result};
use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use crate::scheduler::{
    is_worker, plan::PipelinePlan, spawn_local, spawn_local_fifo, RoutablePipeline,
    Spawner,
};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures::channel::mpsc;
use futures::task::ArcWake;
use futures::{ready, Stream, StreamExt};
use log::{debug, trace};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

/// Spawns a [`PipelinePlan`] using the provided [`Spawner`]
pub(crate) fn spawn_plan(plan: PipelinePlan, spawner: Spawner) -> ExecutionResults {
    debug!("Spawning pipeline plan: {:#?}", plan);

    let (senders, receivers) = (0..plan.output_partitions)
        .map(|_| mpsc::unbounded())
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let context = Arc::new(ExecutionContext {
        spawner,
        pipelines: plan.pipelines,
        schema: plan.schema,
        output: senders,
    });

    for (pipeline_idx, query_pipeline) in context.pipelines.iter().enumerate() {
        for partition in 0..query_pipeline.pipeline.output_partitions() {
            context.spawner.spawn(Task {
                context: context.clone(),
                waker: Arc::new(TaskWaker {
                    context: Arc::downgrade(&context),
                    wake_count: AtomicUsize::new(1),
                    pipeline: pipeline_idx,
                    partition,
                }),
            });
        }
    }

    let partitions = receivers
        .into_iter()
        .map(|receiver| ExecutionResultStream {
            receiver,
            context: context.clone(),
        })
        .collect();

    ExecutionResults {
        streams: partitions,
        context,
    }
}

/// A [`Task`] identifies an output partition within a given pipeline that may be able to
/// make progress. The [`Scheduler`][super::Scheduler] maintains a list of outstanding
/// [`Task`] and distributes them amongst its worker threads.
pub struct Task {
    /// Maintain a link to the [`ExecutionContext`] this is necessary to be able to
    /// route the output of the partition to its destination
    context: Arc<ExecutionContext>,

    /// A [`ArcWake`] that can be used to re-schedule this [`Task`] for execution
    waker: Arc<TaskWaker>,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = &self.context.pipelines[self.waker.pipeline].output;

        f.debug_struct("Task")
            .field("pipeline", &self.waker.pipeline)
            .field("partition", &self.waker.partition)
            .field("output", &output)
            .finish()
    }
}

impl Task {
    fn handle_error(
        &self,
        partition: usize,
        routable: &RoutablePipeline,
        error: DataFusionError,
    ) {
        self.context.send_query_output(partition, Err(error));
        if let Some(link) = routable.output {
            trace!(
                "Closing pipeline: {:?}, partition: {}, due to error",
                link,
                self.waker.partition,
            );

            self.context.pipelines[link.pipeline]
                .pipeline
                .close(link.child, self.waker.partition);
        }
    }

    /// Call [`Pipeline::poll_partition`], attempting to make progress on query execution
    pub fn do_work(self) {
        assert!(is_worker(), "Task::do_work called outside of worker pool");
        if self.context.is_cancelled() {
            return;
        }

        // Capture the wake count prior to calling [`Pipeline::poll_partition`]
        // this allows us to detect concurrent wake ups and handle them correctly
        let wake_count = self.waker.wake_count.load(Ordering::SeqCst);

        let node = self.waker.pipeline;
        let partition = self.waker.partition;

        let waker = futures::task::waker_ref(&self.waker);
        let mut cx = Context::from_waker(&waker);

        let pipelines = &self.context.pipelines;
        let routable = &pipelines[node];
        match routable.pipeline.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                trace!("Poll {:?}: Ok: {}", self, batch.num_rows());
                match routable.output {
                    Some(link) => {
                        trace!(
                            "Publishing batch to pipeline {:?} partition {}",
                            link,
                            partition
                        );

                        let r = pipelines[link.pipeline]
                            .pipeline
                            .push(batch, link.child, partition);

                        if let Err(e) = r {
                            self.handle_error(partition, routable, e);

                            // Return without rescheduling this output again
                            return;
                        }
                    }
                    None => {
                        trace!("Publishing batch to output");
                        self.context.send_query_output(partition, Ok(batch))
                    }
                }

                // Reschedule this pipeline again
                //
                // We want to prioritise running tasks triggered by the most recent
                // batch, so reschedule with FIFO ordering
                //
                // Note: We must schedule after we have routed the batch, otherwise
                // we introduce a potential ordering race where the newly scheduled
                // task runs before this task finishes routing the output
                spawn_local_fifo(self);
            }
            Poll::Ready(Some(Err(e))) => {
                trace!("Poll {:?}: Error: {:?}", self, e);
                self.handle_error(partition, routable, e)
            }
            Poll::Ready(None) => {
                trace!("Poll {:?}: None", self);
                match routable.output {
                    Some(link) => {
                        trace!("Closing pipeline: {:?}, partition: {}", link, partition);
                        pipelines[link.pipeline]
                            .pipeline
                            .close(link.child, partition)
                    }
                    None => self.context.finish(partition),
                }
            }
            Poll::Pending => {
                trace!("Poll {:?}: Pending", self);
                // Attempt to reset the wake count with the value obtained prior
                // to calling [`Pipeline::poll_partition`].
                //
                // If this fails it indicates a wakeup was received whilst executing
                // [`Pipeline::poll_partition`] and we should reschedule the task
                let reset = self.waker.wake_count.compare_exchange(
                    wake_count,
                    0,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );

                if reset.is_err() {
                    trace!("Wakeup triggered whilst polling: {:?}", self);
                    spawn_local(self);
                }
            }
        }
    }
}

/// The results of the execution of a query
pub struct ExecutionResults {
    /// [`ExecutionResultStream`] for each partition of this query
    streams: Vec<ExecutionResultStream>,

    /// Keep a reference to the [`ExecutionContext`] so it isn't dropped early
    context: Arc<ExecutionContext>,
}

impl ExecutionResults {
    /// Returns a [`SendableRecordBatchStream`] of this execution
    ///
    /// In the event of multiple output partitions, the output will be interleaved
    pub fn stream(self) -> SendableRecordBatchStream {
        let schema = self.context.schema.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::select_all(self.streams),
        ))
    }

    /// Returns a [`SendableRecordBatchStream`] for each partition of this execution
    pub fn stream_partitioned(self) -> Vec<SendableRecordBatchStream> {
        self.streams.into_iter().map(|s| Box::pin(s) as _).collect()
    }
}

/// A result stream for the execution of a query
struct ExecutionResultStream {
    receiver: mpsc::UnboundedReceiver<Option<Result<RecordBatch>>>,

    /// Keep a reference to the [`ExecutionContext`] so it isn't dropped early
    context: Arc<ExecutionContext>,
}

impl Stream for ExecutionResultStream {
    type Item = arrow::error::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let opt = ready!(self.receiver.poll_next_unpin(cx)).flatten();
        Poll::Ready(opt.map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e)))))
    }
}

impl RecordBatchStream for ExecutionResultStream {
    fn schema(&self) -> SchemaRef {
        self.context.schema.clone()
    }
}

/// The shared state of all [`Task`] created from the same [`PipelinePlan`]
#[derive(Debug)]
struct ExecutionContext {
    /// Spawner for this query
    spawner: Spawner,

    /// List of pipelines that belong to this query, pipelines are addressed
    /// based on their index within this list
    pipelines: Vec<RoutablePipeline>,

    /// Schema of this plans output
    pub schema: SchemaRef,

    /// The output streams, per partition, for this query's execution
    output: Vec<mpsc::UnboundedSender<Option<Result<RecordBatch>>>>,
}

impl Drop for ExecutionContext {
    fn drop(&mut self) {
        debug!("ExecutionContext dropped");
    }
}

impl ExecutionContext {
    /// Returns `true` if this query has been dropped, specifically if the
    /// stream returned by [`super::Scheduler::schedule`] has been dropped
    fn is_cancelled(&self) -> bool {
        self.output.iter().all(|x| x.is_closed())
    }

    /// Sends `output` to this query's output stream
    fn send_query_output(&self, partition: usize, output: Result<RecordBatch>) {
        let _ = self.output[partition].unbounded_send(Some(output));
    }

    /// Mark this partition as finished
    fn finish(&self, partition: usize) {
        let _ = self.output[partition].unbounded_send(None);
    }
}

struct TaskWaker {
    /// Store a weak reference to the [`ExecutionContext`] to avoid reference cycles if this
    /// [`Waker`] is stored within a [`Pipeline`] owned by the [`ExecutionContext`]
    context: Weak<ExecutionContext>,

    /// A counter that stores the number of times this has been awoken
    ///
    /// A value > 0, implies the task is either in the ready queue or
    /// currently being executed
    ///
    /// `TaskWaker::wake` always increments the `wake_count`, however, it only
    /// re-enqueues the [`Task`] if the value prior to increment was 0
    ///
    /// This ensures that a given [`Task`] is not enqueued multiple times
    ///
    /// We store an integer, as opposed to a boolean, so that wake ups that
    /// occur during [`Pipeline::poll_partition`] can be detected and handled
    /// after it has finished executing
    ///
    wake_count: AtomicUsize,

    /// The index of the pipeline within `query` to poll
    pipeline: usize,

    /// The partition of the pipeline within `query` to poll
    partition: usize,
}

impl ArcWake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if self.wake_count.fetch_add(1, Ordering::SeqCst) != 0 {
            trace!("Ignoring duplicate wakeup");
            return;
        }

        if let Some(context) = self.context.upgrade() {
            let task = Task {
                context,
                waker: self.clone(),
            };

            trace!("Wakeup {:?}", task);

            // If called from a worker, spawn to the current worker's
            // local queue, otherwise reschedule on any worker
            match is_worker() {
                true => spawn_local(task),
                false => task.context.spawner.clone().spawn(task),
            }
        } else {
            trace!("Dropped wakeup");
        }
    }

    fn wake_by_ref(s: &Arc<Self>) {
        ArcWake::wake(s.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::scheduler::{pipeline::Pipeline, plan::RoutablePipeline, Scheduler};
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::{channel::oneshot, ready, FutureExt, StreamExt};
    use parking_lot::Mutex;
    use std::fmt::Debug;
    use std::time::Duration;

    /// Tests that waker can be sent to tokio pool
    #[derive(Debug)]
    struct TokioPipeline {
        handle: tokio::runtime::Handle,
        state: Mutex<State>,
    }

    #[derive(Debug)]
    enum State {
        Init,
        Wait(oneshot::Receiver<Result<RecordBatch>>),
        Finished,
    }

    impl Default for State {
        fn default() -> Self {
            Self::Init
        }
    }

    impl Pipeline for TokioPipeline {
        fn push(
            &self,
            _input: RecordBatch,
            _child: usize,
            _partition: usize,
        ) -> Result<()> {
            unreachable!()
        }

        fn close(&self, _child: usize, _partition: usize) {}

        fn output_partitions(&self) -> usize {
            1
        }

        fn poll_partition(
            &self,
            cx: &mut Context<'_>,
            _partition: usize,
        ) -> Poll<Option<Result<RecordBatch>>> {
            let mut state = self.state.lock();
            loop {
                match &mut *state {
                    State::Init => {
                        let (sender, receiver) = oneshot::channel();
                        self.handle.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            let array = Int32Array::from_iter_values([1, 2, 3]);
                            sender.send(
                                RecordBatch::try_from_iter([(
                                    "int",
                                    Arc::new(array) as ArrayRef,
                                )])
                                .map_err(DataFusionError::ArrowError),
                            )
                        });
                        *state = State::Wait(receiver)
                    }
                    State::Wait(r) => {
                        let v = ready!(r.poll_unpin(cx)).ok();
                        *state = State::Finished;
                        return Poll::Ready(v);
                    }
                    State::Finished => return Poll::Ready(None),
                }
            }
        }
    }

    #[test]
    fn test_tokio_waker() {
        let scheduler = Scheduler::new(2);

        // A tokio runtime
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        // A pipeline that dispatches to a tokio worker
        let pipeline = TokioPipeline {
            handle: runtime.handle().clone(),
            state: Default::default(),
        };

        let plan = PipelinePlan {
            schema: Arc::new(Schema::new(vec![Field::new(
                "int",
                DataType::Int32,
                false,
            )])),
            output_partitions: 1,
            pipelines: vec![RoutablePipeline {
                pipeline: Box::new(pipeline),
                output: None,
            }],
        };

        let mut receiver = scheduler.schedule_plan(plan).stream();

        runtime.block_on(async move {
            // Should wait for output
            let batch = receiver.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 3);

            // Next batch should be none
            assert!(receiver.next().await.is_none());
        })
    }
}
