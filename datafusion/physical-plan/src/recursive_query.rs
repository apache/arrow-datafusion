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

//! Defines the recursive query plan

use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::Partitioning;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use super::expressions::PhysicalSortExpr;
use super::metrics::BaselineMetrics;
use super::RecordBatchStream;
use super::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    SendableRecordBatchStream, Statistics,
};
use arrow::error::ArrowError;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{DisplayAs, DisplayFormatType, ExecutionPlan};

/// Recursive query execution plan.
///
/// This plan has two components: a base part (the static term) and
/// a dynamic part (the recursive term). The execution will start from
/// the base, and as long as the previous iteration produced at least
/// a single new row (taking care of the distinction) the recursive
/// part will be continuously executed.
///
/// Before each execution of the dynamic part, the rows from the previous
/// iteration will be available in a "working table" (not a real table,
/// can be only accessed using a continuance operation).
///
/// Note that there won't be any limit or checks applied to detect
/// an infinite recursion, so it is up to the planner to ensure that
/// it won't happen.
#[derive(Debug)]
pub struct RecursiveQueryExec {
    /// Name of the query handler
    name: String,
    /// The base part (static term)
    static_term: Arc<dyn ExecutionPlan>,
    /// The dynamic part (recursive term)
    recursive_term: Arc<dyn ExecutionPlan>,
    /// Distinction
    is_distinct: bool,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl RecursiveQueryExec {
    /// Create a new RecursiveQueryExec
    pub fn new(
        name: String,
        static_term: Arc<dyn ExecutionPlan>,
        recursive_term: Arc<dyn ExecutionPlan>,
        is_distinct: bool,
    ) -> Self {
        RecursiveQueryExec {
            name,
            static_term,
            recursive_term,
            is_distinct,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for RecursiveQueryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.static_term.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.static_term.clone(), self.recursive_term.clone()]
    }

    // Distribution on a recursive query is really tricky to handle.
    // For now, we are going to use a single partition but in the
    // future we might find a better way to handle this.
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    // TODO: control these hints and see whether we can
    // infer some from the child plans (static/recurisve terms).
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        vec![
            datafusion_physical_expr::Distribution::SinglePartition,
            datafusion_physical_expr::Distribution::SinglePartition,
        ]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RecursiveQueryExec::new(
            self.name.clone(),
            children[0].clone(),
            children[1].clone(),
            self.is_distinct,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: we might be able to handle multiple partitions in the future.
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RecursiveQueryExec got an invalid partition {} (expected 0)",
                partition
            )));
        }

        let static_stream = self.static_term.execute(partition, context.clone())?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecursiveQueryStream::new(
            context,
            self.name.clone(),
            self.recursive_term.clone(),
            static_stream,
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl DisplayAs for RecursiveQueryExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RecursiveQueryExec: is_distinct={}", self.is_distinct)
            }
        }
    }
}

/// The actual logic of the recursive queries happens during the streaming
/// process. A simplified version of the algorithm is the following:
///
/// buffer = []
///
/// while batch := static_stream.next():
///    buffer.push(batch)
///    yield buffer
///
/// while buffer.len() > 0:
///    sender, receiver = Channel()
///    register_continuation(handle_name, receiver)
///    sender.send(buffer.drain())
///    recursive_stream = recursive_term.execute()
///    while batch := recursive_stream.next():
///        buffer.append(batch)
///        yield buffer
///
struct RecursiveQueryStream {
    /// The context to be used for managing handlers & executing new tasks
    task_context: Arc<TaskContext>,
    /// Name of the relation handler to be used by the recursive term
    name: String,
    /// The dynamic part (recursive term) as is (without being executed)
    recursive_term: Arc<dyn ExecutionPlan>,
    /// The static part (static term) as a stream. If the processing of this
    /// part is completed, then it will be None.
    static_stream: Option<SendableRecordBatchStream>,
    /// The dynamic part (recursive term) as a stream. If the processing of this
    /// part has not started yet, or has been completed, then it will be None.
    recursive_stream: Option<SendableRecordBatchStream>,
    /// The schema of the output.
    schema: SchemaRef,
    /// In-memory buffer for storing a copy of the current results. Will be
    /// cleared after each iteration.
    buffer: Vec<RecordBatch>,
    // /// Metrics.
    _baseline_metrics: BaselineMetrics,
}

impl RecursiveQueryStream {
    /// Create a new recursive query stream
    fn new(
        task_context: Arc<TaskContext>,
        name: String,
        recursive_term: Arc<dyn ExecutionPlan>,
        static_stream: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = static_stream.schema();
        Self {
            task_context,
            name,
            recursive_term,
            static_stream: Some(static_stream),
            recursive_stream: None,
            schema,
            buffer: vec![],
            _baseline_metrics: baseline_metrics,
        }
    }

    /// Push a clone of the given batch to the in memory buffer, and then return
    /// a poll with it.
    fn push_batch(
        mut self: std::pin::Pin<&mut Self>,
        batch: RecordBatch,
    ) -> Poll<Option<Result<RecordBatch>>> {
        self.buffer.push(batch.clone());
        Poll::Ready(Some(Ok(batch)))
    }

    /// Start polling for the next iteration, will be called either after the static term
    /// is completed or another term is completed. It will follow the algorithm above on
    /// to check whether the recursion has ended.
    fn poll_next_iteration(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let total_length = self
            .buffer
            .iter()
            .fold(0, |acc, batch| acc + batch.num_rows());

        if total_length == 0 {
            return Poll::Ready(None);
        }

        // The initial capacity of the channels is the same as the number of partitions
        // we currently hold in the buffer.
        let (sender, receiver): (
            Sender<Result<RecordBatch>>,
            Receiver<Result<RecordBatch>>,
        ) = mpsc::channel(self.buffer.len() + 1);

        // There shouldn't be any handlers with this name, since the execution of recursive
        // term will immediately consume the relation handler.
        self.task_context
            .push_relation_handler(self.name.clone(), receiver)?;

        // This part heavily assumes that the buffer is not going to change. Maybe we
        // should use a mutex?
        for batch in self.buffer.drain(..) {
            match sender.try_send(Ok(batch.clone())) {
                Ok(_) => {}
                Err(e) => {
                    return Poll::Ready(Some(Err(DataFusionError::ArrowError(
                        ArrowError::from_external_error(Box::new(e)),
                    ))));
                }
            }
        }

        // We always execute (and re-execute iteratively) the first partition.
        // Downstream plans should not expect any partitioning.
        let partition = 0;

        self.recursive_stream = Some(
            self.recursive_term
                .execute(partition, self.task_context.clone())?,
        );
        self.poll_next(cx)
    }
}

impl Stream for RecursiveQueryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // TODO: we should use this poll to record some metrics!
        if let Some(static_stream) = &mut self.static_stream {
            // While the static term's stream is available, we'll be forwarding the batches from it (also
            // saving them for the initial iteration of the recursive term).
            let poll = static_stream.poll_next_unpin(cx);
            match &poll {
                Poll::Ready(None) => {
                    // Once this is done, we can start running the setup for the recursive term.
                    self.static_stream = None;
                    self.poll_next_iteration(cx)
                }
                Poll::Ready(Some(Ok(batch))) => self.push_batch(batch.clone()),
                _ => poll,
            }
        } else if let Some(recursive_stream) = &mut self.recursive_stream {
            let poll = recursive_stream.poll_next_unpin(cx);
            match &poll {
                Poll::Ready(None) => {
                    self.recursive_stream = None;
                    self.poll_next_iteration(cx)
                }
                Poll::Ready(Some(Ok(batch))) => self.push_batch(batch.clone()),
                _ => poll,
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for RecursiveQueryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {}
