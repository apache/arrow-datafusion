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

use super::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    work_table::{ReservedBatches, WorkTable, WorkTableExec},
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

use futures::{ready, Stream, StreamExt};

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
    /// The working table of cte
    work_table: Arc<WorkTable>,
    /// The base part (static term)
    static_term: Arc<dyn ExecutionPlan>,
    /// The dynamic part (recursive term)
    recursive_term: Arc<dyn ExecutionPlan>,
    /// Distinction
    is_distinct: bool,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl RecursiveQueryExec {
    /// Create a new RecursiveQueryExec
    pub fn try_new(
        name: String,
        static_term: Arc<dyn ExecutionPlan>,
        recursive_term: Arc<dyn ExecutionPlan>,
        is_distinct: bool,
    ) -> Result<Self> {
        // Each recursive query needs its own work table
        let work_table = Arc::new(WorkTable::new());
        // Use the same work table for both the WorkTableExec and the recursive term
        let recursive_term = assign_work_table(recursive_term, Arc::clone(&work_table))?;
        let cache = Self::compute_properties(static_term.schema());
        Ok(RecursiveQueryExec {
            name,
            static_term,
            recursive_term,
            is_distinct,
            work_table,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
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

impl ExecutionPlan for RecursiveQueryExec {
    fn name(&self) -> &'static str {
        "RecursiveQueryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.static_term, &self.recursive_term]
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        RecursiveQueryExec::try_new(
            self.name.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.is_distinct,
        )
        .map(|e| Arc::new(e) as _)
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

        let static_stream = self.static_term.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecursiveQueryStream::new(
            context,
            Arc::clone(&self.work_table),
            Arc::clone(&self.recursive_term),
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
                write!(
                    f,
                    "RecursiveQueryExec: name={}, is_distinct={}",
                    self.name, self.is_distinct
                )
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
    /// The working table state, representing the self referencing cte table
    work_table: Arc<WorkTable>,
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
    /// Tracks the memory used by the buffer
    reservation: MemoryReservation,
    // /// Metrics.
    _baseline_metrics: BaselineMetrics,
}

impl RecursiveQueryStream {
    /// Create a new recursive query stream
    fn new(
        task_context: Arc<TaskContext>,
        work_table: Arc<WorkTable>,
        recursive_term: Arc<dyn ExecutionPlan>,
        static_stream: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = static_stream.schema();
        let reservation =
            MemoryConsumer::new("RecursiveQuery").register(task_context.memory_pool());
        Self {
            task_context,
            work_table,
            recursive_term,
            static_stream: Some(static_stream),
            recursive_stream: None,
            schema,
            buffer: vec![],
            reservation,
            _baseline_metrics: baseline_metrics,
        }
    }

    /// Push a clone of the given batch to the in memory buffer, and then return
    /// a poll with it.
    fn push_batch(
        mut self: std::pin::Pin<&mut Self>,
        batch: RecordBatch,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if let Err(e) = self.reservation.try_grow(batch.get_array_memory_size()) {
            return Poll::Ready(Some(Err(e)));
        }

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

        // Update the work table with the current buffer
        let reserved_batches = ReservedBatches::new(
            std::mem::take(&mut self.buffer),
            self.reservation.take(),
        );
        self.work_table.update(reserved_batches);

        // We always execute (and re-execute iteratively) the first partition.
        // Downstream plans should not expect any partitioning.
        let partition = 0;

        let recursive_plan = reset_plan_states(Arc::clone(&self.recursive_term))?;
        self.recursive_stream =
            Some(recursive_plan.execute(partition, Arc::clone(&self.task_context))?);
        self.poll_next(cx)
    }
}

fn assign_work_table(
    plan: Arc<dyn ExecutionPlan>,
    work_table: Arc<WorkTable>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut work_table_refs = 0;
    plan.transform_down(|plan| {
        if let Some(exec) = plan.as_any().downcast_ref::<WorkTableExec>() {
            if work_table_refs > 0 {
                not_impl_err!(
                    "Multiple recursive references to the same CTE are not supported"
                )
            } else {
                work_table_refs += 1;
                Ok(Transformed::yes(Arc::new(
                    exec.with_work_table(Arc::clone(&work_table)),
                )))
            }
        } else if plan.as_any().is::<RecursiveQueryExec>() {
            not_impl_err!("Recursive queries cannot be nested")
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .data()
}

/// Some plans will change their internal states after execution, making them unable to be executed again.
/// This function uses `ExecutionPlan::with_new_children` to fork a new plan with initial states.
///
/// An example is `CrossJoinExec`, which loads the left table into memory and stores it in the plan.
/// However, if the data of the left table is derived from the work table, it will become outdated
/// as the work table changes. When the next iteration executes this plan again, we must clear the left table.
fn reset_plan_states(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        // WorkTableExec's states have already been updated correctly.
        if plan.as_any().is::<WorkTableExec>() {
            Ok(Transformed::no(plan))
        } else {
            let new_plan = Arc::clone(&plan)
                .with_new_children(plan.children().into_iter().cloned().collect())?;
            Ok(Transformed::yes(new_plan))
        }
    })
    .data()
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
            let batch_result = ready!(static_stream.poll_next_unpin(cx));
            match &batch_result {
                None => {
                    // Once this is done, we can start running the setup for the recursive term.
                    self.static_stream = None;
                    self.poll_next_iteration(cx)
                }
                Some(Ok(batch)) => self.push_batch(batch.clone()),
                _ => Poll::Ready(batch_result),
            }
        } else if let Some(recursive_stream) = &mut self.recursive_stream {
            let batch_result = ready!(recursive_stream.poll_next_unpin(cx));
            match batch_result {
                None => {
                    self.recursive_stream = None;
                    self.poll_next_iteration(cx)
                }
                Some(Ok(batch)) => self.push_batch(batch.clone()),
                _ => Poll::Ready(batch_result),
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for RecursiveQueryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {}
