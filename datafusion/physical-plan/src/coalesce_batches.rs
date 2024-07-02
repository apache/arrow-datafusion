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

//! CoalesceBatchesExec combines small batches into larger batches for more efficient use of
//! vectorized processing by upstream operators.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties, Statistics};
use crate::{
    DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;

use futures::stream::{Stream, StreamExt};
use log::trace;

/// CoalesceBatchesExec combines small batches into larger batches for more efficient use of
/// vectorized processing by upstream operators.
#[derive(Debug)]
pub struct CoalesceBatchesExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl CoalesceBatchesExec {
    /// Create a new CoalesceBatchesExec
    pub fn new(input: Arc<dyn ExecutionPlan>, target_batch_size: usize) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            target_batch_size,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Minimum number of rows for coalesces batches
    pub fn target_batch_size(&self) -> usize {
        self.target_batch_size
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // The coalesce batches operator does not make any changes to the
        // partitioning of its input.
        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            input.output_partitioning().clone(),    // Output Partitioning
            input.execution_mode(),                 // Execution Mode
        )
    }
}

impl DisplayAs for CoalesceBatchesExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CoalesceBatchesExec: target_batch_size={}",
                    self.target_batch_size
                )
            }
        }
    }
}

impl ExecutionPlan for CoalesceBatchesExec {
    fn name(&self) -> &'static str {
        "CoalesceBatchesExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CoalesceBatchesExec::new(
            Arc::clone(&children[0]),
            self.target_batch_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceBatchesStream {
            input: self.input.execute(partition, context)?,
            schema: self.input.schema(),
            target_batch_size: self.target_batch_size,
            buffer: Vec::new(),
            buffered_rows: 0,
            is_closed: false,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

struct CoalesceBatchesStream {
    /// The input plan
    input: SendableRecordBatchStream,
    /// The input schema
    schema: SchemaRef,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Buffered batches
    buffer: Vec<RecordBatch>,
    /// Buffered row count
    buffered_rows: usize,
    /// Whether the stream has finished returning all of its data or not
    is_closed: bool,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
}

impl Stream for CoalesceBatchesStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we can't predict the size of incoming batches so re-use the size hint from the input
        self.input.size_hint()
    }
}

impl CoalesceBatchesStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        // Get a clone (uses same underlying atomic) as self gets borrowed below
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();

        if self.is_closed {
            return Poll::Ready(None);
        }
        loop {
            let input_batch = self.input.poll_next_unpin(cx);
            // records time on drop
            let _timer = cloned_time.timer();
            match input_batch {
                Poll::Ready(x) => match x {
                    Some(Ok(batch)) => {
                        if batch.num_rows() >= self.target_batch_size
                            && self.buffer.is_empty()
                        {
                            return Poll::Ready(Some(Ok(batch)));
                        } else if batch.num_rows() == 0 {
                            // discard empty batches
                        } else {
                            // add to the buffered batches
                            self.buffered_rows += batch.num_rows();
                            self.buffer.push(batch);
                            // check to see if we have enough batches yet
                            if self.buffered_rows >= self.target_batch_size {
                                // combine the batches and return
                                let batch = concat_batches(
                                    &self.schema,
                                    &self.buffer,
                                    self.buffered_rows,
                                )?;
                                // reset buffer state
                                self.buffer.clear();
                                self.buffered_rows = 0;
                                // return batch
                                return Poll::Ready(Some(Ok(batch)));
                            }
                        }
                    }
                    None => {
                        self.is_closed = true;
                        // we have reached the end of the input stream but there could still
                        // be buffered batches
                        if self.buffer.is_empty() {
                            return Poll::Ready(None);
                        } else {
                            // combine the batches and return
                            let batch = concat_batches(
                                &self.schema,
                                &self.buffer,
                                self.buffered_rows,
                            )?;
                            // reset buffer state
                            self.buffer.clear();
                            self.buffered_rows = 0;
                            // return batch
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }
                    other => return Poll::Ready(other),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for CoalesceBatchesStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Concatenates an array of `RecordBatch` into one batch
pub fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    row_count: usize,
) -> ArrowResult<RecordBatch> {
    trace!(
        "Combined {} batches containing {} rows",
        batches.len(),
        row_count
    );
    arrow::compute::concat_batches(schema, batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::MemoryExec, repartition::RepartitionExec, Partitioning};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::UInt32Array;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_batches() -> Result<()> {
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 10);
        let partitions = vec![partition];

        let output_partitions = coalesce_batches(&schema, partitions, 21).await?;
        assert_eq!(1, output_partitions.len());

        // input is 10 batches x 8 rows (80 rows)
        // expected output is batches of at least 20 rows (except for the final batch)
        let batches = &output_partitions[0];
        assert_eq!(4, batches.len());
        assert_eq!(24, batches[0].num_rows());
        assert_eq!(24, batches[1].num_rows());
        assert_eq!(24, batches[2].num_rows());
        assert_eq!(8, batches[3].num_rows());

        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    async fn coalesce_batches(
        schema: &SchemaRef,
        input_partitions: Vec<Vec<RecordBatch>>,
        target_batch_size: usize,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, Arc::clone(schema), None)?;
        let exec =
            RepartitionExec::try_new(Arc::new(exec), Partitioning::RoundRobinBatch(1))?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(CoalesceBatchesExec::new(Arc::new(exec), target_batch_size));

        // execute and collect results
        let output_partition_count = exec.output_partitioning().partition_count();
        let mut output_partitions = Vec::with_capacity(output_partition_count);
        for i in 0..output_partition_count {
            // execute this *output* partition and collect all batches
            let task_ctx = Arc::new(TaskContext::default());
            let mut stream = exec.execute(i, Arc::clone(&task_ctx))?;
            let mut batches = vec![];
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            output_partitions.push(batches);
        }
        Ok(output_partitions)
    }

    /// Create vector batches
    fn create_vec_batches(schema: &Schema, n: usize) -> Vec<RecordBatch> {
        let batch = create_batch(schema);
        let mut vec = Vec::with_capacity(n);
        for _ in 0..n {
            vec.push(batch.clone());
        }
        vec
    }

    /// Create batch
    fn create_batch(schema: &Schema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }
}
