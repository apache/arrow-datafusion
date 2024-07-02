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

//! Defines the work table query plan

use std::any::Any;
use std::sync::{Arc, Mutex};

use super::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    SendableRecordBatchStream, Statistics,
};
use crate::memory::MemoryStream;
use crate::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_datafusion_err, internal_err, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

/// A vector of record batches with a memory reservation.
#[derive(Debug)]
pub(super) struct ReservedBatches {
    batches: Vec<RecordBatch>,
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl ReservedBatches {
    pub(super) fn new(batches: Vec<RecordBatch>, reservation: MemoryReservation) -> Self {
        ReservedBatches {
            batches,
            reservation,
        }
    }
}

/// The name is from PostgreSQL's terminology.
/// See <https://wiki.postgresql.org/wiki/CTEReadme#How_Recursion_Works>
/// This table serves as a mirror or buffer between each iteration of a recursive query.
#[derive(Debug)]
pub(super) struct WorkTable {
    batches: Mutex<Option<ReservedBatches>>,
}

impl WorkTable {
    /// Create a new work table.
    pub(super) fn new() -> Self {
        Self {
            batches: Mutex::new(None),
        }
    }

    /// Take the previously written batches from the work table.
    /// This will be called by the [`WorkTableExec`] when it is executed.
    fn take(&self) -> Result<ReservedBatches> {
        self.batches
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| internal_datafusion_err!("Unexpected empty work table"))
    }

    /// Update the results of a recursive query iteration to the work table.
    pub(super) fn update(&self, batches: ReservedBatches) {
        self.batches.lock().unwrap().replace(batches);
    }
}

/// A temporary "working table" operation where the input data will be
/// taken from the named handle during the execution and will be re-published
/// as is (kind of like a mirror).
///
/// Most notably used in the implementation of recursive queries where the
/// underlying relation does not exist yet but the data will come as the previous
/// term is evaluated. This table will be used such that the recursive plan
/// will register a receiver in the task context and this plan will use that
/// receiver to get the data and stream it back up so that the batches are available
/// in the next iteration.
#[derive(Clone, Debug)]
pub struct WorkTableExec {
    /// Name of the relation handler
    name: String,
    /// The schema of the stream
    schema: SchemaRef,
    /// The work table
    work_table: Arc<WorkTable>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl WorkTableExec {
    /// Create a new execution plan for a worktable exec.
    pub fn new(name: String, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema));
        Self {
            name,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            work_table: Arc::new(WorkTable::new()),
            cache,
        }
    }

    pub(super) fn with_work_table(&self, work_table: Arc<WorkTable>) -> Self {
        Self {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            metrics: ExecutionPlanMetricsSet::new(),
            work_table,
            cache: self.cache.clone(),
        }
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

impl DisplayAs for WorkTableExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WorkTableExec: name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for WorkTableExec {
    fn name(&self) -> &'static str {
        "WorkTableExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self) as Arc<dyn ExecutionPlan>)
    }

    /// Stream the batches that were written to the work table.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // WorkTable streams must be the plan base.
        if partition != 0 {
            return internal_err!(
                "WorkTableExec got an invalid partition {partition} (expected 0)"
            );
        }
        let batch = self.work_table.take()?;
        Ok(Box::pin(
            MemoryStream::try_new(batch.batches, Arc::clone(&self.schema), None)?
                .with_reservation(batch.reservation),
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use datafusion_execution::memory_pool::{MemoryConsumer, UnboundedMemoryPool};

    #[test]
    fn test_work_table() {
        let work_table = WorkTable::new();
        // cann't take from empty work_table
        assert!(work_table.take().is_err());

        let pool = Arc::new(UnboundedMemoryPool::default()) as _;
        let mut reservation = MemoryConsumer::new("test_work_table").register(&pool);

        // update batch to work_table
        let array: ArrayRef = Arc::new((0..5).collect::<Int32Array>());
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();
        reservation.try_grow(100).unwrap();
        work_table.update(ReservedBatches::new(vec![batch.clone()], reservation));
        // take from work_table
        let reserved_batches = work_table.take().unwrap();
        assert_eq!(reserved_batches.batches, vec![batch.clone()]);

        // consume the batch by the MemoryStream
        let memory_stream =
            MemoryStream::try_new(reserved_batches.batches, batch.schema(), None)
                .unwrap()
                .with_reservation(reserved_batches.reservation);

        // should still be reserved
        assert_eq!(pool.reserved(), 100);

        // the reservation should be freed after drop the memory_stream
        drop(memory_stream);
        assert_eq!(pool.reserved(), 0);
    }
}
