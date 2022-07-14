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

//! Metrics with memory usage tracking capability

use crate::execution::runtime_env::RuntimeEnv;
use crate::execution::MemoryConsumerId;
use crate::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Time,
};
use std::sync::Arc;
use std::task::Poll;

use crate::record_batch::RecordBatch;
use arrow::error::Error as ArrowError;

/// Simplified version of tracking memory consumer,
/// see also: [`Tracking`](crate::execution::memory_manager::ConsumerType::Tracking)
///
/// You could use this to replace [BaselineMetrics], report the memory,
/// and get the memory usage bookkeeping in the memory manager easily.
#[derive(Debug)]
pub struct MemTrackingMetrics {
    id: MemoryConsumerId,
    runtime: Option<Arc<RuntimeEnv>>,
    metrics: BaselineMetrics,
}

/// Delegates most of the metrics functionalities to the inner BaselineMetrics,
/// intercept memory metrics functionalities and do memory manager bookkeeping.
impl MemTrackingMetrics {
    /// Create metrics similar to [BaselineMetrics]
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let id = MemoryConsumerId::new(partition);
        Self {
            id,
            runtime: None,
            metrics: BaselineMetrics::new(metrics, partition),
        }
    }

    /// Create memory tracking metrics with reference to runtime
    pub fn new_with_rt(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let id = MemoryConsumerId::new(partition);
        Self {
            id,
            runtime: Some(runtime),
            metrics: BaselineMetrics::new(metrics, partition),
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &Time {
        self.metrics.elapsed_compute()
    }

    /// return the size for current memory usage
    pub fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }

    /// setup initial memory usage and register it with memory manager
    pub fn init_mem_used(&self, size: usize) {
        self.metrics.mem_used().set(size);
        if let Some(rt) = self.runtime.as_ref() {
            rt.memory_manager.grow_tracker_usage(size);
        }
    }

    /// return the metric for the total number of output rows produced
    pub fn output_rows(&self) -> &Count {
        self.metrics.output_rows()
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    ///
    /// Note care should be taken to call `done()` manually if
    /// `MemTrackingMetrics` is not `drop`ped immediately upon operator
    /// completion, as async streams may not be dropped immediately
    /// depending on the consumer.
    pub fn done(&self) {
        self.metrics.done()
    }

    /// Record that some number of rows have been produced as output
    ///
    /// See the [`super::RecordOutput`] for conveniently recording record
    /// batch output for other thing
    pub fn record_output(&self, num_rows: usize) {
        self.metrics.record_output(num_rows)
    }

    /// Process a poll result of a stream producing output for an
    /// operator, recording the output rows and stream done time and
    /// returning the same poll result
    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch, ArrowError>>>,
    ) -> Poll<Option<Result<RecordBatch, ArrowError>>> {
        self.metrics.record_poll(poll)
    }
}

impl Drop for MemTrackingMetrics {
    fn drop(&mut self) {
        self.metrics.try_done();
        if self.mem_used() != 0 {
            if let Some(rt) = self.runtime.as_ref() {
                rt.drop_consumer(&self.id, self.mem_used());
            }
        }
    }
}
