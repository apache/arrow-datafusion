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

//! Metrics common for almost all operators

use std::task::Poll;

use super::{Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time, Timestamp};
use arrow::error::Error as ArrowError;
use datafusion_common::record_batch::RecordBatch;

/// Helper for creating and tracking common "baseline" metrics for
/// each operator
///
/// Example:
/// ```
/// use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
/// let metrics = ExecutionPlanMetricsSet::new();
///
/// let partition = 2;
/// let baseline_metrics = BaselineMetrics::new(&metrics, partition);
///
/// // during execution, in CPU intensive operation:
/// let timer = baseline_metrics.elapsed_compute().timer();
/// // .. do CPU intensive work
/// timer.done();
///
/// // when operator is finished:
/// baseline_metrics.done();
/// ```
#[derive(Debug)]
pub struct BaselineMetrics {
    /// end_time is set when `ExecutionMetrics::done()` is called
    end_time: Timestamp,

    /// amount of time the operator was actively trying to use the CPU
    elapsed_compute: Time,

    /// count of spills during the execution of the operator
    spill_count: Count,

    /// total spilled bytes during the execution of the operator
    spilled_bytes: Count,

    /// current memory usage for the operator
    mem_used: Gauge,

    /// output rows: the total output rows
    output_rows: Count,
}

impl BaselineMetrics {
    /// Create a new BaselineMetric structure, and set  `start_time` to now
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        Self {
            end_time: MetricBuilder::new(metrics).end_timestamp(partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            mem_used: MetricBuilder::new(metrics).mem_used(partition),
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &Time {
        &self.elapsed_compute
    }

    /// return the metric for the total number of spills triggered during execution
    pub fn spill_count(&self) -> &Count {
        &self.spill_count
    }

    /// return the metric for the total spilled bytes during execution
    pub fn spilled_bytes(&self) -> &Count {
        &self.spilled_bytes
    }

    /// return the metric for current memory usage
    pub fn mem_used(&self) -> &Gauge {
        &self.mem_used
    }

    /// Record a spill of `spilled_bytes` size.
    pub fn record_spill(&self, spilled_bytes: usize) {
        self.spill_count.add(1);
        self.spilled_bytes.add(spilled_bytes);
    }

    /// return the metric for the total number of output rows produced
    pub fn output_rows(&self) -> &Count {
        &self.output_rows
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    ///
    /// Note care should be taken to call `done()` manually if
    /// `BaselineMetrics` is not `drop`ped immediately upon operator
    /// completion, as async streams may not be dropped immediately
    /// depending on the consumer.
    pub fn done(&self) {
        self.end_time.record()
    }

    /// Record that some number of rows have been produced as output
    ///
    /// See the [`RecordOutput`] for conveniently recording record
    /// batch output for other thing
    pub fn record_output(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }

    /// If not previously recorded `done()`, record
    pub fn try_done(&self) {
        if self.end_time.value().is_none() {
            self.end_time.record()
        }
    }

    /// Process a poll result of a stream producing output for an
    /// operator, recording the output rows and stream done time and
    /// returning the same poll result
    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch, ArrowError>>>,
    ) -> Poll<Option<Result<RecordBatch, ArrowError>>> {
        if let Poll::Ready(maybe_batch) = &poll {
            match maybe_batch {
                Some(Ok(batch)) => {
                    batch.record_output(self);
                }
                Some(Err(_)) => self.done(),
                None => self.done(),
            }
        }
        poll
    }
}

impl Drop for BaselineMetrics {
    fn drop(&mut self) {
        self.try_done()
    }
}

/// Trait for things that produce output rows as a result of execution.
pub trait RecordOutput {
    /// Record that some number of output rows have been produced
    ///
    /// Meant to be composable so that instead of returning `batch`
    /// the operator can return `batch.record_output(baseline_metrics)`
    fn record_output(self, bm: &BaselineMetrics) -> Self;
}

impl RecordOutput for usize {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        bm.record_output(self);
        self
    }
}

impl RecordOutput for RecordBatch {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        bm.record_output(self.num_rows());
        self
    }
}

impl RecordOutput for &RecordBatch {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        bm.record_output(self.num_rows());
        self
    }
}

impl RecordOutput for Option<&RecordBatch> {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        if let Some(record_batch) = &self {
            record_batch.record_output(bm);
        }
        self
    }
}

impl RecordOutput for Option<RecordBatch> {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        if let Some(record_batch) = &self {
            record_batch.record_output(bm);
        }
        self
    }
}

impl RecordOutput for arrow::error::Result<RecordBatch> {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        if let Ok(record_batch) = &self {
            record_batch.record_output(bm);
        }
        self
    }
}
