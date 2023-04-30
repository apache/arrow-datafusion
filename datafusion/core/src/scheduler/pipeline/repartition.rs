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

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::physical_plan::repartition::BatchPartitioner;
use crate::physical_plan::Partitioning;

use crate::scheduler::pipeline::Pipeline;

/// A [`Pipeline`] that can repartition its input
#[derive(Debug)]
pub struct RepartitionPipeline {
    output_count: usize,
    state: Mutex<RepartitionState>,
}

impl RepartitionPipeline {
    /// Create a new [`RepartitionPipeline`] with the given `input` and `output` partitioning
    pub fn try_new(input: Partitioning, output: Partitioning) -> Result<Self> {
        let input_count = input.partition_count();
        let output_count = output.partition_count();
        assert_ne!(input_count, 0);
        assert_ne!(output_count, 0);

        // TODO: metrics support
        let partitioner =
            BatchPartitioner::try_new(output, 0, output_count, Default::default())?;

        let state = Mutex::new(RepartitionState {
            partitioner,
            partition_closed: vec![false; input_count],
            input_closed: false,
            output_buffers: (0..output_count).map(|_| Default::default()).collect(),
        });

        Ok(Self {
            state,
            output_count,
        })
    }
}

struct RepartitionState {
    partitioner: BatchPartitioner,
    partition_closed: Vec<bool>,
    input_closed: bool,
    output_buffers: Vec<OutputBuffer>,
}

impl std::fmt::Debug for RepartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepartitionState")
            .field("partition_closed", &self.partition_closed)
            .field("input_closed", &self.input_closed)
            .finish()
    }
}

impl Pipeline for RepartitionPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        assert_eq!(child, 0);

        let mut state = self.state.lock();
        assert!(
            !state.partition_closed[partition],
            "attempt to push to closed partition {partition} of RepartitionPipeline({state:?})"
        );

        let state = &mut *state;
        state.partitioner.partition(input, |partition, batch| {
            state.output_buffers[partition].push_batch(batch);
            Ok(())
        })
    }

    fn close(&self, child: usize, partition: usize) {
        assert_eq!(child, 0);

        let mut state = self.state.lock();
        assert!(
            !state.partition_closed[partition],
            "attempt to close already closed partition {partition} of RepartitionPipeline({state:?})"
        );

        state.partition_closed[partition] = true;

        // If all input streams exhausted, wake outputs
        if state.partition_closed.iter().all(|x| *x) {
            state.input_closed = true;
            for buffer in &mut state.output_buffers {
                for waker in buffer.wait_list.drain(..) {
                    waker.wake()
                }
            }
        }
    }

    fn output_partitions(&self) -> usize {
        self.output_count
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut state = self.state.lock();
        let input_closed = state.input_closed;
        let buffer = &mut state.output_buffers[partition];

        match buffer.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if input_closed => Poll::Ready(None),
            _ => {
                buffer.wait_list.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Debug, Default)]
struct OutputBuffer {
    batches: VecDeque<RecordBatch>,
    wait_list: Vec<Waker>,
}

impl OutputBuffer {
    fn push_batch(&mut self, batch: RecordBatch) {
        self.batches.push_back(batch);

        for waker in self.wait_list.drain(..) {
            waker.wake()
        }
    }
}
