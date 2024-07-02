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

//! Utilities for testing datafusion-physical-plan

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{Future, FutureExt};

use crate::memory::MemoryExec;
use crate::stream::RecordBatchStreamAdapter;
use crate::streaming::PartitionStream;
use crate::ExecutionPlan;

pub mod exec;

/// Asserts that given future is pending.
pub fn assert_is_pending<'a, T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send + 'a>>) {
    let waker = futures::task::noop_waker();
    let mut cx = futures::task::Context::from_waker(&waker);
    let poll = fut.poll_unpin(&mut cx);

    assert!(poll.is_pending());
}

/// Get the schema for the aggregate_test_* csv files
pub fn aggr_test_schema() -> SchemaRef {
    let mut f1 = Field::new("c1", DataType::Utf8, false);
    f1.set_metadata(HashMap::from_iter(vec![("testing".into(), "test".into())]));
    let schema = Schema::new(vec![
        f1,
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]);

    Arc::new(schema)
}

/// returns record batch with 3 columns of i32 in memory
pub fn build_table_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap()
}

/// returns memory table scan wrapped around record batch with 3 columns of i32
pub fn build_table_scan_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let batch = build_table_i32(a, b, c);
    let schema = batch.schema();
    Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
}

/// Return a RecordBatch with a single Int32 array with values (0..sz) in a field named "i"
pub fn make_partition(sz: i32) -> RecordBatch {
    let seq_start = 0;
    let seq_end = sz;
    let values = (seq_start..seq_end).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from(values));
    let arr = arr as ArrayRef;

    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Returns a `MemoryExec` that scans `partitions` of 100 batches each
pub fn scan_partitioned(partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(mem_exec(partitions))
}

/// Returns a `MemoryExec` that scans `partitions` of 100 batches each
pub fn mem_exec(partitions: usize) -> MemoryExec {
    let data: Vec<Vec<_>> = (0..partitions).map(|_| vec![make_partition(100)]).collect();

    let schema = data[0][0].schema();
    let projection = None;
    MemoryExec::try_new(&data, schema, projection).unwrap()
}

// construct a stream partition for test purposes
pub struct TestPartitionStream {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl TestPartitionStream {
    /// Create a new stream partition with the provided batches
    pub fn new_with_batches(batches: Vec<RecordBatch>) -> Self {
        let schema = batches[0].schema();
        Self { schema, batches }
    }
}
impl PartitionStream for TestPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream = futures::stream::iter(self.batches.clone().into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}
