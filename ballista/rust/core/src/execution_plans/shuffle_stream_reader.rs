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

use std::cell::Cell;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};
use std::{any::Any, pin::Pin};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Metric, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::RecordBatchStream,
};
use futures::{future, Stream, StreamExt};
use log::info;
use std::time::Instant;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::expressions::PhysicalSortExpr;

/// ShuffleStreamReaderExec reads partitions streams that are pushed by the multiple ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleStreamReaderExec {
    /// The query stage id which the shuffle reader depends on
    pub stage_id: usize,

    /// Schema
    pub(crate) schema: SchemaRef,

    /// Partition count
    pub partition_count: usize,

    /// Record Batch input receiver
    batch_input: Arc<Mutex<Vec<Receiver<ArrowResult<RecordBatch>>>>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleStreamReaderExec {
    /// Create a new ShuffleStreamReaderExec
    pub fn new(stage_id: usize, schema: SchemaRef, partition_count: usize) -> Self {
        Self {
            stage_id,
            schema,
            partition_count,
            batch_input: Arc::new(Mutex::new(Vec::new())),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn create_record_batch_channel(&self) -> Sender<ArrowResult<RecordBatch>> {
        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(10);
        self.batch_input.lock().unwrap().push(response_rx);
        response_tx
    }

    /// Returns the the streaming shuffle readers in the execution plan
    pub fn find_stream_shuffle_readers(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Vec<ShuffleStreamReaderExec> {
        if let Some(shuffle_reader) =
            plan.as_any().downcast_ref::<ShuffleStreamReaderExec>()
        {
            vec![shuffle_reader.clone()]
        } else {
            let readers = plan
                .children()
                .into_iter()
                .map(|child| ShuffleStreamReaderExec::find_stream_shuffle_readers(child))
                .collect::<Vec<_>>()
                .into_iter()
                .flatten()
                .collect();
            readers
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleStreamReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // TODO partitioning may be known and could be populated here
        // see https://github.com/apache/arrow-datafusion/issues/758
        Partitioning::UnknownPartitioning(self.partition_count)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista ShuffleStreamReaderExec does not support with_new_children()"
                .to_owned(),
        ))
    }

    async fn execute(
        &self,
        partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        info!("ShuffleStreamReaderExec::execute({})", partition);
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let (sender, receiver): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let schema = &self.schema;
        let mut rx = self.batch_input.lock().unwrap().pop().unwrap();
        let join_handle = task::spawn(async move {
            while let Some(batch) = rx.recv().await {
                output_rows.add(batch.as_ref().unwrap().num_rows());
                sender.send(batch).await.ok();
            }
        });
        Ok(RecordBatchReceiverStream::create(
            schema,
            receiver,
            Some(join_handle),
        ))

        // let schema = &self.schema;
        // let rx = self.batch_receiver.lock().unwrap().pop().unwrap();
        // let join_handle = tokio::task::spawn(async move {});
        // Ok(RecordBatchReceiverStream::create(schema, rx, join_handle))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "ShuffleStreamReaderExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // TODD need to implement
        Statistics::default()
    }
}
