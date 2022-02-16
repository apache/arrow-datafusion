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

//! Stream wrappers for physical operators

use arrow::{
    datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use super::common::AbortOnDropSingle;
use super::{RecordBatchStream, SendableRecordBatchStream};

/// Adapter for a tokio [`ReceiverStream`] that implements the
/// [`SendableRecordBatchStream`]
/// interface
pub struct RecordBatchReceiverStream {
    schema: SchemaRef,

    inner: ReceiverStream<ArrowResult<RecordBatch>>,

    #[allow(dead_code)]
    drop_helper: Option<AbortOnDropSingle<()>>,
}

impl RecordBatchReceiverStream {
    /// Construct a new [`RecordBatchReceiverStream`] which will send
    /// batches of the specfied schema from `inner`
    pub fn create(
        schema: &SchemaRef,
        rx: tokio::sync::mpsc::Receiver<ArrowResult<RecordBatch>>,
        join_handle: Option<JoinHandle<()>>,
    ) -> SendableRecordBatchStream {
        let schema = schema.clone();
        let inner = ReceiverStream::new(rx);
        Box::pin(Self {
            schema,
            inner,
            drop_helper: join_handle.map(AbortOnDropSingle::new),
        })
    }
}

impl Stream for RecordBatchReceiverStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for RecordBatchReceiverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
