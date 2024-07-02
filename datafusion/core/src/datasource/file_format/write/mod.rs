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

//! Module containing helper methods/traits related to enabling
//! write support for the various file formats

use std::io::Write;
use std::sync::Arc;

use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::error::Result;

use arrow_array::RecordBatch;

use bytes::Bytes;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::io::AsyncWrite;

pub mod demux;
pub(crate) mod orchestration;

/// A buffer with interior mutability shared by the SerializedFileWriter and
/// ObjectStore writer
#[derive(Clone)]
pub struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    pub buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(capacity))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

/// A trait that defines the methods required for a RecordBatch serializer.
pub trait BatchSerializer: Sync + Send {
    /// Asynchronously serializes a `RecordBatch` and returns the serialized bytes.
    /// Parameter `initial` signals whether the given batch is the first batch.
    /// This distinction is important for certain serializers (like CSV).
    fn serialize(&self, batch: RecordBatch, initial: bool) -> Result<Bytes>;
}

/// Returns an [`AsyncWrite`] which writes to the given object store location
/// with the specified compression.
/// We drop the `AbortableWrite` struct and the writer will not try to cleanup on failure.
/// Users can configure automatic cleanup with their cloud provider.
pub async fn create_writer(
    file_compression_type: FileCompressionType,
    location: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Box<dyn AsyncWrite + Send + Unpin>> {
    let buf_writer = BufWriter::new(object_store, location.clone());
    file_compression_type.convert_async_writer(buf_writer)
}
