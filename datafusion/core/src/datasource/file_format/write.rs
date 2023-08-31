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

use std::io::Error;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::physical_plan::FileMeta;
use crate::error::Result;
use crate::physical_plan::SendableRecordBatchStream;

use arrow_array::RecordBatch;
use datafusion_common::{exec_err, internal_err, DataFusionError, FileCompressionType};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_execution::RecordBatchStream;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::{ready, StreamExt};
use object_store::path::Path;
use object_store::{MultipartId, ObjectMeta, ObjectStore};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::task::{self, JoinHandle, JoinSet};

/// `AsyncPutWriter` is an object that facilitates asynchronous writing to object stores.
/// It is specifically designed for the `object_store` crate's `put` method and sends
/// whole bytes at once when the buffer is flushed.
pub struct AsyncPutWriter {
    /// Object metadata
    object_meta: ObjectMeta,
    /// A shared reference to the object store
    store: Arc<dyn ObjectStore>,
    /// A buffer that stores the bytes to be sent
    current_buffer: Vec<u8>,
    /// Used for async handling in flush method
    inner_state: AsyncPutState,
}

impl AsyncPutWriter {
    /// Constructor for the `AsyncPutWriter` object
    pub fn new(object_meta: ObjectMeta, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_meta,
            store,
            current_buffer: vec![],
            // The writer starts out in buffering mode
            inner_state: AsyncPutState::Buffer,
        }
    }

    /// Separate implementation function that unpins the [`AsyncPutWriter`] so
    /// that partial borrows work correctly
    fn poll_shutdown_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        loop {
            match &mut self.inner_state {
                AsyncPutState::Buffer => {
                    // Convert the current buffer to bytes and take ownership of it
                    let bytes = Bytes::from(mem::take(&mut self.current_buffer));
                    // Set the inner state to Put variant with the bytes
                    self.inner_state = AsyncPutState::Put { bytes }
                }
                AsyncPutState::Put { bytes } => {
                    // Send the bytes to the object store's put method
                    return Poll::Ready(
                        ready!(self
                            .store
                            .put(&self.object_meta.location, bytes.clone())
                            .poll_unpin(cx))
                        .map_err(Error::from),
                    );
                }
            }
        }
    }
}

/// An enum that represents the inner state of AsyncPut
enum AsyncPutState {
    /// Building Bytes struct in this state
    Buffer,
    /// Data in the buffer is being sent to the object store
    Put { bytes: Bytes },
}

impl AsyncWrite for AsyncPutWriter {
    // Define the implementation of the AsyncWrite trait for the `AsyncPutWriter` struct
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        // Extend the current buffer with the incoming buffer
        self.current_buffer.extend_from_slice(buf);
        // Return a ready poll with the length of the incoming buffer
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        // Return a ready poll with an empty result
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        // Call the poll_shutdown_inner method to handle the actual sending of data to the object store
        self.poll_shutdown_inner(cx)
    }
}

/// Stores data needed during abortion of MultiPart writers
pub(crate) struct MultiPart {
    /// A shared reference to the object store
    store: Arc<dyn ObjectStore>,
    multipart_id: MultipartId,
    location: Path,
}

impl MultiPart {
    /// Create a new `MultiPart`
    pub fn new(
        store: Arc<dyn ObjectStore>,
        multipart_id: MultipartId,
        location: Path,
    ) -> Self {
        Self {
            store,
            multipart_id,
            location,
        }
    }
}

pub(crate) enum AbortMode {
    Put,
    Append,
    MultiPart(MultiPart),
}

/// A wrapper struct with abort method and writer
pub(crate) struct AbortableWrite<W: AsyncWrite + Unpin + Send> {
    writer: W,
    mode: AbortMode,
}

impl<W: AsyncWrite + Unpin + Send> AbortableWrite<W> {
    /// Create a new `AbortableWrite` instance with the given writer, and write mode.
    pub(crate) fn new(writer: W, mode: AbortMode) -> Self {
        Self { writer, mode }
    }

    /// handling of abort for different write modes
    pub(crate) fn abort_writer(&self) -> Result<BoxFuture<'static, Result<()>>> {
        match &self.mode {
            AbortMode::Put => Ok(async { Ok(()) }.boxed()),
            AbortMode::Append => exec_err!("Cannot abort in append mode"),
            AbortMode::MultiPart(MultiPart {
                store,
                multipart_id,
                location,
            }) => {
                let location = location.clone();
                let multipart_id = multipart_id.clone();
                let store = store.clone();
                Ok(Box::pin(async move {
                    store
                        .abort_multipart(&location, &multipart_id)
                        .await
                        .map_err(DataFusionError::ObjectStore)
                }))
            }
        }
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncWrite for AbortableWrite<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}

/// An enum that defines different file writer modes.
#[derive(Debug, Clone, Copy)]
pub enum FileWriterMode {
    /// Data is appended to an existing file.
    Append,
    /// Data is written to a new file.
    Put,
    /// Data is written to a new file in multiple parts.
    PutMultipart,
}
/// A trait that defines the methods required for a RecordBatch serializer.
#[async_trait]
pub trait BatchSerializer: Unpin + Send {
    /// Asynchronously serializes a `RecordBatch` and returns the serialized bytes.
    async fn serialize(&mut self, batch: RecordBatch) -> Result<Bytes>;
    /// Duplicates self to support serializing multiple batches in parralell on multiple cores
    fn duplicate(&mut self) -> Result<Box<dyn BatchSerializer>> {
        Err(DataFusionError::NotImplemented(
            "Parallel serialization is not implemented for this file type".into(),
        ))
    }
}

/// Returns an [`AbortableWrite`] which writes to the given object store location
/// with the specified compression
pub(crate) async fn create_writer(
    writer_mode: FileWriterMode,
    file_compression_type: FileCompressionType,
    file_meta: FileMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>> {
    let object = &file_meta.object_meta;
    match writer_mode {
        // If the mode is append, call the store's append method and return wrapped in
        // a boxed trait object.
        FileWriterMode::Append => {
            let writer = object_store
                .append(&object.location)
                .await
                .map_err(DataFusionError::ObjectStore)?;
            let writer = AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::Append,
            );
            Ok(writer)
        }
        // If the mode is put, create a new AsyncPut writer and return it wrapped in
        // a boxed trait object
        FileWriterMode::Put => {
            let writer = Box::new(AsyncPutWriter::new(object.clone(), object_store));
            let writer = AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::Put,
            );
            Ok(writer)
        }
        // If the mode is put multipart, call the store's put_multipart method and
        // return the writer wrapped in a boxed trait object.
        FileWriterMode::PutMultipart => {
            let (multipart_id, writer) = object_store
                .put_multipart(&object.location)
                .await
                .map_err(DataFusionError::ObjectStore)?;
            Ok(AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::MultiPart(MultiPart::new(
                    object_store,
                    multipart_id,
                    object.location.clone(),
                )),
            ))
        }
    }
}

/// Serializes a single data stream in parallel and writes to an ObjectStore
/// concurrently. Data order is preserved. In the event of an error, 
/// the ObjectStore writer is returned to the caller in addition to an error,
/// so that the caller may handle aborting failed writes. 
async fn serialize_rb_stream_to_object_store(
    mut data_stream: Pin<Box<dyn RecordBatchStream + Send>>,
    mut serializer: Box<dyn BatchSerializer>,
    mut writer: AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>,
) -> std::result::Result<
    (Box<dyn BatchSerializer>, AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>, u64),
    (
        AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>,
        DataFusionError,
    ),
> {
    let mut row_count = 0;
    // Not using JoinSet here since we want to ulimately write to ObjectStore preserving file order
    let mut serialize_tasks: Vec<JoinHandle<Result<(usize, Bytes), DataFusionError>>> =
        Vec::new();
    while let Some(maybe_batch) = data_stream.next().await {
        let mut serializer_clone = match serializer.duplicate() {
            Ok(s) => s,
            Err(_) => {
                return Err((
                    writer,
                    DataFusionError::Internal(
                        "Unknown error writing to object store".into(),
                    ),
                ))
            }
        };
        serialize_tasks.push(task::spawn(async move {
            let batch = maybe_batch?;
            let num_rows = batch.num_rows();
            let bytes = serializer_clone.serialize(batch).await?;
            Ok((num_rows, bytes))
        }));
    }
    for serialize_result in serialize_tasks {
        let result = serialize_result.await;
        match result {
            Ok(res) => {
                let (cnt, bytes) = match res {
                    Ok(r) => r,
                    Err(e) => return Err((writer, e)),
                };
                row_count += cnt;
                match writer.write_all(&bytes).await {
                    Ok(_) => (),
                    Err(_) => {
                        return Err((
                            writer,
                            DataFusionError::Internal(
                                "Unknown error writing to object store".into(),
                            ),
                        ))
                    }
                };
            }
            Err(_) => {
                return Err((
                    writer,
                    DataFusionError::Internal(
                        "Unknown error writing to object store".into(),
                    ),
                ))
            }
        }
    }

    Ok((serializer, writer, row_count as u64))
}

/// Contains the common logic for serializing RecordBatches and
/// writing the resulting bytes to an ObjectStore.
/// Serialization is assumed to be stateless, i.e.
/// each RecordBatch can be serialized without any
/// dependency on the RecordBatches before or after.
pub(crate) async fn stateless_serialize_and_write_files(
    data: Vec<SendableRecordBatchStream>,
    mut serializers: Vec<Box<dyn BatchSerializer>>,
    mut writers: Vec<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>>,
    single_file_output: bool,
) -> Result<u64> {
    if single_file_output && (serializers.len() != 1 || writers.len() != 1) {
        return internal_err!("single_file_output is true, but got more than 1 writer!");
    }
    let num_partitions = data.len();
    let num_writers = writers.len();
    if !single_file_output && (num_partitions != num_writers) {
        return internal_err!("single_file_ouput is false, but did not get 1 writer for each output partition!");
    }
    let mut row_count = 0;
    // tracks if any writers encountered an error triggering the need to abort
    let mut any_errors = false;
    // tracks the specific error triggering abort
    let mut triggering_error = None;
    // tracks if any errors were encountered in the process of aborting writers.
    // if true, we may not have a guarentee that all written data was cleaned up.
    let mut any_abort_errors = false;
    match single_file_output {
        false => {
            let mut join_set = JoinSet::new();
            for (data_stream, serializer, writer) in data
                .into_iter()
                .zip(serializers.into_iter())
                .zip(writers.into_iter())
                .map(|((a, b), c)| (a, b, c))
            {
                join_set.spawn(async move {
                    serialize_rb_stream_to_object_store(data_stream, serializer, writer)
                        .await
                });
            }
            let mut finished_writers = Vec::with_capacity(num_writers);
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(res) => match res {
                        Ok((_, writer, cnt)) => {
                            finished_writers.push(writer);
                            row_count += cnt;
                        }
                        Err((writer, e)) => {
                            finished_writers.push(writer);
                            any_errors = true;
                            triggering_error = Some(e);
                        }
                    },
                    Err(_) => {
                        // Don't panic, instead try to clean up as many writers as possible.
                        // If we hit this code, ownership of a writer was not joined back to
                        // this thread, so we cannot clean it up (hence any_abort_errors is true)
                        any_errors = true;
                        any_abort_errors = true;
                    }
                }
            }

            // Finalize or abort writers as appropriate
            for mut writer in finished_writers.into_iter() {
                match any_errors {
                    true => {
                        let abort_result = writer.abort_writer();
                        if abort_result.is_err() {
                            any_abort_errors = true;
                        }
                    }
                    false => {
                        // TODO if we encounter an error during shutdown, delete previously written files?
                        writer.shutdown()
                            .await
                            .map_err(|_| DataFusionError::Internal("Error encountered while finalizing writes! Partial results may have been written to ObjectStore!".into()))?;
                    }
                }
            }
        }
        true => {
            let mut writer = writers.remove(0);
            let mut serializer = serializers.remove(0);
            let mut cnt;
            for data_stream in data.into_iter() {
                (serializer, writer, cnt) = match serialize_rb_stream_to_object_store(data_stream, serializer, writer).await{
                    Ok((s, w, c)) => (s, w, c),
                    Err((w, e)) => {
                        any_errors = true;
                        triggering_error = Some(e);
                        writer = w;
                        break;
                    }
                };
                row_count += cnt;
            }
            match any_errors {
                true => {
                    let abort_result = writer.abort_writer();
                    if abort_result.is_err() {
                        any_abort_errors = true;
                    }
                }
                false => writer.shutdown().await?,
            }
        }
    }

    if any_errors {
        match any_abort_errors{
            true => return Err(DataFusionError::Internal("Error encountered during writing to ObjectStore and failed to abort all writers. Partial result may have been written.".into())),
            false => match triggering_error {
                Some(e) => return Err(e),
                None => return Err(DataFusionError::Internal("Unknown Error encountered during writing to ObjectStore. All writers succesfully aborted.".into()))
            }
        }
    }

    Ok(row_count)
}
