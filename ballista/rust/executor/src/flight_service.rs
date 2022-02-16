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

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use std::convert::TryFrom;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use crate::executor::Executor;
use arrow_flight::SchemaAsIpc;
use ballista_core::error::BallistaError;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;

use arrow::datatypes::Schema;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::{
    error::ArrowError, ipc::reader::FileReader, ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};
use log::{info, warn};
use std::io::{Read, Seek};
use std::task::{Context, Poll};
use tokio::sync::mpsc::channel;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

type FlightDataSender = Sender<Result<FlightData, Status>>;
type FlightDataReceiver = Receiver<Result<FlightData, Status>>;

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightService {
    /// Executor
    _executor: Arc<Executor>,
}

impl BallistaFlightService {
    pub fn new(_executor: Arc<Executor>) -> Self {
        Self { _executor }
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action =
            decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::FetchPartition { path, .. } => {
                info!("FetchPartition reading {}", &path);
                let file = File::open(&path)
                    .map_err(|e| {
                        BallistaError::General(format!(
                            "Failed to open partition file at {}: {:?}",
                            path, e
                        ))
                    })
                    .map_err(|e| from_ballista_err(&e))?;
                let reader = FileReader::try_new(file).map_err(|e| from_arrow_err(&e))?;

                let (tx, rx): (FlightDataSender, FlightDataReceiver) = channel(2);

                // Arrow IPC reader does not implement Sync + Send so we need to use a channel
                // to communicate
                task::spawn(async move {
                    if let Err(e) = stream_flight_data(reader, tx).await {
                        warn!("Error streaming results: {:?}", e);
                    }
                });

                Ok(Response::new(
                    Box::pin(ReceiverStream::new(rx)) as Self::DoGetStream
                ))
            }
            _ => Err(Status::unimplemented("unimplemented action")),
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        let first_block_maybe = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty FlightData stream"))?;

        let first_block = match first_block_maybe {
            Ok(fb) => fb,
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "first block did not contain a schema: {}",
                    e
                )));
            }
        };

        // Extract the FlightDescriptor and Schema from the first FlightData block
        let descriptor = match &first_block.flight_descriptor {
            Some(d) => d,
            None => {
                return Err(Status::invalid_argument(
                    "No Flight Descriptor in first block",
                ));
            }
        };

        let arrow_schema = Schema::try_from(&first_block).map_err(|e| {
            Status::invalid_argument(format!(
                "first block did not contain a schema: {}",
                e
            ))
        })?;

        let arrow_schema_arc = Arc::new(arrow_schema);

        let action =
            decode_protobuf(&descriptor.cmd).map_err(|e| from_ballista_err(&e))?;

        let (sender, channel_key) = match action {
            BallistaAction::PushPartition {
                job_id,
                stage_id,
                partition_id,
            } => {
                info!("Trying to read PushPartition job {:?}, stage {:?} and partition {:?}", job_id, stage_id, partition_id);
                {
                    let channels_map = self._executor.channels.read();
                    let channel_key = (job_id, stage_id, partition_id);
                    match channels_map.get(&channel_key) {
                        Some(d) => (d.clone(), channel_key),
                        None => {
                            return Err(Status::invalid_argument(format!(
                                "No receive channels registered for this PushPartition job {:?}, stage {:?} ", &channel_key.0, &channel_key.1)
                            ));
                        }
                    }
                }
            }
            _ => return Err(Status::unimplemented("Invalid Action")),
        };

        while let Some(flight_data) = stream.message().await? {
            let record_batch =
                flight_data_to_arrow_batch(&flight_data, arrow_schema_arc.clone(), &[]);

            // ignore the error
            sender.send(record_batch).await.ok();
        }

        {
            let mut channels_map = self._executor.channels.write();
            channels_map.remove(&channel_key);
        }

        Ok(Response::new(
            Box::pin(EmptyPutStream {}) as Self::DoPutStream
        ))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        let _action =
            decode_protobuf(&action.body.to_vec()).map_err(|e| from_ballista_err(&e))?;

        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// Convert a single RecordBatch into an iterator of FlightData (containing
/// dictionaries and batches)
fn create_flight_iter(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> Box<dyn Iterator<Item = Result<FlightData, Status>>> {
    let (flight_dictionaries, flight_batch) =
        arrow_flight::utils::flight_data_from_arrow_batch(batch, options);
    Box::new(
        flight_dictionaries
            .into_iter()
            .chain(std::iter::once(flight_batch))
            .map(Ok),
    )
}

async fn stream_flight_data<T>(
    reader: FileReader<T>,
    tx: FlightDataSender,
) -> Result<(), Status>
where
    T: Read + Seek,
{
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let schema_flight_data = SchemaAsIpc::new(reader.schema().as_ref(), &options).into();
    send_response(&tx, Ok(schema_flight_data)).await?;

    let mut row_count = 0;
    for batch in reader {
        if let Ok(x) = &batch {
            row_count += x.num_rows();
        }
        let batch_flight_data: Vec<_> = batch
            .map(|b| create_flight_iter(&b, &options).collect())
            .map_err(|e| from_arrow_err(&e))?;
        for batch in batch_flight_data.into_iter() {
            send_response(&tx, batch).await?;
        }
    }
    info!("FetchPartition streamed {} rows", row_count);
    Ok(())
}

async fn send_response(
    tx: &FlightDataSender,
    data: Result<FlightData, Status>,
) -> Result<(), Status> {
    tx.send(data)
        .await
        .map_err(|e| Status::internal(format!("{:?}", e)))
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {:?}", e))
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {:?}", e))
}

pub struct EmptyPutStream {}

impl Stream for EmptyPutStream {
    type Item = Result<PutResult, tonic::Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
