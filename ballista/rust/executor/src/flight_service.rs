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

use arrow::array::ArrayRef;
use arrow::chunk::Chunk;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use crate::executor::Executor;
use ballista_core::error::BallistaError;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;

use arrow::io::ipc::read::read_file_metadata;
use arrow_format::flight::data::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use arrow_format::flight::service::flight_service_server::FlightService;
use datafusion::arrow::{
    error::Error as ArrowError, io::ipc::read::FileReader, io::ipc::write::WriteOptions,
};
use futures::{Stream, StreamExt};
use log::{info, warn};
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
    type DoActionStream = BoxedFlightStream<arrow_format::flight::data::Result>;
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
                let (tx, rx): (FlightDataSender, FlightDataReceiver) = channel(2);
                let path = path.clone();
                // Arrow IPC reader does not implement Sync + Send so we need to use a channel
                // to communicate
                task::spawn(async move {
                    if let Err(e) = stream_flight_data(path, tx).await {
                        warn!("Error streaming results: {:?}", e);
                    }
                });

                Ok(Response::new(
                    Box::pin(ReceiverStream::new(rx)) as Self::DoGetStream
                ))
            }
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
        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let _data = data?;
        }

        Err(Status::unimplemented("do_put"))
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
    chunk: &Chunk<ArrayRef>,
    options: &WriteOptions,
) -> Box<dyn Iterator<Item = Result<FlightData, Status>>> {
    let (flight_dictionaries, flight_batch) =
        arrow::io::flight::serialize_batch(chunk, &[], options);
    Box::new(
        flight_dictionaries
            .into_iter()
            .chain(std::iter::once(flight_batch))
            .map(Ok),
    )
}

async fn stream_flight_data(path: String, tx: FlightDataSender) -> Result<(), Status> {
    let mut file = File::open(&path)
        .map_err(|e| {
            BallistaError::General(format!(
                "Failed to open partition file at {}: {:?}",
                path, e
            ))
        })
        .map_err(|e| from_ballista_err(&e))?;
    let file_meta = read_file_metadata(&mut file).map_err(|e| from_arrow_err(&e))?;
    let reader = FileReader::new(&mut file, file_meta, None);

    let options = WriteOptions::default();
    let schema_flight_data = arrow::io::flight::serialize_schema(reader.schema(), None);
    send_response(&tx, Ok(schema_flight_data)).await?;

    let mut row_count = 0;
    for batch in reader {
        if let Ok(x) = &batch {
            row_count += x.len();
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
