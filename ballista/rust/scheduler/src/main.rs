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

//! Ballista Rust scheduler binary.

use anyhow::{Context, Result};
use ballista_scheduler::externalscaler::external_scaler_server::ExternalScalerServer;
use futures::future::{self, Either, TryFutureExt};
use hyper::{server::conn::AddrStream, service::make_service_fn, Server};
use std::convert::Infallible;
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::server::Connected;
use tonic::transport::Server as TonicServer;
use tower::Service;

use ballista_core::BALLISTA_VERSION;
use ballista_core::{
    print_version,
    serde::protobuf::{
        scheduler_grpc_server::SchedulerGrpcServer, LogicalPlanNode, PhysicalPlanNode,
    },
};
use ballista_scheduler::api::{get_routes, EitherBody, Error};
#[cfg(feature = "etcd")]
use ballista_scheduler::state::EtcdClient;
#[cfg(feature = "sled")]
use ballista_scheduler::state::StandaloneClient;
use ballista_scheduler::{
    state::ConfigBackendClient, ConfigBackend, SchedulerEnv, SchedulerServer,
    TaskScheduler,
};

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::BallistaCodec;
use log::info;
use tokio::sync::{mpsc, RwLock};

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(
        env!("OUT_DIR"),
        "/scheduler_configure_me_config.rs"
    ));
}

use config::prelude::*;
use datafusion::prelude::ExecutionContext;

async fn start_server(
    config_backend: Arc<dyn ConfigBackendClient>,
    namespace: String,
    addr: SocketAddr,
    policy: TaskSchedulingPolicy,
) -> Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
    );
    //should only call SchedulerServer::new() once in the process
    info!(
        "Starting Scheduler grpc server with task scheduling policy of {:?}",
        policy
    );
    let scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        match policy {
            TaskSchedulingPolicy::Push => {
                // TODO make the buffer size configurable
                let (tx_job, rx_job) = mpsc::channel::<String>(10000);
                let scheduler_server = SchedulerServer::new_with_policy(
                    config_backend.clone(),
                    namespace.clone(),
                    policy,
                    Some(SchedulerEnv { tx_job }),
                    Arc::new(RwLock::new(ExecutionContext::new())),
                    BallistaCodec::default(),
                );
                let task_scheduler =
                    TaskScheduler::new(Arc::new(scheduler_server.clone()));
                task_scheduler.start(rx_job);
                scheduler_server
            }
            _ => SchedulerServer::new(
                config_backend.clone(),
                namespace.clone(),
                Arc::new(RwLock::new(ExecutionContext::new())),
                BallistaCodec::default(),
            ),
        };

    Ok(Server::bind(&addr)
        .serve(make_service_fn(move |request: &AddrStream| {
            let scheduler_grpc_server =
                SchedulerGrpcServer::new(scheduler_server.clone());

            let keda_scaler = ExternalScalerServer::new(scheduler_server.clone());

            let mut tonic = TonicServer::builder()
                .add_service(scheduler_grpc_server)
                .add_service(keda_scaler)
                .into_service();
            let mut warp = warp::service(get_routes(scheduler_server.clone()));

            let connect_info = request.connect_info();
            future::ok::<_, Infallible>(tower::service_fn(
                move |req: hyper::Request<hyper::Body>| {
                    // Set the connect info from hyper to tonic
                    let (mut parts, body) = req.into_parts();
                    parts.extensions.insert(connect_info.clone());
                    let req = http::Request::from_parts(parts, body);

                    let header = req.headers().get(hyper::header::ACCEPT);
                    if header.is_some() && header.unwrap().eq("application/json") {
                        return Either::Left(
                            warp.call(req)
                                .map_ok(|res| res.map(EitherBody::Left))
                                .map_err(Error::from),
                        );
                    }
                    Either::Right(
                        tonic
                            .call(req)
                            .map_ok(|res| res.map(EitherBody::Right))
                            .map_err(Error::from),
                    )
                },
            ))
        }))
        .await
        .context("Could not start grpc server")?)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // parse options
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/scheduler.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let namespace = opt.namespace;
    let bind_host = opt.bind_host;
    let port = opt.bind_port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr.parse()?;

    let client: Arc<dyn ConfigBackendClient> = match opt.config_backend {
        #[cfg(not(any(feature = "sled", feature = "etcd")))]
        _ => std::compile_error!(
            "To build the scheduler enable at least one config backend feature (`etcd` or `sled`)"
        ),
        #[cfg(feature = "etcd")]
        ConfigBackend::Etcd => {
            let etcd = etcd_client::Client::connect(&[opt.etcd_urls], None)
                .await
                .context("Could not connect to etcd")?;
            Arc::new(EtcdClient::new(etcd))
        }
        #[cfg(not(feature = "etcd"))]
        ConfigBackend::Etcd => {
            unimplemented!(
                "build the scheduler with the `etcd` feature to use the etcd config backend"
            )
        }
        #[cfg(feature = "sled")]
        ConfigBackend::Standalone => {
            // TODO: Use a real file and make path is configurable
            Arc::new(
                StandaloneClient::try_new_temporary()
                    .context("Could not create standalone config backend")?,
            )
        }
        #[cfg(not(feature = "sled"))]
        ConfigBackend::Standalone => {
            unimplemented!(
                "build the scheduler with the `sled` feature to use the standalone config backend"
            )
        }
    };

    let policy: TaskSchedulingPolicy = opt.scheduler_policy;
    start_server(client, namespace, addr, policy).await?;
    Ok(())
}
