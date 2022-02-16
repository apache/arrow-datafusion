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

//! Ballista Rust executor binary.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_executor::{execution_loop, executor_server};
use log::info;
use tempfile::TempDir;
use tonic::transport::Server;
use uuid::Uuid;

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::protobuf::{
    executor_registration, scheduler_grpc_client::SchedulerGrpcClient,
    ExecutorRegistration, LogicalPlanNode, PhysicalPlanNode,
};
use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::serde::BallistaCodec;
use ballista_core::{print_version, BALLISTA_VERSION};
use ballista_executor::executor::Executor;
use ballista_executor::flight_service::BallistaFlightService;
use config::prelude::*;
use datafusion::prelude::ExecutionContext;

#[macro_use]
extern crate configure_me;

#[allow(clippy::all, warnings)]
mod config {
    // Ideally we would use the include_config macro from configure_me, but then we cannot use
    // #[allow(clippy::all)] to silence clippy warnings from the generated code
    include!(concat!(env!("OUT_DIR"), "/executor_configure_me_config.rs"));
}

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // parse command-line arguments
    let (opt, _remaining_args) =
        Config::including_optional_config_files(&["/etc/ballista/executor.toml"])
            .unwrap_or_exit();

    if opt.version {
        print_version();
        std::process::exit(0);
    }

    let external_host = opt.external_host;
    let bind_host = opt.bind_host;
    let port = opt.bind_port;
    let grpc_port = opt.bind_grpc_port;

    let addr = format!("{}:{}", bind_host, port);
    let addr = addr
        .parse()
        .with_context(|| format!("Could not parse address: {}", addr))?;

    let scheduler_host = opt.scheduler_host;
    let scheduler_port = opt.scheduler_port;
    let scheduler_url = format!("http://{}:{}", scheduler_host, scheduler_port);

    let work_dir = opt.work_dir.unwrap_or(
        TempDir::new()?
            .into_path()
            .into_os_string()
            .into_string()
            .unwrap(),
    );
    info!("Running with config:");
    info!("work_dir: {}", work_dir);
    info!("concurrent_tasks: {}", opt.concurrent_tasks);

    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        optional_host: external_host
            .clone()
            .map(executor_registration::OptionalHost::Host),
        port: port as u32,
        grpc_port: grpc_port as u32,
    };
    let executor_specification = ExecutorSpecification {
        task_slots: opt.concurrent_tasks as u32,
    };
    let executor: Arc<Executor> = Arc::new(Executor::new_with_specification(
        &work_dir,
        executor_specification,
        Arc::new(ExecutionContext::new()),
    ));

    let scheduler = SchedulerGrpcClient::connect(scheduler_url)
        .await
        .context("Could not connect to scheduler")?;

    let default_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
        BallistaCodec::default();

    let scheduler_policy = opt.task_scheduling_policy;
    match scheduler_policy {
        TaskSchedulingPolicy::Push => {
            tokio::spawn(executor_server::startup(
                scheduler,
                executor.clone(),
                executor_meta,
                default_codec,
            ));
        }
        _ => {
            tokio::spawn(execution_loop::poll_loop(
                scheduler,
                executor.clone(),
                executor_meta,
                opt.concurrent_tasks,
                default_codec,
            ));
        }
    }

    // Arrow flight service
    {
        let service = BallistaFlightService::new(executor.clone());
        let server = FlightServiceServer::new(service);
        info!(
            "Ballista v{} Rust Executor listening on {:?}",
            BALLISTA_VERSION, addr
        );
        let server_future =
            tokio::spawn(Server::builder().add_service(server).serve(addr));
        server_future
            .await
            .context("Tokio error")?
            .context("Could not start executor server")?;
    }

    Ok(())
}
