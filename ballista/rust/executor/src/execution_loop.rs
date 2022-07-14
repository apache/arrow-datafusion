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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::{sync::Arc, time::Duration};

use datafusion::physical_plan::ExecutionPlan;
use log::{debug, error, info, warn};
use tonic::transport::Channel;

use ballista_core::serde::protobuf::{
    scheduler_grpc_client::SchedulerGrpcClient, PollWorkParams, PollWorkResult,
    TaskDefinition, TaskStatus,
};

use crate::as_task_status;
use crate::executor::Executor;
use ballista_core::error::BallistaError;
use ballista_core::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};

pub async fn poll_loop<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    mut scheduler: SchedulerGrpcClient<Channel>,
    executor: Arc<Executor>,
    codec: BallistaCodec<T, U>,
) {
    let executor_specification: ExecutorSpecification = executor
        .metadata
        .specification
        .as_ref()
        .unwrap()
        .clone()
        .into();
    let available_tasks_slots =
        Arc::new(AtomicUsize::new(executor_specification.task_slots as usize));
    let (task_status_sender, mut task_status_receiver) =
        std::sync::mpsc::channel::<TaskStatus>();

    loop {
        debug!("Starting registration loop with scheduler");

        let task_status: Vec<TaskStatus> =
            sample_tasks_status(&mut task_status_receiver).await;

        // Keeps track of whether we received task in last iteration
        // to avoid going in sleep mode between polling
        let mut active_job = false;

        let poll_work_result: Result<tonic::Response<PollWorkResult>, tonic::Status> =
            scheduler
                .poll_work(PollWorkParams {
                    metadata: Some(executor.metadata.clone()),
                    can_accept_task: available_tasks_slots.load(Ordering::SeqCst) > 0,
                    task_status,
                })
                .await;

        let task_status_sender = task_status_sender.clone();

        match poll_work_result {
            Ok(result) => {
                if let Some(task) = result.into_inner().task {
                    match run_received_tasks(
                        executor.clone(),
                        available_tasks_slots.clone(),
                        task_status_sender,
                        task,
                        &codec,
                    )
                    .await
                    {
                        Ok(_) => {
                            active_job = true;
                        }
                        Err(e) => {
                            warn!("Failed to run task: {:?}", e);
                            active_job = false;
                        }
                    }
                } else {
                    active_job = false;
                }
            }
            Err(error) => {
                warn!("Executor registration failed. If this continues to happen the executor might be marked as dead by the scheduler. Error: {}", error);
            }
        }
        if !active_job {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn run_received_tasks<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    executor: Arc<Executor>,
    available_tasks_slots: Arc<AtomicUsize>,
    task_status_sender: Sender<TaskStatus>,
    task: TaskDefinition,
    codec: &BallistaCodec<T, U>,
) -> Result<(), BallistaError> {
    let task_id = task.task_id.unwrap();
    let task_id_log = format!(
        "{}/{}/{}",
        task_id.job_id, task_id.stage_id, task_id.partition_id
    );
    info!("Received task {}", task_id_log);
    available_tasks_slots.fetch_sub(1, Ordering::SeqCst);

    let plan: Arc<dyn ExecutionPlan> =
        U::try_decode(task.plan.as_slice()).and_then(|proto| {
            proto.try_into_physical_plan(
                executor.ctx.as_ref(),
                codec.physical_extension_codec(),
            )
        })?;

    let shuffle_output_partitioning =
        parse_protobuf_hash_partitioning(task.output_partitioning.as_ref())?;

    tokio::spawn(async move {
        let execution_result = executor
            .execute_shuffle_write(
                task_id.job_id.clone(),
                task_id.stage_id as usize,
                task_id.partition_id as usize,
                plan,
                shuffle_output_partitioning,
            )
            .await;
        info!("Done with task {}", task_id_log);
        debug!("Statistics: {:?}", execution_result);
        available_tasks_slots.fetch_add(1, Ordering::SeqCst);
        let _ = task_status_sender.send(as_task_status(
            execution_result,
            executor.metadata.id.clone(),
            task_id,
        ));
    });

    Ok(())
}

async fn sample_tasks_status(
    task_status_receiver: &mut Receiver<TaskStatus>,
) -> Vec<TaskStatus> {
    let mut task_status: Vec<TaskStatus> = vec![];

    loop {
        match task_status_receiver.try_recv() {
            anyhow::Result::Ok(status) => {
                task_status.push(status);
            }
            Err(TryRecvError::Empty) => {
                break;
            }
            Err(TryRecvError::Disconnected) => {
                error!("Task statuses channel disconnected");
            }
        }
    }

    task_status
}
