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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventAction;
use ballista_core::serde::protobuf::{LaunchTaskParams, TaskDefinition};
use ballista_core::serde::scheduler::ExecutorDeltaData;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};

use crate::scheduler_server::ExecutorsClient;
use crate::state::task_scheduler::TaskScheduler;
use crate::state::SchedulerState;

#[derive(Clone)]
pub(crate) enum SchedulerServerEvent {
    JobSubmitted(String),
}

pub(crate) struct SchedulerServerEventAction<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    executors_client: ExecutorsClient,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    SchedulerServerEventAction<T, U>
{
    pub fn new(
        state: Arc<SchedulerState<T, U>>,
        executors_client: ExecutorsClient,
    ) -> Self {
        Self {
            state,
            executors_client,
        }
    }

    async fn offer_resources(
        &self,
        job_id: String,
    ) -> Result<Option<SchedulerServerEvent>> {
        let mut available_executors = self.state.get_available_executors_data();
        // In case of there's no enough resources, reschedule the tasks of the job
        if available_executors.is_empty() {
            // TODO Maybe it's better to use an exclusive runtime for this kind task scheduling
            warn!("Not enough available executors for task running");
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(Some(SchedulerServerEvent::JobSubmitted(job_id)));
        }

        let mut executors_delta_data: Vec<ExecutorDeltaData> = available_executors
            .iter()
            .map(|executor_data| ExecutorDeltaData {
                executor_id: executor_data.executor_id.clone(),
                task_slots: executor_data.available_task_slots as i32,
            })
            .collect();

        let (tasks_assigment, num_tasks) = self
            .state
            .fetch_tasks(&mut available_executors, &job_id)
            .await?;
        for (delta_data, data) in executors_delta_data
            .iter_mut()
            .zip(available_executors.iter())
        {
            delta_data.task_slots =
                data.available_task_slots as i32 - delta_data.task_slots;
        }

        if num_tasks > 0 {
            self.launch_tasks(&executors_delta_data, tasks_assigment)
                .await?;
        }

        Ok(None)
    }

    async fn launch_tasks(
        &self,
        executors: &[ExecutorDeltaData],
        tasks_assigment: Vec<Vec<TaskDefinition>>,
    ) -> Result<()> {
        for (idx_executor, tasks) in tasks_assigment.into_iter().enumerate() {
            if !tasks.is_empty() {
                let executor_delta_data = &executors[idx_executor];
                debug!(
                    "Start to launch tasks {:?} to executor {:?}",
                    tasks
                        .iter()
                        .map(|task| {
                            if let Some(task_id) = task.task_id.as_ref() {
                                format!(
                                    "{}/{}/{}",
                                    task_id.job_id,
                                    task_id.stage_id,
                                    task_id.partition_id
                                )
                            } else {
                                "".to_string()
                            }
                        })
                        .collect::<Vec<String>>(),
                    executor_delta_data.executor_id
                );
                let mut client = {
                    let clients = self.executors_client.read().await;
                    clients
                        .get(&executor_delta_data.executor_id)
                        .unwrap()
                        .clone()
                };
                // Update the resources first
                self.state.update_executor_data(executor_delta_data);
                // TODO check whether launching task is successful or not
                client.launch_task(LaunchTaskParams { task: tasks }).await?;
            } else {
                // Since the task assignment policy is round robin,
                // if find tasks for one executor is empty, just break fast
                break;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<SchedulerServerEvent> for SchedulerServerEventAction<T, U>
{
    // TODO
    fn on_start(&self) {}

    // TODO
    fn on_stop(&self) {}

    async fn on_receive(
        &self,
        event: SchedulerServerEvent,
    ) -> Result<Option<SchedulerServerEvent>> {
        match event {
            SchedulerServerEvent::JobSubmitted(job_id) => {
                self.offer_resources(job_id).await
            }
        }
    }

    // TODO
    fn on_error(&self, _error: BallistaError) {}
}
