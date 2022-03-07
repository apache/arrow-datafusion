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

use datafusion::logical_plan::LogicalPlan;

#[derive(Clone)]
pub(crate) enum SchedulerServerEvent {
    // number of offer rounds
    ReviveOffers(u32),
}

#[derive(Clone)]
pub enum QueryStageSchedulerEvent {
    JobSubmitted(String, Box<LogicalPlan>),
    StageFinished(String, u32),
    JobFinished(String),
    JobFailed(String, u32, String),
}
