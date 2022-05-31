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

//! This module contains the shared state available at different parts
//! of query planning and execution
//!
//! # Runtime Environment
//!
//! [`runtime_env::RuntimeEnv`] can be created from a [`runtime_env::RuntimeConfig`] and
//! stores state to be shared across multiple sessions. In most applications there will
//! be a single [`runtime_env::RuntimeEnv`] for the entire process
//!
//! # Session Context
//!
//! [`context::SessionContext`] can be created from a [`context::SessionConfig`] and
//! an optional [`runtime_env::RuntimeConfig`], and stores the state for a particular
//! query session.
//!
//! In particular [`context::SessionState`] is the information available to query planning
//!
//! # Task Context
//!
//! [`context::TaskContext`] is typically created from a [`context::SessionContext`] or
//! [`context::SessionState`], and represents the state available to query execution.
//!
//! In particular it is the state passed to [`crate::physical_plan::ExecutionPlan::execute`]
//!

pub mod context;
pub mod disk_manager;
pub mod memory_manager;
pub mod options;
pub mod runtime_env;
pub mod async_executor;

pub use disk_manager::DiskManager;
pub use memory_manager::{
    human_readable_size, MemoryConsumer, MemoryConsumerId, MemoryManager,
};
