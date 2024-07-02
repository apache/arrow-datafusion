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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion_common::{plan_datafusion_err, DataFusionError, Result};
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};

use crate::{
    config::SessionConfig,
    memory_pool::MemoryPool,
    registry::FunctionRegistry,
    runtime_env::{RuntimeConfig, RuntimeEnv},
};

/// Task Execution Context
///
/// A [`TaskContext`] contains the state required during a single
/// query's execution. Please see [`SessionContext`] for a user level
/// multi-query API.
///
/// [`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
#[derive(Debug)]
pub struct TaskContext {
    /// Session Id
    session_id: String,
    /// Optional Task Identify
    task_id: Option<String>,
    /// Session configuration
    session_config: SessionConfig,
    /// Scalar functions associated with this task context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions associated with this task context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window functions associated with this task context
    window_functions: HashMap<String, Arc<WindowUDF>>,
    /// Runtime environment associated with this task context
    runtime: Arc<RuntimeEnv>,
}

impl Default for TaskContext {
    fn default() -> Self {
        let runtime = RuntimeEnv::new(RuntimeConfig::new())
            .expect("default runtime created successfully");

        // Create a default task context, mostly useful for testing
        Self {
            session_id: "DEFAULT".to_string(),
            task_id: None,
            session_config: SessionConfig::new(),
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            window_functions: HashMap::new(),
            runtime: Arc::new(runtime),
        }
    }
}

impl TaskContext {
    /// Create a new [`TaskContext`] instance.
    ///
    /// Most users will use [`SessionContext::task_ctx`] to create [`TaskContext`]s
    ///
    /// [`SessionContext::task_ctx`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.task_ctx
    pub fn new(
        task_id: Option<String>,
        session_id: String,
        session_config: SessionConfig,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        window_functions: HashMap<String, Arc<WindowUDF>>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            task_id,
            session_id,
            session_config,
            scalar_functions,
            aggregate_functions,
            window_functions,
            runtime,
        }
    }

    /// Return the SessionConfig associated with this [TaskContext]
    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    /// Return the `session_id` of this [TaskContext]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the `task_id` of this [TaskContext]
    pub fn task_id(&self) -> Option<String> {
        self.task_id.clone()
    }

    /// Return the [`MemoryPool`] associated with this [TaskContext]
    pub fn memory_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.runtime.memory_pool
    }

    /// Return the [RuntimeEnv] associated with this [TaskContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.runtime)
    }

    /// Update the [`SessionConfig`]
    pub fn with_session_config(mut self, session_config: SessionConfig) -> Self {
        self.session_config = session_config;
        self
    }

    /// Update the [`RuntimeEnv`]
    pub fn with_runtime(mut self, runtime: Arc<RuntimeEnv>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl FunctionRegistry for TaskContext {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDF named \"{name}\" in the TaskContext")
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDAF named \"{name}\" in the TaskContext")
        })
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        let result = self.window_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDWF named \"{name}\" in the TaskContext"
            ))
        })
    }
    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> Result<Option<Arc<AggregateUDF>>> {
        udaf.aliases().iter().for_each(|alias| {
            self.aggregate_functions
                .insert(alias.clone(), Arc::clone(&udaf));
        });
        Ok(self.aggregate_functions.insert(udaf.name().into(), udaf))
    }
    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
        udwf.aliases().iter().for_each(|alias| {
            self.window_functions
                .insert(alias.clone(), Arc::clone(&udwf));
        });
        Ok(self.window_functions.insert(udwf.name().into(), udwf))
    }
    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        udf.aliases().iter().for_each(|alias| {
            self.scalar_functions
                .insert(alias.clone(), Arc::clone(&udf));
        });
        Ok(self.scalar_functions.insert(udf.name().into(), udf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{
        config::{ConfigExtension, ConfigOptions, Extensions},
        extensions_options,
    };

    extensions_options! {
        struct TestExtension {
            value: usize, default = 42
        }
    }

    impl ConfigExtension for TestExtension {
        const PREFIX: &'static str = "test";
    }

    #[test]
    fn task_context_extensions() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let mut extensions = Extensions::new();
        extensions.insert(TestExtension::default());

        let mut config = ConfigOptions::new().with_extensions(extensions);
        config.set("test.value", "24")?;
        let session_config = SessionConfig::from(config);

        let task_context = TaskContext::new(
            Some("task_id".to_string()),
            "session_id".to_string(),
            session_config,
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            runtime,
        );

        let test = task_context
            .session_config()
            .options()
            .extensions
            .get::<TestExtension>();
        assert!(test.is_some());

        assert_eq!(test.unwrap().value, 24);

        Ok(())
    }
}
