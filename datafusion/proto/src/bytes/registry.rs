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

use std::{collections::HashSet, sync::Arc};

use datafusion_common::plan_err;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::registry::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};

/// A default [`FunctionRegistry`] registry that does not resolve any
/// user defined functions
pub(crate) struct NoRegistry {}

impl FunctionRegistry for NoRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Function '{name}'")
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Aggregate Function '{name}'")
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        plan_err!("No function registry provided to deserialize, so can not deserialize User Defined Window Function '{name}'")
    }
}
