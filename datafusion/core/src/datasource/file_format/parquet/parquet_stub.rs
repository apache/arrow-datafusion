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

use crate::datasource::file_format::FileFormat;
use crate::datasource::physical_plan::FileScanConfig;
use crate::execution::context::SessionState;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::{
    not_impl_datafusion_err, DataFusionError, FileType, Result, Statistics,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};
use std::any::Any;
use std::sync::Arc;

/// Stub implementation of `ParquetFormat` that always returns a NotYetImplemented error.
#[derive(Debug, Default)]
pub struct ParquetFormat;

impl ParquetFormat {
    /// Create a new instance of the Parquet format
    pub fn new() -> Self {
        Self
    }
}

fn nyi_error() -> DataFusionError {
    not_impl_datafusion_err!(
        "Parquet support is not enabled. Hint enable the `parquet` feature flag"
    )
}

#[async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _: &SessionState,
        _: &Arc<dyn ObjectStore>,
        _: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        Err(nyi_error())
    }

    async fn infer_stats(
        &self,
        _: &SessionState,
        _: &Arc<dyn ObjectStore>,
        _: SchemaRef,
        _: &ObjectMeta,
    ) -> Result<Statistics> {
        Err(nyi_error())
    }

    async fn create_physical_plan(
        &self,
        _: &SessionState,
        _: FileScanConfig,
        _: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(nyi_error())
    }

    fn file_type(&self) -> FileType {
        FileType::PARQUET
    }
}
