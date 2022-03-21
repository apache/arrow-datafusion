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

//! Module containing helper methods for the various file formats
pub mod arrow_file;
pub mod avro;
pub mod csv;
pub mod json;
pub mod parquet;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::FileScanConfig;
use crate::physical_plan::{ExecutionPlan, Statistics};

use async_trait::async_trait;

use super::object_store::{ObjectReader, ObjectReaderStream};

/// This trait abstracts all the file format specific implementations
/// from the `TableProvider`. This helps code re-utilization accross
/// providers that support the the same file formats.
#[async_trait]
pub trait FileFormat: Send + Sync + fmt::Debug {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(&self, readers: ObjectReaderStream) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    async fn infer_stats(&self, reader: Arc<dyn ObjectReader>) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
