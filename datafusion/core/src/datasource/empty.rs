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

//! [`EmptyTable`] useful for testing.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::*;
use async_trait::async_trait;
use datafusion_common::logical_type::schema::LogicalSchemaRef;
use datafusion_common::project_schema;

use crate::datasource::{TableProvider, TableType};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::logical_expr::Expr;
use crate::physical_plan::{empty::EmptyExec, ExecutionPlan};

/// An empty plan that is useful for testing and generating plans
/// without mapping them to actual data.
pub struct EmptyTable {
    schema: SchemaRef,
    partitions: usize,
}

impl EmptyTable {
    /// Initialize a new `EmptyTable` from a schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            partitions: 1,
        }
    }

    /// Creates a new EmptyTable with specified partition number.
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }
}

#[async_trait]
impl TableProvider for EmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.schema.clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // even though there is no data, projections apply
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(
            EmptyExec::new(projected_schema).with_partitions(self.partitions),
        ))
    }
}
