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

//! CteWorkTable implementation used for recursive queries

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::logical_type::schema::LogicalSchemaRef;
use datafusion_physical_plan::work_table::WorkTableExec;

use crate::{
    error::Result,
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

use crate::datasource::{TableProvider, TableType};
use crate::execution::context::SessionState;

/// The temporary working table where the previous iteration of a recursive query is stored
/// Naming is based on PostgreSQL's implementation.
/// See here for more details: www.postgresql.org/docs/11/queries-with.html#id-1.5.6.12.5.4
pub struct CteWorkTable {
    /// The name of the CTE work table
    // WIP, see https://github.com/apache/datafusion/issues/462
    #[allow(dead_code)]
    name: String,
    /// This schema must be shared across both the static and recursive terms of a recursive query
    table_schema: SchemaRef,
}

impl CteWorkTable {
    /// construct a new CteWorkTable with the given name and schema
    /// This schema must match the schema of the recursive term of the query
    /// Since the scan method will contain an physical plan that assumes this schema
    pub fn new(name: &str, table_schema: SchemaRef) -> Self {
        Self {
            name: name.to_owned(),
            table_schema,
        }
    }
}

#[async_trait]
impl TableProvider for CteWorkTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.table_schema.clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: pushdown filters and limits
        Ok(Arc::new(WorkTableExec::new(
            self.name.clone(),
            self.table_schema.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // TODO: should we support filter pushdown?
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
