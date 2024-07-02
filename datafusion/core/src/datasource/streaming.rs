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

//! A simplified [`TableProvider`] for streaming partitioned datasets

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use datafusion_common::{plan_err, Result};
use datafusion_expr::{Expr, TableType};
use log::debug;
use datafusion_common::logical_type::schema::LogicalSchemaRef;
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;
use crate::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use crate::physical_plan::ExecutionPlan;

/// A [`TableProvider`] that streams a set of [`PartitionStream`]
pub struct StreamingTable {
    schema: SchemaRef,
    partitions: Vec<Arc<dyn PartitionStream>>,
    infinite: bool,
}

impl StreamingTable {
    /// Try to create a new [`StreamingTable`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStream>>,
    ) -> Result<Self> {
        for x in partitions.iter() {
            let partition_schema = x.schema();
            if !schema.contains(partition_schema) {
                debug!(
                    "target schema does not contain partition schema. \
                        Target_schema: {schema:?}. Partiton Schema: {partition_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        Ok(Self {
            schema,
            partitions,
            infinite: false,
        })
    }
    /// Sets streaming table can be infinite.
    pub fn with_infinite_table(mut self, infinite: bool) -> Self {
        self.infinite = infinite;
        self
    }
}

#[async_trait]
impl TableProvider for StreamingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.schema.clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            self.partitions.clone(),
            projection,
            None,
            self.infinite,
            limit,
        )?))
    }
}
