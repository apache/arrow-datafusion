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

//! Execution plan for reading Arrow files

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::{
    error::{DataFusionError, Result},
    physical_plan::{
        file_format::FileScanConfig,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use datafusion_physical_expr::PhysicalSortExpr;
use log::debug;

use fmt::Debug;

use crate::execution::runtime_env::RuntimeEnv;
use async_trait::async_trait;

use super::file_stream::{BatchIter, FileStream};

/// Execution plan for scanning one or more Arrow partitions
#[derive(Debug, Clone)]
pub struct ArrowExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

/// Stores metrics about the arrow execution for a particular arrow file
#[derive(Debug, Clone)]
struct ArrowFileMetrics {}

impl ArrowExec {
    /// Create a new Arrow reader execution plan provided file list and schema.
    /// Even if `limit` is set, Arrow rounds up the number of records to the next `batch_size`.
    pub fn new(base_config: FileScanConfig) -> Self {
        debug!(
            "Creating ArrowExec, files: {:?}, projection {:?}, limit: {:?}",
            base_config.file_groups, base_config.projection, base_config.limit
        );

        let metrics = ExecutionPlanMetricsSet::new();

        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

#[async_trait]
impl ExecutionPlan for ArrowExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(
        &self,
        partition_index: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let fun = move |file, _remaining: &Option<usize>| {
            let arrow_reader = arrow::ipc::reader::FileReader::try_new(file);

            match arrow_reader {
                Ok(r) => Box::new(r) as BatchIter,
                Err(e) => Box::new(
                    vec![Err(ArrowError::ExternalError(Box::new(e)))].into_iter(),
                ),
            }
        };

        Ok(Box::pin(FileStream::new(
            Arc::clone(&self.base_config.object_store),
            self.base_config.file_groups[partition_index].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.base_config.limit,
            self.base_config.table_partition_cols.clone(),
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ArrowExec: limit={:?}, partitions={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups)
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

#[cfg(test)]
mod tests {}
