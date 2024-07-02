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

//! EmptyRelation produce_one_row=true execution plan

use std::any::Any;
use std::sync::Arc;

use super::{
    common, DisplayAs, ExecutionMode, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use crate::{memory::MemoryStream, DisplayFormatType, ExecutionPlan, Partitioning};

use arrow::array::{ArrayRef, NullArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::RecordBatchOptions;
use datafusion_common::{internal_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use log::trace;

/// Execution plan for empty relation with produce_one_row=true
#[derive(Debug)]
pub struct PlaceholderRowExec {
    /// The schema for the produced row
    schema: SchemaRef,
    /// Number of partitions
    partitions: usize,
    cache: PlanProperties,
}

impl PlaceholderRowExec {
    /// Create a new PlaceholderRowExec
    pub fn new(schema: SchemaRef) -> Self {
        let partitions = 1;
        let cache = Self::compute_properties(Arc::clone(&schema), partitions);
        PlaceholderRowExec {
            schema,
            partitions,
            cache,
        }
    }

    /// Create a new PlaceholderRowExecPlaceholderRowExec with specified partition number
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        // Update output partitioning when updating partitions:
        let output_partitioning = Self::output_partitioning_helper(self.partitions);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
    }

    fn data(&self) -> Result<Vec<RecordBatch>> {
        Ok({
            let n_field = self.schema.fields.len();
            vec![RecordBatch::try_new_with_options(
                Arc::new(Schema::new(
                    (0..n_field)
                        .map(|i| {
                            Field::new(format!("placeholder_{i}"), DataType::Null, true)
                        })
                        .collect::<Fields>(),
                )),
                (0..n_field)
                    .map(|_i| {
                        let ret: ArrayRef = Arc::new(NullArray::new(1));
                        ret
                    })
                    .collect(),
                // Even if column number is empty we can generate single row.
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?]
        })
    }

    fn output_partitioning_helper(n_partitions: usize) -> Partitioning {
        Partitioning::UnknownPartitioning(n_partitions)
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef, n_partitions: usize) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        // Get output partitioning:
        let output_partitioning = Self::output_partitioning_helper(n_partitions);

        PlanProperties::new(eq_properties, output_partitioning, ExecutionMode::Bounded)
    }
}

impl DisplayAs for PlaceholderRowExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PlaceholderRowExec")
            }
        }
    }
}

impl ExecutionPlan for PlaceholderRowExec {
    fn name(&self) -> &'static str {
        "PlaceholderRowExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start PlaceholderRowExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        if partition >= self.partitions {
            return internal_err!(
                "PlaceholderRowExec invalid partition {} (expected less than {})",
                partition,
                self.partitions
            );
        }

        Ok(Box::pin(MemoryStream::try_new(
            self.data()?,
            Arc::clone(&self.schema),
            None,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        let batch = self
            .data()
            .expect("Create single row placeholder RecordBatch should not fail");
        Ok(common::compute_record_batch_statistics(
            &[batch],
            &self.schema,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test, with_new_children_if_necessary};

    #[test]
    fn with_new_children() -> Result<()> {
        let schema = test::aggr_test_schema();

        let placeholder = Arc::new(PlaceholderRowExec::new(schema));

        let placeholder_2 = with_new_children_if_necessary(
            Arc::clone(&placeholder) as Arc<dyn ExecutionPlan>,
            vec![],
        )?;
        assert_eq!(placeholder.schema(), placeholder_2.schema());

        let too_many_kids = vec![placeholder_2];
        assert!(
            with_new_children_if_necessary(placeholder, too_many_kids).is_err(),
            "expected error when providing list of kids"
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_execute() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let placeholder = PlaceholderRowExec::new(schema);

        // ask for the wrong partition
        assert!(placeholder.execute(1, Arc::clone(&task_ctx)).is_err());
        assert!(placeholder.execute(20, task_ctx).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn produce_one_row() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let placeholder = PlaceholderRowExec::new(schema);

        let iter = placeholder.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;

        // should have one item
        assert_eq!(batches.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn produce_one_row_multiple_partition() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test::aggr_test_schema();
        let partitions = 3;
        let placeholder = PlaceholderRowExec::new(schema).with_partitions(partitions);

        for n in 0..partitions {
            let iter = placeholder.execute(n, Arc::clone(&task_ctx))?;
            let batches = common::collect(iter).await?;

            // should have one item
            assert_eq!(batches.len(), 1);
        }

        Ok(())
    }
}
