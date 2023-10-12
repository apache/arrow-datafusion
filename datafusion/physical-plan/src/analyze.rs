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

//! Defines the ANALYZE operator

use std::sync::Arc;
use std::{any::Any, time::Instant};

use crate::{
    display::DisplayableExecutionPlan, DisplayFormatType, ExecutionPlan, Partitioning,
    Statistics,
};
use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::StreamExt;

use super::expressions::PhysicalSortExpr;
use super::stream::{
    ReceiverStream, RecordBatchReceiverStreamAdaptor, RecordBatchStreamAdapter,
};
use super::{DisplayAs, Distribution, SendableRecordBatchStream};
use datafusion_execution::TaskContext;

/// `EXPLAIN ANALYZE` execution plan operator. This operator runs its input,
/// discards the results, and then prints out an annotated plan with metrics
#[derive(Debug, Clone)]
pub struct AnalyzeExec {
    /// control how much extra to print
    verbose: bool,
    /// if statistics should be displayed
    show_statistics: bool,
    /// The input plan (the plan being analyzed)
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// The output schema for RecordBatches of this exec node
    schema: SchemaRef,
}

impl AnalyzeExec {
    /// Create a new AnalyzeExec
    pub fn new(
        verbose: bool,
        show_statistics: bool,
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Self {
        AnalyzeExec {
            verbose,
            show_statistics,
            input,
            schema,
        }
    }

    /// access to verbose
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    /// access to show_statistics
    pub fn show_statistics(&self) -> bool {
        self.show_statistics
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for AnalyzeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AnalyzeExec verbose={}", self.verbose)
            }
        }
    }
}

impl ExecutionPlan for AnalyzeExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// AnalyzeExec is handled specially so this value is ignored
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        internal_err!("Optimization not supported for ANALYZE")
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.verbose,
            self.show_statistics,
            children.pop().unwrap(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!(
                "AnalyzeExec invalid partition. Expected 0, got {partition}"
            );
        }

        // Gather futures that will run each input partition in
        // parallel (on a separate tokio task) using a JoinSet to
        // cancel outstanding futures on drop
        let num_input_partitions = self.input.output_partitioning().partition_count();
        let mut builder = ReceiverStream::builder(self.schema(), num_input_partitions);
        let input = Arc::new(RecordBatchReceiverStreamAdaptor::new(self.input.clone()));

        for input_partition in 0..num_input_partitions {
            builder.run_input(input.clone(), input_partition, context.clone());
        }

        // Create future that computes thefinal output
        let start = Instant::now();
        let captured_input = self.input.clone();
        let captured_schema = self.schema.clone();
        let verbose = self.verbose;
        let show_statistics = self.show_statistics;

        // future that gathers the results from all the tasks in the
        // JoinSet that computes the overall row count and final
        // record batch
        let mut input_stream = builder.build();
        let output = async move {
            let mut total_rows = 0;
            while let Some(batch) = input_stream.next().await.transpose()? {
                total_rows += batch.num_rows();
            }

            let duration = Instant::now() - start;
            create_output_batch(
                verbose,
                show_statistics,
                total_rows,
                duration,
                captured_input,
                captured_schema,
            )
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(output),
        )))
    }

    fn statistics(&self) -> Statistics {
        // Statistics an an ANALYZE plan are not relevant
        Statistics::default()
    }
}

/// Creates the ouput of AnalyzeExec as a RecordBatch
fn create_output_batch(
    verbose: bool,
    show_statistics: bool,
    total_rows: usize,
    duration: std::time::Duration,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut type_builder = StringBuilder::with_capacity(1, 1024);
    let mut plan_builder = StringBuilder::with_capacity(1, 1024);

    // TODO use some sort of enum rather than strings?
    type_builder.append_value("Plan with Metrics");

    let annotated_plan = DisplayableExecutionPlan::with_metrics(input.as_ref())
        .set_show_statistics(show_statistics)
        .indent(verbose)
        .to_string();
    plan_builder.append_value(annotated_plan);

    // Verbose output
    // TODO make this more sophisticated
    if verbose {
        type_builder.append_value("Plan with Full Metrics");

        let annotated_plan = DisplayableExecutionPlan::with_full_metrics(input.as_ref())
            .set_show_statistics(show_statistics)
            .indent(verbose)
            .to_string();
        plan_builder.append_value(annotated_plan);

        type_builder.append_value("Output Rows");
        plan_builder.append_value(total_rows.to_string());

        type_builder.append_value("Duration");
        plan_builder.append_value(format!("{duration:?}"));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(type_builder.finish()),
            Arc::new(plan_builder.finish()),
        ],
    )
    .map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::FutureExt;

    use crate::{
        collect,
        test::{
            assert_is_pending,
            exec::{assert_strong_count_converges_to_zero, BlockingExec},
        },
    };

    use super::*;

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let analyze_exec = Arc::new(AnalyzeExec::new(true, false, blocking_exec, schema));

        let fut = collect(analyze_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
