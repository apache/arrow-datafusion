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

//! Stream and channel implementations for window function expressions.

use crate::error::{DataFusionError, Result};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::common::AbortOnDropSingle;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::{
    common, ColumnStatistics, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics, WindowExpr,
};
use crate::record_batch::RecordBatch;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::{Error as ArrowError, Result as ArrowResult},
};
use async_trait::async_trait;
use datafusion_common::field_util::SchemaExt;
use futures::stream::Stream;
use futures::FutureExt;
use pin_project_lite::pin_project;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Window execution plan
#[derive(Debug)]
pub struct WindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Schema before the window
    input_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input_schema, &window_expr)?;
        let schema = Arc::new(schema);
        Ok(Self {
            input,
            window_expr,
            schema,
            input_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any window functions are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

#[async_trait]
impl ExecutionPlan for WindowAggExec {
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

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // because we can have repartitioning using the partition keys
        // this would be either 1 or more than 1 depending on the presense of
        // repartitioning
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> bool {
        true
    }

    fn relies_on_input_order(&self) -> bool {
        true
    }

    fn required_child_distribution(&self) -> Distribution {
        if self
            .window_expr()
            .iter()
            .all(|expr| expr.partition_by().is_empty())
        {
            Distribution::SinglePartition
        } else {
            Distribution::UnspecifiedDistribution
        }
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(WindowAggExec::try_new(
                self.window_expr.clone(),
                children[0].clone(),
                self.input_schema.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "WindowAggExec wrong number of children".to_owned(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, runtime).await?;
        let stream = Box::pin(WindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
        ));
        Ok(stream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "WindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| format!("{}: {:?}", e.name().to_owned(), e.field()))
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
            }
        }
        Ok(())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stat = self.input.statistics();
        let win_cols = self.window_expr.len();
        let input_cols = self.input_schema.fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = vec![ColumnStatistics::default(); win_cols];
        if let Some(input_col_stats) = input_stat.column_statistics {
            column_statistics.extend(input_col_stats);
        } else {
            column_statistics.extend(vec![ColumnStatistics::default(); input_cols]);
        }
        Statistics {
            is_exact: input_stat.is_exact,
            num_rows: input_stat.num_rows,
            column_statistics: Some(column_statistics),
            // TODO stats: knowing the type of the new columns we can guess the output size
            total_byte_size: None,
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + window_expr.len());
    for expr in window_expr {
        fields.push(expr.field()?);
    }
    fields.extend_from_slice(input_schema.fields());
    Ok(Schema::new(fields))
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: Vec<Arc<dyn WindowExpr>>,
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

pin_project! {
    /// stream for window aggregation plan
    pub struct WindowAggStream {
        schema: SchemaRef,
        drop_helper: AbortOnDropSingle<()>,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<RecordBatch>>,
        finished: bool,
        baseline_metrics: BaselineMetrics,
    }
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();
        let schema_clone = schema.clone();
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let join_handle = tokio::spawn(async move {
            let schema = schema_clone.clone();
            let result =
                WindowAggStream::process(input, window_expr, schema, elapsed_compute)
                    .await;

            // failing here is OK, the receiver is gone and does not care about the result
            tx.send(result).ok();
        });

        Self {
            schema,
            drop_helper: AbortOnDropSingle::new(join_handle),
            output: rx,
            finished: false,
            baseline_metrics,
        }
    }

    async fn process(
        input: SendableRecordBatchStream,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        schema: SchemaRef,
        elapsed_compute: crate::physical_plan::metrics::Time,
    ) -> ArrowResult<RecordBatch> {
        let input_schema = input.schema();
        let batches = common::collect(input).await?;

        // record compute time on drop
        let _timer = elapsed_compute.timer();

        let batch = common::combine_batches(&batches, input_schema.clone())?;
        if let Some(batch) = batch {
            // calculate window cols
            let mut columns = compute_window_aggregates(window_expr, &batch)?;
            // combine with the original cols
            // note the setup of window aggregates is that they newly calculated window
            // expressions are always prepended to the columns
            columns.extend_from_slice(batch.columns());
            RecordBatch::try_new(schema, columns)
        } else {
            Ok(RecordBatch::new_empty(schema))
        }
    }
}

impl Stream for WindowAggStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let output_poll = self.output.poll_unpin(cx);

        match output_poll {
            Poll::Ready(result) => {
                self.finished = true;
                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => {
                        Some(Err(ArrowError::External("".to_string(), Box::new(e))))
                    } // error receiving
                    Ok(result) => Some(result),
                };
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
