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

//! A memory-conscious aggregation implementation that limits group buckets to a fixed number

use crate::aggregates::topk::priority_map::PriorityMap;
use crate::aggregates::{
    aggregate_expressions, evaluate_group_by, evaluate_many, AggregateExec,
    PhysicalGroupBy,
};
use crate::{RecordBatchStream, SendableRecordBatchStream};
use arrow::util::pretty::print_batches;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use futures::stream::{Stream, StreamExt};
use log::{trace, Level};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct GroupedTopKAggregateStream {
    partition: usize,
    row_count: usize,
    started: bool,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    group_by: PhysicalGroupBy,
    priority_map: PriorityMap,
}

impl GroupedTopKAggregateStream {
    pub fn new(
        aggr: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
        limit: usize,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&aggr.schema);
        let group_by = aggr.group_by.clone();
        let input = aggr.input.execute(partition, Arc::clone(&context))?;
        let aggregate_arguments =
            aggregate_expressions(&aggr.aggr_expr, &aggr.mode, group_by.expr.len())?;
        let (val_field, desc) = aggr
            .get_minmax_desc()
            .ok_or_else(|| DataFusionError::Internal("Min/max required".to_string()))?;

        let (expr, _) = &aggr.group_expr().expr()[0];
        let kt = expr.data_type(&aggr.input().schema())?;
        let vt = val_field.data_type().clone();

        let priority_map = PriorityMap::new(kt, vt, limit, desc)?;

        Ok(GroupedTopKAggregateStream {
            partition,
            started: false,
            row_count: 0,
            schema: agg_schema,
            input,
            aggregate_arguments,
            group_by,
            priority_map,
        })
    }
}

impl RecordBatchStream for GroupedTopKAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl GroupedTopKAggregateStream {
    fn intern(&mut self, ids: ArrayRef, vals: ArrayRef) -> Result<()> {
        let len = ids.len();
        self.priority_map.set_batch(ids, Arc::clone(&vals));

        let has_nulls = vals.null_count() > 0;
        for row_idx in 0..len {
            if has_nulls && vals.is_null(row_idx) {
                continue;
            }
            self.priority_map.insert(row_idx)?;
        }
        Ok(())
    }
}

impl Stream for GroupedTopKAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(res) = self.input.poll_next_unpin(cx) {
            match res {
                // got a batch, convert to rows and append to our TreeMap
                Some(Ok(batch)) => {
                    self.started = true;
                    trace!(
                        "partition {} has {} rows and got batch with {} rows",
                        self.partition,
                        self.row_count,
                        batch.num_rows()
                    );
                    if log::log_enabled!(Level::Trace) && batch.num_rows() < 20 {
                        print_batches(&[batch.clone()])?;
                    }
                    self.row_count += batch.num_rows();
                    let batches = &[batch];
                    let group_by_values =
                        evaluate_group_by(&self.group_by, batches.first().unwrap())?;
                    assert_eq!(
                        group_by_values.len(),
                        1,
                        "Exactly 1 group value required"
                    );
                    assert_eq!(
                        group_by_values[0].len(),
                        1,
                        "Exactly 1 group value required"
                    );
                    let group_by_values = Arc::clone(&group_by_values[0][0]);
                    let input_values = evaluate_many(
                        &self.aggregate_arguments,
                        batches.first().unwrap(),
                    )?;
                    assert_eq!(input_values.len(), 1, "Exactly 1 input required");
                    assert_eq!(input_values[0].len(), 1, "Exactly 1 input required");
                    let input_values = Arc::clone(&input_values[0][0]);

                    // iterate over each column of group_by values
                    (*self).intern(group_by_values, input_values)?;
                }
                // inner is done, emit all rows and switch to producing output
                None => {
                    if self.priority_map.is_empty() {
                        trace!("partition {} emit None", self.partition);
                        return Poll::Ready(None);
                    }
                    let cols = self.priority_map.emit()?;
                    let batch = RecordBatch::try_new(Arc::clone(&self.schema), cols)?;
                    trace!(
                        "partition {} emit batch with {} rows",
                        self.partition,
                        batch.num_rows()
                    );
                    if log::log_enabled!(Level::Trace) {
                        print_batches(&[batch.clone()])?;
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                // inner had error, return to caller
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
        Poll::Pending
    }
}
