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

//! Physical exec for aggregate window function expressions.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Accumulator, WindowFrame};

use crate::window::window_expr::AggregateWindowExpr;
use crate::window::{
    PartitionBatches, PartitionWindowAggStates, PlainAggregateWindowExpr, WindowExpr,
};
use crate::{
    expressions::PhysicalSortExpr, reverse_order_bys, AggregateExpr, PhysicalExpr,
};

/// A window expr that takes the form of an aggregate function that
/// can be incrementally computed over sliding windows.
///
/// See comments on [`WindowExpr`] for more details.
#[derive(Debug)]
pub struct SlidingAggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
}

impl SlidingAggregateWindowExpr {
    /// Create a new (sliding) aggregate window function expression.
    pub fn new(
        aggregate: Arc<dyn AggregateExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Arc<WindowFrame>,
    ) -> Self {
        Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }

    /// Get the [AggregateExpr] of this object.
    pub fn get_aggregate_expr(&self) -> &Arc<dyn AggregateExpr> {
        &self.aggregate
    }
}

/// Incrementally update window function using the fact that batch is
/// pre-sorted given the sort columns and then per partition point.
///
/// Evaluates the peer group (e.g. `SUM` or `MAX` gives the same results
/// for peers) and concatenate the results.
impl WindowExpr for SlidingAggregateWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        self.aggregate.field()
    }

    fn name(&self) -> &str {
        self.aggregate.name()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.aggregate.expressions()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.aggregate_evaluate(batch)
    }

    fn evaluate_stateful(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        self.aggregate_evaluate_stateful(partition_batches, window_agg_state)
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn get_window_frame(&self) -> &Arc<WindowFrame> {
        &self.window_frame
    }

    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>> {
        self.aggregate.reverse_expr().map(|reverse_expr| {
            let reverse_window_frame = self.window_frame.reverse();
            if reverse_window_frame.start_bound.is_unbounded() {
                Arc::new(PlainAggregateWindowExpr::new(
                    reverse_expr,
                    &self.partition_by.clone(),
                    &reverse_order_bys(&self.order_by),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            } else {
                Arc::new(SlidingAggregateWindowExpr::new(
                    reverse_expr,
                    &self.partition_by.clone(),
                    &reverse_order_bys(&self.order_by),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            }
        })
    }

    fn uses_bounded_memory(&self) -> bool {
        !self.window_frame.end_bound.is_unbounded()
    }

    fn with_new_expressions(
        &self,
        args: Vec<Arc<dyn PhysicalExpr>>,
        partition_bys: Vec<Arc<dyn PhysicalExpr>>,
        order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn WindowExpr>> {
        debug_assert_eq!(self.order_by.len(), order_by_exprs.len());

        let new_order_by = self
            .order_by
            .iter()
            .zip(order_by_exprs)
            .map(|(req, new_expr)| PhysicalSortExpr {
                expr: new_expr,
                options: req.options,
            })
            .collect::<Vec<_>>();
        Some(Arc::new(SlidingAggregateWindowExpr {
            aggregate: self.aggregate.with_new_expressions(args, vec![])?,
            partition_by: partition_bys,
            order_by: new_order_by,
            window_frame: Arc::clone(&self.window_frame),
        }))
    }
}

impl AggregateWindowExpr for SlidingAggregateWindowExpr {
    fn get_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.aggregate.create_sliding_accumulator()
    }

    /// Given current range and the last range, calculates the accumulator
    /// result for the range of interest.
    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
    ) -> Result<ScalarValue> {
        if cur_range.start == cur_range.end {
            // We produce None if the window is empty.
            ScalarValue::try_from(self.aggregate.field()?.data_type())
        } else {
            // Accumulate any new rows that have entered the window:
            let update_bound = cur_range.end - last_range.end;
            if update_bound > 0 {
                let update: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.end, update_bound))
                    .collect();
                accumulator.update_batch(&update)?
            }
            // Remove rows that have now left the window:
            let retract_bound = cur_range.start - last_range.start;
            if retract_bound > 0 {
                let retract: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.start, retract_bound))
                    .collect();
                accumulator.retract_batch(&retract)?
            }
            accumulator.evaluate()
        }
    }
}
