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
use std::iter::IntoIterator;
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::{concat, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use crate::{window::WindowExpr, AggregateExpr};

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<Arc<WindowFrame>>,
}

impl AggregateWindowExpr {
    /// create a new aggregate window function expression
    pub fn new(
        aggregate: Arc<dyn AggregateExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Option<Arc<WindowFrame>>,
    ) -> Self {
        Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }
}

/// peer based evaluation based on the fact that batch is pre-sorted given the sort columns
/// and then per partition point we'll evaluate the peer group (e.g. SUM or MAX gives the same
/// results for peers) and concatenate the results.

impl WindowExpr for AggregateWindowExpr {
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
        let num_rows = batch.num_rows();
        let partition_points =
            self.evaluate_partition_points(num_rows, &self.partition_columns(batch)?)?;
        let values = self.evaluate_args(batch)?;

        let columns = self.sort_columns(batch)?;
        let order_columns: Vec<&ArrayRef> = columns.iter().map(|s| &s.values).collect();
        // Sort values, this will make the same partitions consecutive. Also, within the partition
        // range, values will be sorted.
        let order_bys =
            &order_columns[self.partition_by.len()..order_columns.len()].to_vec();
        let window_frame = match (&order_bys[..], &self.window_frame) {
            ([column, ..], None) => {
                // OVER (ORDER BY a) case
                // We create an implicit window for ORDER BY.
                let empty_bound = ScalarValue::try_from(column.data_type())?;
                Some(Arc::new(WindowFrame {
                    units: WindowFrameUnits::Range,
                    start_bound: WindowFrameBound::Preceding(empty_bound),
                    end_bound: WindowFrameBound::CurrentRow,
                }))
            }
            _ => self.window_frame.clone(),
        };
        let results = partition_points
            .iter()
            .map(|partition_range| {
                // let mut accumulator = self.aggregate.create_accumulator()?;
                let mut accumulator = self.aggregate.create_accumulator()?;
                // We iterate on each row to perform a running calculation.
                // First, cur_range is calculated, then it is compared with last_range.
                let length = partition_range.end - partition_range.start;
                let slice_order_bys = order_bys
                    .iter()
                    .map(|v| v.slice(partition_range.start, length))
                    .collect::<Vec<_>>();
                let sort_options: Vec<SortOptions> =
                    self.order_by.iter().map(|o| o.options).collect();
                let value_slice = values
                    .iter()
                    .map(|v| v.slice(partition_range.start, length))
                    .collect::<Vec<_>>();

                let mut row_wise_results: Vec<ScalarValue> = vec![];
                let mut last_range: (usize, usize) = (0, 0);

                for i in 0..length {
                    let cur_range = self.calculate_range(
                        &window_frame,
                        &slice_order_bys,
                        &sort_options,
                        length,
                        i,
                    )?;
                    // println!("cur range: {:?}", cur_range);
                    if cur_range.0 == cur_range.1 {
                        // We produce None if the window is empty.
                        row_wise_results.push(ScalarValue::try_from(
                            self.aggregate.field()?.data_type(),
                        )?)
                    } else {
                        // Accumulate any new rows that have entered the window:
                        let update_bound = cur_range.1 - last_range.1;
                        if update_bound > 0 {
                            let update: Vec<ArrayRef> = value_slice
                                .iter()
                                .map(|v| v.slice(last_range.1, update_bound))
                                .collect();
                            accumulator.update_batch(&update)?
                        }
                        // Remove rows that have now left the window:
                        let retract_bound = cur_range.0 - last_range.0;
                        if retract_bound > 0 {
                            let retract: Vec<ArrayRef> = value_slice
                                .iter()
                                .map(|v| v.slice(last_range.0, retract_bound))
                                .collect();
                            accumulator.retract_batch(&retract)?
                        }
                        row_wise_results.push(accumulator.evaluate()?);
                    }
                    last_range = cur_range;
                }
                Ok(vec![ScalarValue::iter_to_array(
                    row_wise_results.into_iter(),
                )?])
            })
            .collect::<Result<Vec<Vec<ArrayRef>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<ArrayRef>>();
        let results = results.iter().map(|i| i.as_ref()).collect::<Vec<_>>();
        concat(&results).map_err(DataFusionError::ArrowError)
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }
}
