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

//! Physical exec for built-in window function expressions.

use crate::error::{DataFusionError, Result};
use crate::logical_plan::window_frames::WindowFrame;
use crate::physical_plan::{
    expressions::PhysicalSortExpr,
    window_functions::{BuiltInWindowFunction, BuiltInWindowFunctionExpr},
    PhysicalExpr, WindowExpr,
};
use arrow::compute::concatenate;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use std::any::Any;
use std::sync::Arc;

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {
    fun: BuiltInWindowFunction,
    expr: Arc<dyn BuiltInWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<WindowFrame>,
}

impl BuiltInWindowExpr {
    /// create a new built-in window function expression
    pub(super) fn new(
        fun: BuiltInWindowFunction,
        expr: Arc<dyn BuiltInWindowFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Option<WindowFrame>,
    ) -> Self {
        Self {
            fun,
            expr,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }
}

impl WindowExpr for BuiltInWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.expr.name()
    }

    fn field(&self) -> Result<Field> {
        self.expr.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr.expressions()
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let evaluator = self.expr.create_evaluator(batch)?;
        let num_rows = batch.num_rows();
        let partition_points =
            self.evaluate_partition_points(num_rows, &self.partition_columns(batch)?)?;
        let results = if evaluator.include_rank() {
            let sort_partition_points =
                self.evaluate_partition_points(num_rows, &self.sort_columns(batch)?)?;
            evaluator.evaluate_with_rank(partition_points, sort_partition_points)?
        } else {
            evaluator.evaluate(partition_points)?
        };
        let results = results.iter().map(|i| i.as_ref()).collect::<Vec<_>>();
        concatenate::concatenate(&results)
            .map(ArrayRef::from)
            .map_err(DataFusionError::ArrowError)
    }
}
