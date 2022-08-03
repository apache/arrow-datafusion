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

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::PhysicalExpr;
use arrow::datatypes::Field;
use datafusion_common::Result;
use datafusion_expr::Accumulator;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

pub(crate) mod approx_distinct;
pub(crate) mod approx_median;
pub(crate) mod approx_percentile_cont;
pub(crate) mod approx_percentile_cont_with_weight;
pub(crate) mod array_agg;
pub(crate) mod array_agg_distinct;
pub(crate) mod average;
pub(crate) mod coercion_rule;
pub(crate) mod correlation;
pub(crate) mod count;
pub(crate) mod count_distinct;
pub(crate) mod covariance;
pub(crate) mod grouping;
pub(crate) mod median;
#[macro_use]
pub(crate) mod min_max;
pub mod build_in;
mod hyperloglog;
pub mod row_accumulator;
pub(crate) mod stats;
pub(crate) mod stddev;
pub(crate) mod sum;
pub(crate) mod sum_distinct;
mod tdigest;
pub mod utils;
pub(crate) mod variance;

/// An aggregate expression that:
/// * knows its resulting field
/// * knows how to create its accumulator
/// * knows its accumulator's state's field
/// * knows the expressions from whose its accumulator will receive values
pub trait AggregateExpr: Send + Sync + Debug {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// the fields that encapsulate the Accumulator's state
    /// the number of fields here equals the number of states that the accumulator contains
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Human readable name such as `"MIN(c2)"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "AggregateExpr: default name"
    }

    /// If the aggregate expression is supported by row format
    fn row_accumulator_supported(&self) -> bool {
        false
    }

    /// RowAccumulator to access/update row-based aggregation state in-place.
    /// Currently, row accumulator only supports states of fixed-sized type.
    ///
    /// We recommend implementing `RowAccumulator` along with the standard `Accumulator`,
    /// when its state is of fixed size, as RowAccumulator is more memory efficient and CPU-friendly.
    fn create_row_accumulator(
        &self,
        _start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        unreachable!()
    }
}
