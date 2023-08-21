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

use crate::expressions::{FirstValue, LastValue, OrderSensitiveArrayAgg};
use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::datatypes::Field;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use self::groups_accumulator::GroupsAccumulator;

pub(crate) mod approx_distinct;
pub(crate) mod approx_median;
pub(crate) mod approx_percentile_cont;
pub(crate) mod approx_percentile_cont_with_weight;
pub(crate) mod array_agg;
pub(crate) mod array_agg_distinct;
pub(crate) mod array_agg_ordered;
pub(crate) mod average;
pub(crate) mod bit_and_or_xor;
pub(crate) mod bool_and_or;
pub(crate) mod correlation;
pub(crate) mod count;
pub(crate) mod count_distinct;
pub(crate) mod covariance;
pub(crate) mod first_last;
pub(crate) mod grouping;
pub(crate) mod percentile;
#[macro_use]
pub(crate) mod min_max;
pub mod build_in;
pub(crate) mod groups_accumulator;
mod hyperloglog;
pub mod moving_min_max;
pub(crate) mod regr;
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
///
/// Any implementation of this trait also needs to implement the
/// `PartialEq<dyn Any>` to allows comparing equality between the
/// trait objects.
pub trait AggregateExpr: Send + Sync + Debug + PartialEq<dyn Any> {
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

    /// Order by requirements for the aggregate function
    /// By default it is `None` (there is no requirement)
    /// Order-sensitive aggregators, such as `FIRST_VALUE(x ORDER BY y)` should implement this
    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    /// Human readable name such as `"MIN(c2)"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "AggregateExpr: default name"
    }

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator`] will be called.
    fn groups_accumulator_supported(&self) -> bool {
        false
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        Err(DataFusionError::NotImplemented(format!(
            "GroupsAccumulator hasn't been implemented for {self:?} yet"
        )))
    }

    /// Construct an expression that calculates the aggregate in reverse.
    /// Typically the "reverse" expression is itself (e.g. SUM, COUNT).
    /// For aggregates that do not support calculation in reverse,
    /// returns None (which is the default value).
    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        None
    }

    /// Creates accumulator implementation that supports retract
    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Err(DataFusionError::NotImplemented(format!(
            "Retractable Accumulator hasn't been implemented for {self:?} yet"
        )))
    }
}

/// Checks whether the given aggregate expression is order-sensitive.
/// For instance, a `SUM` aggregation doesn't depend on the order of its inputs.
/// However, a `FirstValue` depends on the input ordering (if the order changes,
/// the first value in the list would change).
pub fn is_order_sensitive(aggr_expr: &Arc<dyn AggregateExpr>) -> bool {
    aggr_expr.as_any().is::<FirstValue>()
        || aggr_expr.as_any().is::<LastValue>()
        || aggr_expr.as_any().is::<OrderSensitiveArrayAgg>()
}
