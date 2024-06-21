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

pub use datafusion_physical_expr_common::aggregate::AggregateExpr;

pub(crate) mod array_agg;
pub(crate) mod array_agg_distinct;
pub(crate) mod array_agg_ordered;
pub(crate) mod average;
pub(crate) mod correlation;
pub(crate) mod covariance;
pub(crate) mod grouping;
pub(crate) mod nth_value;
#[macro_use]
pub(crate) mod min_max;
pub(crate) mod groups_accumulator;
pub(crate) mod stats;
pub(crate) mod stddev;
pub(crate) mod variance;

pub mod build_in;
pub mod moving_min_max;
pub mod utils {
    pub use datafusion_physical_expr_common::aggregate::utils::{
        adjust_output_array, down_cast_any_ref, get_accum_scalar_values_as_arrays,
        get_sort_options, ordering_fields, DecimalAverager, Hashable,
    };
}
