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

//! Utilities used in aggregates

use arrow::array::ArrayRef;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;

/// Extract scalar values from an accumulator. This can return an error if the accumulator
/// has any non-scalar values.
pub fn get_accum_scalar_values(accum: &dyn Accumulator) -> Result<Vec<ScalarValue>> {
    accum
        .state()?
        .iter()
        .map(|agg| agg.as_scalar().map(|v| v.clone()))
        .collect::<Result<Vec<_>>>()
}

/// Convert scalar values from an accumulator into arrays. This can return an error if the
/// accumulator has any non-scalar values.
pub fn get_accum_scalar_values_as_arrays(
    accum: &dyn Accumulator,
) -> Result<Vec<ArrayRef>> {
    accum
        .state()?
        .iter()
        .map(|v| {
            v.as_scalar()
                .map(|s| vec![s.clone()])
                .and_then(ScalarValue::iter_to_array)
        })
        .collect::<Result<Vec<_>>>()
}
