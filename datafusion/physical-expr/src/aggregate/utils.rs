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

use crate::expressions::Literal;
use crate::{AggregateExpr, PhysicalExpr, PhysicalSortExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION};
use arrow_array::cast::AsArray;
use arrow_array::types::Decimal128Type;
use arrow_schema::{DataType, Field};
use datafusion_common::{internal_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// Convert scalar values from an accumulator into arrays.
pub fn get_accum_scalar_values_as_arrays(
    accum: &dyn Accumulator,
) -> Result<Vec<ArrayRef>> {
    Ok(accum
        .state()?
        .iter()
        .map(|s| s.to_array_of_size(1))
        .collect::<Vec<_>>())
}

/// Computes averages for `Decimal128` values, checking for overflow
///
/// This is needed because different precisions for Decimal128 can
/// store different ranges of values and thus sum/count may not fit in
/// the target type.
///
/// For example, the precision is 3, the max of value is `999` and the min
/// value is `-999`
pub(crate) struct Decimal128Averager {
    /// scale factor for sum values (10^sum_scale)
    sum_mul: i128,
    /// scale factor for target (10^target_scale)
    target_mul: i128,
    /// The minimum output value possible to represent with the target precision
    target_min: i128,
    /// The maximum output value possible to represent with the target precision
    target_max: i128,
}

impl Decimal128Averager {
    /// Create a new `Decimal128Averager`:
    ///
    /// * sum_scale: the scale of `sum` values passed to [`Self::avg`]
    /// * target_precision: the output precision
    /// * target_scale: the output scale
    ///
    /// Errors if the resulting data can not be stored
    pub fn try_new(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Result<Self> {
        let sum_mul = 10_i128.pow(sum_scale as u32);
        let target_mul = 10_i128.pow(target_scale as u32);
        let target_min = MIN_DECIMAL_FOR_EACH_PRECISION[target_precision as usize - 1];
        let target_max = MAX_DECIMAL_FOR_EACH_PRECISION[target_precision as usize - 1];

        if target_mul >= sum_mul {
            Ok(Self {
                sum_mul,
                target_mul,
                target_min,
                target_max,
            })
        } else {
            // can't convert the lit decimal to the returned data type
            Err(DataFusionError::Execution(
                "Arithmetic Overflow in AvgAccumulator".to_string(),
            ))
        }
    }

    /// Returns the `sum`/`count` as a i128 Decimal128 with
    /// target_scale and target_precision and reporting overflow.
    ///
    /// * sum: The total sum value stored as Decimal128 with sum_scale
    /// (passed to `Self::try_new`)
    /// * count: total count, stored as a i128 (*NOT* a Decimal128 value)
    #[inline(always)]
    pub fn avg(&self, sum: i128, count: i128) -> Result<i128> {
        if let Some(value) = sum.checked_mul(self.target_mul / self.sum_mul) {
            let new_value = value / count;
            if new_value >= self.target_min && new_value <= self.target_max {
                Ok(new_value)
            } else {
                Err(DataFusionError::Execution(
                    "Arithmetic Overflow in AvgAccumulator".to_string(),
                ))
            }
        } else {
            // can't convert the lit decimal to the returned data type
            Err(DataFusionError::Execution(
                "Arithmetic Overflow in AvgAccumulator".to_string(),
            ))
        }
    }
}

/// Returns `sum`/`count` for decimal values, detecting and reporting overflow.
///
/// * sum:  stored as Decimal128 with `sum_scale` scale
/// * count: stored as a i128 (*NOT* a Decimal128 value)
/// * sum_scale: the scale of `sum`
/// * target_type: the output decimal type
pub fn calculate_result_decimal_for_avg(
    sum: i128,
    count: i128,
    sum_scale: i8,
    target_type: &DataType,
) -> Result<ScalarValue> {
    match target_type {
        DataType::Decimal128(target_precision, target_scale) => {
            let new_value =
                Decimal128Averager::try_new(sum_scale, *target_precision, *target_scale)?
                    .avg(sum, count)?;

            Ok(ScalarValue::Decimal128(
                Some(new_value),
                *target_precision,
                *target_scale,
            ))
        }
        other => internal_err!("Invalid target type in AvgAccumulator {other:?}"),
    }
}

/// Adjust array type metadata if needed
///
/// Since `Decimal128Arrays` created from `Vec<NativeType>` have
/// default precision and scale, this function adjusts the output to
/// match `data_type`, if necessary
pub fn adjust_output_array(
    data_type: &DataType,
    array: ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let array = match data_type {
        DataType::Decimal128(p, s) => Arc::new(
            array
                .as_primitive::<Decimal128Type>()
                .clone()
                .with_precision_and_scale(*p, *s)?,
        ),
        // no adjustment needed for other arrays
        _ => array,
    };
    Ok(array)
}

/// Downcast a `Box<dyn AggregateExpr>` or `Arc<dyn AggregateExpr>`
/// and return the inner trait object as [`Any`](std::any::Any) so
/// that it can be downcast to a specific implementation.
///
/// This method is used when implementing the `PartialEq<dyn Any>`
/// for [`AggregateExpr`] aggregation expressions and allows comparing the equality
/// between the trait objects.
pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn AggregateExpr>>() {
        any.downcast_ref::<Arc<dyn AggregateExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn AggregateExpr>>() {
        any.downcast_ref::<Box<dyn AggregateExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

/// Construct corresponding fields for lexicographical ordering requirement expression
pub(crate) fn ordering_fields(
    ordering_req: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<Field> {
    ordering_req
        .iter()
        .zip(data_types.iter())
        .map(|(expr, dtype)| {
            Field::new(
                expr.to_string().as_str(),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            )
        })
        .collect()
}

/// parse and validate percentile scores like 0.5 in `quantile_cont(data, 0.5);`
pub fn validate_input_percentile_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    // Extract the desired percentile literal
    let lit = expr
        .as_any()
        .downcast_ref::<Literal>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "desired percentile argument must be float literal".to_string(),
            )
        })?
        .value();
    let percentile = match lit {
        ScalarValue::Float32(Some(q)) => *q as f64,
        ScalarValue::Float64(Some(q)) => *q,
        ScalarValue::Int64(Some(q)) => *q as f64, // to support 0 or 1
        got => {
            return Err(DataFusionError::NotImplemented(format!(
                "Percentile value must be Float32 or Float64 literal (got data type {})",
                got.get_datatype()
            )))
        }
    };

    // Ensure the percentile is between 0 and 1.
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}
