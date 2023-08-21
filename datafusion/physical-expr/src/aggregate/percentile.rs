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

//! # Percentile

use crate::aggregate::utils::{down_cast_any_ref, validate_input_percentile_expr};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::sort_to_indices;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

#[derive(PartialEq, Debug, Clone, Copy)]
/// Enum representing if interpolation is used for the percentile aggregate expression.
pub enum PercentileInterpolationType {
    /// Interpolates between adjacent values if the desired percentile lies between them.
    Continuous,
    /// Always returns an actual data point from the dataset.
    Discrete,
}

/// QUANTILE_CONT/QUANTILE_DISC expression
///
/// This uses a lot of memory because all values need to be
/// stored in memory before a result can be computed. If an approximation is sufficient
/// then APPROX_PERCENTILE_CONT provides a much more efficient solution.
#[derive(Debug)]
pub struct Quantile {
    name: String,
    quantile_type: PercentileInterpolationType,
    expr_value: Arc<dyn PhysicalExpr>,
    percentile_score: f64,
    data_type: DataType,
}

impl Quantile {
    pub fn new(
        name: impl Into<String>,
        quantile_type: PercentileInterpolationType,
        expr_value: Arc<dyn PhysicalExpr>,
        expr_percentile_score: Arc<dyn PhysicalExpr>,
        data_type: DataType,
    ) -> Result<Self> {
        let percentile_score = validate_input_percentile_expr(&expr_percentile_score)?;

        Ok(Self {
            name: name.into(),
            quantile_type,
            expr_value,
            percentile_score,
            data_type,
        })
    }
}

impl AggregateExpr for Quantile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileAccumulator {
            percentile_score: self.percentile_score,
            interpolation_type: self.quantile_type,
            data_type: self.data_type.clone(),
            arrays: vec![],
            all_values: vec![],
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        //Intermediate state is a list of the elements we have collected so far
        let field = Field::new("item", self.data_type.clone(), true);
        let data_type = DataType::List(Arc::new(field));

        Ok(vec![Field::new(
            format_state_name(&self.name, "median"),
            data_type,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr_value.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Quantile {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr_value.eq(&x.expr_value)
                    && self.quantile_type == x.quantile_type
                    && self.percentile_score == x.percentile_score
            })
            .unwrap_or(false)
    }
}

/// MEDIAN aggregate expression.
/// MEDIAN(x) is equivalent to QUANTILE_CONT(x, 0.5)
///
/// This uses a lot of memory because all values need to be
/// stored in memory before a result can be computed. If an approximation is sufficient
/// then APPROX_MEDIAN provides a much more efficient solution.
#[derive(Debug)]
pub struct Median {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl Median {
    /// Create a new MEDIAN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
        }
    }
}

impl AggregateExpr for Median {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileAccumulator {
            percentile_score: 0.5,
            interpolation_type: PercentileInterpolationType::Continuous,
            data_type: self.data_type.clone(),
            arrays: vec![],
            all_values: vec![],
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        //Intermediate state is a list of the elements we have collected so far
        let field = Field::new("item", self.data_type.clone(), true);
        let data_type = DataType::List(Arc::new(field));

        Ok(vec![Field::new(
            format_state_name(&self.name, "median"),
            data_type,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Median {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
/// The accumulator for median/quantile_cont/quantile/disc aggregate functions
/// It accumulates the raw input values as `ScalarValue`s
///
/// The intermediate state is represented as a List of scalar values updated by
/// `merge_batch` and a `Vec` of `ArrayRef` that are converted to scalar values
/// in the final evaluation step so that we avoid expensive conversions and
/// allocations during `update_batch`.
struct PercentileAccumulator {
    percentile_score: f64,
    interpolation_type: PercentileInterpolationType,
    data_type: DataType,
    arrays: Vec<ArrayRef>,
    all_values: Vec<ScalarValue>,
}

macro_rules! safe_average {
    (f32, $v1:expr, $v2:expr) => {
        $v1 / 2.0 + $v2 / 2.0
    };
    ( f64, $v1:expr, $v2:expr) => {
        $v1 / 2.0 + $v2 / 2.0
    };
    ($val_type:ty, $v1:expr, $v2:expr) => {
        match $v1.checked_add($v2) {
            Some(sum) => sum / (2 as $val_type),
            None => $v1 / (2 as $val_type) + $v2 / (2 as $val_type),
        }
    };
}

// Example: `target_percentile` is 0.12 and it's landed between dp1 and dp2
// dp1 has percentile 0.10 and value 0
// dp2 has percentile 0.20 and value 100
// `quantile_cont()` do linear interpolation:
//      Then interpolation result = 0 + (0.12 - 0.10) / (0.20 - 0.10) * (100 - 0)
//                                = 20
// `quantile_disc()` choose the closer dp (pick one with lower percentile if equally close)
//      `target_percentile` is closer to dp1's percentile, result = 0
macro_rules! interpolate_logic {
    ($data_type:ident, $val_type:ident, $dp1_val:expr, $dp2_val:expr, $dp1_percentile:expr, $dp2_percentile:expr, $target_percentile:expr, $interpolation_type: expr) => {{
        if $dp1_percentile == $target_percentile {
            ScalarValue::$data_type(Some($dp1_val))
        } else {
            match $interpolation_type {
                PercentileInterpolationType::Continuous => {
                    let (v1, v2) = ($dp1_val as $val_type, $dp2_val as $val_type);
                    let result = if $target_percentile == 0.5 {
                        // HACK: special-case median()
                        // float arithmetic for interpolation (in else branch) might get very lossy for
                        // $val_type like i8
                        safe_average!($val_type, v1, v2)
                    } else {
                        v1 + (($target_percentile - $dp1_percentile)
                            / ($dp2_percentile - $dp1_percentile)
                            * ((v2 - v1) as f64)) as $val_type
                    };
                    ScalarValue::$data_type(Some(result as $val_type))
                },
                PercentileInterpolationType::Discrete => {
                    let dp1_to_target = $target_percentile - $dp1_percentile;
                    let target_to_dp2 = $dp2_percentile - $target_percentile;
                    if dp1_to_target <= target_to_dp2 {
                        ScalarValue::$data_type(Some($dp1_val))
                    } else {
                        ScalarValue::$data_type(Some($dp2_val))
                    }
                }
            }
        }
    }};
}

impl Accumulator for PercentileAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let all_values = to_scalar_values(&self.arrays)?;
        let state = ScalarValue::new_list(Some(all_values), self.data_type.clone());

        Ok(vec![state])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1);
        let array = &values[0];

        // Defer conversions to scalar values to final evaluation.
        assert_eq!(array.data_type(), &self.data_type);
        self.arrays.push(array.clone());

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(states.len(), 1);

        let array = &states[0];
        assert!(matches!(array.data_type(), DataType::List(_)));
        for index in 0..array.len() {
            match ScalarValue::try_from_array(array, index)? {
                ScalarValue::List(Some(mut values), _) => {
                    self.all_values.append(&mut values);
                }
                ScalarValue::List(None, _) => {} // skip empty state
                v => {
                    return internal_err!(
                        "unexpected state in median. Expected DataType::List, got {v:?}"
                    )
                }
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let batch_values = to_scalar_values(&self.arrays)?;

        if !self
            .all_values
            .iter()
            .chain(batch_values.iter())
            .any(|v| !v.is_null())
        {
            return ScalarValue::try_from(&self.data_type);
        }

        // Create an array of all the non null values and find the
        // sorted indexes
        let array = ScalarValue::iter_to_array(
            self.all_values
                .iter()
                .chain(batch_values.iter())
                // ignore null values
                .filter(|v| !v.is_null())
                .cloned(),
        )?;

        let len = array.len();
        if len == 1 {
            return ScalarValue::try_from_array(&array, 0);
        }

        // Suppose target percentile score land between dp1 and dp2 in the sorted array
        // self.percentile_score is in [dp1_percentile, dp2_percentile)
        let dp1_index = (self.percentile_score * (len as f64 - 1_f64)) as usize;
        let dp2_index = dp1_index + 1;
        let percentile_per_gap = 1_f64 / ((len - 1) as f64);
        let (dp1_percentile, dp2_percentile) = (
            dp1_index as f64 * percentile_per_gap,
            dp2_index as f64 * percentile_per_gap,
        );

        // only sort up to the top len * self.percentile_score elements
        let limit = Some(dp1_index + 2);
        let options = None;
        let indices = sort_to_indices(&array, options, limit)?;

        if self.percentile_score == 1.0 {
            return scalar_at_index(&array, &indices, dp1_index);
        }

        // pick the relevant indices in the original arrays
        let result = {
            let s1 = scalar_at_index(&array, &indices, dp1_index)?;
            let s2 = scalar_at_index(&array, &indices, dp2_index)?;
            match (s1, s2) {
                (ScalarValue::Int8(Some(dp1_val)), ScalarValue::Int8(Some(dp2_val))) => {
                    interpolate_logic!(
                        Int8,
                        i8,
                        dp1_val,
                        dp2_val,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }
                (
                    ScalarValue::Int16(Some(dp1_val)),
                    ScalarValue::Int16(Some(dp2_val)),
                ) => {
                    interpolate_logic!(
                        Int16,
                        i16,
                        dp1_val,
                        dp2_val,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }
                (ScalarValue::Int32(Some(v1)), ScalarValue::Int32(Some(v2))) => {
                    interpolate_logic!(
                        Int32,
                        i32,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }
                (ScalarValue::Int64(Some(v1)), ScalarValue::Int64(Some(v2))) => {
                    interpolate_logic!(
                        Int64,
                        i64,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (ScalarValue::UInt8(Some(v1)), ScalarValue::UInt8(Some(v2))) => {
                    interpolate_logic!(
                        UInt8,
                        u8,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (ScalarValue::UInt16(Some(v1)), ScalarValue::UInt16(Some(v2))) => {
                    interpolate_logic!(
                        UInt16,
                        u16,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (ScalarValue::UInt32(Some(v1)), ScalarValue::UInt32(Some(v2))) => {
                    interpolate_logic!(
                        UInt32,
                        u32,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (ScalarValue::UInt64(Some(v1)), ScalarValue::UInt64(Some(v2))) => {
                    interpolate_logic!(
                        UInt64,
                        u64,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (ScalarValue::Float32(Some(v1)), ScalarValue::Float32(Some(v2))) => {
                    interpolate_logic!(
                        Float32,
                        f32,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (ScalarValue::Float64(Some(v1)), ScalarValue::Float64(Some(v2))) => {
                    interpolate_logic!(
                        Float64,
                        f64,
                        v1,
                        v2,
                        dp1_percentile,
                        dp2_percentile,
                        self.percentile_score,
                        self.interpolation_type
                    )
                }

                (
                    s1 @ ScalarValue::Decimal128(_, _, _),
                    s2 @ ScalarValue::Decimal128(_, _, _),
                ) => {
                    // HACK: Decimal is now only supported in median() aggregate function
                    let is_median = self.percentile_score == 0.5
                        && self.interpolation_type
                            == PercentileInterpolationType::Continuous;
                    if is_median {
                        if let ScalarValue::Decimal128(Some(v), p, s) = s1.add(s2)? {
                            ScalarValue::Decimal128(Some(v / 2), p, s)
                        } else {
                            return internal_err!("{}", "Unreachable".to_string());
                        }
                    } else {
                        return internal_err!("{}", "Decimal type not supported in quantile_cont() or quantile_disc() aggregate function".to_string());
                    }
                }
                (scalar_value, _) => {
                    return internal_err!(
                        "{}",
                        format!(
                            "Unsupported type in PercentileAccumulator: {scalar_value:?}"
                        )
                    );
                }
            }
        };

        Ok(result)
    }

    fn size(&self) -> usize {
        let arrays_size: usize = self.arrays.iter().map(|a| a.len()).sum();

        std::mem::size_of_val(self)
            + ScalarValue::size_of_vec(&self.all_values)
            + arrays_size
            - std::mem::size_of_val(&self.all_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

fn to_scalar_values(arrays: &[ArrayRef]) -> Result<Vec<ScalarValue>> {
    let num_values: usize = arrays.iter().map(|a| a.len()).sum();
    let mut all_values = Vec::with_capacity(num_values);

    for array in arrays {
        for index in 0..array.len() {
            all_values.push(ScalarValue::try_from_array(&array, index)?);
        }
    }

    Ok(all_values)
}

/// Given a returns `array[indicies[indicie_index]]` as a `ScalarValue`
fn scalar_at_index(
    array: &dyn Array,
    indices: &UInt32Array,
    indicies_index: usize,
) -> Result<ScalarValue> {
    let array_index = indices
        .value(indicies_index)
        .try_into()
        .expect("Convert uint32 to usize");
    ScalarValue::try_from_array(array, array_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn median_decimal() -> Result<()> {
        // test median
        let array: ArrayRef = Arc::new(
            (1..7)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 4)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal128(10, 4),
            Median,
            ScalarValue::Decimal128(Some(3), 10, 4)
        )
    }

    #[test]
    fn median_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 4)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 4),
            Median,
            ScalarValue::Decimal128(Some(3), 10, 4)
        )
    }

    #[test]
    fn median_decimal_all_nulls() -> Result<()> {
        // test median
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 4)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 4),
            Median,
            ScalarValue::Decimal128(None, 10, 4)
        )
    }

    #[test]
    fn median_i32_odd() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::from(3_i32))
    }

    #[test]
    fn median_i32_even() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::from(3_i32))
    }

    #[test]
    fn median_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::from(3i32))
    }

    #[test]
    fn median_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::Int32(None))
    }

    #[test]
    fn median_u32_odd() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(a, DataType::UInt32, Median, ScalarValue::from(3u32))
    }

    #[test]
    fn median_u32_even() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from(vec![
            1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32,
        ]));
        generic_test_op!(a, DataType::UInt32, Median, ScalarValue::from(3u32))
    }

    #[test]
    fn median_f32_odd() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(a, DataType::Float32, Median, ScalarValue::from(3_f32))
    }

    #[test]
    fn median_f32_even() -> Result<()> {
        let a: ArrayRef = Arc::new(Float32Array::from(vec![
            1_f32, 2_f32, 3_f32, 4_f32, 5_f32, 6_f32,
        ]));
        generic_test_op!(a, DataType::Float32, Median, ScalarValue::from(3.5_f32))
    }

    #[test]
    fn median_f64_odd() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(a, DataType::Float64, Median, ScalarValue::from(3_f64))
    }

    #[test]
    fn median_f64_even() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![
            1_f64, 2_f64, 3_f64, 4_f64, 5_f64, 6_f64,
        ]));
        generic_test_op!(a, DataType::Float64, Median, ScalarValue::from(3.5_f64))
    }
}
