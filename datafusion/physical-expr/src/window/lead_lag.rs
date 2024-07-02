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

//! Defines physical expression for `lead` and `lag` that can evaluated
//! at runtime during query execution
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::Array;
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::PartitionEvaluator;
use std::any::Any;
use std::cmp::min;
use std::collections::VecDeque;
use std::ops::{Neg, Range};
use std::sync::Arc;

/// window shift expression
#[derive(Debug)]
pub struct WindowShift {
    name: String,
    /// Output data type
    data_type: DataType,
    shift_offset: i64,
    expr: Arc<dyn PhysicalExpr>,
    default_value: ScalarValue,
    ignore_nulls: bool,
}

impl WindowShift {
    /// Get shift_offset of window shift expression
    pub fn get_shift_offset(&self) -> i64 {
        self.shift_offset
    }

    /// Get the default_value for window shift expression.
    pub fn get_default_value(&self) -> ScalarValue {
        self.default_value.clone()
    }
}

/// lead() window function
pub fn lead(
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    shift_offset: Option<i64>,
    default_value: ScalarValue,
    ignore_nulls: bool,
) -> WindowShift {
    WindowShift {
        name,
        data_type,
        shift_offset: shift_offset.map(|v| v.neg()).unwrap_or(-1),
        expr,
        default_value,
        ignore_nulls,
    }
}

/// lag() window function
pub fn lag(
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    shift_offset: Option<i64>,
    default_value: ScalarValue,
    ignore_nulls: bool,
) -> WindowShift {
    WindowShift {
        name,
        data_type,
        shift_offset: shift_offset.unwrap_or(1),
        expr,
        default_value,
        ignore_nulls,
    }
}

impl BuiltInWindowFunctionExpr for WindowShift {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![Arc::clone(&self.expr)]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(WindowShiftEvaluator {
            shift_offset: self.shift_offset,
            default_value: self.default_value.clone(),
            ignore_nulls: self.ignore_nulls,
            non_null_offsets: VecDeque::new(),
        }))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        Some(Arc::new(Self {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            shift_offset: -self.shift_offset,
            expr: Arc::clone(&self.expr),
            default_value: self.default_value.clone(),
            ignore_nulls: self.ignore_nulls,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct WindowShiftEvaluator {
    shift_offset: i64,
    default_value: ScalarValue,
    ignore_nulls: bool,
    // VecDeque contains offset values that between non-null entries
    non_null_offsets: VecDeque<usize>,
}

impl WindowShiftEvaluator {
    fn is_lag(&self) -> bool {
        // Mode is LAG, when shift_offset is positive
        self.shift_offset > 0
    }
}

// implement ignore null for evaluate_all
fn evaluate_all_with_ignore_null(
    array: &ArrayRef,
    offset: i64,
    default_value: &ScalarValue,
    is_lag: bool,
) -> Result<ArrayRef, DataFusionError> {
    let valid_indices: Vec<usize> =
        array.nulls().unwrap().valid_indices().collect::<Vec<_>>();
    let direction = !is_lag;
    let new_array_results: Result<Vec<_>, DataFusionError> = (0..array.len())
        .map(|id| {
            let result_index = match valid_indices.binary_search(&id) {
                Ok(pos) => if direction {
                    pos.checked_add(offset as usize)
                } else {
                    pos.checked_sub(offset.unsigned_abs() as usize)
                }
                .and_then(|new_pos| {
                    if new_pos < valid_indices.len() {
                        Some(valid_indices[new_pos])
                    } else {
                        None
                    }
                }),
                Err(pos) => if direction {
                    pos.checked_add(offset as usize)
                } else if pos > 0 {
                    pos.checked_sub(offset.unsigned_abs() as usize)
                } else {
                    None
                }
                .and_then(|new_pos| {
                    if new_pos < valid_indices.len() {
                        Some(valid_indices[new_pos])
                    } else {
                        None
                    }
                }),
            };

            match result_index {
                Some(index) => ScalarValue::try_from_array(array, index),
                None => Ok(default_value.clone()),
            }
        })
        .collect();

    let new_array = new_array_results?;
    ScalarValue::iter_to_array(new_array)
}
// TODO: change the original arrow::compute::kernels::window::shift impl to support an optional default value
fn shift_with_default_value(
    array: &ArrayRef,
    offset: i64,
    default_value: &ScalarValue,
) -> Result<ArrayRef> {
    use arrow::compute::concat;

    let value_len = array.len() as i64;
    if offset == 0 {
        Ok(Arc::clone(array))
    } else if offset == i64::MIN || offset.abs() >= value_len {
        default_value.to_array_of_size(value_len as usize)
    } else {
        let slice_offset = (-offset).clamp(0, value_len) as usize;
        let length = array.len() - offset.unsigned_abs() as usize;
        let slice = array.slice(slice_offset, length);

        // Generate array with remaining `null` items
        let nulls = offset.unsigned_abs() as usize;
        let default_values = default_value.to_array_of_size(nulls)?;

        // Concatenate both arrays, add nulls after if shift > 0 else before
        if offset > 0 {
            concat(&[default_values.as_ref(), slice.as_ref()])
                .map_err(|e| arrow_datafusion_err!(e))
        } else {
            concat(&[slice.as_ref(), default_values.as_ref()])
                .map_err(|e| arrow_datafusion_err!(e))
        }
    }
}

impl PartitionEvaluator for WindowShiftEvaluator {
    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        if self.is_lag() {
            let start = if self.non_null_offsets.len() == self.shift_offset as usize {
                // How many rows needed previous than the current row to get necessary lag result
                let offset: usize = self.non_null_offsets.iter().sum();
                idx.saturating_sub(offset)
            } else if !self.ignore_nulls {
                let offset = self.shift_offset as usize;
                idx.saturating_sub(offset)
            } else {
                0
            };
            let end = idx + 1;
            Ok(Range { start, end })
        } else {
            let end = if self.non_null_offsets.len() == (-self.shift_offset) as usize {
                // How many rows needed further than the current row to get necessary lead result
                let offset: usize = self.non_null_offsets.iter().sum();
                min(idx + offset + 1, n_rows)
            } else if !self.ignore_nulls {
                let offset = (-self.shift_offset) as usize;
                min(idx + offset, n_rows)
            } else {
                n_rows
            };
            Ok(Range { start: idx, end })
        }
    }

    fn is_causal(&self) -> bool {
        // Lagging windows are causal by definition:
        self.is_lag()
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let array = &values[0];
        let len = array.len();

        // LAG mode
        let i = if self.is_lag() {
            (range.end as i64 - self.shift_offset - 1) as usize
        } else {
            // LEAD mode
            (range.start as i64 - self.shift_offset) as usize
        };

        let mut idx: Option<usize> = if i < len { Some(i) } else { None };

        // LAG with IGNORE NULLS calculated as the current row index - offset, but only for non-NULL rows
        // If current row index points to NULL value the row is NOT counted
        if self.ignore_nulls && self.is_lag() {
            // LAG when NULLS are ignored.
            // Find the nonNULL row index that shifted by offset comparing to current row index
            idx = if self.non_null_offsets.len() == self.shift_offset as usize {
                let total_offset: usize = self.non_null_offsets.iter().sum();
                Some(range.end - 1 - total_offset)
            } else {
                None
            };

            // Keep track of offset values between non-null entries
            if array.is_valid(range.end - 1) {
                // Non-null add new offset
                self.non_null_offsets.push_back(1);
                if self.non_null_offsets.len() > self.shift_offset as usize {
                    // WE do not need to keep track of more than `lag number of offset` values.
                    self.non_null_offsets.pop_front();
                }
            } else if !self.non_null_offsets.is_empty() {
                // Entry is null, increment offset value of the last entry.
                let end_idx = self.non_null_offsets.len() - 1;
                self.non_null_offsets[end_idx] += 1;
            }
        } else if self.ignore_nulls && !self.is_lag() {
            // LEAD when NULLS are ignored.
            // Stores the necessary non-null entry number further than the current row.
            let non_null_row_count = (-self.shift_offset) as usize;

            if self.non_null_offsets.is_empty() {
                // When empty, fill non_null offsets with the data further than the current row.
                let mut offset_val = 1;
                for idx in range.start + 1..range.end {
                    if array.is_valid(idx) {
                        self.non_null_offsets.push_back(offset_val);
                        offset_val = 1;
                    } else {
                        offset_val += 1;
                    }
                    // It is enough to keep track of `non_null_row_count + 1` non-null offset.
                    // further data is unnecessary for the result.
                    if self.non_null_offsets.len() == non_null_row_count + 1 {
                        break;
                    }
                }
            } else if range.end < len && array.is_valid(range.end) {
                // Update `non_null_offsets` with the new end data.
                if array.is_valid(range.end) {
                    // When non-null, append a new offset.
                    self.non_null_offsets.push_back(1);
                } else {
                    // When null, increment offset count of the last entry
                    let last_idx = self.non_null_offsets.len() - 1;
                    self.non_null_offsets[last_idx] += 1;
                }
            }

            // Find the nonNULL row index that shifted by offset comparing to current row index
            idx = if self.non_null_offsets.len() >= non_null_row_count {
                let total_offset: usize =
                    self.non_null_offsets.iter().take(non_null_row_count).sum();
                Some(range.start + total_offset)
            } else {
                None
            };
            // Prune `self.non_null_offsets` from the start. so that at next iteration
            // start of the `self.non_null_offsets` matches with current row.
            if !self.non_null_offsets.is_empty() {
                self.non_null_offsets[0] -= 1;
                if self.non_null_offsets[0] == 0 {
                    // When offset is 0. Remove it.
                    self.non_null_offsets.pop_front();
                }
            }
        }

        // Set the default value if
        // - index is out of window bounds
        // OR
        // - ignore nulls mode and current value is null and is within window bounds
        // .unwrap() is safe here as there is a none check in front
        #[allow(clippy::unnecessary_unwrap)]
        if !(idx.is_none() || (self.ignore_nulls && array.is_null(idx.unwrap()))) {
            ScalarValue::try_from_array(array, idx.unwrap())
        } else {
            Ok(self.default_value.clone())
        }
    }

    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        _num_rows: usize,
    ) -> Result<ArrayRef> {
        // LEAD, LAG window functions take single column, values will have size 1
        let value = &values[0];
        if !self.ignore_nulls {
            shift_with_default_value(value, self.shift_offset, &self.default_value)
        } else {
            evaluate_all_with_ignore_null(
                value,
                self.shift_offset,
                &self.default_value,
                self.is_lag(),
            )
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::cast::as_int32_array;

    fn test_i32_result(expr: WindowShift, expected: Int32Array) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let values = expr.evaluate_args(&batch)?;
        let result = expr
            .create_evaluator()?
            .evaluate_all(&values, batch.num_rows())?;
        let result = as_int32_array(&result)?;
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn lead_lag_get_range() -> Result<()> {
        // LAG(2)
        let lag_fn = WindowShiftEvaluator {
            shift_offset: 2,
            default_value: ScalarValue::Null,
            ignore_nulls: false,
            non_null_offsets: Default::default(),
        };
        assert_eq!(lag_fn.get_range(6, 10)?, Range { start: 4, end: 7 });
        assert_eq!(lag_fn.get_range(0, 10)?, Range { start: 0, end: 1 });

        // LAG(2 ignore nulls)
        let lag_fn = WindowShiftEvaluator {
            shift_offset: 2,
            default_value: ScalarValue::Null,
            ignore_nulls: true,
            // models data received [<Some>, <Some>, <Some>, NULL, <Some>, NULL, <current row>, ...]
            non_null_offsets: vec![2, 2].into(), // [1, 1, 2, 2] actually, just last 2 is used
        };
        assert_eq!(lag_fn.get_range(6, 10)?, Range { start: 2, end: 7 });

        // LEAD(2)
        let lead_fn = WindowShiftEvaluator {
            shift_offset: -2,
            default_value: ScalarValue::Null,
            ignore_nulls: false,
            non_null_offsets: Default::default(),
        };
        assert_eq!(lead_fn.get_range(6, 10)?, Range { start: 6, end: 8 });
        assert_eq!(lead_fn.get_range(9, 10)?, Range { start: 9, end: 10 });

        // LEAD(2 ignore nulls)
        let lead_fn = WindowShiftEvaluator {
            shift_offset: -2,
            default_value: ScalarValue::Null,
            ignore_nulls: true,
            // models data received [..., <current row>, NULL, <Some>, NULL, <Some>, ..]
            non_null_offsets: vec![2, 2].into(),
        };
        assert_eq!(lead_fn.get_range(4, 10)?, Range { start: 4, end: 9 });

        Ok(())
    }

    #[test]
    fn lead_lag_window_shift() -> Result<()> {
        test_i32_result(
            lead(
                "lead".to_owned(),
                DataType::Int32,
                Arc::new(Column::new("c3", 0)),
                None,
                ScalarValue::Null.cast_to(&DataType::Int32)?,
                false,
            ),
            [
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
                Some(8),
                None,
            ]
            .iter()
            .collect::<Int32Array>(),
        )?;

        test_i32_result(
            lag(
                "lead".to_owned(),
                DataType::Int32,
                Arc::new(Column::new("c3", 0)),
                None,
                ScalarValue::Null.cast_to(&DataType::Int32)?,
                false,
            ),
            [
                None,
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
            ]
            .iter()
            .collect::<Int32Array>(),
        )?;

        test_i32_result(
            lag(
                "lead".to_owned(),
                DataType::Int32,
                Arc::new(Column::new("c3", 0)),
                None,
                ScalarValue::Int32(Some(100)),
                false,
            ),
            [
                Some(100),
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
            ]
            .iter()
            .collect::<Int32Array>(),
        )?;
        Ok(())
    }
}
