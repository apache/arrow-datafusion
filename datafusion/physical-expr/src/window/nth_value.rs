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

//! Defines physical expressions for `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`
//! functions that can be evaluated at run time during query execution.

use std::any::Any;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use crate::window::window_expr::{NthValueKind, NthValueState};
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field};
use datafusion_common::Result;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::PartitionEvaluator;

/// nth_value expression
#[derive(Debug)]
pub struct NthValue {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    /// Output data type
    data_type: DataType,
    kind: NthValueKind,
    ignore_nulls: bool,
}

impl NthValue {
    /// Create a new FIRST_VALUE window aggregate function
    pub fn first(
        name: impl Into<String>,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            kind: NthValueKind::First,
            ignore_nulls,
        }
    }

    /// Create a new LAST_VALUE window aggregate function
    pub fn last(
        name: impl Into<String>,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            kind: NthValueKind::Last,
            ignore_nulls,
        }
    }

    /// Create a new NTH_VALUE window aggregate function
    pub fn nth(
        name: impl Into<String>,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        n: i64,
        ignore_nulls: bool,
    ) -> Result<Self> {
        match n {
            0 => exec_err!("NTH_VALUE expects n to be non-zero"),
            _ => Ok(Self {
                name: name.into(),
                expr,
                data_type,
                kind: NthValueKind::Nth(n),
                ignore_nulls,
            }),
        }
    }

    /// Get the NTH_VALUE kind
    pub fn get_kind(&self) -> NthValueKind {
        self.kind
    }
}

impl BuiltInWindowFunctionExpr for NthValue {
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
        let state = NthValueState {
            finalized_result: None,
            kind: self.kind,
        };
        Ok(Box::new(NthValueEvaluator {
            state,
            ignore_nulls: self.ignore_nulls,
        }))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        let reversed_kind = match self.kind {
            NthValueKind::First => NthValueKind::Last,
            NthValueKind::Last => NthValueKind::First,
            NthValueKind::Nth(idx) => NthValueKind::Nth(-idx),
        };
        Some(Arc::new(Self {
            name: self.name.clone(),
            expr: Arc::clone(&self.expr),
            data_type: self.data_type.clone(),
            kind: reversed_kind,
            ignore_nulls: self.ignore_nulls,
        }))
    }
}

/// Value evaluator for nth_value functions
#[derive(Debug)]
pub(crate) struct NthValueEvaluator {
    state: NthValueState,
    ignore_nulls: bool,
}

impl PartitionEvaluator for NthValueEvaluator {
    /// When the window frame has a fixed beginning (e.g UNBOUNDED PRECEDING),
    /// for some functions such as FIRST_VALUE, LAST_VALUE and NTH_VALUE, we
    /// can memoize the result.  Once result is calculated, it will always stay
    /// same. Hence, we do not need to keep past data as we process the entire
    /// dataset.
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let out = &state.out_col;
        let size = out.len();
        let mut buffer_size = 1;
        // Decide if we arrived at a final result yet:
        let (is_prunable, is_reverse_direction) = match self.state.kind {
            NthValueKind::First => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                (n_range > 0 && size > 0, false)
            }
            NthValueKind::Last => (true, true),
            NthValueKind::Nth(n) => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                match n.cmp(&0) {
                    Ordering::Greater => {
                        (n_range >= (n as usize) && size > (n as usize), false)
                    }
                    Ordering::Less => {
                        let reverse_index = (-n) as usize;
                        buffer_size = reverse_index;
                        // Negative index represents reverse direction.
                        (n_range >= reverse_index, true)
                    }
                    Ordering::Equal => {
                        // The case n = 0 is not valid for the NTH_VALUE function.
                        unreachable!();
                    }
                }
            }
        };
        // Do not memoize results when nulls are ignored.
        if is_prunable && !self.ignore_nulls {
            if self.state.finalized_result.is_none() && !is_reverse_direction {
                let result = ScalarValue::try_from_array(out, size - 1)?;
                self.state.finalized_result = Some(result);
            }
            state.window_frame_range.start =
                state.window_frame_range.end.saturating_sub(buffer_size);
        }
        Ok(())
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        if let Some(ref result) = self.state.finalized_result {
            Ok(result.clone())
        } else {
            // FIRST_VALUE, LAST_VALUE, NTH_VALUE window functions take a single column, values will have size 1.
            let arr = &values[0];
            let n_range = range.end - range.start;
            if n_range == 0 {
                // We produce None if the window is empty.
                return ScalarValue::try_from(arr.data_type());
            }

            // Extract valid indices if ignoring nulls.
            let valid_indices = if self.ignore_nulls {
                // Calculate valid indices, inside the window frame boundaries
                let slice = arr.slice(range.start, n_range);
                let valid_indices = slice
                    .nulls()
                    .map(|nulls| {
                        nulls
                            .valid_indices()
                            // Add offset `range.start` to valid indices, to point correct index in the original arr.
                            .map(|idx| idx + range.start)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if valid_indices.is_empty() {
                    return ScalarValue::try_from(arr.data_type());
                }
                Some(valid_indices)
            } else {
                None
            };
            match self.state.kind {
                NthValueKind::First => {
                    if let Some(valid_indices) = &valid_indices {
                        ScalarValue::try_from_array(arr, valid_indices[0])
                    } else {
                        ScalarValue::try_from_array(arr, range.start)
                    }
                }
                NthValueKind::Last => {
                    if let Some(valid_indices) = &valid_indices {
                        ScalarValue::try_from_array(
                            arr,
                            valid_indices[valid_indices.len() - 1],
                        )
                    } else {
                        ScalarValue::try_from_array(arr, range.end - 1)
                    }
                }
                NthValueKind::Nth(n) => {
                    match n.cmp(&0) {
                        Ordering::Greater => {
                            // SQL indices are not 0-based.
                            let index = (n as usize) - 1;
                            if index >= n_range {
                                // Outside the range, return NULL:
                                ScalarValue::try_from(arr.data_type())
                            } else if let Some(valid_indices) = valid_indices {
                                if index >= valid_indices.len() {
                                    return ScalarValue::try_from(arr.data_type());
                                }
                                ScalarValue::try_from_array(&arr, valid_indices[index])
                            } else {
                                ScalarValue::try_from_array(arr, range.start + index)
                            }
                        }
                        Ordering::Less => {
                            let reverse_index = (-n) as usize;
                            if n_range < reverse_index {
                                // Outside the range, return NULL:
                                ScalarValue::try_from(arr.data_type())
                            } else if let Some(valid_indices) = valid_indices {
                                if reverse_index > valid_indices.len() {
                                    return ScalarValue::try_from(arr.data_type());
                                }
                                let new_index =
                                    valid_indices[valid_indices.len() - reverse_index];
                                ScalarValue::try_from_array(&arr, new_index)
                            } else {
                                ScalarValue::try_from_array(
                                    arr,
                                    range.start + n_range - reverse_index,
                                )
                            }
                        }
                        Ordering::Equal => {
                            // The case n = 0 is not valid for the NTH_VALUE function.
                            unreachable!();
                        }
                    }
                }
            }
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::cast::as_int32_array;

    fn test_i32_result(expr: NthValue, expected: Int32Array) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let values = vec![arr];
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
        let mut ranges: Vec<Range<usize>> = vec![];
        for i in 0..8 {
            ranges.push(Range {
                start: 0,
                end: i + 1,
            })
        }
        let mut evaluator = expr.create_evaluator()?;
        let values = expr.evaluate_args(&batch)?;
        let result = ranges
            .iter()
            .map(|range| evaluator.evaluate(&values, range))
            .collect::<Result<Vec<ScalarValue>>>()?;
        let result = ScalarValue::iter_to_array(result.into_iter())?;
        let result = as_int32_array(&result)?;
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    fn first_value() -> Result<()> {
        let first_value = NthValue::first(
            "first_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
            false,
        );
        test_i32_result(first_value, Int32Array::from(vec![1; 8]))?;
        Ok(())
    }

    #[test]
    fn last_value() -> Result<()> {
        let last_value = NthValue::last(
            "last_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
            false,
        );
        test_i32_result(
            last_value,
            Int32Array::from(vec![
                Some(1),
                Some(-2),
                Some(3),
                Some(-4),
                Some(5),
                Some(-6),
                Some(7),
                Some(8),
            ]),
        )?;
        Ok(())
    }

    #[test]
    fn nth_value_1() -> Result<()> {
        let nth_value = NthValue::nth(
            "nth_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
            1,
            false,
        )?;
        test_i32_result(nth_value, Int32Array::from(vec![1; 8]))?;
        Ok(())
    }

    #[test]
    fn nth_value_2() -> Result<()> {
        let nth_value = NthValue::nth(
            "nth_value".to_owned(),
            Arc::new(Column::new("arr", 0)),
            DataType::Int32,
            2,
            false,
        )?;
        test_i32_result(
            nth_value,
            Int32Array::from(vec![
                None,
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
                Some(-2),
            ]),
        )?;
        Ok(())
    }
}
