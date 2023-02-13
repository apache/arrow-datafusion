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

//! This module provides utilities for window frame index calculations
//! depending on the window frame mode: RANGE, ROWS, GROUPS.

use arrow::array::ArrayRef;
use arrow::compute::kernels::sort::SortOptions;
use datafusion_common::utils::{compare_rows, get_row_at_idx, search_in_slice};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// This object stores the window frame state for use in incremental calculations.
#[derive(Debug)]
pub enum WindowFrameContext<'a> {
    // ROWS-frames are inherently stateless:
    Rows(&'a Arc<WindowFrame>),
    // TODO: Fix this comment, briefly mention what the state is.
    // RANGE-frames will soon have a stateful implementation that is more efficient than a stateless one:
    Range {
        window_frame: &'a Arc<WindowFrame>,
        state: WindowFrameStateRange,
    },
    // TODO: Fix this comment, briefly mention what the state is.
    // GROUPS-frames have a stateful implementation that is more efficient than a stateless one:
    Groups {
        window_frame: &'a Arc<WindowFrame>,
        state: WindowFrameStateGroups,
    },
}

impl<'a> WindowFrameContext<'a> {
    /// Create a new default state for the given window frame.
    pub fn new(window_frame: &'a Arc<WindowFrame>) -> Self {
        match window_frame.units {
            WindowFrameUnits::Rows => WindowFrameContext::Rows(window_frame),
            WindowFrameUnits::Range => WindowFrameContext::Range {
                window_frame,
                state: WindowFrameStateRange::default(),
            },
            WindowFrameUnits::Groups => WindowFrameContext::Groups {
                window_frame,
                state: WindowFrameStateGroups::default(),
            },
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    pub fn calculate_range(
        &mut self,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
        last_range: &Range<usize>,
    ) -> Result<Range<usize>> {
        match *self {
            WindowFrameContext::Rows(window_frame) => {
                Self::calculate_range_rows(window_frame, length, idx)
            }
            // Sort options is used in RANGE mode calculations because the
            // ordering or position of NULLs impact range calculations and
            // comparison of rows.
            WindowFrameContext::Range {
                window_frame,
                ref mut state,
            } => state.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
                last_range,
            ),
            // Sort options is not used in GROUPS mode calculations as the
            // inequality of two rows indicates a group change, and ordering
            // or position of NULLs do not impact inequality.
            WindowFrameContext::Groups {
                window_frame,
                ref mut state,
            } => state.calculate_range(window_frame, range_columns, length, idx),
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    fn calculate_range_rows(
        window_frame: &Arc<WindowFrame>,
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            // UNBOUNDED PRECEDING
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => 0,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                if idx >= n as usize {
                    idx - n as usize
                } else {
                    0
                }
            }
            WindowFrameBound::CurrentRow => idx,
            // UNBOUNDED FOLLOWING
            WindowFrameBound::Following(ScalarValue::UInt64(None)) => {
                return Err(DataFusionError::Internal(format!(
                    "Frame start cannot be UNBOUNDED FOLLOWING '{window_frame:?}'"
                )))
            }
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                min(idx + n as usize, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return Err(DataFusionError::Internal("Rows should be Uint".to_string()))
            }
        };
        let end = match window_frame.end_bound {
            // UNBOUNDED PRECEDING
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => {
                return Err(DataFusionError::Internal(format!(
                    "Frame end cannot be UNBOUNDED PRECEDING '{window_frame:?}'"
                )))
            }
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                if idx >= n as usize {
                    idx - n as usize + 1
                } else {
                    0
                }
            }
            WindowFrameBound::CurrentRow => idx + 1,
            // UNBOUNDED FOLLOWING
            WindowFrameBound::Following(ScalarValue::UInt64(None)) => length,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                min(idx + n as usize + 1, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return Err(DataFusionError::Internal("Rows should be Uint".to_string()))
            }
        };
        Ok(Range { start, end })
    }
}

// TODO: Fix this struct and the comment when we move "where-we-left-off"
//       information from arguments/return values into the state.
/// This structure encapsulates all the state information we require as we
/// scan ranges of data while processing window frames. Currently we calculate
/// things from scratch every time, but we will make this incremental in the future.
#[derive(Debug, Default)]
pub struct WindowFrameStateRange {}

impl WindowFrameStateRange {
    /// This function calculates beginning/ending indices for the frame of the current row.
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
        last_range: &Range<usize>,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => {
                // TODO: Is this check at the right place? Is this being empty only a problem in this
                //       match arm? Or is it even a problem here? Let's add a test to exercise this
                //       if it doesn't exist, and if necessary, move this check to its right place.
                if range_columns.is_empty() {
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        None,
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    sort_options,
                    idx,
                    Some(n),
                    last_range,
                    length,
                )?,
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    sort_options,
                    idx,
                    Some(n),
                    last_range,
                    length,
                )?,
            WindowFrameBound::CurrentRow => {
                // TODO: Is this check at the right place? Is this being empty only a problem in this
                //       match arm? Or is it even a problem here? Let's add a test to exercise this
                //       if it doesn't exist, and if necessary, move this check to its right place.
                if range_columns.is_empty() {
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        None,
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                        last_range,
                        length,
                    )?
                }
            }
        };
        Ok(Range { start, end })
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively).
    fn calculate_index_of_row<const SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        idx: usize,
        delta: Option<&ScalarValue>,
        last_range: &Range<usize>,
        length: usize,
    ) -> Result<usize> {
        let current_row_values = get_row_at_idx(range_columns, idx)?;
        let end_range = if let Some(delta) = delta {
            let is_descending: bool = sort_options
                .first()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Sort options unexpectedly absent in a window frame".to_string(),
                    )
                })?
                .descending;

            current_row_values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        return Ok(value.clone());
                    }
                    if SEARCH_SIDE == is_descending {
                        // TODO: Handle positive overflows.
                        value.add(delta)
                    } else if value.is_unsigned() && value < delta {
                        // NOTE: This gets a polymorphic zero without having long coercion code for ScalarValue.
                        //       If we decide to implement a "default" construction mechanism for ScalarValue,
                        //       change the following statement to use that.
                        value.sub(value)
                    } else {
                        // TODO: Handle negative overflows.
                        value.sub(delta)
                    }
                })
                .collect::<Result<Vec<ScalarValue>>>()?
        } else {
            current_row_values
        };
        let search_start = if SIDE {
            last_range.start
        } else {
            last_range.end
        };
        let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
            let cmp = compare_rows(current, target, sort_options)?;
            Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
        };
        search_in_slice(range_columns, &end_range, compare_fn, search_start, length)
    }
}

// In GROUPS mode, rows with duplicate sorting values are grouped together.
// Therefore, there must be an ORDER BY clause in the window definition to use GROUPS mode.
// The syntax is as follows:
//     GROUPS frame_start [ frame_exclusion ]
//     GROUPS BETWEEN frame_start AND frame_end [ frame_exclusion ]
// The optional frame_exclusion specifier is not yet supported.
// The frame_start and frame_end parameters allow us to specify which rows the window
// frame starts and ends with. They accept the following values:
//    - UNBOUNDED PRECEDING: Start with the first row of the partition. Possible only in frame_start.
//    - offset PRECEDING: When used in frame_start, it refers to the first row of the group
//                        that comes "offset" groups before the current group (i.e. the group
//                        containing the current row). When used in frame_end, it refers to the
//                        last row of the group that comes "offset" groups before the current group.
//    - CURRENT ROW: When used in frame_start, it refers to the first row of the group containing
//                   the current row. When used in frame_end, it refers to the last row of the group
//                   containing the current row.
//    - offset FOLLOWING: When used in frame_start, it refers to the first row of the group
//                        that comes "offset" groups after the current group (i.e. the group
//                        containing the current row). When used in frame_end, it refers to the
//                        last row of the group that comes "offset" groups after the current group.
//    - UNBOUNDED FOLLOWING: End with the last row of the partition. Possible only in frame_end.

// This structure encapsulates all the state information we require as we
// scan groups of data while processing window frames.
#[derive(Debug, Default)]
pub struct WindowFrameStateGroups {
    /// A tuple containing group values and the row index where the group ends.
    /// Example: [[1, 1], [1, 1], [2, 1], [2, 1], ...] would correspond to
    ///          [([1, 1], 2), ([2, 1], 4), ...].
    group_start_indices: VecDeque<(Vec<ScalarValue>, usize)>,
    /// The group index to which the row index belongs.
    current_group_idx: usize,
}

impl WindowFrameStateGroups {
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        // TODO: This check contradicts with the same check in the handling of CurrentRow
        //       below. See my comment there, and fix this in the context of that.
        if range_columns.is_empty() {
            return Err(DataFusionError::Execution(
                "GROUPS mode requires an ORDER BY clause".to_string(),
            ));
        }
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => {
                // TODO: Is this check at the right place? Is this being empty only a problem in this
                //       match arm? Or is it even a problem here? Let's add a test to exercise this
                //       if it doesn't exist, and if necessary, move this check to its right place.
                if range_columns.is_empty() {
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        idx,
                        None,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    idx,
                    Some(n),
                    length,
                )?,
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    idx,
                    Some(n),
                    length,
                )?,
            WindowFrameBound::CurrentRow => {
                // TODO: Is this check at the right place? Is this being empty only a problem in this
                //       match arm? Or is it even a problem here? Let's add a test to exercise this
                //       if it doesn't exist, and if necessary, move this check to its right place.
                if range_columns.is_empty() {
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        idx,
                        None,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
        };
        Ok(Range { start, end })
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively). Generic argument `SEARCH_SIDE` determines
    /// the sign of `delta` (where true/false represents negative/positive respectively).
    fn calculate_index_of_row<const SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        idx: usize,
        delta: Option<&ScalarValue>,
        length: usize,
    ) -> Result<usize> {
        let delta = if let Some(delta) = delta {
            if let ScalarValue::UInt64(Some(value)) = delta {
                *value as usize
            } else {
                return Err(DataFusionError::Internal(
                    "Unexpectedly got a non-UInt64 value in a GROUPS mode window frame"
                        .to_string(),
                ));
            }
        } else {
            0
        };
        let mut group_start = 0;
        let last_group = self.group_start_indices.back();
        if let Some((_, group_end)) = last_group {
            // Start searching from the last group boundary:
            group_start = *group_end;
        }

        // Advance groups until `idx` is inside a group:
        while idx > group_start {
            let group_row = get_row_at_idx(range_columns, group_start)?;
            // Find end boundary of the group (search right boundary):
            let group_end = search_in_slice(
                range_columns,
                &group_row,
                check_equality,
                group_start,
                length,
            )?;
            self.group_start_indices.push_back((group_row, group_end));
            group_start = group_end;
        }

        // Update the group index `idx` belongs to:
        while self.current_group_idx < self.group_start_indices.len()
            && idx >= self.group_start_indices[self.current_group_idx].1
        {
            self.current_group_idx += 1;
        }

        // Find the group index of the frame boundary:
        let group_idx = if SEARCH_SIDE {
            if self.current_group_idx > delta {
                self.current_group_idx - delta
            } else {
                0
            }
        } else {
            self.current_group_idx + delta
        };

        // Extend `group_start_indices` until it includes at least `group_idx`:
        while self.group_start_indices.len() <= group_idx && group_start < length {
            let group_row = get_row_at_idx(range_columns, group_start)?;
            // Find end boundary of the group (search right boundary):
            let group_end = search_in_slice(
                range_columns,
                &group_row,
                check_equality,
                group_start,
                length,
            )?;
            self.group_start_indices.push_back((group_row, group_end));
            group_start = group_end;
        }

        // Calculate index of the group boundary:
        Ok(match (SIDE, SEARCH_SIDE) {
            // TODO: Is it normal that window frame start and end are asymmetric? You have
            //       PRECEDING and FOLLOWING cases separate for end, but not for start.
            //       Seems like this is OK, but let's make sure.
            // Window frame start:
            (true, _) => {
                let group_idx = min(group_idx, self.group_start_indices.len());
                if group_idx > 0 {
                    // Normally, start at the boundary of the previous group.
                    self.group_start_indices[group_idx - 1].1
                } else {
                    // If previous group is out of the table, start at zero.
                    0
                }
            }
            // Window frame end, PRECEDING n
            (false, true) => {
                if self.current_group_idx >= delta {
                    let group_idx = self.current_group_idx - delta;
                    self.group_start_indices[group_idx].1
                } else {
                    // Group is out of the table, therefore end at zero.
                    0
                }
            }
            // Window frame end, FOLLOWING n
            (false, false) => {
                let group_idx = min(
                    self.current_group_idx + delta,
                    self.group_start_indices.len() - 1,
                );
                self.group_start_indices[group_idx].1
            }
        })
    }
}

fn check_equality(current: &[ScalarValue], target: &[ScalarValue]) -> Result<bool> {
    Ok(current == target)
}

#[cfg(test)]
mod tests {
    use crate::window::window_frame_state::WindowFrameStateGroups;
    use arrow::array::{ArrayRef, Float64Array};
    use arrow_schema::SortOptions;
    use datafusion_common::from_slice::FromSlice;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    use std::ops::Range;
    use std::sync::Arc;

    fn get_test_data() -> (Vec<ArrayRef>, Vec<SortOptions>) {
        let range_columns: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice([
            5.0, 7.0, 8.0, 8.0, 9., 10., 10., 10., 11.,
        ]))];
        let sort_options = vec![SortOptions {
            descending: false,
            nulls_first: false,
        }];

        (range_columns, sort_options)
    }

    fn assert_expected(
        expected_results: Vec<(Range<usize>, usize)>,
        window_frame: &Arc<WindowFrame>,
    ) -> Result<()> {
        let mut window_frame_groups = WindowFrameStateGroups::default();
        let (range_columns, _) = get_test_data();
        let n_row = range_columns[0].len();
        for (idx, (expected_range, expected_group_idx)) in
            expected_results.into_iter().enumerate()
        {
            let range = window_frame_groups.calculate_range(
                window_frame,
                &range_columns,
                n_row,
                idx,
            )?;
            assert_eq!(range, expected_range);
            assert_eq!(window_frame_groups.current_group_idx, expected_group_idx);
        }
        Ok(())
    }

    #[test]
    fn test_window_frame_group_boundaries() -> Result<()> {
        let window_frame = Arc::new(WindowFrame {
            units: WindowFrameUnits::Groups,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            end_bound: WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
        });
        let expected_results = vec![
            (Range { start: 0, end: 2 }, 0),
            (Range { start: 0, end: 4 }, 1),
            (Range { start: 1, end: 5 }, 2),
            (Range { start: 1, end: 5 }, 2),
            (Range { start: 2, end: 8 }, 3),
            (Range { start: 4, end: 9 }, 4),
            (Range { start: 4, end: 9 }, 4),
            (Range { start: 4, end: 9 }, 4),
            (Range { start: 5, end: 9 }, 5),
        ];
        assert_expected(expected_results, &window_frame)
    }

    #[test]
    fn test_window_frame_group_boundaries_both_following() -> Result<()> {
        let window_frame = Arc::new(WindowFrame {
            units: WindowFrameUnits::Groups,
            start_bound: WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
            end_bound: WindowFrameBound::Following(ScalarValue::UInt64(Some(2))),
        });
        let expected_results = vec![
            (Range::<usize> { start: 1, end: 4 }, 0),
            (Range::<usize> { start: 2, end: 5 }, 1),
            (Range::<usize> { start: 4, end: 8 }, 2),
            (Range::<usize> { start: 4, end: 8 }, 2),
            (Range::<usize> { start: 5, end: 9 }, 3),
            (Range::<usize> { start: 8, end: 9 }, 4),
            (Range::<usize> { start: 8, end: 9 }, 4),
            (Range::<usize> { start: 8, end: 9 }, 4),
            (Range::<usize> { start: 9, end: 9 }, 5),
        ];
        assert_expected(expected_results, &window_frame)
    }

    #[test]
    fn test_window_frame_group_boundaries_both_preceding() -> Result<()> {
        let window_frame = Arc::new(WindowFrame {
            units: WindowFrameUnits::Groups,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
            end_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        });
        let expected_results = vec![
            (Range::<usize> { start: 0, end: 0 }, 0),
            (Range::<usize> { start: 0, end: 1 }, 1),
            (Range::<usize> { start: 0, end: 2 }, 2),
            (Range::<usize> { start: 0, end: 2 }, 2),
            (Range::<usize> { start: 1, end: 4 }, 3),
            (Range::<usize> { start: 2, end: 5 }, 4),
            (Range::<usize> { start: 2, end: 5 }, 4),
            (Range::<usize> { start: 2, end: 5 }, 4),
            (Range::<usize> { start: 4, end: 8 }, 5),
        ];
        assert_expected(expected_results, &window_frame)
    }
}
