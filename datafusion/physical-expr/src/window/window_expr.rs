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

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use crate::{LexOrderingRef, PhysicalExpr, PhysicalSortExpr};

use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::compute::kernels::sort::SortColumn;
use arrow::compute::SortOptions;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::compare_rows;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::window_state::{
    PartitionBatchState, WindowAggState, WindowFrameContext, WindowFrameStateGroups,
};
use datafusion_expr::{Accumulator, PartitionEvaluator, WindowFrame, WindowFrameBound};

use indexmap::IndexMap;

/// Common trait for [window function] implementations
///
/// # Aggregate Window Expressions
///
/// These expressions take the form
///
/// ```text
/// OVER({ROWS | RANGE| GROUPS} BETWEEN UNBOUNDED PRECEDING AND ...)
/// ```
///
/// For example, cumulative window frames uses `PlainAggregateWindowExpr`.
///
/// # Non Aggregate Window Expressions
///
/// The expressions have the form
///
/// ```text
/// OVER({ROWS | RANGE| GROUPS} BETWEEN M {PRECEDING| FOLLOWING} AND ...)
/// ```
///
/// For example, sliding window frames use [`SlidingAggregateWindowExpr`].
///
/// [window function]: https://en.wikipedia.org/wiki/Window_function_(SQL)
/// [`PlainAggregateWindowExpr`]: crate::window::PlainAggregateWindowExpr
/// [`SlidingAggregateWindowExpr`]: crate::window::SlidingAggregateWindowExpr
pub trait WindowExpr: Send + Sync + Debug {
    /// Returns the window expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// The field of the final result of this window function.
    fn field(&self) -> Result<Field>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "WindowExpr: default name"
    }

    /// Expressions that are passed to the WindowAccumulator.
    /// Functions which take a single input argument, such as `sum`, return a single [`datafusion_expr::expr::Expr`],
    /// others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Evaluate the window function arguments against the batch and return
    /// array ref, normally the resulting `Vec` is a single element one.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect()
    }

    /// Evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the window function against the batch. This function facilitates
    /// stateful, bounded-memory implementations.
    fn evaluate_stateful(
        &self,
        _partition_batches: &PartitionBatches,
        _window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        internal_err!("evaluate_stateful is not implemented for {}", self.name())
    }

    /// Expressions that's from the window function's partition by clause, empty if absent
    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>];

    /// Expressions that's from the window function's order by clause, empty if absent
    fn order_by(&self) -> &[PhysicalSortExpr];

    /// Get order by columns, empty if absent
    fn order_by_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.order_by()
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<SortColumn>>>()
    }

    /// Get the window frame of this [WindowExpr].
    fn get_window_frame(&self) -> &Arc<WindowFrame>;

    /// Return a flag indicating whether this [WindowExpr] can run with
    /// bounded memory.
    fn uses_bounded_memory(&self) -> bool;

    /// Get the reverse expression of this [WindowExpr].
    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>>;

    /// Returns all expressions used in the [`WindowExpr`].
    /// These expressions are (1) function arguments, (2) partition by expressions, (3) order by expressions.
    fn all_expressions(&self) -> WindowPhysicalExpressions {
        let args = self.expressions();
        let partition_by_exprs = self.partition_by().to_vec();
        let order_by_exprs = self
            .order_by()
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();
        WindowPhysicalExpressions {
            args,
            partition_by_exprs,
            order_by_exprs,
        }
    }

    /// Rewrites [`WindowExpr`], with new expressions given. The argument should be consistent
    /// with the return value of the [`WindowExpr::all_expressions`] method.
    /// Returns `Some(Arc<dyn WindowExpr>)` if re-write is supported, otherwise returns `None`.
    fn with_new_expressions(
        &self,
        _args: Vec<Arc<dyn PhysicalExpr>>,
        _partition_bys: Vec<Arc<dyn PhysicalExpr>>,
        _order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn WindowExpr>> {
        None
    }
}

/// Stores the physical expressions used inside the `WindowExpr`.
pub struct WindowPhysicalExpressions {
    /// Window function arguments
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// PARTITION BY expressions
    pub partition_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// ORDER BY expressions
    pub order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

/// Extension trait that adds common functionality to [`AggregateWindowExpr`]s
pub trait AggregateWindowExpr: WindowExpr {
    /// Get the accumulator for the window expression. Note that distinct
    /// window expressions may return distinct accumulators; e.g. sliding
    /// (non-sliding) expressions will return sliding (normal) accumulators.
    fn get_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// Given current range and the last range, calculates the accumulator
    /// result for the range of interest.
    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
    ) -> Result<ScalarValue>;

    /// Evaluates the window function against the batch.
    fn aggregate_evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut accumulator = self.get_accumulator()?;
        let mut last_range = Range { start: 0, end: 0 };
        let sort_options: Vec<SortOptions> =
            self.order_by().iter().map(|o| o.options).collect();
        let mut window_frame_ctx =
            WindowFrameContext::new(Arc::clone(self.get_window_frame()), sort_options);
        self.get_result_column(
            &mut accumulator,
            batch,
            None,
            &mut last_range,
            &mut window_frame_ctx,
            0,
            false,
        )
    }

    /// Statefully evaluates the window function against the batch. Maintains
    /// state so that it can work incrementally over multiple chunks.
    fn aggregate_evaluate_stateful(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        let field = self.field()?;
        let out_type = field.data_type();
        for (partition_row, partition_batch_state) in partition_batches.iter() {
            if !window_agg_state.contains_key(partition_row) {
                let accumulator = self.get_accumulator()?;
                window_agg_state.insert(
                    partition_row.clone(),
                    WindowState {
                        state: WindowAggState::new(out_type)?,
                        window_fn: WindowFn::Aggregate(accumulator),
                    },
                );
            };
            let window_state =
                window_agg_state.get_mut(partition_row).ok_or_else(|| {
                    DataFusionError::Execution("Cannot find state".to_string())
                })?;
            let accumulator = match &mut window_state.window_fn {
                WindowFn::Aggregate(accumulator) => accumulator,
                _ => unreachable!(),
            };
            let state = &mut window_state.state;
            let record_batch = &partition_batch_state.record_batch;
            let most_recent_row = partition_batch_state.most_recent_row.as_ref();

            // If there is no window state context, initialize it.
            let window_frame_ctx = state.window_frame_ctx.get_or_insert_with(|| {
                let sort_options: Vec<SortOptions> =
                    self.order_by().iter().map(|o| o.options).collect();
                WindowFrameContext::new(Arc::clone(self.get_window_frame()), sort_options)
            });
            let out_col = self.get_result_column(
                accumulator,
                record_batch,
                most_recent_row,
                // Start search from the last range
                &mut state.window_frame_range,
                window_frame_ctx,
                state.last_calculated_index,
                !partition_batch_state.is_end,
            )?;
            state.update(&out_col, partition_batch_state)?;
        }
        Ok(())
    }

    /// Calculates the window expression result for the given record batch.
    /// Assumes that `record_batch` belongs to a single partition.
    #[allow(clippy::too_many_arguments)]
    fn get_result_column(
        &self,
        accumulator: &mut Box<dyn Accumulator>,
        record_batch: &RecordBatch,
        most_recent_row: Option<&RecordBatch>,
        last_range: &mut Range<usize>,
        window_frame_ctx: &mut WindowFrameContext,
        mut idx: usize,
        not_end: bool,
    ) -> Result<ArrayRef> {
        let values = self.evaluate_args(record_batch)?;
        let order_bys = get_orderby_values(self.order_by_columns(record_batch)?);

        let most_recent_row_order_bys = most_recent_row
            .map(|batch| self.order_by_columns(batch))
            .transpose()?
            .map(get_orderby_values);

        // We iterate on each row to perform a running calculation.
        let length = values[0].len();
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        let is_causal = self.get_window_frame().is_causal();
        while idx < length {
            // Start search from the last_range. This squeezes searched range.
            let cur_range =
                window_frame_ctx.calculate_range(&order_bys, last_range, length, idx)?;
            // Exit if the range is non-causal and extends all the way:
            if cur_range.end == length
                && !is_causal
                && not_end
                && !is_end_bound_safe(
                    window_frame_ctx,
                    &order_bys,
                    most_recent_row_order_bys.as_deref(),
                    self.order_by(),
                    idx,
                )?
            {
                break;
            }
            let value = self.get_aggregate_result_inside_range(
                last_range,
                &cur_range,
                &values,
                accumulator,
            )?;
            // Update last range
            *last_range = cur_range;
            row_wise_results.push(value);
            idx += 1;
        }

        if row_wise_results.is_empty() {
            let field = self.field()?;
            let out_type = field.data_type();
            Ok(new_empty_array(out_type))
        } else {
            ScalarValue::iter_to_array(row_wise_results)
        }
    }
}

/// Determines whether the end bound calculation for a window frame context is
/// safe, meaning that the end bound stays the same, regardless of future data,
/// based on the current sort expressions and ORDER BY columns. This function
/// delegates work to specific functions for each frame type.
///
/// # Parameters
///
/// * `window_frame_ctx`: The context of the window frame being evaluated.
/// * `order_bys`: A slice of `ArrayRef` representing the ORDER BY columns.
/// * `most_recent_order_bys`: An optional reference to the most recent ORDER BY
///   columns.
/// * `sort_exprs`: Defines the lexicographical ordering in question.
/// * `idx`: The current index in the window frame.
///
/// # Returns
///
/// A `Result` which is `Ok(true)` if the end bound is safe, `Ok(false)` otherwise.
pub(crate) fn is_end_bound_safe(
    window_frame_ctx: &WindowFrameContext,
    order_bys: &[ArrayRef],
    most_recent_order_bys: Option<&[ArrayRef]>,
    sort_exprs: LexOrderingRef,
    idx: usize,
) -> Result<bool> {
    if sort_exprs.is_empty() {
        // Early return if no sort expressions are present:
        return Ok(false);
    }

    match window_frame_ctx {
        WindowFrameContext::Rows(window_frame) => {
            is_end_bound_safe_for_rows(&window_frame.end_bound)
        }
        WindowFrameContext::Range { window_frame, .. } => is_end_bound_safe_for_range(
            &window_frame.end_bound,
            &order_bys[0],
            most_recent_order_bys.map(|items| &items[0]),
            &sort_exprs[0].options,
            idx,
        ),
        WindowFrameContext::Groups {
            window_frame,
            state,
        } => is_end_bound_safe_for_groups(
            &window_frame.end_bound,
            state,
            &order_bys[0],
            most_recent_order_bys.map(|items| &items[0]),
            &sort_exprs[0].options,
        ),
    }
}

/// For row-based window frames, determines whether the end bound calculation
/// is safe, which is trivially the case for `Preceding` and `CurrentRow` bounds.
/// For 'Following' bounds, it compares the bound value to zero to ensure that
/// it doesn't extend beyond the current row.
///
/// # Parameters
///
/// * `end_bound`: Reference to the window frame bound in question.
///
/// # Returns
///
/// A `Result` indicating whether the end bound is safe for row-based window frames.
fn is_end_bound_safe_for_rows(end_bound: &WindowFrameBound) -> Result<bool> {
    if let WindowFrameBound::Following(value) = end_bound {
        let zero = ScalarValue::new_zero(&value.data_type());
        Ok(zero.map(|zero| value.eq(&zero)).unwrap_or(false))
    } else {
        Ok(true)
    }
}

/// For row-based window frames, determines whether the end bound calculation
/// is safe by comparing it against specific values (zero, current row). It uses
/// the `is_row_ahead` helper function to determine if the current row is ahead
/// of the most recent row based on the ORDER BY column and sorting options.
///
/// # Parameters
///
/// * `end_bound`: Reference to the window frame bound in question.
/// * `orderby_col`: Reference to the column used for ordering.
/// * `most_recent_ob_col`: Optional reference to the most recent order-by column.
/// * `sort_options`: The sorting options used in the window frame.
/// * `idx`: The current index in the window frame.
///
/// # Returns
///
/// A `Result` indicating whether the end bound is safe for range-based window frames.
fn is_end_bound_safe_for_range(
    end_bound: &WindowFrameBound,
    orderby_col: &ArrayRef,
    most_recent_ob_col: Option<&ArrayRef>,
    sort_options: &SortOptions,
    idx: usize,
) -> Result<bool> {
    match end_bound {
        WindowFrameBound::Preceding(value) => {
            let zero = ScalarValue::new_zero(&value.data_type())?;
            if value.eq(&zero) {
                is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
            } else {
                Ok(true)
            }
        }
        WindowFrameBound::CurrentRow => {
            is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
        }
        WindowFrameBound::Following(delta) => {
            let Some(most_recent_ob_col) = most_recent_ob_col else {
                return Ok(false);
            };
            let most_recent_row_value =
                ScalarValue::try_from_array(most_recent_ob_col, 0)?;
            let current_row_value = ScalarValue::try_from_array(orderby_col, idx)?;

            if sort_options.descending {
                current_row_value
                    .sub(delta)
                    .map(|value| value > most_recent_row_value)
            } else {
                current_row_value
                    .add(delta)
                    .map(|value| most_recent_row_value > value)
            }
        }
    }
}

/// For group-based window frames, determines whether the end bound calculation
/// is safe by considering the group offset and whether the current row is ahead
/// of the most recent row in terms of sorting. It checks if the end bound is
/// within the bounds of the current group based on group end indices.
///
/// # Parameters
///
/// * `end_bound`: Reference to the window frame bound in question.
/// * `state`: The state of the window frame for group calculations.
/// * `orderby_col`: Reference to the column used for ordering.
/// * `most_recent_ob_col`: Optional reference to the most recent order-by column.
/// * `sort_options`: The sorting options used in the window frame.
///
/// # Returns
///
/// A `Result` indicating whether the end bound is safe for group-based window frames.
fn is_end_bound_safe_for_groups(
    end_bound: &WindowFrameBound,
    state: &WindowFrameStateGroups,
    orderby_col: &ArrayRef,
    most_recent_ob_col: Option<&ArrayRef>,
    sort_options: &SortOptions,
) -> Result<bool> {
    match end_bound {
        WindowFrameBound::Preceding(value) => {
            let zero = ScalarValue::new_zero(&value.data_type())?;
            if value.eq(&zero) {
                is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
            } else {
                Ok(true)
            }
        }
        WindowFrameBound::CurrentRow => {
            is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
        }
        WindowFrameBound::Following(ScalarValue::UInt64(Some(offset))) => {
            let delta = state.group_end_indices.len() - state.current_group_idx;
            if delta == (*offset as usize) + 1 {
                is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
            } else {
                Ok(false)
            }
        }
        _ => Ok(false),
    }
}

/// This utility function checks whether `current_cols` is ahead of the `old_cols`
/// in terms of `sort_options`.
fn is_row_ahead(
    old_col: &ArrayRef,
    current_col: Option<&ArrayRef>,
    sort_options: &SortOptions,
) -> Result<bool> {
    let Some(current_col) = current_col else {
        return Ok(false);
    };
    if old_col.is_empty() || current_col.is_empty() {
        return Ok(false);
    }
    let last_value = ScalarValue::try_from_array(old_col, old_col.len() - 1)?;
    let current_value = ScalarValue::try_from_array(current_col, 0)?;
    let cmp = compare_rows(&[current_value], &[last_value], &[*sort_options])?;
    Ok(cmp.is_gt())
}

/// Get order by expression results inside `order_by_columns`.
pub(crate) fn get_orderby_values(order_by_columns: Vec<SortColumn>) -> Vec<ArrayRef> {
    order_by_columns.into_iter().map(|s| s.values).collect()
}

#[derive(Debug)]
pub enum WindowFn {
    Builtin(Box<dyn PartitionEvaluator>),
    Aggregate(Box<dyn Accumulator>),
}

/// State for the RANK(percent_rank, rank, dense_rank) built-in window function.
#[derive(Debug, Clone, Default)]
pub struct RankState {
    /// The last values for rank as these values change, we increase n_rank
    pub last_rank_data: Option<Vec<ScalarValue>>,
    /// The index where last_rank_boundary is started
    pub last_rank_boundary: usize,
    /// Keep the number of entries in current rank
    pub current_group_count: usize,
    /// Rank number kept from the start
    pub n_rank: usize,
}

/// State for the 'ROW_NUMBER' built-in window function.
#[derive(Debug, Clone, Default)]
pub struct NumRowsState {
    pub n_rows: usize,
}

/// Tag to differentiate special use cases of the NTH_VALUE built-in window function.
#[derive(Debug, Copy, Clone)]
pub enum NthValueKind {
    First,
    Last,
    Nth(i64),
}

#[derive(Debug, Clone)]
pub struct NthValueState {
    // In certain cases, we can finalize the result early. Consider this usage:
    // ```
    //  FIRST_VALUE(increasing_col) OVER window AS my_first_value
    //  WINDOW (ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS window
    // ```
    // The result will always be the first entry in the table. We can store such
    // early-finalizing results and then just reuse them as necessary. This opens
    // opportunities to prune our datasets.
    pub finalized_result: Option<ScalarValue>,
    pub kind: NthValueKind,
}

/// Key for IndexMap for each unique partition
///
/// For instance, if window frame is `OVER(PARTITION BY a,b)`,
/// PartitionKey would consist of unique `[a,b]` pairs
pub type PartitionKey = Vec<ScalarValue>;

#[derive(Debug)]
pub struct WindowState {
    pub state: WindowAggState,
    pub window_fn: WindowFn,
}
pub type PartitionWindowAggStates = IndexMap<PartitionKey, WindowState>;

/// The IndexMap (i.e. an ordered HashMap) where record batches are separated for each partition.
pub type PartitionBatches = IndexMap<PartitionKey, PartitionBatchState>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::window::window_expr::is_row_ahead;

    use arrow_array::{ArrayRef, Float64Array};
    use arrow_schema::SortOptions;
    use datafusion_common::Result;

    #[test]
    fn test_is_row_ahead() -> Result<()> {
        let old_values: ArrayRef =
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 9., 10.]));

        let new_values1: ArrayRef = Arc::new(Float64Array::from(vec![11.0]));
        let new_values2: ArrayRef = Arc::new(Float64Array::from(vec![10.0]));

        assert!(is_row_ahead(
            &old_values,
            Some(&new_values1),
            &SortOptions {
                descending: false,
                nulls_first: false
            }
        )?);
        assert!(!is_row_ahead(
            &old_values,
            Some(&new_values2),
            &SortOptions {
                descending: false,
                nulls_first: false
            }
        )?);

        Ok(())
    }
}
