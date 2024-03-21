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

//! The [`JoinSelection`] rule tries to modify a given plan so that it can
//! accommodate infinite sources and utilize statistical information (if there
//! is any) to obtain more performant plans. To achieve the first goal, it
//! tries to transform a non-runnable query (with the given infinite sources)
//! into a runnable query by replacing pipeline-breaking join operations with
//! pipeline-friendly ones. To achieve the second goal, it selects the proper
//! `PartitionMode` and the build side using the available statistics for hash joins.

use std::sync::Arc;

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, PartitionMode, StreamJoinPartitionMode,
    SymmetricHashJoinExec,
};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use arrow_schema::Schema;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{internal_err, JoinSide, JoinType};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::sort_properties::SortProperties;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

/// The [`JoinSelection`] rule tries to modify a given plan so that it can
/// accommodate infinite sources and optimize joins in the plan according to
/// available statistical information, if there is any.
#[derive(Default)]
pub struct JoinSelection {}

impl JoinSelection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

// TODO: We need some performance test for Right Semi/Right Join swap to Left Semi/Left Join in case that the right side is smaller but not much smaller.
// TODO: In PrestoSQL, the optimizer flips join sides only if one side is much smaller than the other by more than SIZE_DIFFERENCE_THRESHOLD times, by default is 8 times.
/// Checks statistics for join swap.
fn should_swap_join_order(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
) -> Result<bool> {
    // Get the left and right table's total bytes
    // If both the left and right tables contain total_byte_size statistics,
    // use `total_byte_size` to determine `should_swap_join_order`, else use `num_rows`
    let left_stats = left.statistics()?;
    let right_stats = right.statistics()?;
    // First compare `total_byte_size` of left and right side,
    // if information in this field is insufficient fallback to the `num_rows`
    match (
        left_stats.total_byte_size.get_value(),
        right_stats.total_byte_size.get_value(),
    ) {
        (Some(l), Some(r)) => Ok(l > r),
        _ => match (
            left_stats.num_rows.get_value(),
            right_stats.num_rows.get_value(),
        ) {
            (Some(l), Some(r)) => Ok(l > r),
            _ => Ok(false),
        },
    }
}

fn supports_collect_by_thresholds(
    plan: &dyn ExecutionPlan,
    threshold_byte_size: usize,
    threshold_num_rows: usize,
) -> bool {
    // Currently we do not trust the 0 value from stats, due to stats collection might have bug
    // TODO check the logic in datasource::get_statistics_with_limit()
    let Ok(stats) = plan.statistics() else {
        return false;
    };

    if let Some(byte_size) = stats.total_byte_size.get_value() {
        *byte_size != 0 && *byte_size < threshold_byte_size
    } else if let Some(num_rows) = stats.num_rows.get_value() {
        *num_rows != 0 && *num_rows < threshold_num_rows
    } else {
        false
    }
}

/// Predicate that checks whether the given join type supports input swapping.
fn supports_swap(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
    )
}

/// This function returns the new join type we get after swapping the given
/// join's inputs.
fn swap_join_type(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Inner => JoinType::Inner,
        JoinType::Full => JoinType::Full,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        JoinType::LeftSemi => JoinType::RightSemi,
        JoinType::RightSemi => JoinType::LeftSemi,
        JoinType::LeftAnti => JoinType::RightAnti,
        JoinType::RightAnti => JoinType::LeftAnti,
    }
}

/// This function swaps the given join's projection.
fn swap_join_projection(
    left_schema_len: usize,
    right_schema_len: usize,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<usize>> {
    projection.map(|p| {
        p.iter()
            .map(|i| {
                // If the index is less than the left schema length, it is from the left schema, so we add the right schema length to it.
                // Otherwise, it is from the right schema, so we subtract the left schema length from it.
                if *i < left_schema_len {
                    *i + right_schema_len
                } else {
                    *i - left_schema_len
                }
            })
            .collect()
    })
}

/// This function swaps the inputs of the given join operator.
fn swap_hash_join(
    hash_join: &HashJoinExec,
    partition_mode: PartitionMode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    let new_join = HashJoinExec::try_new(
        Arc::clone(right),
        Arc::clone(left),
        hash_join
            .on()
            .iter()
            .map(|(l, r)| (r.clone(), l.clone()))
            .collect(),
        swap_join_filter(hash_join.filter()),
        &swap_join_type(*hash_join.join_type()),
        swap_join_projection(
            left.schema().fields().len(),
            right.schema().fields().len(),
            hash_join.projection.as_ref(),
        ),
        partition_mode,
        hash_join.null_equals_null(),
    )?;
    if matches!(
        hash_join.join_type(),
        JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
    ) {
        Ok(Arc::new(new_join))
    } else {
        // TODO avoid adding ProjectionExec again and again, only adding Final Projection
        let proj = ProjectionExec::try_new(
            swap_reverting_projection(&left.schema(), &right.schema()),
            Arc::new(new_join),
        )?;
        Ok(Arc::new(proj))
    }
}

/// When the order of the join is changed by the optimizer, the columns in
/// the output should not be impacted. This function creates the expressions
/// that will allow to swap back the values from the original left as the first
/// columns and those on the right next.
fn swap_reverting_projection(
    left_schema: &Schema,
    right_schema: &Schema,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    let right_cols = right_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });
    let right_len = right_cols.len();
    let left_cols = left_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(Column::new(f.name(), right_len + i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });

    left_cols.chain(right_cols).collect()
}

/// Swaps join sides for filter column indices and produces new JoinFilter
fn swap_filter(filter: &JoinFilter) -> JoinFilter {
    let column_indices = filter
        .column_indices()
        .iter()
        .map(|idx| ColumnIndex {
            index: idx.index,
            side: idx.side.negate(),
        })
        .collect();

    JoinFilter::new(
        filter.expression().clone(),
        column_indices,
        filter.schema().clone(),
    )
}

/// Swaps join sides for filter column indices and produces new `JoinFilter` (if exists).
fn swap_join_filter(filter: Option<&JoinFilter>) -> Option<JoinFilter> {
    filter.map(swap_filter)
}

impl PhysicalOptimizerRule for JoinSelection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // First, we make pipeline-fixing modifications to joins so as to accommodate
        // unbounded inputs. Each pipeline-fixing subrule, which is a function
        // of type `PipelineFixerSubrule`, takes a single [`PipelineStatePropagator`]
        // argument storing state variables that indicate the unboundedness status
        // of the current [`ExecutionPlan`] as we traverse the plan tree.
        let subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let new_plan = plan
            .transform_up(&|p| apply_subrules(p, &subrules, config))
            .data()?;
        // Next, we apply another subrule that tries to optimize joins using any
        // statistics their inputs might have.
        // - For a hash join with partition mode [`PartitionMode::Auto`], we will
        //   make a cost-based decision to select which `PartitionMode` mode
        //   (`Partitioned`/`CollectLeft`) is optimal. If the statistics information
        //   is not available, we will fall back to [`PartitionMode::Partitioned`].
        // - We optimize/swap join sides so that the left (build) side of the join
        //   is the small side. If the statistics information is not available, we
        //   do not modify join sides.
        // - We will also swap left and right sides for cross joins so that the left
        //   side is the small side.
        let config = &config.optimizer;
        let collect_threshold_byte_size = config.hash_join_single_partition_threshold;
        let collect_threshold_num_rows = config.hash_join_single_partition_threshold_rows;
        new_plan
            .transform_up(&|plan| {
                statistical_join_selection_subrule(
                    plan,
                    collect_threshold_byte_size,
                    collect_threshold_num_rows,
                )
            })
            .data()
    }

    fn name(&self) -> &str {
        "join_selection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Tries to create a [`HashJoinExec`] in [`PartitionMode::CollectLeft`] when possible.
///
/// This function will first consider the given join type and check whether the
/// `CollectLeft` mode is applicable. Otherwise, it will try to swap the join sides.
/// When the `ignore_threshold` is false, this function will also check left
/// and right sizes in bytes or rows.
fn try_collect_left(
    hash_join: &HashJoinExec,
    ignore_threshold: bool,
    threshold_byte_size: usize,
    threshold_num_rows: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let left = hash_join.left();
    let right = hash_join.right();

    let left_can_collect = ignore_threshold
        || supports_collect_by_thresholds(
            &**left,
            threshold_byte_size,
            threshold_num_rows,
        );
    let right_can_collect = ignore_threshold
        || supports_collect_by_thresholds(
            &**right,
            threshold_byte_size,
            threshold_num_rows,
        );

    match (left_can_collect, right_can_collect) {
        (true, true) => {
            if should_swap_join_order(&**left, &**right)?
                && supports_swap(*hash_join.join_type())
            {
                Ok(Some(swap_hash_join(hash_join, PartitionMode::CollectLeft)?))
            } else {
                Ok(Some(Arc::new(HashJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    hash_join.on().to_vec(),
                    hash_join.filter().cloned(),
                    hash_join.join_type(),
                    hash_join.projection.clone(),
                    PartitionMode::CollectLeft,
                    hash_join.null_equals_null(),
                )?)))
            }
        }
        (true, false) => Ok(Some(Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            hash_join.projection.clone(),
            PartitionMode::CollectLeft,
            hash_join.null_equals_null(),
        )?))),
        (false, true) => {
            if supports_swap(*hash_join.join_type()) {
                swap_hash_join(hash_join, PartitionMode::CollectLeft).map(Some)
            } else {
                Ok(None)
            }
        }
        (false, false) => Ok(None),
    }
}

fn partitioned_hash_join(hash_join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    if should_swap_join_order(&**left, &**right)? && supports_swap(*hash_join.join_type())
    {
        swap_hash_join(hash_join, PartitionMode::Partitioned)
    } else {
        Ok(Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            hash_join.projection.clone(),
            PartitionMode::Partitioned,
            hash_join.null_equals_null(),
        )?))
    }
}

/// This subrule tries to modify a given plan so that it can
/// optimize hash and cross joins in the plan according to available statistical information.
fn statistical_join_selection_subrule(
    plan: Arc<dyn ExecutionPlan>,
    collect_threshold_byte_size: usize,
    collect_threshold_num_rows: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let transformed =
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            match hash_join.partition_mode() {
                PartitionMode::Auto => try_collect_left(
                    hash_join,
                    false,
                    collect_threshold_byte_size,
                    collect_threshold_num_rows,
                )?
                .map_or_else(
                    || partitioned_hash_join(hash_join).map(Some),
                    |v| Ok(Some(v)),
                )?,
                PartitionMode::CollectLeft => try_collect_left(hash_join, true, 0, 0)?
                    .map_or_else(
                        || partitioned_hash_join(hash_join).map(Some),
                        |v| Ok(Some(v)),
                    )?,
                PartitionMode::Partitioned => {
                    let left = hash_join.left();
                    let right = hash_join.right();
                    if should_swap_join_order(&**left, &**right)?
                        && supports_swap(*hash_join.join_type())
                    {
                        swap_hash_join(hash_join, PartitionMode::Partitioned).map(Some)?
                    } else {
                        None
                    }
                }
            }
        } else if let Some(cross_join) = plan.as_any().downcast_ref::<CrossJoinExec>() {
            let left = cross_join.left();
            let right = cross_join.right();
            if should_swap_join_order(&**left, &**right)? {
                let new_join = CrossJoinExec::new(Arc::clone(right), Arc::clone(left));
                // TODO avoid adding ProjectionExec again and again, only adding Final Projection
                let proj: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                    swap_reverting_projection(&left.schema(), &right.schema()),
                    Arc::new(new_join),
                )?);
                Some(proj)
            } else {
                None
            }
        } else {
            None
        };

    Ok(if let Some(transformed) = transformed {
        Transformed::yes(transformed)
    } else {
        Transformed::no(plan)
    })
}

/// Pipeline-fixing join selection subrule.
pub type PipelineFixerSubrule =
    dyn Fn(Arc<dyn ExecutionPlan>, &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>>;

/// Converts a hash join to a symmetric hash join in the case of infinite inputs on both sides.
///
/// This subrule checks if a hash join can be replaced with a symmetric hash join when dealing
/// with unbounded (infinite) inputs on both sides. This replacement avoids pipeline breaking and
/// preserves query runnability. If the replacement is applicable, this subrule makes this change;
/// otherwise, it leaves the input unchanged.
///
/// # Arguments
/// * `input` - The current state of the pipeline, including the execution plan.
/// * `config_options` - Configuration options that might affect the transformation logic.
///
/// # Returns
/// An `Option` that contains the `Result` of the transformation. If the transformation is not applicable,
/// it returns `None`. If applicable, it returns `Some(Ok(...))` with the modified pipeline state,
/// or `Some(Err(...))` if an error occurs during the transformation.
fn hash_join_convert_symmetric_subrule(
    input: Arc<dyn ExecutionPlan>,
    config_options: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Check if the current plan node is a HashJoinExec.
    if let Some(hash_join) = input.as_any().downcast_ref::<HashJoinExec>() {
        let left_unbounded = hash_join.left.execution_mode().is_unbounded();
        let right_unbounded = hash_join.right.execution_mode().is_unbounded();
        // Process only if both left and right sides are unbounded.
        if left_unbounded && right_unbounded {
            // Determine the partition mode based on configuration.
            let mode = if config_options.optimizer.repartition_joins {
                StreamJoinPartitionMode::Partitioned
            } else {
                StreamJoinPartitionMode::SinglePartition
            };
            // A closure to determine the required sort order for each side of the join in the SymmetricHashJoinExec.
            // This function checks if the columns involved in the filter have any specific ordering requirements.
            // If the child nodes (left or right side of the join) already have a defined order and the columns used in the
            // filter predicate are ordered, this function captures that ordering requirement. The identified order is then
            // used in the SymmetricHashJoinExec to maintain bounded memory during join operations.
            // However, if the child nodes do not have an inherent order, or if the filter columns are unordered,
            // the function concludes that no specific order is required for the SymmetricHashJoinExec. This approach
            // ensures that the symmetric hash join operation only imposes ordering constraints when necessary,
            // based on the properties of the child nodes and the filter condition.
            let determine_order = |side: JoinSide| -> Option<Vec<PhysicalSortExpr>> {
                hash_join
                    .filter()
                    .map(|filter| {
                        filter.column_indices().iter().any(
                            |ColumnIndex {
                                 index,
                                 side: column_side,
                             }| {
                                // Skip if column side does not match the join side.
                                if *column_side != side {
                                    return false;
                                }
                                // Retrieve equivalence properties and schema based on the side.
                                let (equivalence, schema) = match side {
                                    JoinSide::Left => (
                                        hash_join.left().equivalence_properties(),
                                        hash_join.left().schema(),
                                    ),
                                    JoinSide::Right => (
                                        hash_join.right().equivalence_properties(),
                                        hash_join.right().schema(),
                                    ),
                                };

                                let name = schema.field(*index).name();
                                let col = Arc::new(Column::new(name, *index)) as _;
                                // Check if the column is ordered.
                                equivalence.get_expr_ordering(col).data
                                    != SortProperties::Unordered
                            },
                        )
                    })
                    .unwrap_or(false)
                    .then(|| {
                        match side {
                            JoinSide::Left => hash_join.left().output_ordering(),
                            JoinSide::Right => hash_join.right().output_ordering(),
                        }
                        .map(|p| p.to_vec())
                    })
                    .flatten()
            };

            // Determine the sort order for both left and right sides.
            let left_order = determine_order(JoinSide::Left);
            let right_order = determine_order(JoinSide::Right);

            return SymmetricHashJoinExec::try_new(
                hash_join.left().clone(),
                hash_join.right().clone(),
                hash_join.on().to_vec(),
                hash_join.filter().cloned(),
                hash_join.join_type(),
                hash_join.null_equals_null(),
                left_order,
                right_order,
                mode,
            )
            .map(|exec| Arc::new(exec) as _);
        }
    }
    Ok(input)
}

/// This subrule will swap build/probe sides of a hash join depending on whether
/// one of its inputs may produce an infinite stream of records. The rule ensures
/// that the left (build) side of the hash join always operates on an input stream
/// that will produce a finite set of records. If the left side can not be chosen
/// to be "finite", the join sides stay the same as the original query.
/// ```text
/// For example, this rule makes the following transformation:
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Infinite     |    true      | Hash         |\true
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              |       |              |
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Finite       |    false     | Hash         |/false
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Finite       |    false     | Hash         |\false
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              | true  |              | true
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |-----
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Infinite     |    true      | Hash         |/true
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
/// ```
fn hash_join_swap_subrule(
    mut input: Arc<dyn ExecutionPlan>,
    _config_options: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(hash_join) = input.as_any().downcast_ref::<HashJoinExec>() {
        if hash_join.left.execution_mode().is_unbounded()
            && !hash_join.right.execution_mode().is_unbounded()
            && matches!(
                *hash_join.join_type(),
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
            )
        {
            input = swap_join_according_to_unboundedness(hash_join)?;
        }
    }
    Ok(input)
}

/// This function swaps sides of a hash join to make it runnable even if one of
/// its inputs are infinite. Note that this is not always possible; i.e.
/// [`JoinType::Full`], [`JoinType::Right`], [`JoinType::RightAnti`] and
/// [`JoinType::RightSemi`] can not run with an unbounded left side, even if
/// we swap join sides. Therefore, we do not consider them here.
fn swap_join_according_to_unboundedness(
    hash_join: &HashJoinExec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_mode = hash_join.partition_mode();
    let join_type = hash_join.join_type();
    match (*partition_mode, *join_type) {
        (
            _,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::Full,
        ) => internal_err!("{join_type} join cannot be swapped for unbounded input."),
        (PartitionMode::Partitioned, _) => {
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (PartitionMode::CollectLeft, _) => {
            swap_hash_join(hash_join, PartitionMode::CollectLeft)
        }
        (PartitionMode::Auto, _) => {
            internal_err!("Auto is not acceptable for unbounded input here.")
        }
    }
}

/// Apply given `PipelineFixerSubrule`s to a given plan. This plan, along with
/// auxiliary boundedness information, is in the `PipelineStatePropagator` object.
fn apply_subrules(
    mut input: Arc<dyn ExecutionPlan>,
    subrules: &Vec<Box<PipelineFixerSubrule>>,
    config_options: &ConfigOptions,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    for subrule in subrules {
        input = subrule(input, config_options)?;
    }
    Ok(Transformed::yes(input))
}

#[cfg(test)]
mod tests_statistical {
    use std::sync::Arc;

    use super::*;
    use crate::{
        physical_plan::{
            displayable, joins::PartitionMode, ColumnStatistics, Statistics,
        },
        test::StatisticsExec,
    };

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{stats::Precision, JoinType, ScalarValue};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

    /// Return statistcs for empty table
    fn empty_statistics() -> Statistics {
        Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        }
    }

    /// Get table thresholds: (num_rows, byte_size)
    fn get_thresholds() -> (usize, usize) {
        let optimizer_options = ConfigOptions::new().optimizer;
        (
            optimizer_options.hash_join_single_partition_threshold_rows,
            optimizer_options.hash_join_single_partition_threshold,
        )
    }

    /// Return statistcs for small table
    fn small_statistics() -> Statistics {
        let (threshold_num_rows, threshold_byte_size) = get_thresholds();
        Statistics {
            num_rows: Precision::Inexact(threshold_num_rows / 128),
            total_byte_size: Precision::Inexact(threshold_byte_size / 128),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        }
    }

    /// Return statistcs for big table
    fn big_statistics() -> Statistics {
        let (threshold_num_rows, threshold_byte_size) = get_thresholds();
        Statistics {
            num_rows: Precision::Inexact(threshold_num_rows * 2),
            total_byte_size: Precision::Inexact(threshold_byte_size * 2),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        }
    }

    /// Return statistcs for big table
    fn bigger_statistics() -> Statistics {
        let (threshold_num_rows, threshold_byte_size) = get_thresholds();
        Statistics {
            num_rows: Precision::Inexact(threshold_num_rows * 4),
            total_byte_size: Precision::Inexact(threshold_byte_size * 4),
            column_statistics: vec![ColumnStatistics::new_unknown()],
        }
    }

    fn create_big_and_small() -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
        let big = Arc::new(StatisticsExec::new(
            big_statistics(),
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let small = Arc::new(StatisticsExec::new(
            small_statistics(),
            Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
        ));
        (big, small)
    }

    /// Create a column statistics vector for a single column
    /// that has the given min/max/distinct_count properties.
    ///
    /// Given min/max will be mapped to a [`ScalarValue`] if
    /// they are not `None`.
    fn create_column_stats(
        min: Option<u64>,
        max: Option<u64>,
        distinct_count: Option<usize>,
    ) -> Vec<ColumnStatistics> {
        vec![ColumnStatistics {
            distinct_count: distinct_count
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent),
            min_value: min
                .map(|size| Precision::Inexact(ScalarValue::UInt64(Some(size))))
                .unwrap_or(Precision::Absent),
            max_value: max
                .map(|size| Precision::Inexact(ScalarValue::UInt64(Some(size))))
                .unwrap_or(Precision::Absent),
            ..Default::default()
        }]
    }

    /// Returns three plans with statistics of (min, max, distinct_count)
    /// * big 100K rows @ (0, 50k, 50k)
    /// * medium 10K rows @ (1k, 5k, 1k)
    /// * small 1K rows @ (0, 100k, 1k)
    fn create_nested_with_min_max() -> (
        Arc<dyn ExecutionPlan>,
        Arc<dyn ExecutionPlan>,
        Arc<dyn ExecutionPlan>,
    ) {
        let big = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(100_000),
                column_statistics: create_column_stats(
                    Some(0),
                    Some(50_000),
                    Some(50_000),
                ),
                total_byte_size: Precision::Absent,
            },
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let medium = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(10_000),
                column_statistics: create_column_stats(
                    Some(1000),
                    Some(5000),
                    Some(1000),
                ),
                total_byte_size: Precision::Absent,
            },
            Schema::new(vec![Field::new("medium_col", DataType::Int32, false)]),
        ));

        let small = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(1000),
                column_statistics: create_column_stats(
                    Some(0),
                    Some(100_000),
                    Some(1000),
                ),
                total_byte_size: Precision::Absent,
            },
            Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
        ));

        (big, medium, small)
    }

    pub(crate) fn crosscheck_plans(plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let new_plan = plan
            .transform_up(&|p| apply_subrules(p, &subrules, &ConfigOptions::new()))
            .data()?;
        // TODO: End state payloads will be checked here.
        let config = ConfigOptions::new().optimizer;
        let collect_left_threshold = config.hash_join_single_partition_threshold;
        let collect_threshold_num_rows = config.hash_join_single_partition_threshold_rows;
        let _ = new_plan.transform_up(&|plan| {
            statistical_join_selection_subrule(
                plan,
                collect_left_threshold,
                collect_threshold_num_rows,
            )
        })?;
        Ok(())
    }

    #[tokio::test]
    async fn test_join_with_swap() {
        let (big, small) = create_big_and_small();

        let join = Arc::new(
            HashJoinExec::try_new(
                Arc::clone(&big),
                Arc::clone(&small),
                vec![(
                    Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                    Arc::new(
                        Column::new_with_schema("small_col", &small.schema()).unwrap(),
                    ),
                )],
                None,
                &JoinType::Left,
                None,
                PartitionMode::CollectLeft,
                false,
            )
            .unwrap(),
        );

        let optimized_join = JoinSelection::new()
            .optimize(join.clone(), &ConfigOptions::new())
            .unwrap();

        let swapping_projection = optimized_join
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("A proj is required to swap columns back to their original order");

        assert_eq!(swapping_projection.expr().len(), 2);
        let (col, name) = &swapping_projection.expr()[0];
        assert_eq!(name, "big_col");
        assert_col_expr(col, "big_col", 1);
        let (col, name) = &swapping_projection.expr()[1];
        assert_eq!(name, "small_col");
        assert_col_expr(col, "small_col", 0);

        let swapped_join = swapping_projection
            .input()
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(
            swapped_join.left().statistics().unwrap().total_byte_size,
            Precision::Inexact(8192)
        );
        assert_eq!(
            swapped_join.right().statistics().unwrap().total_byte_size,
            Precision::Inexact(2097152)
        );
        crosscheck_plans(join.clone()).unwrap();
    }

    #[tokio::test]
    async fn test_left_join_no_swap() {
        let (big, small) = create_big_and_small();

        let join = Arc::new(
            HashJoinExec::try_new(
                Arc::clone(&small),
                Arc::clone(&big),
                vec![(
                    Arc::new(
                        Column::new_with_schema("small_col", &small.schema()).unwrap(),
                    ),
                    Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                )],
                None,
                &JoinType::Left,
                None,
                PartitionMode::CollectLeft,
                false,
            )
            .unwrap(),
        );

        let optimized_join = JoinSelection::new()
            .optimize(join.clone(), &ConfigOptions::new())
            .unwrap();

        let swapped_join = optimized_join
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(
            swapped_join.left().statistics().unwrap().total_byte_size,
            Precision::Inexact(8192)
        );
        assert_eq!(
            swapped_join.right().statistics().unwrap().total_byte_size,
            Precision::Inexact(2097152)
        );
        crosscheck_plans(join.clone()).unwrap();
    }

    #[tokio::test]
    async fn test_join_with_swap_semi() {
        let join_types = [JoinType::LeftSemi, JoinType::LeftAnti];
        for join_type in join_types {
            let (big, small) = create_big_and_small();

            let join = Arc::new(
                HashJoinExec::try_new(
                    Arc::clone(&big),
                    Arc::clone(&small),
                    vec![(
                        Arc::new(
                            Column::new_with_schema("big_col", &big.schema()).unwrap(),
                        ),
                        Arc::new(
                            Column::new_with_schema("small_col", &small.schema())
                                .unwrap(),
                        ),
                    )],
                    None,
                    &join_type,
                    None,
                    PartitionMode::Partitioned,
                    false,
                )
                .unwrap(),
            );

            let original_schema = join.schema();

            let optimized_join = JoinSelection::new()
                .optimize(join.clone(), &ConfigOptions::new())
                .unwrap();

            let swapped_join = optimized_join
                .as_any()
                .downcast_ref::<HashJoinExec>()
                .expect(
                    "A proj is not required to swap columns back to their original order",
                );

            assert_eq!(swapped_join.schema().fields().len(), 1);
            assert_eq!(
                swapped_join.left().statistics().unwrap().total_byte_size,
                Precision::Inexact(8192)
            );
            assert_eq!(
                swapped_join.right().statistics().unwrap().total_byte_size,
                Precision::Inexact(2097152)
            );
            assert_eq!(original_schema, swapped_join.schema());
            crosscheck_plans(join).unwrap();
        }
    }

    /// Compare the input plan with the plan after running the probe order optimizer.
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines =
                $EXPECTED_LINES.iter().map(|s| *s).collect::<Vec<&str>>();

            let plan = Arc::new($PLAN);
            let optimized = JoinSelection::new()
                .optimize(plan.clone(), &ConfigOptions::new())
                .unwrap();

            let plan_string = displayable(optimized.as_ref()).indent(true).to_string();
            let actual_lines = plan_string.split("\n").collect::<Vec<&str>>();

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
            crosscheck_plans(plan).unwrap();
        };
    }

    #[tokio::test]
    async fn test_nested_join_swap() {
        let (big, medium, small) = create_nested_with_min_max();

        // Form the inner join: big JOIN small
        let child_join = HashJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            vec![(
                Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            false,
        )
        .unwrap();
        let child_schema = child_join.schema();

        // Form join tree `medium LEFT JOIN (big JOIN small)`
        let join = HashJoinExec::try_new(
            Arc::clone(&medium),
            Arc::new(child_join),
            vec![(
                Arc::new(
                    Column::new_with_schema("medium_col", &medium.schema()).unwrap(),
                ),
                Arc::new(Column::new_with_schema("small_col", &child_schema).unwrap()),
            )],
            None,
            &JoinType::Left,
            None,
            PartitionMode::CollectLeft,
            false,
        )
        .unwrap();

        // Hash join uses the left side to build the hash table, and right side to probe it. We want
        // to keep left as small as possible, so if we can estimate (with a reasonable margin of error)
        // that the left side is smaller than the right side, we should swap the sides.
        //
        // The first hash join's left is 'small' table (with 1000 rows), and the second hash join's
        // left is the F(small IJ big) which has an estimated cardinality of 2000 rows (vs medium which
        // has an exact cardinality of 10_000 rows).
        let expected = [
            "ProjectionExec: expr=[medium_col@2 as medium_col, big_col@0 as big_col, small_col@1 as small_col]",
            "  HashJoinExec: mode=CollectLeft, join_type=Right, on=[(small_col@1, medium_col@0)]",
            "    ProjectionExec: expr=[big_col@1 as big_col, small_col@0 as small_col]",
            "      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(small_col@0, big_col@0)]",
            "        StatisticsExec: col_count=1, row_count=Inexact(1000)",
            "        StatisticsExec: col_count=1, row_count=Inexact(100000)",
            "    StatisticsExec: col_count=1, row_count=Inexact(10000)",
            "",
        ];
        assert_optimized!(expected, join);
    }

    #[tokio::test]
    async fn test_join_no_swap() {
        let (big, small) = create_big_and_small();
        let join = Arc::new(
            HashJoinExec::try_new(
                Arc::clone(&small),
                Arc::clone(&big),
                vec![(
                    Arc::new(
                        Column::new_with_schema("small_col", &small.schema()).unwrap(),
                    ),
                    Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                false,
            )
            .unwrap(),
        );

        let optimized_join = JoinSelection::new()
            .optimize(join.clone(), &ConfigOptions::new())
            .unwrap();

        let swapped_join = optimized_join
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(
            swapped_join.left().statistics().unwrap().total_byte_size,
            Precision::Inexact(8192)
        );
        assert_eq!(
            swapped_join.right().statistics().unwrap().total_byte_size,
            Precision::Inexact(2097152)
        );
        crosscheck_plans(join).unwrap();
    }

    #[tokio::test]
    async fn test_swap_reverting_projection() {
        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![Field::new("c", DataType::Int32, false)]);

        let proj = swap_reverting_projection(&left_schema, &right_schema);

        assert_eq!(proj.len(), 3);

        let (col, name) = &proj[0];
        assert_eq!(name, "a");
        assert_col_expr(col, "a", 1);

        let (col, name) = &proj[1];
        assert_eq!(name, "b");
        assert_col_expr(col, "b", 2);

        let (col, name) = &proj[2];
        assert_eq!(name, "c");
        assert_col_expr(col, "c", 0);
    }

    fn assert_col_expr(expr: &Arc<dyn PhysicalExpr>, name: &str, index: usize) {
        let col = expr
            .as_any()
            .downcast_ref::<Column>()
            .expect("Projection items should be Column expression");
        assert_eq!(col.name(), name);
        assert_eq!(col.index(), index);
    }

    #[tokio::test]
    async fn test_join_selection_collect_left() {
        let big = Arc::new(StatisticsExec::new(
            big_statistics(),
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let small = Arc::new(StatisticsExec::new(
            small_statistics(),
            Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
        ));

        let empty = Arc::new(StatisticsExec::new(
            empty_statistics(),
            Schema::new(vec![Field::new("empty_col", DataType::Int32, false)]),
        ));

        let join_on = vec![(
            Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(
            small.clone(),
            big.clone(),
            join_on,
            false,
            PartitionMode::CollectLeft,
        );

        let join_on = vec![(
            Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(
            big.clone(),
            small.clone(),
            join_on,
            true,
            PartitionMode::CollectLeft,
        );

        let join_on = vec![(
            Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("empty_col", &empty.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(
            small.clone(),
            empty.clone(),
            join_on,
            false,
            PartitionMode::CollectLeft,
        );

        let join_on = vec![(
            Arc::new(Column::new_with_schema("empty_col", &empty.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(
            empty.clone(),
            small.clone(),
            join_on,
            true,
            PartitionMode::CollectLeft,
        );
    }

    #[tokio::test]
    async fn test_join_selection_partitioned() {
        let bigger = Arc::new(StatisticsExec::new(
            bigger_statistics(),
            Schema::new(vec![Field::new("bigger_col", DataType::Int32, false)]),
        ));

        let big = Arc::new(StatisticsExec::new(
            big_statistics(),
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let empty = Arc::new(StatisticsExec::new(
            empty_statistics(),
            Schema::new(vec![Field::new("empty_col", DataType::Int32, false)]),
        ));

        let join_on = vec![(
            Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("bigger_col", &bigger.schema()).unwrap())
                as _,
        )];
        check_join_partition_mode(
            big.clone(),
            bigger.clone(),
            join_on,
            false,
            PartitionMode::Partitioned,
        );

        let join_on = vec![(
            Arc::new(Column::new_with_schema("bigger_col", &bigger.schema()).unwrap())
                as _,
            Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(
            bigger.clone(),
            big.clone(),
            join_on,
            true,
            PartitionMode::Partitioned,
        );

        let join_on = vec![(
            Arc::new(Column::new_with_schema("empty_col", &empty.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(
            empty.clone(),
            big.clone(),
            join_on,
            false,
            PartitionMode::Partitioned,
        );

        let join_on = vec![(
            Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("empty_col", &empty.schema()).unwrap()) as _,
        )];
        check_join_partition_mode(big, empty, join_on, false, PartitionMode::Partitioned);
    }

    fn check_join_partition_mode(
        left: Arc<StatisticsExec>,
        right: Arc<StatisticsExec>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        is_swapped: bool,
        expected_mode: PartitionMode,
    ) {
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Auto,
                false,
            )
            .unwrap(),
        );

        let optimized_join = JoinSelection::new()
            .optimize(join.clone(), &ConfigOptions::new())
            .unwrap();

        if !is_swapped {
            let swapped_join = optimized_join
                .as_any()
                .downcast_ref::<HashJoinExec>()
                .expect("The type of the plan should not be changed");
            assert_eq!(*swapped_join.partition_mode(), expected_mode);
        } else {
            let swapping_projection = optimized_join
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .expect(
                    "A proj is required to swap columns back to their original order",
                );
            let swapped_join = swapping_projection
                .input()
                .as_any()
                .downcast_ref::<HashJoinExec>()
                .expect("The type of the plan should not be changed");

            assert_eq!(*swapped_join.partition_mode(), expected_mode);
        }
        crosscheck_plans(join).unwrap();
    }
}

#[cfg(test)]
mod util_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, NegativeExpr};
    use datafusion_physical_expr::intervals::utils::check_support;
    use datafusion_physical_expr::PhysicalExpr;

    #[test]
    fn check_expr_supported() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let supported_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("a", 0)),
        )) as Arc<dyn PhysicalExpr>;
        assert!(check_support(&supported_expr, &schema));
        let supported_expr_2 = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        assert!(check_support(&supported_expr_2, &schema));
        let unsupported_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Or,
            Arc::new(Column::new("a", 0)),
        )) as Arc<dyn PhysicalExpr>;
        assert!(!check_support(&unsupported_expr, &schema));
        let unsupported_expr_2 = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Or,
            Arc::new(NegativeExpr::new(Arc::new(Column::new("a", 0)))),
        )) as Arc<dyn PhysicalExpr>;
        assert!(!check_support(&unsupported_expr_2, &schema));
    }
}

#[cfg(test)]
mod hash_join_tests {
    use std::sync::Arc;

    use self::tests_statistical::crosscheck_plans;
    use super::*;
    use crate::physical_optimizer::join_selection::swap_join_type;
    use crate::physical_optimizer::test_utils::SourceType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::joins::PartitionMode;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::test_util::UnboundedExec;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::utils::DataPtr;
    use datafusion_common::JoinType;
    use datafusion_physical_plan::ExecutionPlanProperties;

    struct TestCase {
        case: String,
        initial_sources_unbounded: (SourceType, SourceType),
        initial_join_type: JoinType,
        initial_mode: PartitionMode,
        expected_sources_unbounded: (SourceType, SourceType),
        expected_join_type: JoinType,
        expected_mode: PartitionMode,
        expecting_swap: bool,
    }

    #[tokio::test]
    async fn test_join_with_swap_full() -> Result<()> {
        // NOTE: Currently, some initial conditions are not viable after join order selection.
        //       For example, full join always comes in partitioned mode. See the warning in
        //       function "swap". If this changes in the future, we should update these tests.
        let cases = vec![
            TestCase {
                case: "Bounded - Unbounded 1".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Unbounded - Bounded 2".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Bounded - Bounded 3".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Unbounded - Unbounded 4".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
        ];
        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cases_without_collect_left_check() -> Result<()> {
        let mut cases = vec![];
        let join_types = vec![JoinType::LeftSemi, JoinType::Inner];
        for join_type in join_types {
            cases.push(TestCase {
                case: "Unbounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_not_support_collect_left() -> Result<()> {
        let mut cases = vec![];
        // After [JoinSelection] optimization, these join types cannot run in CollectLeft mode except
        // [JoinType::LeftSemi]
        let the_ones_not_support_collect_left = vec![JoinType::Left, JoinType::LeftAnti];
        for join_type in the_ones_not_support_collect_left {
            cases.push(TestCase {
                case: "Unbounded - Bounded".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_not_supporting_swaps_possible_collect_left() -> Result<()> {
        let mut cases = vec![];
        let the_ones_not_support_collect_left =
            vec![JoinType::Right, JoinType::RightAnti, JoinType::RightSemi];
        for join_type in the_ones_not_support_collect_left {
            // We expect that (SourceType::Unbounded, SourceType::Bounded) will change, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Unbounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            // We expect that (SourceType::Bounded, SourceType::Unbounded) will stay same, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Bounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            //
            cases.push(TestCase {
                case: "Bounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            // If cases are partitioned, only unbounded & bounded check will affect the order.
            cases.push(TestCase {
                case: "Unbounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) -> Result<()> {
        let left_unbounded = t.initial_sources_unbounded.0 == SourceType::Unbounded;
        let right_unbounded = t.initial_sources_unbounded.1 == SourceType::Unbounded;
        let left_exec = Arc::new(UnboundedExec::new(
            (!left_unbounded).then_some(1),
            RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Int32,
                false,
            )]))),
            2,
        )) as Arc<dyn ExecutionPlan>;
        let right_exec = Arc::new(UnboundedExec::new(
            (!right_unbounded).then_some(1),
            RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                "b",
                DataType::Int32,
                false,
            )]))),
            2,
        )) as Arc<dyn ExecutionPlan>;

        let join = Arc::new(HashJoinExec::try_new(
            Arc::clone(&left_exec),
            Arc::clone(&right_exec),
            vec![(
                Arc::new(Column::new_with_schema("a", &left_exec.schema())?),
                Arc::new(Column::new_with_schema("b", &right_exec.schema())?),
            )],
            None,
            &t.initial_join_type,
            None,
            t.initial_mode,
            false,
        )?);

        let optimized_join_plan =
            hash_join_swap_subrule(join.clone(), &ConfigOptions::new())?;

        // If swap did happen
        let projection_added = optimized_join_plan.as_any().is::<ProjectionExec>();
        let plan = if projection_added {
            let proj = optimized_join_plan
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .expect(
                    "A proj is required to swap columns back to their original order",
                );
            proj.input().clone()
        } else {
            optimized_join_plan
        };

        if let Some(HashJoinExec {
            left,
            right,
            join_type,
            mode,
            ..
        }) = plan.as_any().downcast_ref::<HashJoinExec>()
        {
            let left_changed = Arc::data_ptr_eq(left, &right_exec);
            let right_changed = Arc::data_ptr_eq(right, &left_exec);
            // If this is not equal, we have a bigger problem.
            assert_eq!(left_changed, right_changed);
            assert_eq!(
                (
                    t.case.as_str(),
                    if left.execution_mode().is_unbounded() {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    if right.execution_mode().is_unbounded() {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    join_type,
                    mode,
                    left_changed && right_changed
                ),
                (
                    t.case.as_str(),
                    t.expected_sources_unbounded.0,
                    t.expected_sources_unbounded.1,
                    &t.expected_join_type,
                    &t.expected_mode,
                    t.expecting_swap
                )
            );
        };
        crosscheck_plans(plan).unwrap();
        Ok(())
    }
}
