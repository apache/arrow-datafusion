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

//! Collection of utility functions that are leveraged by the query optimizer rules

use datafusion_expr::RecursiveQuery;
use datafusion_physical_plan::recursive_query::RecursiveQueryExec;
use itertools::concat;
use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{displayable, ExecutionPlan};

use datafusion_physical_expr::{LexRequirementRef, PhysicalSortRequirement};

/// This object implements a tree that we use while keeping track of paths
/// leading to [`SortExec`]s.
#[derive(Debug, Clone)]
pub(crate) struct ExecTree {
    /// The `ExecutionPlan` associated with this node
    pub plan: Arc<dyn ExecutionPlan>,
    /// Child index of the plan in its parent
    pub idx: usize,
    /// Children of the plan that would need updating if we remove leaf executors
    pub children: Vec<ExecTree>,
}

impl fmt::Display for ExecTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let plan_string = get_plan_string(&self.plan);
        write!(f, "\nidx: {:?}", self.idx)?;
        write!(f, "\nplan: {:?}", plan_string)?;
        for child in self.children.iter() {
            write!(f, "\nexec_tree:{}", child)?;
        }
        writeln!(f)
    }
}

impl ExecTree {
    /// Create new Exec tree
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        idx: usize,
        children: Vec<ExecTree>,
    ) -> Self {
        ExecTree {
            plan,
            idx,
            children,
        }
    }
}

/// Get `ExecTree` for each child of the plan if they are tracked.
/// # Arguments
///
/// * `n_children` - Children count of the plan of interest
/// * `onward` - Contains `Some(ExecTree)` of the plan tracked.
///            - Contains `None` is plan is not tracked.
///
/// # Returns
///
/// A `Vec<Option<ExecTree>>` that contains tracking information of each child.
/// If a child is `None`, it is not tracked. If `Some(ExecTree)` child is tracked also.
pub(crate) fn get_children_exectrees(
    n_children: usize,
    onward: &Option<ExecTree>,
) -> Vec<Option<ExecTree>> {
    let mut children_onward = vec![None; n_children];
    if let Some(exec_tree) = &onward {
        for child in &exec_tree.children {
            children_onward[child.idx] = Some(child.clone());
        }
    }
    children_onward
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning.
pub fn add_sort_above(
    node: &mut Arc<dyn ExecutionPlan>,
    sort_requirement: LexRequirementRef,
    fetch: Option<usize>,
) {
    // If the ordering requirement is already satisfied, do not add a sort.
    if !node
        .equivalence_properties()
        .ordering_satisfy_requirement(sort_requirement)
    {
        let sort_expr = PhysicalSortRequirement::to_sort_exprs(sort_requirement.to_vec());
        let new_sort = SortExec::new(sort_expr, node.clone()).with_fetch(fetch);

        *node = Arc::new(if node.output_partitioning().partition_count() > 1 {
            new_sort.with_preserve_partitioning(true)
        } else {
            new_sort
        }) as _
    }
}

/// Checks whether the given operator is a limit;
/// i.e. either a [`LocalLimitExec`] or a [`GlobalLimitExec`].
pub fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>() || plan.as_any().is::<LocalLimitExec>()
}

/// Checks whether the given operator is a window;
/// i.e. either a [`WindowAggExec`] or a [`BoundedWindowAggExec`].
pub fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<WindowAggExec>() || plan.as_any().is::<BoundedWindowAggExec>()
}

/// Checks whether the given operator is a [`SortExec`].
pub fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortExec>()
}

/// Checks whether the given operator is a [`SortPreservingMergeExec`].
pub fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortPreservingMergeExec>()
}

/// Checks whether the given operator is a [`CoalescePartitionsExec`].
pub fn is_coalesce_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<CoalescePartitionsExec>()
}

/// Checks whether the given operator is a [`UnionExec`].
pub fn is_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<UnionExec>()
}

/// Checks whether the given operator is a [`RepartitionExec`].
pub fn is_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<RepartitionExec>()
}

pub fn is_recursive_query(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<RecursiveQueryExec>()
}

/// Utility function yielding a string representation of the given [`ExecutionPlan`].
pub fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    actual.iter().map(|elem| elem.to_string()).collect()
}
