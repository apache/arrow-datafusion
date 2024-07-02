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

//! Planner for [`LogicalPlan`] to [`ExecutionPlan`]

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use crate::datasource::file_format::file_type_to_format;
use crate::datasource::listing::ListingTableUrl;
use crate::datasource::physical_plan::FileSinkConfig;
use crate::datasource::source_as_provider;
use crate::error::{DataFusionError, Result};
use crate::execution::context::{ExecutionProps, SessionState};
use crate::logical_expr::utils::generate_sort_key;
use crate::logical_expr::{
    Aggregate, EmptyRelation, Join, Projection, Sort, TableScan, Unnest, Window,
};
use crate::logical_expr::{
    Expr, LogicalPlan, Partitioning as LogicalPartitioning, PlanType, Repartition,
    UserDefinedLogicalNode,
};
use crate::logical_expr::{Limit, Values};
use crate::physical_expr::{create_physical_expr, create_physical_exprs};
use crate::physical_optimizer::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::analyze::AnalyzeExec;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils as join_utils;
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::recursive_query::RecursiveQueryExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::unnest::UnnestExec;
use crate::physical_plan::values::ValuesExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{
    aggregates, displayable, udaf, windows, AggregateExpr, ExecutionPlan,
    ExecutionPlanProperties, InputOrderMode, Partitioning, PhysicalExpr, WindowExpr,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use datafusion_common::display::ToStringifiedPlan;
use datafusion_common::{
    exec_err, internal_datafusion_err, internal_err, not_impl_err, plan_err, DFSchema,
    ScalarValue,
};
use datafusion_expr::dml::CopyTo;
use datafusion_expr::expr::{
    self, AggregateFunction, AggregateFunctionDefinition, Alias, Between, BinaryExpr,
    Cast, GroupingSet, InList, Like, TryCast, WindowFunction,
};
use datafusion_expr::expr_rewriter::unnormalize_cols;
use datafusion_expr::expr_vec_fmt;
use datafusion_expr::logical_plan::builder::wrap_projection_for_join_if_necessary;
use datafusion_expr::{
    DescribeTable, DmlStatement, Extension, Filter, RecursiveQuery, StringifiedPlan,
    WindowFrame, WindowFrameBound, WriteOp,
};
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_sql::utils::window_expr_common_partition_keys;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use itertools::{multiunzip, Itertools};
use log::{debug, trace};
use sqlparser::ast::NullTreatment;
use tokio::sync::Mutex;

fn create_function_physical_name(
    fun: &str,
    distinct: bool,
    args: &[Expr],
    order_by: Option<&Vec<Expr>>,
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_physical_name(e, false))
        .collect::<Result<_>>()?;

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };

    let phys_name = format!("{}({}{})", fun, distinct_str, names.join(","));

    Ok(order_by
        .map(|order_by| format!("{} ORDER BY [{}]", phys_name, expr_vec_fmt!(order_by)))
        .unwrap_or(phys_name))
}

fn physical_name(e: &Expr) -> Result<String> {
    create_physical_name(e, true)
}

fn create_physical_name(e: &Expr, is_first_expr: bool) -> Result<String> {
    match e {
        Expr::Unnest(_) => {
            internal_err!(
                "Expr::Unnest should have been converted to LogicalPlan::Unnest"
            )
        }
        Expr::Column(c) => {
            if is_first_expr {
                Ok(c.name.clone())
            } else {
                Ok(c.flat_name())
            }
        }
        Expr::Alias(Alias { name, .. }) => Ok(name.clone()),
        Expr::ScalarVariable(_, variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{left} {op} {right}"))
        }
        Expr::Case(case) => {
            let mut name = "CASE ".to_string();
            if let Some(e) = &case.expr {
                let _ = write!(name, "{} ", create_physical_name(e, false)?);
            }
            for (w, t) in &case.when_then_expr {
                let _ = write!(
                    name,
                    "WHEN {} THEN {} ",
                    create_physical_name(w, false)?,
                    create_physical_name(t, false)?
                );
            }
            if let Some(e) = &case.else_expr {
                let _ = write!(name, "ELSE {} ", create_physical_name(e, false)?);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast(Cast { expr, .. }) => {
            // CAST does not change the expression name
            create_physical_name(expr, false)
        }
        Expr::TryCast(TryCast { expr, .. }) => {
            // CAST does not change the expression name
            create_physical_name(expr, false)
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::Negative(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("(- {expr})"))
        }
        Expr::IsNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsUnknown(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS UNKNOWN"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT TRUE"))
        }
        Expr::IsNotFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::IsNotUnknown(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT UNKNOWN"))
        }
        Expr::ScalarFunction(fun) => fun.func.display_name(&fun.args),
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            order_by,
            ..
        }) => {
            create_function_physical_name(&fun.to_string(), false, args, Some(order_by))
        }
        Expr::AggregateFunction(AggregateFunction {
            func_def,
            distinct,
            args,
            filter: _,
            order_by,
            null_treatment: _,
        }) => create_function_physical_name(
            func_def.name(),
            *distinct,
            args,
            order_by.as_ref(),
        ),
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => Ok(format!(
                "ROLLUP ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::Cube(exprs) => Ok(format!(
                "CUBE ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::GroupingSets(lists_of_exprs) => {
                let mut strings = vec![];
                for exprs in lists_of_exprs {
                    let exprs_str = exprs
                        .iter()
                        .map(|e| create_physical_name(e, false))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    strings.push(format!("({exprs_str})"));
                }
                Ok(format!("GROUPING SETS ({})", strings.join(", ")))
            }
        },

        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let list = list.iter().map(|expr| create_physical_name(expr, false));
            if *negated {
                Ok(format!("{expr} NOT IN ({list:?})"))
            } else {
                Ok(format!("{expr} IN ({list:?})"))
            }
        }
        Expr::Exists { .. } => {
            not_impl_err!("EXISTS is not yet supported in the physical plan")
        }
        Expr::InSubquery(_) => {
            not_impl_err!("IN subquery is not yet supported in the physical plan")
        }
        Expr::ScalarSubquery(_) => {
            not_impl_err!("Scalar subqueries are not yet supported in the physical plan")
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let low = create_physical_name(low, false)?;
            let high = create_physical_name(high, false)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} AND {high}"))
            } else {
                Ok(format!("{expr} BETWEEN {low} AND {high}"))
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT {op_name} {pattern}{escape}"))
            } else {
                Ok(format!("{expr} {op_name} {pattern}{escape}"))
            }
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive: _,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT SIMILAR TO {pattern}{escape}"))
            } else {
                Ok(format!("{expr} SIMILAR TO {pattern}{escape}"))
            }
        }
        Expr::Sort { .. } => {
            internal_err!("Create physical name does not support sort expression")
        }
        Expr::Wildcard { .. } => {
            internal_err!("Create physical name does not support wildcard")
        }
        Expr::Placeholder(_) => {
            internal_err!("Create physical name does not support placeholder")
        }
        Expr::OuterReferenceColumn(_, _) => {
            internal_err!("Create physical name does not support OuterReferenceColumn")
        }
    }
}

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
#[async_trait]
pub trait ExtensionPlanner {
    /// Create a physical plan for a [`UserDefinedLogicalNode`].
    ///
    /// `input_dfschema`: the logical plan schema for the inputs to this node
    ///
    /// Returns an error when the planner knows how to plan the concrete
    /// implementation of `node` but errors while doing so.
    ///
    /// Returns `None` when the planner does not know how to plan the
    /// `node` and wants to delegate the planning to another
    /// [`ExtensionPlanner`].
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
///
/// This planner will first flatten the `LogicalPlan` tree via a
/// depth first approach, which allows it to identify the leaves
/// of the tree.
///
/// Tasks are spawned from these leaves and traverse back up the
/// tree towards the root, converting each `LogicalPlan` node it
/// reaches into their equivalent `ExecutionPlan` node. When these
/// tasks reach a common node, they will terminate until the last
/// task reaches the node which will then continue building up the
/// tree.
///
/// Up to [`planning_concurrency`] tasks are buffered at once to
/// execute concurrently.
///
/// [`planning_concurrency`]: crate::config::ExecutionOptions::planning_concurrency
#[derive(Default)]
pub struct DefaultPhysicalPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.handle_explain(logical_plan, session_state).await? {
            Some(plan) => Ok(plan),
            None => {
                let plan = self
                    .create_initial_plan(logical_plan, session_state)
                    .await?;

                self.optimize_internal(plan, session_state, |_, _| {})
            }
        }
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `e`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `e`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(expr, input_dfschema, session_state.execution_props())
    }
}

#[derive(Debug)]
struct ExecutionPlanChild {
    /// Index needed to order children of parent to ensure consistency with original
    /// `LogicalPlan`
    index: usize,
    plan: Arc<dyn ExecutionPlan>,
}

#[derive(Debug)]
enum NodeState {
    ZeroOrOneChild,
    /// Nodes with multiple children will have multiple tasks accessing it,
    /// and each task will append their contribution until the last task takes
    /// all the children to build the parent node.
    TwoOrMoreChildren(Mutex<Vec<ExecutionPlanChild>>),
}

/// To avoid needing to pass single child wrapped in a Vec for nodes
/// with only one child.
enum ChildrenContainer {
    None,
    One(Arc<dyn ExecutionPlan>),
    Multiple(Vec<Arc<dyn ExecutionPlan>>),
}

impl ChildrenContainer {
    fn one(self) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::One(p) => Ok(p),
            _ => internal_err!("More than one child in ChildrenContainer"),
        }
    }

    fn two(self) -> Result<[Arc<dyn ExecutionPlan>; 2]> {
        match self {
            Self::Multiple(v) if v.len() == 2 => Ok(v.try_into().unwrap()),
            _ => internal_err!("ChildrenContainer doesn't contain exactly 2 children"),
        }
    }

    fn vec(self) -> Vec<Arc<dyn ExecutionPlan>> {
        match self {
            Self::None => vec![],
            Self::One(p) => vec![p],
            Self::Multiple(v) => v,
        }
    }
}

#[derive(Debug)]
struct LogicalNode<'a> {
    node: &'a LogicalPlan,
    // None if root
    parent_index: Option<usize>,
    state: NodeState,
}

impl DefaultPhysicalPlanner {
    /// Create a physical planner that uses `extension_planners` to
    /// plan user-defined logical nodes [`LogicalPlan::Extension`].
    /// The planner uses the first [`ExtensionPlanner`] to return a non-`None`
    /// plan.
    pub fn with_extension_planners(
        extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        Self { extension_planners }
    }

    /// Create a physical plan from a logical plan
    async fn create_initial_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // DFS the tree to flatten it into a Vec.
        // This will allow us to build the Physical Plan from the leaves up
        // to avoid recursion, and also to make it easier to build a valid
        // Physical Plan from the start and not rely on some intermediate
        // representation (since parents need to know their children at
        // construction time).
        let mut flat_tree = vec![];
        let mut dfs_visit_stack = vec![(None, logical_plan)];
        // Use this to be able to find the leaves to start construction bottom
        // up concurrently.
        let mut flat_tree_leaf_indices = vec![];
        while let Some((parent_index, node)) = dfs_visit_stack.pop() {
            let current_index = flat_tree.len();
            // Because of how we extend the visit stack here, we visit the children
            // in reverse order of how they appear, so later we need to reverse
            // the order of children when building the nodes.
            dfs_visit_stack
                .extend(node.inputs().iter().map(|&n| (Some(current_index), n)));
            let state = match node.inputs().len() {
                0 => {
                    flat_tree_leaf_indices.push(current_index);
                    NodeState::ZeroOrOneChild
                }
                1 => NodeState::ZeroOrOneChild,
                _ => {
                    let ready_children = Vec::with_capacity(node.inputs().len());
                    let ready_children = Mutex::new(ready_children);
                    NodeState::TwoOrMoreChildren(ready_children)
                }
            };
            let node = LogicalNode {
                node,
                parent_index,
                state,
            };
            flat_tree.push(node);
        }
        let flat_tree = Arc::new(flat_tree);

        let planning_concurrency = session_state
            .config_options()
            .execution
            .planning_concurrency;
        // Can never spawn more tasks than leaves in the tree, as these tasks must
        // all converge down to the root node, which can only be processed by a
        // single task.
        let max_concurrency = planning_concurrency.min(flat_tree_leaf_indices.len());

        // Spawning tasks which will traverse leaf up to the root.
        let tasks = flat_tree_leaf_indices
            .into_iter()
            .map(|index| self.task_helper(index, Arc::clone(&flat_tree), session_state));
        let mut outputs = futures::stream::iter(tasks)
            .buffer_unordered(max_concurrency)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        // Ideally this never happens if we have a valid LogicalPlan tree
        if outputs.len() != 1 {
            return internal_err!(
                "Failed to convert LogicalPlan to ExecutionPlan: More than one root detected"
            );
        }
        let plan = outputs.pop().unwrap();
        Ok(plan)
    }

    /// These tasks start at a leaf and traverse up the tree towards the root, building
    /// an ExecutionPlan as they go. When they reach a node with two or more children,
    /// they append their current result (a child of the parent node) to the children
    /// vector, and if this is sufficient to create the parent then continues traversing
    /// the tree to create nodes. Otherwise, the task terminates.
    async fn task_helper<'a>(
        &'a self,
        leaf_starter_index: usize,
        flat_tree: Arc<Vec<LogicalNode<'a>>>,
        session_state: &'a SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // We always start with a leaf, so can ignore status and pass empty children
        let mut node = flat_tree.get(leaf_starter_index).ok_or_else(|| {
            internal_datafusion_err!(
                "Invalid index whilst creating initial physical plan"
            )
        })?;
        let mut plan = self
            .map_logical_node_to_physical(
                node.node,
                session_state,
                ChildrenContainer::None,
            )
            .await?;
        let mut current_index = leaf_starter_index;
        // parent_index is None only for root
        while let Some(parent_index) = node.parent_index {
            node = flat_tree.get(parent_index).ok_or_else(|| {
                internal_datafusion_err!(
                    "Invalid index whilst creating initial physical plan"
                )
            })?;
            match &node.state {
                NodeState::ZeroOrOneChild => {
                    plan = self
                        .map_logical_node_to_physical(
                            node.node,
                            session_state,
                            ChildrenContainer::One(plan),
                        )
                        .await?;
                }
                // See if we have all children to build the node.
                NodeState::TwoOrMoreChildren(children) => {
                    let mut children: Vec<ExecutionPlanChild> = {
                        let mut guard = children.lock().await;
                        // Add our contribution to this parent node.
                        // Vec is pre-allocated so no allocation should occur here.
                        guard.push(ExecutionPlanChild {
                            index: current_index,
                            plan,
                        });
                        if guard.len() < node.node.inputs().len() {
                            // This node is not ready yet, still pending more children.
                            // This task is finished forever.
                            return Ok(None);
                        }

                        // With this task's contribution we have enough children.
                        // This task is the only one building this node now, and thus
                        // no other task will need the Mutex for this node, so take
                        // all children.
                        std::mem::take(guard.as_mut())
                    };

                    // Indices refer to position in flat tree Vec, which means they are
                    // guaranteed to be unique, hence unstable sort used.
                    //
                    // We reverse sort because of how we visited the node in the initial
                    // DFS traversal (see above).
                    children.sort_unstable_by_key(|epc| std::cmp::Reverse(epc.index));
                    let children = children.into_iter().map(|epc| epc.plan).collect();
                    let children = ChildrenContainer::Multiple(children);
                    plan = self
                        .map_logical_node_to_physical(node.node, session_state, children)
                        .await?;
                }
            }
            current_index = parent_index;
        }
        // Only one task should ever reach this point for a valid LogicalPlan tree.
        Ok(Some(plan))
    }

    /// Given a single LogicalPlan node, map it to it's physical ExecutionPlan counterpart.
    async fn map_logical_node_to_physical(
        &self,
        node: &LogicalPlan,
        session_state: &SessionState,
        children: ChildrenContainer,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec_node: Arc<dyn ExecutionPlan> = match node {
            // Leaves (no children)
            LogicalPlan::TableScan(TableScan {
                source,
                projection,
                filters,
                fetch,
                ..
            }) => {
                let source = source_as_provider(source)?;
                // Remove all qualifiers from the scan as the provider
                // doesn't know (nor should care) how the relation was
                // referred to in the query
                let filters = unnormalize_cols(filters.iter().cloned());
                source
                    .scan(session_state, projection.as_ref(), &filters, *fetch)
                    .await?
            }
            LogicalPlan::Values(Values { values, schema }) => {
                let exec_schema = schema.as_ref().to_owned().into();
                let exprs = values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                self.create_physical_expr(expr, schema, session_state)
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let value_exec = ValuesExec::try_new(SchemaRef::new(exec_schema), exprs)?;
                Arc::new(value_exec)
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }) => Arc::new(EmptyExec::new(SchemaRef::new(
                schema.as_ref().to_owned().into(),
            ))),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema,
            }) => Arc::new(PlaceholderRowExec::new(SchemaRef::new(
                schema.as_ref().to_owned().into(),
            ))),
            LogicalPlan::DescribeTable(DescribeTable {
                schema,
                output_schema,
            }) => {
                let output_schema: Schema = output_schema.as_ref().into();
                self.plan_describe(Arc::clone(schema), Arc::new(output_schema))?
            }

            // 1 Child
            LogicalPlan::Copy(CopyTo {
                input,
                output_url,
                file_type,
                partition_by,
                options: source_option_tuples,
            }) => {
                let input_exec = children.one()?;
                let parsed_url = ListingTableUrl::parse(output_url)?;
                let object_store_url = parsed_url.object_store();

                let schema: Schema = (**input.schema()).clone().into();

                // Note: the DataType passed here is ignored for the purposes of writing and inferred instead
                // from the schema of the RecordBatch being written. This allows COPY statements to specify only
                // the column name rather than column name + explicit data type.
                let table_partition_cols = partition_by
                    .iter()
                    .map(|s| (s.to_string(), arrow_schema::DataType::Null))
                    .collect::<Vec<_>>();

                let keep_partition_by_columns = match source_option_tuples
                    .get("execution.keep_partition_by_columns")
                    .map(|v| v.trim()) {
                    None => session_state.config().options().execution.keep_partition_by_columns,
                    Some("true") => true,
                    Some("false") => false,
                    Some(value) =>
                        return Err(DataFusionError::Configuration(format!("provided value for 'execution.keep_partition_by_columns' was not recognized: \"{}\"", value))),
                };

                // Set file sink related options
                let config = FileSinkConfig {
                    object_store_url,
                    table_paths: vec![parsed_url],
                    file_groups: vec![],
                    output_schema: Arc::new(schema),
                    table_partition_cols,
                    overwrite: false,
                    keep_partition_by_columns,
                };

                let sink_format = file_type_to_format(file_type)?
                    .create(session_state, source_option_tuples)?;

                sink_format
                    .create_writer_physical_plan(input_exec, session_state, config, None)
                    .await?
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertInto,
                ..
            }) => {
                let name = table_name.table();
                let schema = session_state.schema_for_ref(table_name.clone())?;
                if let Some(provider) = schema.table(name).await? {
                    let input_exec = children.one()?;
                    provider
                        .insert_into(session_state, input_exec, false)
                        .await?
                } else {
                    return exec_err!("Table '{table_name}' does not exist");
                }
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertOverwrite,
                ..
            }) => {
                let name = table_name.table();
                let schema = session_state.schema_for_ref(table_name.clone())?;
                if let Some(provider) = schema.table(name).await? {
                    let input_exec = children.one()?;
                    provider
                        .insert_into(session_state, input_exec, true)
                        .await?
                } else {
                    return exec_err!("Table '{table_name}' does not exist");
                }
            }
            LogicalPlan::Window(Window {
                input, window_expr, ..
            }) => {
                if window_expr.is_empty() {
                    return internal_err!("Impossibly got empty window expression");
                }

                let input_exec = children.one()?;

                // at this moment we are guaranteed by the logical planner
                // to have all the window_expr to have equal sort key
                let partition_keys = window_expr_common_partition_keys(window_expr)?;

                let can_repartition = !partition_keys.is_empty()
                    && session_state.config().target_partitions() > 1
                    && session_state.config().repartition_window_functions();

                let physical_partition_keys = if can_repartition {
                    partition_keys
                        .iter()
                        .map(|e| {
                            self.create_physical_expr(e, input.schema(), session_state)
                        })
                        .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?
                } else {
                    vec![]
                };

                let get_sort_keys = |expr: &Expr| match expr {
                    Expr::WindowFunction(WindowFunction {
                        ref partition_by,
                        ref order_by,
                        ..
                    }) => generate_sort_key(partition_by, order_by),
                    Expr::Alias(Alias { expr, .. }) => {
                        // Convert &Box<T> to &T
                        match &**expr {
                            Expr::WindowFunction(WindowFunction {
                                ref partition_by,
                                ref order_by,
                                ..
                            }) => generate_sort_key(partition_by, order_by),
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                };
                let sort_keys = get_sort_keys(&window_expr[0])?;
                if window_expr.len() > 1 {
                    debug_assert!(
                            window_expr[1..]
                                .iter()
                                .all(|expr| get_sort_keys(expr).unwrap() == sort_keys),
                            "all window expressions shall have the same sort keys, as guaranteed by logical planning"
                        );
                }

                let logical_schema = node.schema();
                let window_expr = window_expr
                    .iter()
                    .map(|e| {
                        create_window_expr(
                            e,
                            logical_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let uses_bounded_memory =
                    window_expr.iter().all(|e| e.uses_bounded_memory());
                // If all window expressions can run with bounded memory,
                // choose the bounded window variant:
                if uses_bounded_memory {
                    Arc::new(BoundedWindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        physical_partition_keys,
                        InputOrderMode::Sorted,
                    )?)
                } else {
                    Arc::new(WindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        physical_partition_keys,
                    )?)
                }
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            }) => {
                // Initially need to perform the aggregate and then merge the partitions
                let input_exec = children.one()?;
                let physical_input_schema = input_exec.schema();
                let logical_input_schema = input.as_ref().schema();

                let groups = self.create_grouping_physical_expr(
                    group_expr,
                    logical_input_schema,
                    &physical_input_schema,
                    session_state,
                )?;

                let agg_filter = aggr_expr
                    .iter()
                    .map(|e| {
                        create_aggregate_expr_and_maybe_filter(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let (aggregates, filters, _order_bys): (Vec<_>, Vec<_>, Vec<_>) =
                    multiunzip(agg_filter);

                let initial_aggr = Arc::new(AggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates.clone(),
                    filters.clone(),
                    input_exec,
                    Arc::clone(&physical_input_schema),
                )?);

                // update group column indices based on partial aggregate plan evaluation
                let final_group: Vec<Arc<dyn PhysicalExpr>> =
                    initial_aggr.output_group_expr();

                let can_repartition = !groups.is_empty()
                    && session_state.config().target_partitions() > 1
                    && session_state.config().repartition_aggregations();

                // Some aggregators may be modified during initialization for
                // optimization purposes. For example, a FIRST_VALUE may turn
                // into a LAST_VALUE with the reverse ordering requirement.
                // To reflect such changes to subsequent stages, use the updated
                // `AggregateExpr`/`PhysicalSortExpr` objects.
                let updated_aggregates = initial_aggr.aggr_expr().to_vec();

                let next_partition_mode = if can_repartition {
                    // construct a second aggregation with 'AggregateMode::FinalPartitioned'
                    AggregateMode::FinalPartitioned
                } else {
                    // construct a second aggregation, keeping the final column name equal to the
                    // first aggregation and the expressions corresponding to the respective aggregate
                    AggregateMode::Final
                };

                let final_grouping_set = PhysicalGroupBy::new_single(
                    final_group
                        .iter()
                        .enumerate()
                        .map(|(i, expr)| (Arc::clone(expr), groups.expr()[i].1.clone()))
                        .collect(),
                );

                Arc::new(AggregateExec::try_new(
                    next_partition_mode,
                    final_grouping_set,
                    updated_aggregates,
                    filters,
                    initial_aggr,
                    Arc::clone(&physical_input_schema),
                )?)
            }
            LogicalPlan::Projection(Projection { input, expr, .. }) => self
                .create_project_physical_exec(
                    session_state,
                    children.one()?,
                    input,
                    expr,
                )?,
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let physical_input = children.one()?;
                let input_dfschema = input.schema();

                let runtime_expr =
                    self.create_physical_expr(predicate, input_dfschema, session_state)?;
                let selectivity = session_state
                    .config()
                    .options()
                    .optimizer
                    .default_filter_selectivity;
                let filter = FilterExec::try_new(runtime_expr, physical_input)?;
                Arc::new(filter.with_default_selectivity(selectivity)?)
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                let physical_input = children.one()?;
                let input_dfschema = input.as_ref().schema();
                let physical_partitioning = match partitioning_scheme {
                    LogicalPartitioning::RoundRobinBatch(n) => {
                        Partitioning::RoundRobinBatch(*n)
                    }
                    LogicalPartitioning::Hash(expr, n) => {
                        let runtime_expr = expr
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(
                                    e,
                                    input_dfschema,
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Partitioning::Hash(runtime_expr, *n)
                    }
                    LogicalPartitioning::DistributeBy(_) => {
                        return not_impl_err!(
                            "Physical plan does not support DistributeBy partitioning"
                        );
                    }
                };
                Arc::new(RepartitionExec::try_new(
                    physical_input,
                    physical_partitioning,
                )?)
            }
            LogicalPlan::Sort(Sort {
                expr, input, fetch, ..
            }) => {
                let physical_input = children.one()?;
                let input_dfschema = input.as_ref().schema();
                let sort_expr = create_physical_sort_exprs(
                    expr,
                    input_dfschema,
                    session_state.execution_props(),
                )?;
                let new_sort =
                    SortExec::new(sort_expr, physical_input).with_fetch(*fetch);
                Arc::new(new_sort)
            }
            LogicalPlan::Subquery(_) => todo!(),
            LogicalPlan::SubqueryAlias(_) => children.one()?,
            LogicalPlan::Limit(Limit { skip, fetch, .. }) => {
                let input = children.one()?;

                // GlobalLimitExec requires a single partition for input
                let input = if input.output_partitioning().partition_count() == 1 {
                    input
                } else {
                    // Apply a LocalLimitExec to each partition. The optimizer will also insert
                    // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                    if let Some(fetch) = fetch {
                        Arc::new(LocalLimitExec::new(input, *fetch + skip))
                    } else {
                        input
                    }
                };

                Arc::new(GlobalLimitExec::new(input, *skip, *fetch))
            }
            LogicalPlan::Unnest(Unnest {
                list_type_columns,
                struct_type_columns,
                schema,
                options,
                ..
            }) => {
                let input = children.one()?;
                let schema = SchemaRef::new(schema.as_ref().to_owned().into());
                Arc::new(UnnestExec::new(
                    input,
                    list_type_columns.clone(),
                    struct_type_columns.clone(),
                    schema,
                    options.clone(),
                ))
            }

            // 2 Children
            LogicalPlan::Join(Join {
                left,
                right,
                on: keys,
                filter,
                join_type,
                null_equals_null,
                schema: join_schema,
                ..
            }) => {
                let null_equals_null = *null_equals_null;

                let [physical_left, physical_right] = children.two()?;

                // If join has expression equijoin keys, add physical projection.
                let has_expr_join_key = keys.iter().any(|(l, r)| {
                    !(matches!(l, Expr::Column(_)) && matches!(r, Expr::Column(_)))
                });
                let (new_logical, physical_left, physical_right) = if has_expr_join_key {
                    // TODO: Can we extract this transformation to somewhere before physical plan
                    //       creation?
                    let (left_keys, right_keys): (Vec<_>, Vec<_>) =
                        keys.iter().cloned().unzip();

                    let (left, left_col_keys, left_projected) =
                        wrap_projection_for_join_if_necessary(
                            &left_keys,
                            left.as_ref().clone(),
                        )?;
                    let (right, right_col_keys, right_projected) =
                        wrap_projection_for_join_if_necessary(
                            &right_keys,
                            right.as_ref().clone(),
                        )?;
                    let column_on = (left_col_keys, right_col_keys);

                    let left = Arc::new(left);
                    let right = Arc::new(right);
                    let new_join = LogicalPlan::Join(Join::try_new_with_project_input(
                        node,
                        Arc::clone(&left),
                        Arc::clone(&right),
                        column_on,
                    )?);

                    // If inputs were projected then create ExecutionPlan for these new
                    // LogicalPlan nodes.
                    let physical_left = match (left_projected, left.as_ref()) {
                        // If left_projected is true we are guaranteed that left is a Projection
                        (
                            true,
                            LogicalPlan::Projection(Projection { input, expr, .. }),
                        ) => self.create_project_physical_exec(
                            session_state,
                            physical_left,
                            input,
                            expr,
                        )?,
                        _ => physical_left,
                    };
                    let physical_right = match (right_projected, right.as_ref()) {
                        // If right_projected is true we are guaranteed that right is a Projection
                        (
                            true,
                            LogicalPlan::Projection(Projection { input, expr, .. }),
                        ) => self.create_project_physical_exec(
                            session_state,
                            physical_right,
                            input,
                            expr,
                        )?,
                        _ => physical_right,
                    };

                    // Remove temporary projected columns
                    if left_projected || right_projected {
                        let final_join_result =
                            join_schema.iter().map(Expr::from).collect::<Vec<_>>();
                        let projection = LogicalPlan::Projection(Projection::try_new(
                            final_join_result,
                            Arc::new(new_join),
                        )?);
                        // LogicalPlan mutated
                        (Cow::Owned(projection), physical_left, physical_right)
                    } else {
                        // LogicalPlan mutated
                        (Cow::Owned(new_join), physical_left, physical_right)
                    }
                } else {
                    // LogicalPlan unchanged
                    (Cow::Borrowed(node), physical_left, physical_right)
                };

                // Retrieving new left/right and join keys (in case plan was mutated above)
                let (left, right, keys, new_project) = match new_logical.as_ref() {
                    LogicalPlan::Projection(Projection { input, expr, .. }) => {
                        if let LogicalPlan::Join(Join {
                            left, right, on, ..
                        }) = input.as_ref()
                        {
                            (left, right, on, Some((input, expr)))
                        } else {
                            unreachable!()
                        }
                    }
                    LogicalPlan::Join(Join {
                        left, right, on, ..
                    }) => (left, right, on, None),
                    // Should either be the original Join, or Join with a Projection on top
                    _ => unreachable!(),
                };

                // All equi-join keys are columns now, create physical join plan
                let left_df_schema = left.schema();
                let right_df_schema = right.schema();
                let execution_props = session_state.execution_props();
                let join_on = keys
                    .iter()
                    .map(|(l, r)| {
                        let l = create_physical_expr(l, left_df_schema, execution_props)?;
                        let r =
                            create_physical_expr(r, right_df_schema, execution_props)?;
                        Ok((l, r))
                    })
                    .collect::<Result<join_utils::JoinOn>>()?;

                let join_filter = match filter {
                    Some(expr) => {
                        // Extract columns from filter expression and saved in a HashSet
                        let cols = expr.column_refs();

                        // Collect left & right field indices, the field indices are sorted in ascending order
                        let left_field_indices = cols
                            .iter()
                            .filter_map(|c| match left_df_schema.index_of_column(c) {
                                Ok(idx) => Some(idx),
                                _ => None,
                            })
                            .sorted()
                            .collect::<Vec<_>>();
                        let right_field_indices = cols
                            .iter()
                            .filter_map(|c| match right_df_schema.index_of_column(c) {
                                Ok(idx) => Some(idx),
                                _ => None,
                            })
                            .sorted()
                            .collect::<Vec<_>>();

                        // Collect DFFields and Fields required for intermediate schemas
                        let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) =
                            left_field_indices
                                .clone()
                                .into_iter()
                                .map(|i| {
                                    (
                                        left_df_schema.qualified_field(i),
                                        physical_left.schema().field(i).clone(),
                                    )
                                })
                                .chain(right_field_indices.clone().into_iter().map(|i| {
                                    (
                                        right_df_schema.qualified_field(i),
                                        physical_right.schema().field(i).clone(),
                                    )
                                }))
                                .unzip();
                        let filter_df_fields = filter_df_fields
                            .into_iter()
                            .map(|(qualifier, field)| {
                                (qualifier.cloned(), Arc::new(field.clone()))
                            })
                            .collect();

                        // Construct intermediate schemas used for filtering data and
                        // convert logical expression to physical according to filter schema
                        let filter_df_schema = DFSchema::new_with_metadata(
                            filter_df_fields,
                            HashMap::new(),
                        )?;
                        let filter_schema =
                            Schema::new_with_metadata(filter_fields, HashMap::new());
                        let filter_expr = create_physical_expr(
                            expr,
                            &filter_df_schema,
                            session_state.execution_props(),
                        )?;
                        let column_indices = join_utils::JoinFilter::build_column_indices(
                            left_field_indices,
                            right_field_indices,
                        );

                        Some(join_utils::JoinFilter::new(
                            filter_expr,
                            column_indices,
                            filter_schema,
                        ))
                    }
                    _ => None,
                };

                let prefer_hash_join =
                    session_state.config_options().optimizer.prefer_hash_join;

                let join: Arc<dyn ExecutionPlan> = if join_on.is_empty() {
                    // there is no equal join condition, use the nested loop join
                    // TODO optimize the plan, and use the config of `target_partitions` and `repartition_joins`
                    Arc::new(NestedLoopJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_filter,
                        join_type,
                    )?)
                } else if session_state.config().target_partitions() > 1
                    && session_state.config().repartition_joins()
                    && !prefer_hash_join
                {
                    // Use SortMergeJoin if hash join is not preferred
                    // Sort-Merge join support currently is experimental

                    let join_on_len = join_on.len();
                    Arc::new(SortMergeJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        *join_type,
                        vec![SortOptions::default(); join_on_len],
                        null_equals_null,
                    )?)
                } else if session_state.config().target_partitions() > 1
                    && session_state.config().repartition_joins()
                    && prefer_hash_join
                {
                    let partition_mode = {
                        if session_state.config().collect_statistics() {
                            PartitionMode::Auto
                        } else {
                            PartitionMode::Partitioned
                        }
                    };
                    Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        join_type,
                        None,
                        partition_mode,
                        null_equals_null,
                    )?)
                } else {
                    Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        join_type,
                        None,
                        PartitionMode::CollectLeft,
                        null_equals_null,
                    )?)
                };

                // If plan was mutated previously then need to create the ExecutionPlan
                // for the new Projection that was applied on top.
                if let Some((input, expr)) = new_project {
                    self.create_project_physical_exec(session_state, join, input, expr)?
                } else {
                    join
                }
            }
            LogicalPlan::CrossJoin(_) => {
                let [left, right] = children.two()?;
                Arc::new(CrossJoinExec::new(left, right))
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name, is_distinct, ..
            }) => {
                let [static_term, recursive_term] = children.two()?;
                Arc::new(RecursiveQueryExec::try_new(
                    name.clone(),
                    static_term,
                    recursive_term,
                    *is_distinct,
                )?)
            }

            // N Children
            LogicalPlan::Union(_) => Arc::new(UnionExec::new(children.vec())),
            LogicalPlan::Extension(Extension { node }) => {
                let mut maybe_plan = None;
                let children = children.vec();
                for planner in &self.extension_planners {
                    if maybe_plan.is_some() {
                        break;
                    }

                    let logical_input = node.inputs();
                    maybe_plan = planner
                        .plan_extension(
                            self,
                            node.as_ref(),
                            &logical_input,
                            &children,
                            session_state,
                        )
                        .await?;
                }

                let plan = match maybe_plan {
                        Some(v) => Ok(v),
                        _ => plan_err!("No installed planner was able to convert the custom node to an execution plan: {:?}", node)
                    }?;

                // Ensure the ExecutionPlan's schema matches the
                // declared logical schema to catch and warn about
                // logic errors when creating user defined plans.
                if !node.schema().matches_arrow_schema(&plan.schema()) {
                    return plan_err!(
                            "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                            LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                            node, node.schema(), plan.schema()
                        );
                } else {
                    plan
                }
            }

            // Other
            LogicalPlan::Statement(statement) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                let name = statement.name();
                return not_impl_err!("Unsupported logical plan: Statement({name})");
            }
            LogicalPlan::Prepare(_) => {
                // There is no default plan for "PREPARE" -- it must be
                // handled at a higher level (so that the appropriate
                // statement can be prepared)
                return not_impl_err!("Unsupported logical plan: Prepare");
            }
            LogicalPlan::Dml(dml) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                return not_impl_err!("Unsupported logical plan: Dml({0})", dml.op);
            }
            LogicalPlan::Ddl(ddl) => {
                // There is no default plan for DDl statements --
                // it must be handled at a higher level (so that
                // the appropriate table can be registered with
                // the context)
                let name = ddl.name();
                return not_impl_err!("Unsupported logical plan: {name}");
            }
            LogicalPlan::Explain(_) => {
                return internal_err!(
                    "Unsupported logical plan: Explain must be root of the plan"
                )
            }
            LogicalPlan::Distinct(_) => {
                return internal_err!(
                    "Unsupported logical plan: Distinct should be replaced to Aggregate"
                )
            }
            LogicalPlan::Analyze(_) => {
                return internal_err!(
                    "Unsupported logical plan: Analyze must be root of the plan"
                )
            }
        };
        Ok(exec_node)
    }

    fn create_grouping_physical_expr(
        &self,
        group_expr: &[Expr],
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<PhysicalGroupBy> {
        if group_expr.len() == 1 {
            match &group_expr[0] {
                Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)) => {
                    merge_grouping_set_physical_expr(
                        grouping_sets,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => create_cube_physical_expr(
                    exprs,
                    input_dfschema,
                    input_schema,
                    session_state,
                ),
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    create_rollup_physical_expr(
                        exprs,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                expr => Ok(PhysicalGroupBy::new_single(vec![tuple_err((
                    self.create_physical_expr(expr, input_dfschema, session_state),
                    physical_name(expr),
                ))?])),
            }
        } else {
            Ok(PhysicalGroupBy::new_single(
                group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(e, input_dfschema, session_state),
                            physical_name(e),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
    }
}

/// Expand and align a GROUPING SET expression.
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
///
/// This will take a list of grouping sets and ensure that each group is
/// properly aligned for the physical execution plan. We do this by
/// identifying all unique expression in each group and conforming each
/// group to the same set of expression types and ordering.
/// For example, if we have something like `GROUPING SETS ((a,b,c),(a),(b),(b,c))`
/// we would expand this to `GROUPING SETS ((a,b,c),(a,NULL,NULL),(NULL,b,NULL),(NULL,b,c))
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn merge_grouping_set_physical_expr(
    grouping_sets: &[Vec<Expr>],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_groups = grouping_sets.len();
    let mut all_exprs: Vec<Expr> = vec![];
    let mut grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    for expr in grouping_sets.iter().flatten() {
        if !all_exprs.contains(expr) {
            all_exprs.push(expr.clone());

            grouping_set_expr.push(get_physical_expr_pair(
                expr,
                input_dfschema,
                session_state,
            )?);

            null_exprs.push(get_null_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);
        }
    }

    let mut merged_sets: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    for expr_group in grouping_sets.iter() {
        let group: Vec<bool> = all_exprs
            .iter()
            .map(|expr| !expr_group.contains(expr))
            .collect();

        merged_sets.push(group)
    }

    Ok(PhysicalGroupBy::new(
        grouping_set_expr,
        null_exprs,
        merged_sets,
    ))
}

/// Expand and align a CUBE expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn create_cube_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();
    let num_groups = num_of_exprs * num_of_exprs;

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(expr, input_dfschema, session_state)?)
    }

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    groups.push(vec![false; num_of_exprs]);

    for null_count in 1..=num_of_exprs {
        for null_idx in (0..num_of_exprs).combinations(null_count) {
            let mut next_group: Vec<bool> = vec![false; num_of_exprs];
            null_idx.into_iter().for_each(|i| next_group[i] = true);
            groups.push(next_group);
        }
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// Expand and align a ROLLUP expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn create_rollup_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_of_exprs + 1);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(expr, input_dfschema, session_state)?)
    }

    for total in 0..=num_of_exprs {
        let mut group: Vec<bool> = Vec::with_capacity(num_of_exprs);

        for index in 0..num_of_exprs {
            if index < total {
                group.push(false);
            } else {
                group.push(true);
            }
        }

        groups.push(group)
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// For a given logical expr, get a properly typed NULL ScalarValue physical expression
fn get_null_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr =
        create_physical_expr(expr, input_dfschema, session_state.execution_props())?;
    let physical_name = physical_name(&expr.clone())?;

    let data_type = physical_expr.data_type(input_schema)?;
    let null_value: ScalarValue = (&data_type).try_into()?;

    let null_value = Literal::new(null_value);
    Ok((Arc::new(null_value), physical_name))
}

fn get_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr =
        create_physical_expr(expr, input_dfschema, session_state.execution_props())?;
    let physical_name = physical_name(expr)?;
    Ok((physical_expr, physical_name))
}

/// Check if window bounds are valid after schema information is available, and
/// window_frame bounds are casted to the corresponding column type.
/// queries like:
/// OVER (ORDER BY a RANGES BETWEEN 3 PRECEDING AND 5 PRECEDING)
/// OVER (ORDER BY a RANGES BETWEEN INTERVAL '3 DAY' PRECEDING AND '5 DAY' PRECEDING)  are rejected
pub fn is_window_frame_bound_valid(window_frame: &WindowFrame) -> bool {
    match (&window_frame.start_bound, &window_frame.end_bound) {
        (WindowFrameBound::Following(_), WindowFrameBound::Preceding(_))
        | (WindowFrameBound::Following(_), WindowFrameBound::CurrentRow)
        | (WindowFrameBound::CurrentRow, WindowFrameBound::Preceding(_)) => false,
        (WindowFrameBound::Preceding(lhs), WindowFrameBound::Preceding(rhs)) => {
            !rhs.is_null() && (lhs.is_null() || (lhs >= rhs))
        }
        (WindowFrameBound::Following(lhs), WindowFrameBound::Following(rhs)) => {
            !lhs.is_null() && (rhs.is_null() || (lhs <= rhs))
        }
        _ => true,
    }
}

/// Create a window expression with a name from a logical expression
pub fn create_window_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_schema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    let name = name.into();
    let physical_schema: &Schema = &logical_schema.into();
    match e {
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
            null_treatment,
        }) => {
            let physical_args =
                create_physical_exprs(args, logical_schema, execution_props)?;
            let partition_by =
                create_physical_exprs(partition_by, logical_schema, execution_props)?;
            let order_by =
                create_physical_sort_exprs(order_by, logical_schema, execution_props)?;

            if !is_window_frame_bound_valid(window_frame) {
                return plan_err!(
                        "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
                        window_frame.start_bound, window_frame.end_bound
                    );
            }

            let window_frame = Arc::new(window_frame.clone());
            let ignore_nulls = null_treatment.unwrap_or(NullTreatment::RespectNulls)
                == NullTreatment::IgnoreNulls;
            windows::create_window_expr(
                fun,
                name,
                &physical_args,
                args,
                &partition_by,
                &order_by,
                window_frame,
                physical_schema,
                ignore_nulls,
            )
        }
        other => plan_err!("Invalid window expression '{other:?}'"),
    }
}

/// Create a window expression from a logical expression or an alias
pub fn create_window_expr(
    e: &Expr,
    logical_schema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    // unpack aliased logical expressions, e.g. "sum(col) over () as total"
    let (name, e) = match e {
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        _ => (e.display_name()?, e),
    };
    create_window_expr_with_name(e, name, logical_schema, execution_props)
}

type AggregateExprWithOptionalArgs = (
    Arc<dyn AggregateExpr>,
    // The filter clause, if any
    Option<Arc<dyn PhysicalExpr>>,
    // Ordering requirements, if any
    Option<Vec<PhysicalSortExpr>>,
);

/// Create an aggregate expression with a name from a logical expression
pub fn create_aggregate_expr_with_name_and_maybe_filter(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    match e {
        Expr::AggregateFunction(AggregateFunction {
            func_def,
            distinct,
            args,
            filter,
            order_by,
            null_treatment,
        }) => {
            let physical_args =
                create_physical_exprs(args, logical_input_schema, execution_props)?;
            let filter = match filter {
                Some(e) => Some(create_physical_expr(
                    e,
                    logical_input_schema,
                    execution_props,
                )?),
                None => None,
            };

            let ignore_nulls = null_treatment
                .unwrap_or(sqlparser::ast::NullTreatment::RespectNulls)
                == NullTreatment::IgnoreNulls;

            let (agg_expr, filter, order_by) = match func_def {
                AggregateFunctionDefinition::BuiltIn(fun) => {
                    let physical_sort_exprs = match order_by {
                        Some(exprs) => Some(create_physical_sort_exprs(
                            exprs,
                            logical_input_schema,
                            execution_props,
                        )?),
                        None => None,
                    };
                    let ordering_reqs: Vec<PhysicalSortExpr> =
                        physical_sort_exprs.clone().unwrap_or(vec![]);
                    let agg_expr = aggregates::create_aggregate_expr(
                        fun,
                        *distinct,
                        &physical_args,
                        &ordering_reqs,
                        physical_input_schema,
                        name,
                        ignore_nulls,
                    )?;
                    (agg_expr, filter, physical_sort_exprs)
                }
                AggregateFunctionDefinition::UDF(fun) => {
                    let sort_exprs = order_by.clone().unwrap_or(vec![]);
                    let physical_sort_exprs = match order_by {
                        Some(exprs) => Some(create_physical_sort_exprs(
                            exprs,
                            logical_input_schema,
                            execution_props,
                        )?),
                        None => None,
                    };
                    let ordering_reqs: Vec<PhysicalSortExpr> =
                        physical_sort_exprs.clone().unwrap_or(vec![]);
                    let agg_expr = udaf::create_aggregate_expr(
                        fun,
                        &physical_args,
                        args,
                        &sort_exprs,
                        &ordering_reqs,
                        physical_input_schema,
                        name,
                        ignore_nulls,
                        *distinct,
                    )?;
                    (agg_expr, filter, physical_sort_exprs)
                }
            };
            Ok((agg_expr, filter, order_by))
        }
        other => internal_err!("Invalid aggregate expression '{other:?}'"),
    }
}

/// Create an aggregate expression from a logical expression or an alias
pub fn create_aggregate_expr_and_maybe_filter(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    // unpack (nested) aliased logical expressions, e.g. "sum(col) as total"
    let (name, e) = match e {
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        Expr::AggregateFunction(_) => (e.display_name().unwrap_or(physical_name(e)?), e),
        _ => (physical_name(e)?, e),
    };

    create_aggregate_expr_with_name_and_maybe_filter(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    if let Expr::Sort(expr::Sort {
        expr,
        asc,
        nulls_first,
    }) = e
    {
        Ok(PhysicalSortExpr {
            expr: create_physical_expr(expr, input_dfschema, execution_props)?,
            options: SortOptions {
                descending: !asc,
                nulls_first: *nulls_first,
            },
        })
    } else {
        internal_err!("Expects a sort expression")
    }
}

/// Create vector of physical sort expression from a vector of logical expression
pub fn create_physical_sort_exprs(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<LexOrdering> {
    exprs
        .iter()
        .map(|expr| create_physical_sort_expr(expr, input_dfschema, execution_props))
        .collect::<Result<Vec<_>>>()
}

impl DefaultPhysicalPlanner {
    /// Handles capturing the various plans for EXPLAIN queries
    ///
    /// Returns
    /// Some(plan) if optimized, and None if logical_plan was not an
    /// explain (and thus needs to be optimized as normal)
    async fn handle_explain(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let LogicalPlan::Explain(e) = logical_plan {
            use PlanType::*;
            let mut stringified_plans = vec![];

            let config = &session_state.config_options().explain;

            if !config.physical_plan_only {
                stringified_plans.clone_from(&e.stringified_plans);
                if e.logical_optimization_succeeded {
                    stringified_plans.push(e.plan.to_stringified(FinalLogicalPlan));
                }
            }

            if !config.logical_plan_only && e.logical_optimization_succeeded {
                match self
                    .create_initial_plan(e.plan.as_ref(), session_state)
                    .await
                {
                    Ok(input) => {
                        // Include statistics / schema if enabled
                        stringified_plans.push(
                            displayable(input.as_ref())
                                .set_show_statistics(config.show_statistics)
                                .set_show_schema(config.show_schema)
                                .to_stringified(e.verbose, InitialPhysicalPlan),
                        );

                        // Show statistics + schema in verbose output even if not
                        // explicitly requested
                        if e.verbose {
                            if !config.show_statistics {
                                stringified_plans.push(
                                    displayable(input.as_ref())
                                        .set_show_statistics(true)
                                        .to_stringified(
                                            e.verbose,
                                            InitialPhysicalPlanWithStats,
                                        ),
                                );
                            }
                            if !config.show_schema {
                                stringified_plans.push(
                                    displayable(input.as_ref())
                                        .set_show_schema(true)
                                        .to_stringified(
                                            e.verbose,
                                            InitialPhysicalPlanWithSchema,
                                        ),
                                );
                            }
                        }

                        let optimized_plan = self.optimize_internal(
                            input,
                            session_state,
                            |plan, optimizer| {
                                let optimizer_name = optimizer.name().to_string();
                                let plan_type = OptimizedPhysicalPlan { optimizer_name };
                                stringified_plans.push(
                                    displayable(plan)
                                        .set_show_statistics(config.show_statistics)
                                        .set_show_schema(config.show_schema)
                                        .to_stringified(e.verbose, plan_type),
                                );
                            },
                        );
                        match optimized_plan {
                            Ok(input) => {
                                // This plan will includes statistics if show_statistics is on
                                stringified_plans.push(
                                    displayable(input.as_ref())
                                        .set_show_statistics(config.show_statistics)
                                        .set_show_schema(config.show_schema)
                                        .to_stringified(e.verbose, FinalPhysicalPlan),
                                );

                                // Show statistics + schema in verbose output even if not
                                // explicitly requested
                                if e.verbose {
                                    if !config.show_statistics {
                                        stringified_plans.push(
                                            displayable(input.as_ref())
                                                .set_show_statistics(true)
                                                .to_stringified(
                                                    e.verbose,
                                                    FinalPhysicalPlanWithStats,
                                                ),
                                        );
                                    }
                                    if !config.show_schema {
                                        stringified_plans.push(
                                            displayable(input.as_ref())
                                                .set_show_schema(true)
                                                .to_stringified(
                                                    e.verbose,
                                                    FinalPhysicalPlanWithSchema,
                                                ),
                                        );
                                    }
                                }
                            }
                            Err(DataFusionError::Context(optimizer_name, e)) => {
                                let plan_type = OptimizedPhysicalPlan { optimizer_name };
                                stringified_plans
                                    .push(StringifiedPlan::new(plan_type, e.to_string()))
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => stringified_plans
                        .push(StringifiedPlan::new(InitialPhysicalPlan, e.to_string())),
                }
            }

            Ok(Some(Arc::new(ExplainExec::new(
                SchemaRef::new(e.schema.as_ref().to_owned().into()),
                stringified_plans,
                e.verbose,
            ))))
        } else if let LogicalPlan::Analyze(a) = logical_plan {
            let input = self.create_physical_plan(&a.input, session_state).await?;
            let schema = SchemaRef::new((*a.schema).clone().into());
            let show_statistics = session_state.config_options().explain.show_statistics;
            Ok(Some(Arc::new(AnalyzeExec::new(
                a.verbose,
                show_statistics,
                input,
                schema,
            ))))
        } else {
            Ok(None)
        }
    }

    /// Optimize a physical plan by applying each physical optimizer,
    /// calling observer(plan, optimizer after each one)
    fn optimize_internal<F>(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
        mut observer: F,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        F: FnMut(&dyn ExecutionPlan, &dyn PhysicalOptimizerRule),
    {
        let optimizers = session_state.physical_optimizers();
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent(false)
        );
        trace!(
            "Detailed input physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        let mut new_plan = plan;
        for optimizer in optimizers {
            let before_schema = new_plan.schema();
            new_plan = optimizer
                .optimize(new_plan, session_state.config_options())
                .map_err(|e| {
                    DataFusionError::Context(optimizer.name().to_string(), Box::new(e))
                })?;
            if optimizer.schema_check() && new_plan.schema() != before_schema {
                let e = DataFusionError::Internal(format!(
                    "PhysicalOptimizer rule '{}' failed, due to generate a different schema, original schema: {:?}, new schema: {:?}",
                    optimizer.name(),
                    before_schema,
                    new_plan.schema()
                ));
                return Err(DataFusionError::Context(
                    optimizer.name().to_string(),
                    Box::new(e),
                ));
            }
            trace!(
                "Optimized physical plan by {}:\n{}\n",
                optimizer.name(),
                displayable(new_plan.as_ref()).indent(false)
            );
            observer(new_plan.as_ref(), optimizer.as_ref())
        }
        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent(false)
        );
        trace!("Detailed optimized physical plan:\n{:?}", new_plan);
        Ok(new_plan)
    }

    // return an record_batch which describes a table's schema.
    fn plan_describe(
        &self,
        table_schema: Arc<Schema>,
        output_schema: Arc<Schema>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut column_names = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = StringBuilder::new();
        for field in table_schema.fields() {
            column_names.append_value(field.name());

            // "System supplied type" --> Use debug format of the datatype
            let data_type = field.data_type();
            data_types.append_value(format!("{data_type:?}"));

            // "YES if the column is possibly nullable, NO if it is known not nullable. "
            let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
            is_nullables.append_value(nullable_str);
        }

        let record_batch = RecordBatch::try_new(
            output_schema,
            vec![
                Arc::new(column_names.finish()),
                Arc::new(data_types.finish()),
                Arc::new(is_nullables.finish()),
            ],
        )?;

        let schema = record_batch.schema();
        let partitions = vec![vec![record_batch]];
        let projection = None;
        let mem_exec = MemoryExec::try_new(&partitions, schema, projection)?;
        Ok(Arc::new(mem_exec))
    }

    fn create_project_physical_exec(
        &self,
        session_state: &SessionState,
        input_exec: Arc<dyn ExecutionPlan>,
        input: &Arc<LogicalPlan>,
        expr: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.as_ref().schema();
        let physical_exprs = expr
            .iter()
            .map(|e| {
                // For projections, SQL planner and logical plan builder may convert user
                // provided expressions into logical Column expressions if their results
                // are already provided from the input plans. Because we work with
                // qualified columns in logical plane, derived columns involve operators or
                // functions will contain qualifiers as well. This will result in logical
                // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                //
                // If we run these logical columns through physical_name function, we will
                // get physical names with column qualifiers, which violates DataFusion's
                // field name semantics. To account for this, we need to derive the
                // physical name from physical input instead.
                //
                // This depends on the invariant that logical schema field index MUST match
                // with physical schema field index.
                let physical_name = if let Expr::Column(col) = e {
                    match input_schema.index_of_column(col) {
                        Ok(idx) => {
                            // index physical field using logical field index
                            Ok(input_exec.schema().field(idx).name().to_string())
                        }
                        // logical column is not a derived column, safe to pass along to
                        // physical_name
                        Err(_) => physical_name(e),
                    }
                } else {
                    physical_name(e)
                };

                tuple_err((
                    self.create_physical_expr(e, input_schema, session_state),
                    physical_name,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(ProjectionExec::try_new(
            physical_exprs,
            input_exec,
        )?))
    }
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::fmt::{self, Debug};
    use std::ops::{BitAnd, Not};

    use super::*;
    use crate::datasource::file_format::options::CsvReadOptions;
    use crate::datasource::MemTable;
    use crate::physical_plan::{
        expressions, DisplayAs, DisplayFormatType, ExecutionMode, PlanProperties,
        SendableRecordBatchStream,
    };
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test_util::{scan_empty, scan_empty_with_partitions};

    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_common::{assert_contains, DFSchemaRef, TableReference};
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_execution::TaskContext;
    use datafusion_expr::{col, lit, LogicalPlanBuilder, UserDefinedLogicalNodeCore};
    use datafusion_functions_aggregate::expr_fn::sum;
    use datafusion_physical_expr::EquivalenceProperties;

    fn make_session_state() -> SessionState {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(4);
        let config = config.set_bool("datafusion.optimizer.skip_failed_rules", false);
        SessionState::new_with_config_rt(config, runtime)
    }

    async fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let session_state = make_session_state();
        // optimize the logical plan
        let logical_plan = session_state.optimize(logical_plan)?;
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
    }

    #[tokio::test]
    async fn test_all_operators() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .sort(vec![col("c1").sort(true, true)])?
            .limit(3, Some(10))?
            .build()?;

        let exec_plan = plan(&logical_plan).await?;

        // verify that the plan correctly casts u8 to i64
        // the cast from u8 to i64 for literal will be simplified, and get lit(int64(5))
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "BinaryExpr { left: Column { name: \"c7\", index: 2 }, op: Lt, right: Literal { value: Int64(5) } }";
        assert!(format!("{exec_plan:?}").contains(expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_create_cube_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let cube = create_cube_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        let expected = r#"Ok(PhysicalGroupBy { expr: [(Column { name: "c1", index: 0 }, "c1"), (Column { name: "c2", index: 1 }, "c2"), (Column { name: "c3", index: 2 }, "c3")], null_expr: [(Literal { value: Utf8(NULL) }, "c1"), (Literal { value: Int64(NULL) }, "c2"), (Literal { value: Int64(NULL) }, "c3")], groups: [[false, false, false], [true, false, false], [false, true, false], [false, false, true], [true, true, false], [true, false, true], [false, true, true], [true, true, true]] })"#;

        assert_eq!(format!("{cube:?}"), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_rollup_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let rollup = create_rollup_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        let expected = r#"Ok(PhysicalGroupBy { expr: [(Column { name: "c1", index: 0 }, "c1"), (Column { name: "c2", index: 1 }, "c2"), (Column { name: "c3", index: 2 }, "c3")], null_expr: [(Literal { value: Utf8(NULL) }, "c1"), (Literal { value: Int64(NULL) }, "c2"), (Literal { value: Int64(NULL) }, "c3")], groups: [[true, true, true], [false, true, true], [false, false, true], [false, false, false]] })"#;

        assert_eq!(format!("{rollup:?}"), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let dfschema = DFSchema::try_from(schema.clone())?;

        let planner = DefaultPhysicalPlanner::default();

        let expr = planner.create_physical_expr(
            &col("a").not(),
            &dfschema,
            &make_session_state(),
        )?;
        let expected = expressions::not(expressions::col("a", &schema)?)?;

        assert_eq!(format!("{expr:?}"), format!("{expected:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn test_with_csv_plan() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .filter(col("c7").lt(col("c12")))?
            .limit(3, None)?
            .build()?;

        let plan = plan(&logical_plan).await?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        // the cast here is implicit so has CastOptions with safe=true
        let _expected = "predicate: BinaryExpr { left: TryCastExpr { expr: Column { name: \"c7\", index: 6 }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\", index: 11 } }";
        let plan_debug_str = format!("{plan:?}");
        assert!(plan_debug_str.contains("GlobalLimitExec"));
        assert!(plan_debug_str.contains("skip: 3"));
        Ok(())
    }

    #[tokio::test]
    async fn error_during_extension_planning() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            ErrorExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        match planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
        {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(e.to_string().contains("BOOM"),),
        }
    }

    #[tokio::test]
    async fn test_with_zero_offset_plan() -> Result<()> {
        let logical_plan = test_csv_scan().await?.limit(0, None)?.build()?;
        let plan = plan(&logical_plan).await?;
        assert!(!format!("{plan:?}").contains("limit="));
        Ok(())
    }

    #[tokio::test]
    async fn test_limit_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty_with_partitions(Some("test"), &schema, None, 2)?
            .limit(3, Some(5))?
            .build()?;
        let plan = plan(&logical_plan).await?;

        assert!(format!("{plan:?}").contains("GlobalLimitExec"));
        assert!(format!("{plan:?}").contains("skip: 3, fetch: Some(5)"));

        // LocalLimitExec adjusts the `fetch`
        assert!(format!("{plan:?}").contains("LocalLimitExec"));
        assert!(format!("{plan:?}").contains("fetch: 8"));
        Ok(())
    }

    #[tokio::test]
    async fn errors() -> Result<()> {
        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 = utf8
            col("c1").eq(col("c1")),
            // u8 AND u8
            col("c3").bitand(col("c3")),
            // utf8 = u8
            col("c1").eq(col("c3")),
            // bool AND bool
            bool_expr.clone().and(bool_expr),
        ];
        for case in cases {
            test_csv_scan().await?.project(vec![case.clone()]).unwrap();
        }
        Ok(())
    }

    #[tokio::test]
    async fn default_extension_planner() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::default();
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error =
            "No installed planner was able to convert the custom node to an execution plan: NoOp";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{e}' did not contain expected error '{expected_error}'"
            ),
        }
    }

    #[tokio::test]
    async fn bad_extension_planner() {
        // Test that creating an execution plan whose schema doesn't
        // match the logical plan's schema generates an error.
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            BadExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error: &str = "Error during planning: \
            Extension planner for NoOp created an ExecutionPlan with mismatched schema. \
            LogicalPlan schema: \
            DFSchema { inner: Schema { fields: \
                [Field { name: \"a\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, metadata: {} }], \
                metadata: {} }, field_qualifiers: [None], \
                functional_dependencies: FunctionalDependencies { deps: [] } }, \
            ExecutionPlan schema: Schema { fields: \
                [Field { name: \"b\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, metadata: {} }], \
                metadata: {} }";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{e}' did not contain expected error '{expected_error}'"
            ),
        }
    }

    #[tokio::test]
    async fn in_list_types() -> Result<()> {
        // expression: "a in ('a', 1)"
        let list = vec![lit("a"), lit(1i64)];
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await?;
        // verify that the plan correctly adds cast from Int64(1) to Utf8, and the const will be evaluated.

        let expected = "expr: [(BinaryExpr { left: BinaryExpr { left: Column { name: \"c1\", index: 0 }, op: Eq, right: Literal { value: Utf8(\"a\") } }, op: Or, right: BinaryExpr { left: Column { name: \"c1\", index: 0 }, op: Eq, right: Literal { value: Utf8(\"1\") } } }";

        let actual = format!("{execution_plan:?}");
        assert!(actual.contains(expected), "{}", actual);

        Ok(())
    }

    #[tokio::test]
    async fn in_list_types_struct_literal() -> Result<()> {
        // expression: "a in (struct::null, 'a')"
        let list = vec![struct_literal(), lit("a")];

        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c12").lt_eq(lit(0.025)).in_list(list, false)])?
            .build()?;
        let e = plan(&logical_plan).await.unwrap_err().to_string();

        assert_contains!(
            &e,
            r#"Error during planning: Can not find compatible types to compare Boolean with [Struct([Field { name: "foo", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), Utf8]"#
        );

        Ok(())
    }

    /// Return a `null` literal representing a struct type like: `{ a: bool }`
    fn struct_literal() -> Expr {
        let struct_literal = ScalarValue::try_from(DataType::Struct(
            vec![Field::new("foo", DataType::Boolean, false)].into(),
        ))
        .unwrap();

        lit(struct_literal)
    }

    #[tokio::test]
    async fn hash_agg_input_schema() -> Result<()> {
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "sum(aggregate_test_100.c2)",
            final_hash_agg.schema().field(1).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c2", final_hash_agg.input_schema().field(1).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_input_schema() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "sum(aggregate_test_100.c3)",
            final_hash_agg.schema().field(2).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c3", final_hash_agg.input_schema().field(2).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned_on_dicts() -> Result<()> {
        let dict_array: DictionaryArray<Int32Type> =
            vec!["A", "B", "A", "A", "C", "A"].into_iter().collect();
        let val_array: Int32Array = vec![1, 2, 2, 4, 1, 1].into();

        let batch = RecordBatch::try_from_iter(vec![
            ("d1", Arc::new(dict_array) as ArrayRef),
            ("d2", Arc::new(val_array) as ArrayRef),
        ])
        .unwrap();

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let ctx = SessionContext::new();

        let logical_plan = LogicalPlanBuilder::from(
            ctx.read_table(Arc::new(table))?.into_optimized_plan()?,
        )
        .aggregate(vec![col("d1")], vec![sum(col("d2"))])?
        .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));
        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_by_partitioned() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_with_alias() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
        ]));

        let logical_plan = scan_empty(None, schema.as_ref(), None)?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .project(vec![col("c1"), sum(col("c2")).alias("total_salary")])?
            .build()?;

        let physical_plan = plan(&logical_plan).await?;
        assert_eq!("c1", physical_plan.schema().field(0).name().as_str());
        assert_eq!(
            "total_salary",
            physical_plan.schema().field(1).name().as_str()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_explain() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .explain(true, false)
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();
        if let Some(plan) = plan.as_any().downcast_ref::<ExplainExec>() {
            let stringified_plans = plan.stringified_plans();
            assert!(stringified_plans.len() >= 4);
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalLogicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::InitialPhysicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::OptimizedPhysicalPlan { .. })));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalPhysicalPlan)));
        } else {
            panic!(
                "Plan was not an explain plan: {}",
                displayable(plan.as_ref()).indent(true)
            );
        }
    }

    struct ErrorExtensionPlanner {}

    #[async_trait]
    impl ExtensionPlanner for ErrorExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            internal_err!("BOOM")
        }
    }
    /// An example extension node that doesn't do anything
    #[derive(PartialEq, Eq, Hash)]
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::from_unqualified_fields(
                        vec![Field::new("a", DataType::Int32, false)].into(),
                        HashMap::new(),
                    )
                    .unwrap(),
                ),
            }
        }
    }

    impl Debug for NoOpExtensionNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }
    }

    impl UserDefinedLogicalNodeCore for NoOpExtensionNode {
        fn name(&self) -> &str {
            "NoOp"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            _inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            unimplemented!("NoOp");
        }
    }

    #[derive(Debug)]
    struct NoOpExecutionPlan {
        cache: PlanProperties,
    }

    impl NoOpExecutionPlan {
        fn new(schema: SchemaRef) -> Self {
            let cache = Self::compute_properties(schema.clone());
            Self { cache }
        }

        /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            let eq_properties = EquivalenceProperties::new(schema);
            PlanProperties::new(
                eq_properties,
                // Output Partitioning
                Partitioning::UnknownPartitioning(1),
                // Execution Mode
                ExecutionMode::Bounded,
            )
        }
    }

    impl DisplayAs for NoOpExecutionPlan {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "NoOpExecutionPlan")
                }
            }
        }
    }

    impl ExecutionPlan for NoOpExecutionPlan {
        fn name(&self) -> &'static str {
            "NoOpExecutionPlan"
        }

        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("NoOpExecutionPlan::with_new_children");
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("NoOpExecutionPlan::execute");
        }
    }

    //  Produces an execution plan where the schema is mismatched from
    //  the logical plan node.
    struct BadExtensionPlanner {}

    #[async_trait]
    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(Some(Arc::new(NoOpExecutionPlan::new(SchemaRef::new(
                Schema::new(vec![Field::new("b", DataType::Int32, false)]),
            )))))
        }
    }

    async fn test_csv_scan_with_name(name: &str) -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{testdata}/csv/aggregate_test_100.csv");
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan =
            match ctx.read_csv(path, options).await?.into_optimized_plan()? {
                LogicalPlan::TableScan(ref scan) => {
                    let mut scan = scan.clone();
                    let table_reference = TableReference::from(name);
                    scan.table_name = table_reference;
                    let new_schema = scan
                        .projected_schema
                        .as_ref()
                        .clone()
                        .replace_qualifier(name.to_string());
                    scan.projected_schema = Arc::new(new_schema);
                    LogicalPlan::TableScan(scan)
                }
                _ => unimplemented!(),
            };
        Ok(LogicalPlanBuilder::from(logical_plan))
    }

    async fn test_csv_scan() -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{testdata}/csv/aggregate_test_100.csv");
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        Ok(LogicalPlanBuilder::from(
            ctx.read_csv(path, options).await?.into_optimized_plan()?,
        ))
    }

    #[tokio::test]
    async fn test_display_plan_in_graphviz_format() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .project(vec![col("id") + lit(2)])
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();

        let expected_graph = r#"
// Begin DataFusion GraphViz Plan,
// display it online here: https://dreampuf.github.io/GraphvizOnline

digraph {
    1[shape=box label="ProjectionExec: expr=[id@0 + 2 as employee.id + Int32(2)]", tooltip=""]
    2[shape=box label="EmptyExec", tooltip=""]
    1 -> 2 [arrowhead=none, arrowtail=normal, dir=back]
}
// End DataFusion GraphViz Plan
"#;

        let generated_graph = format!("{}", displayable(&*plan).graphviz());

        assert_eq!(expected_graph, generated_graph);
    }

    #[tokio::test]
    async fn test_display_graphviz_with_statistics() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .project(vec![col("id") + lit(2)])
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();

        let expected_tooltip = ", tooltip=\"statistics=[";

        let generated_graph = format!(
            "{}",
            displayable(&*plan).set_show_statistics(true).graphviz()
        );

        assert_contains!(generated_graph, expected_tooltip);
    }
}
