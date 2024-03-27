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

//! Eliminate common sub-expression.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::utils::is_volatile_expression;
use crate::{utils, OptimizerConfig, OptimizerRule};

use arrow::datatypes::{DataType, Field};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
    TreeNodeVisitor,
};
use datafusion_common::{
    internal_err, qualified_name, Column, DFSchema, DFSchemaRef, DataFusionError, Result,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::logical_plan::{Aggregate, LogicalPlan, Projection, Window};
use datafusion_expr::{col, Expr, ExprSchemable};

/// A map from expression's identifier to tuple including
/// - the expression itself (cloned)
/// - counter
/// - DataType of this expression.
type ExprSet = HashMap<Identifier, (Expr, usize, DataType)>;

/// An ordered map of Identifiers assigned by `ExprIdentifierVisitor` in an
/// initial expression  walk.
///
/// Used by `CommonSubexprRewriter`, which rewrites the expressions to remove
/// common subexpressions.
///
/// Elements in this array are created on the walk down the expression tree
/// during `f_down`. Thus element 0 is the root of the expression tree. The
/// tuple contains:
/// - series_number.
///     - Incremented during `f_up`, start from 1.
///     - Thus, items with higher idx have the lower series_number.
/// - [`Identifier`]
///     - Identifier of the expression. If empty (`""`), expr should not be considered for common elimination.
///
/// # Example
/// An expression like `(a + b)` would have the following `IdArray`:
/// ```text
/// [
///   (3, "a + b"),
///   (2, "a"),
///   (1, "b")
/// ]
/// ```
type IdArray = Vec<(usize, Identifier)>;

/// Identifier for each subexpression.
///
/// Note that the current implementation uses the `Display` of an expression
/// (a `String`) as `Identifier`.
///
/// An identifier should (ideally) be able to "hash", "accumulate", "equal" and "have no
/// collision (as low as possible)"
///
/// Since an identifier is likely to be copied many times, it is better that an identifier
/// is small or "copy". otherwise some kinds of reference count is needed. String description
/// here is not such a good choose.
type Identifier = String;

/// Performs Common Sub-expression Elimination optimization.
///
/// This optimization improves query performance by computing expressions that
/// appear more than once and reusing those results rather than re-computing the
/// same value
///
/// Currently only common sub-expressions within a single `LogicalPlan` are
/// be eliminated.
///
/// # Example
///
/// Given a projection that computes the same expensive expression
/// multiple times such as parsing as string as a date with `to_date` twice:
///
/// ```text
/// ProjectionExec(expr=[extract (day from to_date(c1)), extract (year from to_date(c1))])
/// ```
///
/// This optimization will rewrite the plan to compute the common expression once
/// using a new `ProjectionExec` and then rewrite the original expressions to
/// refer to that new column.
///
/// ```text
/// ProjectionExec(exprs=[extract (day from new_col), extract (year from new_col)]) <-- reuse here
///   ProjectionExec(exprs=[to_date(c1) as new_col]) <-- compute to_date once
/// ```
pub struct CommonSubexprEliminate {}

impl CommonSubexprEliminate {
    fn rewrite_exprs_list(
        &self,
        exprs_list: &[&[Expr]],
        arrays_list: &[&[Vec<(usize, String)>]],
        expr_set: &ExprSet,
        affected_id: &mut BTreeSet<Identifier>,
    ) -> Result<Vec<Vec<Expr>>> {
        exprs_list
            .iter()
            .zip(arrays_list.iter())
            .map(|(exprs, arrays)| {
                exprs
                    .iter()
                    .cloned()
                    .zip(arrays.iter())
                    .map(|(expr, id_array)| {
                        replace_common_expr(expr, id_array, expr_set, affected_id)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()
    }

    fn rewrite_expr(
        &self,
        exprs_list: &[&[Expr]],
        arrays_list: &[&[Vec<(usize, String)>]],
        input: &LogicalPlan,
        expr_set: &ExprSet,
        config: &dyn OptimizerConfig,
    ) -> Result<(Vec<Vec<Expr>>, LogicalPlan)> {
        let mut affected_id = BTreeSet::<Identifier>::new();

        let rewrite_exprs =
            self.rewrite_exprs_list(exprs_list, arrays_list, expr_set, &mut affected_id)?;

        let mut new_input = self
            .try_optimize(input, config)?
            .unwrap_or_else(|| input.clone());
        if !affected_id.is_empty() {
            new_input = build_common_expr_project_plan(new_input, affected_id, expr_set)?;
        }

        Ok((rewrite_exprs, new_input))
    }

    fn try_optimize_window(
        &self,
        window: &Window,
        config: &dyn OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut window_exprs = vec![];
        let mut arrays_per_window = vec![];
        let mut expr_set = ExprSet::new();

        // Get all window expressions inside the consecutive window operators.
        // Consecutive window expressions may refer to same complex expression.
        // If same complex expression is referred more than once by subsequent `WindowAggr`s,
        // we can cache complex expression by evaluating it with a projection before the
        // first WindowAggr.
        // This enables us to cache complex expression "c3+c4" for following plan:
        // WindowAggr: windowExpr=[[SUM(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
        // --WindowAggr: windowExpr=[[SUM(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
        // where, it is referred once by each `WindowAggr` (total of 2) in the plan.
        let mut plan = LogicalPlan::Window(window.clone());
        while let LogicalPlan::Window(window) = plan {
            let Window {
                input, window_expr, ..
            } = window;
            plan = input.as_ref().clone();

            let input_schema = Arc::clone(input.schema());
            let arrays =
                to_arrays(&window_expr, input_schema, &mut expr_set, ExprMask::Normal)?;

            window_exprs.push(window_expr);
            arrays_per_window.push(arrays);
        }

        let mut window_exprs = window_exprs
            .iter()
            .map(|expr| expr.as_slice())
            .collect::<Vec<_>>();
        let arrays_per_window = arrays_per_window
            .iter()
            .map(|arrays| arrays.as_slice())
            .collect::<Vec<_>>();

        assert_eq!(window_exprs.len(), arrays_per_window.len());
        let (mut new_expr, new_input) = self.rewrite_expr(
            &window_exprs,
            &arrays_per_window,
            &plan,
            &expr_set,
            config,
        )?;
        assert_eq!(window_exprs.len(), new_expr.len());

        // Construct consecutive window operator, with their corresponding new window expressions.
        plan = new_input;
        while let Some(new_window_expr) = new_expr.pop() {
            // Since `new_expr` and `window_exprs` length are same. We can safely `.unwrap` here.
            let orig_window_expr = window_exprs.pop().unwrap();
            assert_eq!(new_window_expr.len(), orig_window_expr.len());

            // Rename new re-written window expressions with original name (by giving alias)
            // Otherwise we may receive schema error, in subsequent operators.
            let new_window_expr = new_window_expr
                .into_iter()
                .zip(orig_window_expr.iter())
                .map(|(new_window_expr, window_expr)| {
                    let original_name = window_expr.name_for_alias()?;
                    new_window_expr.alias_if_changed(original_name)
                })
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::Window(Window::try_new(new_window_expr, Arc::new(plan))?);
        }

        Ok(plan)
    }

    fn try_optimize_aggregate(
        &self,
        aggregate: &Aggregate,
        config: &dyn OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let Aggregate {
            group_expr,
            aggr_expr,
            input,
            ..
        } = aggregate;
        let mut expr_set = ExprSet::new();

        // rewrite inputs
        let input_schema = Arc::clone(input.schema());
        let group_arrays = to_arrays(
            group_expr,
            Arc::clone(&input_schema),
            &mut expr_set,
            ExprMask::Normal,
        )?;
        let aggr_arrays =
            to_arrays(aggr_expr, input_schema, &mut expr_set, ExprMask::Normal)?;

        let (mut new_expr, new_input) = self.rewrite_expr(
            &[group_expr, aggr_expr],
            &[&group_arrays, &aggr_arrays],
            input,
            &expr_set,
            config,
        )?;
        // note the reversed pop order.
        let new_aggr_expr = pop_expr(&mut new_expr)?;
        let new_group_expr = pop_expr(&mut new_expr)?;

        // create potential projection on top
        let mut expr_set = ExprSet::new();
        let new_input_schema = Arc::clone(new_input.schema());
        let aggr_arrays = to_arrays(
            &new_aggr_expr,
            new_input_schema.clone(),
            &mut expr_set,
            ExprMask::NormalAndAggregates,
        )?;
        let mut affected_id = BTreeSet::<Identifier>::new();
        let mut rewritten = self.rewrite_exprs_list(
            &[&new_aggr_expr],
            &[&aggr_arrays],
            &expr_set,
            &mut affected_id,
        )?;
        let rewritten = pop_expr(&mut rewritten)?;

        if affected_id.is_empty() {
            // Alias aggregation expressions if they have changed
            let new_aggr_expr = new_aggr_expr
                .iter()
                .zip(aggr_expr.iter())
                .map(|(new_expr, old_expr)| {
                    new_expr.clone().alias_if_changed(old_expr.display_name()?)
                })
                .collect::<Result<Vec<Expr>>>()?;
            // Since group_epxr changes, schema changes also. Use try_new method.
            Aggregate::try_new(Arc::new(new_input), new_group_expr, new_aggr_expr)
                .map(LogicalPlan::Aggregate)
        } else {
            let mut agg_exprs = vec![];

            for id in affected_id {
                match expr_set.get(&id) {
                    Some((expr, _, _)) => {
                        // todo: check `nullable`
                        agg_exprs.push(expr.clone().alias(&id));
                    }
                    _ => {
                        return internal_err!("expr_set invalid state");
                    }
                }
            }

            let mut proj_exprs = vec![];
            for expr in &new_group_expr {
                extract_expressions(expr, &new_input_schema, &mut proj_exprs)?
            }
            for (expr_rewritten, expr_orig) in rewritten.into_iter().zip(new_aggr_expr) {
                if expr_rewritten == expr_orig {
                    if let Expr::Alias(Alias { expr, name, .. }) = expr_rewritten {
                        agg_exprs.push(expr.alias(&name));
                        proj_exprs.push(Expr::Column(Column::from_name(name)));
                    } else {
                        let id = ExprIdentifierVisitor::<'static>::expr_identifier(
                            &expr_rewritten,
                        );
                        let (qualifier, field) =
                            expr_rewritten.to_field(&new_input_schema)?;
                        let out_name = qualified_name(qualifier.as_ref(), field.name());
                        agg_exprs.push(expr_rewritten.alias(&id));
                        proj_exprs
                            .push(Expr::Column(Column::from_name(id)).alias(out_name));
                    }
                } else {
                    proj_exprs.push(expr_rewritten);
                }
            }

            let agg = LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(new_input),
                new_group_expr,
                agg_exprs,
            )?);

            Ok(LogicalPlan::Projection(Projection::try_new(
                proj_exprs,
                Arc::new(agg),
            )?))
        }
    }

    fn try_unary_plan(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let expr = plan.expressions();
        let inputs = plan.inputs();
        let input = inputs[0];
        let input_schema = Arc::clone(input.schema());
        let mut expr_set = ExprSet::new();

        // Visit expr list and build expr identifier to occuring count map (`expr_set`).
        let arrays = to_arrays(&expr, input_schema, &mut expr_set, ExprMask::Normal)?;

        let (mut new_expr, new_input) =
            self.rewrite_expr(&[&expr], &[&arrays], input, &expr_set, config)?;

        plan.with_new_exprs(pop_expr(&mut new_expr)?, vec![new_input])
    }
}

impl OptimizerRule for CommonSubexprEliminate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let optimized_plan = match plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Filter(_) => Some(self.try_unary_plan(plan, config)?),
            LogicalPlan::Window(window) => {
                Some(self.try_optimize_window(window, config)?)
            }
            LogicalPlan::Aggregate(aggregate) => {
                Some(self.try_optimize_aggregate(aggregate, config)?)
            }
            LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Prepare(_) => {
                // apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, config)?
            }
        };

        let original_schema = plan.schema().clone();
        match optimized_plan {
            Some(optimized_plan) if optimized_plan.schema() != &original_schema => {
                // add an additional projection if the output schema changed.
                Ok(Some(build_recover_project_plan(
                    &original_schema,
                    optimized_plan,
                )?))
            }
            plan => Ok(plan),
        }
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

impl Default for CommonSubexprEliminate {
    fn default() -> Self {
        Self::new()
    }
}

impl CommonSubexprEliminate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn pop_expr(new_expr: &mut Vec<Vec<Expr>>) -> Result<Vec<Expr>> {
    new_expr
        .pop()
        .ok_or_else(|| DataFusionError::Internal("Failed to pop expression".to_string()))
}

fn to_arrays(
    expr: &[Expr],
    input_schema: DFSchemaRef,
    expr_set: &mut ExprSet,
    expr_mask: ExprMask,
) -> Result<Vec<Vec<(usize, String)>>> {
    expr.iter()
        .map(|e| {
            let mut id_array = vec![];
            expr_to_identifier(
                e,
                expr_set,
                &mut id_array,
                Arc::clone(&input_schema),
                expr_mask,
            )?;

            Ok(id_array)
        })
        .collect::<Result<Vec<_>>>()
}

/// Build the "intermediate" projection plan that evaluates the extracted common expressions.
fn build_common_expr_project_plan(
    input: LogicalPlan,
    affected_id: BTreeSet<Identifier>,
    expr_set: &ExprSet,
) -> Result<LogicalPlan> {
    let mut project_exprs = vec![];
    let mut fields_set = BTreeSet::new();

    for id in affected_id {
        match expr_set.get(&id) {
            Some((expr, _, data_type)) => {
                // todo: check `nullable`
                let field = Field::new(&id, data_type.clone(), true);
                fields_set.insert(field.name().to_owned());
                project_exprs.push(expr.clone().alias(&id));
            }
            _ => {
                return internal_err!("expr_set invalid state");
            }
        }
    }

    for (qualifier, field) in input.schema().iter() {
        if fields_set.insert(qualified_name(qualifier, field.name())) {
            project_exprs.push(Expr::Column(Column::from((qualifier, field.as_ref()))));
        }
    }

    Ok(LogicalPlan::Projection(Projection::try_new(
        project_exprs,
        Arc::new(input),
    )?))
}

/// Build the projection plan to eliminate unexpected columns produced by
/// the "intermediate" projection plan built in [build_common_expr_project_plan].
///
/// This is for those plans who don't keep its own output schema like `Filter` or `Sort`.
fn build_recover_project_plan(
    schema: &DFSchema,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let col_exprs = schema
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field.as_ref()))))
        .collect();
    Ok(LogicalPlan::Projection(Projection::try_new(
        col_exprs,
        Arc::new(input),
    )?))
}

fn extract_expressions(
    expr: &Expr,
    schema: &DFSchema,
    result: &mut Vec<Expr>,
) -> Result<()> {
    if let Expr::GroupingSet(groupings) = expr {
        for e in groupings.distinct_expr() {
            let (qualifier, field) = e.to_field(schema)?;
            let col = Column::new(qualifier, field.name());
            result.push(Expr::Column(col))
        }
    } else {
        let (qualifier, field) = expr.to_field(schema)?;
        let col = Column::new(qualifier, field.name());
        result.push(Expr::Column(col));
    }

    Ok(())
}

/// Which type of [expressions](Expr) should be considered for rewriting?
#[derive(Debug, Clone, Copy)]
enum ExprMask {
    /// Ignores:
    ///
    /// - [`Literal`](Expr::Literal)
    /// - [`Columns`](Expr::Column)
    /// - [`ScalarVariable`](Expr::ScalarVariable)
    /// - [`Alias`](Expr::Alias)
    /// - [`Sort`](Expr::Sort)
    /// - [`Wildcard`](Expr::Wildcard)
    /// - [`AggregateFunction`](Expr::AggregateFunction)
    Normal,

    /// Like [`Normal`](Self::Normal), but includes [`AggregateFunction`](Expr::AggregateFunction).
    NormalAndAggregates,
}

impl ExprMask {
    fn ignores(&self, expr: &Expr) -> bool {
        let is_normal_minus_aggregates = matches!(
            expr,
            Expr::Literal(..)
                | Expr::Column(..)
                | Expr::ScalarVariable(..)
                | Expr::Alias(..)
                | Expr::Sort { .. }
                | Expr::Wildcard { .. }
        );

        let is_aggr = matches!(expr, Expr::AggregateFunction(..));

        match self {
            Self::Normal => is_normal_minus_aggregates || is_aggr,
            Self::NormalAndAggregates => is_normal_minus_aggregates,
        }
    }
}

/// Go through an expression tree and generate identifier.
///
/// An identifier contains information of the expression itself and its sub-expression.
/// This visitor implementation use a stack `visit_stack` to track traversal, which
/// lets us know when a sub-tree's visiting is finished. When `pre_visit` is called
/// (traversing to a new node), an `EnterMark` and an `ExprItem` will be pushed into stack.
/// And try to pop out a `EnterMark` on leaving a node (`f_up()`). All `ExprItem`
/// before the first `EnterMark` is considered to be sub-tree of the leaving node.
///
/// This visitor also records identifier in `id_array`. Makes the following traverse
/// pass can get the identifier of a node without recalculate it. We assign each node
/// in the expr tree a series number, start from 1, maintained by `series_number`.
/// Series number represents the order we left (`f_up()`) a node. Has the property
/// that child node's series number always smaller than parent's. While `id_array` is
/// organized in the order we enter (`f_down()`) a node. `node_count` helps us to
/// get the index of `id_array` for each node.
///
/// `Expr` without sub-expr (column, literal etc.) will not have identifier
/// because they should not be recognized as common sub-expr.
struct ExprIdentifierVisitor<'a> {
    // param
    expr_set: &'a mut ExprSet,
    /// series number (usize) and identifier.
    id_array: &'a mut IdArray,
    /// input schema for the node that we're optimizing, so we can determine the correct datatype
    /// for each subexpression
    input_schema: DFSchemaRef,
    // inner states
    visit_stack: Vec<VisitRecord>,
    /// increased in fn_down, start from 0.
    node_count: usize,
    /// increased in fn_up, start from 1.
    series_number: usize,
    /// which expression should be skipped?
    expr_mask: ExprMask,
}

/// Record item that used when traversing a expression tree.
enum VisitRecord {
    /// `usize` is the monotone increasing series number assigned in pre_visit().
    /// Starts from 0. Is used to index the identifier array `id_array` in post_visit().
    EnterMark(usize),
    /// the node's children were skipped => jump to f_up on same node
    JumpMark(usize),
    /// Accumulated identifier of sub expression.
    ExprItem(Identifier),
}

impl ExprIdentifierVisitor<'_> {
    fn expr_identifier(expr: &Expr) -> Identifier {
        format!("{expr}")
    }

    /// Find the first `EnterMark` in the stack, and accumulates every `ExprItem`
    /// before it.
    fn pop_enter_mark(&mut self) -> (usize, Identifier) {
        let mut desc = String::new();

        while let Some(item) = self.visit_stack.pop() {
            match item {
                VisitRecord::EnterMark(idx) | VisitRecord::JumpMark(idx) => {
                    return (idx, desc);
                }
                VisitRecord::ExprItem(id) => {
                    desc.push_str(&id);
                }
            }
        }
        unreachable!("Enter mark should paired with node number");
    }
}

impl TreeNodeVisitor for ExprIdentifierVisitor<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: &Expr) -> Result<TreeNodeRecursion> {
        // put placeholder, sets the proper array length
        self.id_array.push((0, "".to_string()));

        // related to https://github.com/apache/arrow-datafusion/issues/8814
        // If the expr contain volatile expression or is a short-circuit expression, skip it.
        if expr.short_circuits() || is_volatile_expression(expr)? {
            self.visit_stack
                .push(VisitRecord::JumpMark(self.node_count));
            return Ok(TreeNodeRecursion::Jump); // go to f_up
        }

        self.visit_stack
            .push(VisitRecord::EnterMark(self.node_count));
        self.node_count += 1;

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, expr: &Expr) -> Result<TreeNodeRecursion> {
        self.series_number += 1;

        let (idx, sub_expr_identifier) = self.pop_enter_mark();

        // skip exprs should not be recognize.
        if self.expr_mask.ignores(expr) {
            let curr_expr_identifier = Self::expr_identifier(expr);
            self.visit_stack
                .push(VisitRecord::ExprItem(curr_expr_identifier));
            self.id_array[idx].0 = self.series_number; // leave Identifer as empty "", since will not use as common expr
            return Ok(TreeNodeRecursion::Continue);
        }
        let mut desc = Self::expr_identifier(expr);
        desc.push_str(&sub_expr_identifier);

        self.id_array[idx] = (self.series_number, desc.clone());
        self.visit_stack.push(VisitRecord::ExprItem(desc.clone()));

        let data_type = expr.get_type(&self.input_schema)?;

        self.expr_set
            .entry(desc)
            .or_insert_with(|| (expr.clone(), 0, data_type))
            .1 += 1;
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Go through an expression tree and generate identifier for every node in this tree.
fn expr_to_identifier(
    expr: &Expr,
    expr_set: &mut ExprSet,
    id_array: &mut Vec<(usize, Identifier)>,
    input_schema: DFSchemaRef,
    expr_mask: ExprMask,
) -> Result<()> {
    expr.visit(&mut ExprIdentifierVisitor {
        expr_set,
        id_array,
        input_schema,
        visit_stack: vec![],
        node_count: 0,
        series_number: 0,
        expr_mask,
    })?;

    Ok(())
}

/// Rewrite expression by replacing detected common sub-expression with
/// the corresponding temporary column name. That column contains the
/// evaluate result of replaced expression.
struct CommonSubexprRewriter<'a> {
    expr_set: &'a ExprSet,
    id_array: &'a IdArray,
    /// Which identifier is replaced.
    affected_id: &'a mut BTreeSet<Identifier>,

    /// the max series number we have rewritten. Other expression nodes
    /// with smaller series number is already replaced and shouldn't
    /// do anything with them.
    max_series_number: usize,
    /// current node's information's index in `id_array`.
    curr_index: usize,
}

impl TreeNodeRewriter for CommonSubexprRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // The `CommonSubexprRewriter` relies on `ExprIdentifierVisitor` to generate
        // the `id_array`, which records the expr's identifier used to rewrite expr. So if we
        // skip an expr in `ExprIdentifierVisitor`, we should skip it here, too.
        if expr.short_circuits() || is_volatile_expression(&expr)? {
            return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
        }

        let (series_number, curr_id) = &self.id_array[self.curr_index];

        // halting conditions
        if self.curr_index >= self.id_array.len()
            || self.max_series_number > *series_number
        {
            return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
        }

        // skip `Expr`s without identifier (empty identifier).
        if curr_id.is_empty() {
            self.curr_index += 1; // incr idx for id_array, when not jumping
            return Ok(Transformed::no(expr));
        }

        // lookup previously visited expression
        match self.expr_set.get(curr_id) {
            Some((_, counter, _)) => {
                // if has a commonly used (a.k.a. 1+ use) expr
                if *counter > 1 {
                    self.affected_id.insert(curr_id.clone());

                    // This expr tree is finished.
                    if self.curr_index >= self.id_array.len() {
                        return Ok(Transformed::new(
                            expr,
                            false,
                            TreeNodeRecursion::Jump,
                        ));
                    }

                    // incr idx for id_array, when not jumping
                    self.curr_index += 1;

                    // series_number was the inverse number ordering (when doing f_up)
                    self.max_series_number = *series_number;
                    // step index to skip all sub-node (which has smaller series number).
                    while self.curr_index < self.id_array.len()
                        && *series_number > self.id_array[self.curr_index].0
                    {
                        self.curr_index += 1;
                    }

                    let expr_name = expr.display_name()?;
                    // Alias this `Column` expr to it original "expr name",
                    // `projection_push_down` optimizer use "expr name" to eliminate useless
                    // projections.
                    Ok(Transformed::new(
                        col(curr_id).alias(expr_name),
                        true,
                        TreeNodeRecursion::Jump,
                    ))
                } else {
                    self.curr_index += 1;
                    Ok(Transformed::no(expr))
                }
            }
            _ => internal_err!("expr_set invalid state"),
        }
    }
}

fn replace_common_expr(
    expr: Expr,
    id_array: &IdArray,
    expr_set: &ExprSet,
    affected_id: &mut BTreeSet<Identifier>,
) -> Result<Expr> {
    expr.rewrite(&mut CommonSubexprRewriter {
        expr_set,
        id_array,
        affected_id,
        max_series_number: 0,
        curr_index: 0,
    })
    .data()
}

#[cfg(test)]
mod test {
    use std::iter;

    use arrow::datatypes::{Field, Schema};

    use datafusion_common::DFSchema;
    use datafusion_expr::logical_plan::{table_scan, JoinType};
    use datafusion_expr::{
        avg, col, lit, logical_plan::builder::LogicalPlanBuilder, sum,
    };
    use datafusion_expr::{
        grouping_set, AccumulatorFactoryFunction, AggregateUDF, Signature,
        SimpleAggregateUDF, Volatility,
    };

    use crate::optimizer::OptimizerContext;
    use crate::test::*;

    use super::*;

    fn assert_optimized_plan_eq(expected: &str, plan: &LogicalPlan) {
        let optimizer = CommonSubexprEliminate {};
        let optimized_plan = optimizer
            .try_optimize(plan, &OptimizerContext::new())
            .unwrap()
            .expect("failed to optimize plan");
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(expected, formatted_plan);
    }

    #[test]
    fn id_array_visitor() -> Result<()> {
        let expr = ((sum(col("a") + lit(1))) - avg(col("c"))) * lit(2);

        let schema = Arc::new(DFSchema::from_unqualifed_fields(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
            ]
            .into(),
            Default::default(),
        )?);

        // skip aggregates
        let mut id_array = vec![];
        expr_to_identifier(
            &expr,
            &mut HashMap::new(),
            &mut id_array,
            Arc::clone(&schema),
            ExprMask::Normal,
        )?;

        let expected = vec![
            (9, "(SUM(a + Int32(1)) - AVG(c)) * Int32(2)Int32(2)SUM(a + Int32(1)) - AVG(c)AVG(c)SUM(a + Int32(1))"),
            (7, "SUM(a + Int32(1)) - AVG(c)AVG(c)SUM(a + Int32(1))"),
            (4, ""),
            (3, "a + Int32(1)Int32(1)a"),
            (1, ""),
            (2, ""),
            (6, ""),
            (5, ""),
            (8, "")
        ]
        .into_iter()
        .map(|(number, id)| (number, id.into()))
        .collect::<Vec<_>>();
        assert_eq!(expected, id_array);

        // include aggregates
        let mut id_array = vec![];
        expr_to_identifier(
            &expr,
            &mut HashMap::new(),
            &mut id_array,
            Arc::clone(&schema),
            ExprMask::NormalAndAggregates,
        )?;

        let expected = vec![
            (9, "(SUM(a + Int32(1)) - AVG(c)) * Int32(2)Int32(2)SUM(a + Int32(1)) - AVG(c)AVG(c)cSUM(a + Int32(1))a + Int32(1)Int32(1)a"),
            (7, "SUM(a + Int32(1)) - AVG(c)AVG(c)cSUM(a + Int32(1))a + Int32(1)Int32(1)a"),
            (4, "SUM(a + Int32(1))a + Int32(1)Int32(1)a"),
            (3, "a + Int32(1)Int32(1)a"),
            (1, ""),
            (2, ""),
            (6, "AVG(c)c"),
            (5, ""),
            (8, "")
        ]
        .into_iter()
        .map(|(number, id)| (number, id.into()))
        .collect::<Vec<_>>();
        assert_eq!(expected, id_array);

        Ok(())
    }

    #[test]
    fn tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T;
        //
        // The manual assembled logical plan don't contains the outermost `Projection`.

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") * (lit(1) - col("b"))),
                    sum((col("a") * (lit(1) - col("b"))) * (lit(1) + col("c"))),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[SUM(test.a * (Int32(1) - test.b)Int32(1) - test.btest.bInt32(1)test.a AS test.a * Int32(1) - test.b), SUM(test.a * (Int32(1) - test.b)Int32(1) - test.btest.bInt32(1)test.a AS test.a * Int32(1) - test.b * (Int32(1) + test.c))]]\
        \n  Projection: test.a * (Int32(1) - test.b) AS test.a * (Int32(1) - test.b)Int32(1) - test.btest.bInt32(1)test.a, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;

        let return_type = DataType::UInt32;
        let accumulator: AccumulatorFactoryFunction = Arc::new(|_| unimplemented!());
        let state_type = vec![DataType::UInt32];
        let udf_agg = |inner: Expr| {
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                Arc::new(AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
                    "my_agg",
                    Signature::exact(vec![DataType::UInt32], Volatility::Stable),
                    return_type.clone(),
                    accumulator.clone(),
                    state_type.clone(),
                ))),
                vec![inner],
                false,
                None,
                None,
            ))
        };

        // test: common aggregates
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    // common: avg(col("a"))
                    avg(col("a")).alias("col1"),
                    avg(col("a")).alias("col2"),
                    // no common
                    avg(col("b")).alias("col3"),
                    avg(col("c")),
                    // common: udf_agg(col("a"))
                    udf_agg(col("a")).alias("col4"),
                    udf_agg(col("a")).alias("col5"),
                    // no common
                    udf_agg(col("b")).alias("col6"),
                    udf_agg(col("c")),
                ],
            )?
            .build()?;

        let expected = "Projection: AVG(test.a)test.a AS AVG(test.a) AS col1, AVG(test.a)test.a AS AVG(test.a) AS col2, col3, AVG(test.c) AS AVG(test.c), my_agg(test.a)test.a AS my_agg(test.a) AS col4, my_agg(test.a)test.a AS my_agg(test.a) AS col5, col6, my_agg(test.c) AS my_agg(test.c)\
        \n  Aggregate: groupBy=[[]], aggr=[[AVG(test.a) AS AVG(test.a)test.a, my_agg(test.a) AS my_agg(test.a)test.a, AVG(test.b) AS col3, AVG(test.c) AS AVG(test.c), my_agg(test.b) AS col6, my_agg(test.c) AS my_agg(test.c)]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        // test: trafo after aggregate
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    lit(1) + avg(col("a")),
                    lit(1) - avg(col("a")),
                    lit(1) + udf_agg(col("a")),
                    lit(1) - udf_agg(col("a")),
                ],
            )?
            .build()?;

        let expected = "Projection: Int32(1) + AVG(test.a)test.a AS AVG(test.a), Int32(1) - AVG(test.a)test.a AS AVG(test.a), Int32(1) + my_agg(test.a)test.a AS my_agg(test.a), Int32(1) - my_agg(test.a)test.a AS my_agg(test.a)\
        \n  Aggregate: groupBy=[[]], aggr=[[AVG(test.a) AS AVG(test.a)test.a, my_agg(test.a) AS my_agg(test.a)test.a]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        // test: transformation before aggregate
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    avg(lit(1u32) + col("a")).alias("col1"),
                    udf_agg(lit(1u32) + col("a")).alias("col2"),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a) AS col1, my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a) AS col2]]\
        \n  Projection: UInt32(1) + test.a AS UInt32(1) + test.atest.aUInt32(1), test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        // test: common between agg and group
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                vec![lit(1u32) + col("a")],
                vec![
                    avg(lit(1u32) + col("a")).alias("col1"),
                    udf_agg(lit(1u32) + col("a")).alias("col2"),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a]], aggr=[[AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a) AS col1, my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a) AS col2]]\
        \n  Projection: UInt32(1) + test.a AS UInt32(1) + test.atest.aUInt32(1), test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        // test: all mixed
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![lit(1u32) + col("a")],
                vec![
                    (lit(1u32) + avg(lit(1u32) + col("a"))).alias("col1"),
                    (lit(1u32) - avg(lit(1u32) + col("a"))).alias("col2"),
                    avg(lit(1u32) + col("a")),
                    (lit(1u32) + udf_agg(lit(1u32) + col("a"))).alias("col3"),
                    (lit(1u32) - udf_agg(lit(1u32) + col("a"))).alias("col4"),
                    udf_agg(lit(1u32) + col("a")),
                ],
            )?
            .build()?;

        let expected = "Projection: UInt32(1) + test.a, UInt32(1) + AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a AS AVG(UInt32(1) + test.a) AS col1, UInt32(1) - AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a AS AVG(UInt32(1) + test.a) AS col2, AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a AS AVG(UInt32(1) + test.a), UInt32(1) + my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a AS my_agg(UInt32(1) + test.a) AS col3, UInt32(1) - my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a AS my_agg(UInt32(1) + test.a) AS col4, my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a AS my_agg(UInt32(1) + test.a)\
        \n  Aggregate: groupBy=[[UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a]], aggr=[[AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a) AS AVG(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a, my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a) AS my_agg(UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a)UInt32(1) + test.atest.aUInt32(1) AS UInt32(1) + test.a]]\
        \n    Projection: UInt32(1) + test.a AS UInt32(1) + test.atest.aUInt32(1), test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn aggregate_with_releations_and_dots() -> Result<()> {
        let schema = Schema::new(vec![Field::new("col.a", DataType::UInt32, false)]);
        let table_scan = table_scan(Some("table.test"), &schema, None)?.build()?;

        let col_a = Expr::Column(Column::new(Some("table.test"), "col.a"));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col_a.clone()],
                vec![
                    (lit(1u32) + avg(lit(1u32) + col_a.clone())),
                    avg(lit(1u32) + col_a),
                ],
            )?
            .build()?;

        let expected = "Projection: table.test.col.a, UInt32(1) + AVG(UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a)UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a AS AVG(UInt32(1) + table.test.col.a), AVG(UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a)UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a AS AVG(UInt32(1) + table.test.col.a)\
        \n  Aggregate: groupBy=[[table.test.col.a]], aggr=[[AVG(UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a) AS AVG(UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a)UInt32(1) + table.test.col.atable.test.col.aUInt32(1) AS UInt32(1) + table.test.col.a]]\
        \n    Projection: UInt32(1) + table.test.col.a AS UInt32(1) + table.test.col.atable.test.col.aUInt32(1), table.test.col.a\
        \n      TableScan: table.test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn subexpr_in_same_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (lit(1) + col("a")).alias("first"),
                (lit(1) + col("a")).alias("second"),
            ])?
            .build()?;

        let expected = "Projection: Int32(1) + test.atest.aInt32(1) AS Int32(1) + test.a AS first, Int32(1) + test.atest.aInt32(1) AS Int32(1) + test.a AS second\
        \n  Projection: Int32(1) + test.a AS Int32(1) + test.atest.aInt32(1), test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn subexpr_in_different_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a"), col("a") + lit(1)])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a, test.a + Int32(1)\
        \n  TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn cross_plans_subexpr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a")])?
            .project(vec![lit(1) + col("a")])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  Projection: Int32(1) + test.a\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, &plan);
        Ok(())
    }

    #[test]
    fn redundant_project_fields() {
        let table_scan = test_table_scan().unwrap();
        let affected_id: BTreeSet<Identifier> =
            ["c+a".to_string(), "b+a".to_string()].into_iter().collect();
        let expr_set_1 = [
            (
                "c+a".to_string(),
                (col("c") + col("a"), 1, DataType::UInt32),
            ),
            (
                "b+a".to_string(),
                (col("b") + col("a"), 1, DataType::UInt32),
            ),
        ]
        .into_iter()
        .collect();
        let expr_set_2 = [
            ("c+a".to_string(), (col("c+a"), 1, DataType::UInt32)),
            ("b+a".to_string(), (col("b+a"), 1, DataType::UInt32)),
        ]
        .into_iter()
        .collect();
        let project =
            build_common_expr_project_plan(table_scan, affected_id.clone(), &expr_set_1)
                .unwrap();
        let project_2 =
            build_common_expr_project_plan(project, affected_id, &expr_set_2).unwrap();

        let mut field_set = BTreeSet::new();
        for name in project_2.schema().field_names() {
            assert!(field_set.insert(name));
        }
    }

    #[test]
    fn redundant_project_fields_join_input() {
        let table_scan_1 = test_table_scan_with_name("test1").unwrap();
        let table_scan_2 = test_table_scan_with_name("test2").unwrap();
        let join = LogicalPlanBuilder::from(table_scan_1)
            .join(table_scan_2, JoinType::Inner, (vec!["a"], vec!["a"]), None)
            .unwrap()
            .build()
            .unwrap();
        let affected_id: BTreeSet<Identifier> =
            ["test1.c+test1.a".to_string(), "test1.b+test1.a".to_string()]
                .into_iter()
                .collect();
        let expr_set_1 = [
            (
                "test1.c+test1.a".to_string(),
                (col("test1.c") + col("test1.a"), 1, DataType::UInt32),
            ),
            (
                "test1.b+test1.a".to_string(),
                (col("test1.b") + col("test1.a"), 1, DataType::UInt32),
            ),
        ]
        .into_iter()
        .collect();
        let expr_set_2 = [
            (
                "test1.c+test1.a".to_string(),
                (col("test1.c+test1.a"), 1, DataType::UInt32),
            ),
            (
                "test1.b+test1.a".to_string(),
                (col("test1.b+test1.a"), 1, DataType::UInt32),
            ),
        ]
        .into_iter()
        .collect();
        let project =
            build_common_expr_project_plan(join, affected_id.clone(), &expr_set_1)
                .unwrap();
        let project_2 =
            build_common_expr_project_plan(project, affected_id, &expr_set_2).unwrap();

        let mut field_set = BTreeSet::new();
        for name in project_2.schema().field_names() {
            assert!(field_set.insert(name));
        }
    }

    #[test]
    fn eliminated_subexpr_datatype() {
        use datafusion_expr::cast;

        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]);

        let plan = table_scan(Some("table"), &schema, None)
            .unwrap()
            .filter(
                cast(col("a"), DataType::Int64)
                    .lt(lit(1_i64))
                    .and(cast(col("a"), DataType::Int64).not_eq(lit(1_i64))),
            )
            .unwrap()
            .build()
            .unwrap();
        let rule = CommonSubexprEliminate {};
        let optimized_plan = rule
            .try_optimize(&plan, &OptimizerContext::new())
            .unwrap()
            .unwrap();

        let schema = optimized_plan.schema();
        let fields_with_datatypes: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.data_type()))
            .collect();
        let formatted_fields_with_datatype = format!("{fields_with_datatypes:#?}");
        let expected = r#"[
    (
        "a",
        UInt64,
    ),
    (
        "b",
        UInt64,
    ),
    (
        "c",
        UInt64,
    ),
]"#;
        assert_eq!(expected, formatted_fields_with_datatype);
    }

    #[test]
    fn filter_schema_changed() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter((lit(1) + col("a") - lit(10)).gt(lit(1) + col("a")))?
            .build()?;

        let expected = "Projection: test.a, test.b, test.c\
        \n  Filter: Int32(1) + test.atest.aInt32(1) - Int32(10) > Int32(1) + test.atest.aInt32(1)\
        \n    Projection: Int32(1) + test.a AS Int32(1) + test.atest.aInt32(1), test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, &plan);

        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_grouping_set() -> Result<()> {
        let mut result = Vec::with_capacity(3);
        let grouping = grouping_set(vec![vec![col("a"), col("b")], vec![col("c")]]);
        let schema = DFSchema::from_unqualifed_fields(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ]
            .into(),
            HashMap::default(),
        )?;
        extract_expressions(&grouping, &schema, &mut result)?;

        assert!(result.len() == 3);
        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_grouping_set_with_identical_expr() -> Result<()> {
        let mut result = Vec::with_capacity(2);
        let grouping = grouping_set(vec![vec![col("a"), col("b")], vec![col("a")]]);
        let schema = DFSchema::from_unqualifed_fields(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]
            .into(),
            HashMap::default(),
        )?;
        extract_expressions(&grouping, &schema, &mut result)?;

        assert!(result.len() == 2);
        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_col() -> Result<()> {
        let mut result = Vec::with_capacity(1);
        let schema = DFSchema::from_unqualifed_fields(
            vec![Field::new("a", DataType::Int32, false)].into(),
            HashMap::default(),
        )?;
        extract_expressions(&col("a"), &schema, &mut result)?;

        assert!(result.len() == 1);
        Ok(())
    }
}
