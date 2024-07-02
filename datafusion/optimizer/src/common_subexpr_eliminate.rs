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

//! [`CommonSubexprEliminate`] to avoid redundant computation of common sub-expressions

use std::collections::{BTreeSet, HashMap};
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

use crate::optimizer::ApplyOrder;
use crate::utils::NamePreserver;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::hash_utils::combine_hashes;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{
    internal_datafusion_err, qualified_name, Column, DFSchema, DFSchemaRef, Result,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use datafusion_expr::logical_plan::{
    Aggregate, Filter, LogicalPlan, Projection, Sort, Window,
};
use datafusion_expr::{col, Expr, ExprSchemable};
use indexmap::IndexMap;

const CSE_PREFIX: &str = "__common_expr";

/// Identifier that represents a subexpression tree.
///
/// This identifier is designed to be efficient and  "hash", "accumulate", "equal" and
/// "have no collision (as low as possible)"
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identifier<'n> {
    // Hash of `expr` built up incrementally during the first, visiting traversal, but its
    // value is not necessarily equal to `expr.hash()`.
    hash: u64,
    expr: &'n Expr,
}

impl<'n> Identifier<'n> {
    fn new(expr: &'n Expr, random_state: &RandomState) -> Self {
        let mut hasher = random_state.build_hasher();
        expr.hash_node(&mut hasher);
        let hash = hasher.finish();
        Self { hash, expr }
    }

    fn combine(mut self, other: Option<Self>) -> Self {
        other.map_or(self, |other_id| {
            self.hash = combine_hashes(self.hash, other_id.hash);
            self
        })
    }
}

impl Hash for Identifier<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// A cache that contains the postorder index and the identifier of expression tree nodes
/// by the preorder index of the nodes.
///
/// This cache is filled by `ExprIdentifierVisitor` during the first traversal and is used
/// by `CommonSubexprRewriter` during the second traversal.
///
/// The purpose of this cache is to quickly find the identifier of a node during the
/// second traversal.
///
/// Elements in this array are added during `f_down` so the indexes represent the preorder
/// index of expression nodes and thus element 0 belongs to the root of the expression
/// tree.
/// The elements of the array are tuples that contain:
/// - Postorder index that belongs to the preorder index. Assigned during `f_up`, start
///   from 0.
/// - Identifier of the expression. If empty (`""`), expr should not be considered for
///   CSE.
///
/// # Example
/// An expression like `(a + b)` would have the following `IdArray`:
/// ```text
/// [
///   (2, "a + b"),
///   (1, "a"),
///   (0, "b")
/// ]
/// ```
type IdArray<'n> = Vec<(usize, Option<Identifier<'n>>)>;

/// A map that contains the number of occurrences of expressions by their identifiers.
type ExprStats<'n> = HashMap<Identifier<'n>, usize>;

/// A map that contains the common expressions and their alias extracted during the
/// second, rewriting traversal.
type CommonExprs<'n> = IndexMap<Identifier<'n>, (Expr, String)>;

/// Performs Common Sub-expression Elimination optimization.
///
/// This optimization improves query performance by computing expressions that
/// appear more than once and reusing those results rather than re-computing the
/// same value
///
/// Currently only common sub-expressions within a single `LogicalPlan` are
/// eliminated.
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
pub struct CommonSubexprEliminate {
    random_state: RandomState,
}

impl CommonSubexprEliminate {
    pub fn new() -> Self {
        Self {
            random_state: RandomState::new(),
        }
    }

    /// Returns the identifier list for each element in `exprs` and a flag to indicate if
    /// rewrite phase of CSE make sense.
    ///
    /// Returns and array with 1 element for each input expr in `exprs`
    ///
    /// Each element is itself the result of [`CommonSubexprEliminate::expr_to_identifier`] for that expr
    /// (e.g. the identifiers for each node in the tree)
    fn to_arrays<'n>(
        &self,
        exprs: &'n [Expr],
        expr_stats: &mut ExprStats<'n>,
        expr_mask: ExprMask,
    ) -> Result<(bool, Vec<IdArray<'n>>)> {
        let mut found_common = false;
        exprs
            .iter()
            .map(|e| {
                let mut id_array = vec![];
                self.expr_to_identifier(e, expr_stats, &mut id_array, expr_mask)
                    .map(|fc| {
                        found_common |= fc;

                        id_array
                    })
            })
            .collect::<Result<Vec<_>>>()
            .map(|id_arrays| (found_common, id_arrays))
    }

    /// Add an identifier to `id_array` for every subexpression in this tree.
    fn expr_to_identifier<'n>(
        &self,
        expr: &'n Expr,
        expr_stats: &mut ExprStats<'n>,
        id_array: &mut IdArray<'n>,
        expr_mask: ExprMask,
    ) -> Result<bool> {
        // Don't consider volatile expressions for CSE.
        Ok(if expr.is_volatile()? {
            false
        } else {
            let mut visitor = ExprIdentifierVisitor {
                expr_stats,
                id_array,
                visit_stack: vec![],
                down_index: 0,
                up_index: 0,
                expr_mask,
                random_state: &self.random_state,
                found_common: false,
            };
            expr.visit(&mut visitor)?;

            visitor.found_common
        })
    }

    /// Rewrites `exprs_list` with common sub-expressions replaced with a new
    /// column.
    ///
    /// `common_exprs` is updated with any sub expressions that were replaced.
    ///
    /// Returns the rewritten expressions
    fn rewrite_exprs_list<'n>(
        &self,
        exprs_list: Vec<Vec<Expr>>,
        arrays_list: Vec<Vec<IdArray<'n>>>,
        expr_stats: &ExprStats<'n>,
        common_exprs: &mut CommonExprs<'n>,
        alias_generator: &AliasGenerator,
    ) -> Result<Transformed<Vec<Vec<Expr>>>> {
        let mut transformed = false;
        exprs_list
            .into_iter()
            .zip(arrays_list.iter())
            .map(|(exprs, arrays)| {
                exprs
                    .into_iter()
                    .zip(arrays.iter())
                    .map(|(expr, id_array)| {
                        let replaced = replace_common_expr(
                            expr,
                            id_array,
                            expr_stats,
                            common_exprs,
                            alias_generator,
                        )?;
                        // remember if this expression was actually replaced
                        transformed |= replaced.transformed;
                        Ok(replaced.data)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()
            .map(|rewritten_exprs_list| {
                // propagate back transformed information
                Transformed::new_transformed(rewritten_exprs_list, transformed)
            })
    }

    /// Rewrites the expression in `exprs_list` with common sub-expressions
    /// replaced with a new colum and adds a ProjectionExec on top of `input`
    /// which computes any replaced common sub-expressions.
    ///
    /// Returns a tuple of:
    /// 1. The rewritten expressions
    /// 2. A `LogicalPlan::Projection` with input of `input` that computes any
    ///    common sub-expressions that were used
    fn rewrite_expr(
        &self,
        exprs_list: Vec<Vec<Expr>>,
        arrays_list: Vec<Vec<IdArray>>,
        input: LogicalPlan,
        expr_stats: &ExprStats,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<(Vec<Vec<Expr>>, LogicalPlan)>> {
        let mut transformed = false;
        let mut common_exprs = CommonExprs::new();

        let rewrite_exprs = self.rewrite_exprs_list(
            exprs_list,
            arrays_list,
            expr_stats,
            &mut common_exprs,
            &config.alias_generator(),
        )?;
        transformed |= rewrite_exprs.transformed;

        let new_input = self.rewrite(input, config)?;
        transformed |= new_input.transformed;
        let mut new_input = new_input.data;

        if !common_exprs.is_empty() {
            assert!(transformed);
            new_input = build_common_expr_project_plan(new_input, common_exprs)?;
        }

        // return the transformed information

        Ok(Transformed::new_transformed(
            (rewrite_exprs.data, new_input),
            transformed,
        ))
    }

    fn try_optimize_proj(
        &self,
        projection: Projection,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Projection {
            expr,
            input,
            schema,
            ..
        } = projection;
        let input = unwrap_arc(input);
        self.try_unary_plan(expr, input, config)?
            .map_data(|(new_expr, new_input)| {
                Projection::try_new_with_schema(new_expr, Arc::new(new_input), schema)
                    .map(LogicalPlan::Projection)
            })
    }
    fn try_optimize_sort(
        &self,
        sort: Sort,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Sort { expr, input, fetch } = sort;
        let input = unwrap_arc(input);
        let new_sort = self.try_unary_plan(expr, input, config)?.update_data(
            |(new_expr, new_input)| {
                LogicalPlan::Sort(Sort {
                    expr: new_expr,
                    input: Arc::new(new_input),
                    fetch,
                })
            },
        );
        Ok(new_sort)
    }

    fn try_optimize_filter(
        &self,
        filter: Filter,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Filter {
            predicate, input, ..
        } = filter;
        let input = unwrap_arc(input);
        let expr = vec![predicate];
        self.try_unary_plan(expr, input, config)?
            .transform_data(|(mut new_expr, new_input)| {
                assert_eq!(new_expr.len(), 1); // passed in vec![predicate]
                let new_predicate = new_expr
                    .pop()
                    .unwrap()
                    .unalias_nested()
                    .update_data(|new_predicate| (new_predicate, new_input));
                Ok(new_predicate)
            })?
            .map_data(|(new_predicate, new_input)| {
                Filter::try_new(new_predicate, Arc::new(new_input))
                    .map(LogicalPlan::Filter)
            })
    }

    fn try_optimize_window(
        &self,
        window: Window,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // collect all window expressions from any number of LogicalPlanWindow
        let (mut window_exprs, mut window_schemas, mut plan) =
            get_consecutive_window_exprs(window);

        let mut found_common = false;
        let mut expr_stats = ExprStats::new();
        let arrays_per_window = window_exprs
            .iter()
            .map(|window_expr| {
                self.to_arrays(window_expr, &mut expr_stats, ExprMask::Normal)
                    .map(|(fc, id_arrays)| {
                        found_common |= fc;

                        id_arrays
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        if found_common {
            // save the original names
            let name_preserver = NamePreserver::new(&plan);
            let mut saved_names = window_exprs
                .iter()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| name_preserver.save(expr))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            assert_eq!(window_exprs.len(), arrays_per_window.len());
            let num_window_exprs = window_exprs.len();
            let rewritten_window_exprs = self.rewrite_expr(
                // Must clone as Identifiers use references to original expressions so we
                // have to keep the original expressions intact.
                window_exprs.clone(),
                arrays_per_window,
                plan,
                &expr_stats,
                config,
            )?;
            let transformed = rewritten_window_exprs.transformed;
            assert!(transformed);

            let (mut new_expr, new_input) = rewritten_window_exprs.data;

            let mut plan = new_input;

            // Construct consecutive window operator, with their corresponding new
            // window expressions.
            //
            // Note this iterates over, `new_expr` and `saved_names` which are the
            // same length, in reverse order
            assert_eq!(num_window_exprs, new_expr.len());
            assert_eq!(num_window_exprs, saved_names.len());
            while let (Some(new_window_expr), Some(saved_names)) =
                (new_expr.pop(), saved_names.pop())
            {
                assert_eq!(new_window_expr.len(), saved_names.len());

                // Rename re-written window expressions with original name, to
                // preserve the output schema
                let new_window_expr = new_window_expr
                    .into_iter()
                    .zip(saved_names.into_iter())
                    .map(|(new_window_expr, saved_name)| {
                        saved_name.restore(new_window_expr)
                    })
                    .collect::<Result<Vec<_>>>()?;
                plan = LogicalPlan::Window(Window::try_new(
                    new_window_expr,
                    Arc::new(plan),
                )?);
            }

            Ok(Transformed::new_transformed(plan, transformed))
        } else {
            while let (Some(window_expr), Some(schema)) =
                (window_exprs.pop(), window_schemas.pop())
            {
                plan = LogicalPlan::Window(Window {
                    input: Arc::new(plan),
                    window_expr,
                    schema,
                });
            }

            Ok(Transformed::no(plan))
        }
    }

    fn try_optimize_aggregate(
        &self,
        aggregate: Aggregate,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Aggregate {
            group_expr,
            aggr_expr,
            input,
            schema: orig_schema,
            ..
        } = aggregate;
        // track transformed information
        let mut transformed = false;

        let name_perserver = NamePreserver::new_for_projection();
        let saved_names = aggr_expr
            .iter()
            .map(|expr| name_perserver.save(expr))
            .collect::<Result<Vec<_>>>()?;

        let mut expr_stats = ExprStats::new();
        // rewrite inputs
        let (group_found_common, group_arrays) =
            self.to_arrays(&group_expr, &mut expr_stats, ExprMask::Normal)?;
        let (aggr_found_common, aggr_arrays) =
            self.to_arrays(&aggr_expr, &mut expr_stats, ExprMask::Normal)?;
        let (new_aggr_expr, new_group_expr, new_input) =
            if group_found_common || aggr_found_common {
                // rewrite both group exprs and aggr_expr
                let rewritten = self.rewrite_expr(
                    // Must clone as Identifiers use references to original expressions so
                    // we have to keep the original expressions intact.
                    vec![group_expr.clone(), aggr_expr.clone()],
                    vec![group_arrays, aggr_arrays],
                    unwrap_arc(input),
                    &expr_stats,
                    config,
                )?;
                assert!(rewritten.transformed);
                transformed |= rewritten.transformed;
                let (mut new_expr, new_input) = rewritten.data;

                // note the reversed pop order.
                let new_aggr_expr = pop_expr(&mut new_expr)?;
                let new_group_expr = pop_expr(&mut new_expr)?;

                (new_aggr_expr, new_group_expr, Arc::new(new_input))
            } else {
                (aggr_expr, group_expr, input)
            };

        // create potential projection on top
        let mut expr_stats = ExprStats::new();
        let (aggr_found_common, aggr_arrays) = self.to_arrays(
            &new_aggr_expr,
            &mut expr_stats,
            ExprMask::NormalAndAggregates,
        )?;
        if aggr_found_common {
            let mut common_exprs = CommonExprs::new();
            let mut rewritten_exprs = self.rewrite_exprs_list(
                // Must clone as Identifiers use references to original expressions so we
                // have to keep the original expressions intact.
                vec![new_aggr_expr.clone()],
                vec![aggr_arrays],
                &expr_stats,
                &mut common_exprs,
                &config.alias_generator(),
            )?;
            assert!(rewritten_exprs.transformed);
            let rewritten = pop_expr(&mut rewritten_exprs.data)?;

            assert!(!common_exprs.is_empty());
            let mut agg_exprs = common_exprs
                .into_values()
                .map(|(expr, expr_alias)| expr.alias(expr_alias))
                .collect::<Vec<_>>();

            let new_input_schema = Arc::clone(new_input.schema());
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
                        let expr_alias = config.alias_generator().next(CSE_PREFIX);
                        let (qualifier, field) =
                            expr_rewritten.to_field(&new_input_schema)?;
                        let out_name = qualified_name(qualifier.as_ref(), field.name());

                        agg_exprs.push(expr_rewritten.alias(&expr_alias));
                        proj_exprs.push(
                            Expr::Column(Column::from_name(expr_alias)).alias(out_name),
                        );
                    }
                } else {
                    proj_exprs.push(expr_rewritten);
                }
            }

            let agg = LogicalPlan::Aggregate(Aggregate::try_new(
                new_input,
                new_group_expr,
                agg_exprs,
            )?);

            Projection::try_new(proj_exprs, Arc::new(agg))
                .map(LogicalPlan::Projection)
                .map(Transformed::yes)
        } else {
            // TODO: How exactly can the name or the schema change in this case?
            //  In theory `new_aggr_expr` and `new_group_expr` are either the original expressions or they were crafted via `rewrite_expr()`, that keeps the original expression names.
            //  If this is really needed can we have UT for it?
            // Alias aggregation expressions if they have changed
            let new_aggr_expr = new_aggr_expr
                .into_iter()
                .zip(saved_names.into_iter())
                .map(|(new_expr, saved_name)| saved_name.restore(new_expr))
                .collect::<Result<Vec<Expr>>>()?;
            // Since group_expr may have changed, schema may also. Use try_new method.
            let new_agg = if transformed {
                Aggregate::try_new(new_input, new_group_expr, new_aggr_expr)?
            } else {
                Aggregate::try_new_with_schema(
                    new_input,
                    new_group_expr,
                    new_aggr_expr,
                    orig_schema,
                )?
            };
            let new_agg = LogicalPlan::Aggregate(new_agg);

            Ok(Transformed::new_transformed(new_agg, transformed))
        }
    }

    /// Rewrites the expr list and input to remove common subexpressions
    ///
    /// # Parameters
    ///
    /// * `exprs`: List of expressions in the node
    /// * `input`: input plan (that produces the columns referred to in `exprs`)
    ///
    /// # Return value
    ///
    ///  Returns `(rewritten_exprs, new_input)`. `new_input` is either:
    ///
    /// 1. The original `input` of no common subexpressions were extracted
    /// 2. A newly added projection on top of the original input
    /// that computes the common subexpressions
    fn try_unary_plan(
        &self,
        expr: Vec<Expr>,
        input: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<(Vec<Expr>, LogicalPlan)>> {
        let mut expr_stats = ExprStats::new();
        let (found_common, id_arrays) =
            self.to_arrays(&expr, &mut expr_stats, ExprMask::Normal)?;

        if found_common {
            let rewritten = self.rewrite_expr(
                // Must clone as Identifiers use references to original expressions so we
                // have to keep the original expressions intact.
                vec![expr.clone()],
                vec![id_arrays],
                input,
                &expr_stats,
                config,
            )?;
            assert!(rewritten.transformed);
            rewritten.map_data(|(mut new_expr, new_input)| {
                assert_eq!(new_expr.len(), 1);
                Ok((new_expr.pop().unwrap(), new_input))
            })
        } else {
            Ok(Transformed::no((expr, input)))
        }
    }
}

/// Get all window expressions inside the consecutive window operators.
///
/// Returns the window expressions, and the input to the deepest child
/// LogicalPlan.
///
/// For example, if the input widnow looks like
///
/// ```text
///   LogicalPlan::Window(exprs=[a, b, c])
///     LogicalPlan::Window(exprs=[d])
///       InputPlan
/// ```
///
/// Returns:
/// *  `window_exprs`: `[[a, b, c], [d]]`
/// * InputPlan
///
/// Consecutive window expressions may refer to same complex expression.
///
/// If same complex expression is referred more than once by subsequent
/// `WindowAggr`s, we can cache complex expression by evaluating it with a
/// projection before the first WindowAggr.
///
/// This enables us to cache complex expression "c3+c4" for following plan:
///
/// ```text
/// WindowAggr: windowExpr=[[sum(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
/// --WindowAggr: windowExpr=[[sum(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
/// ```
///
/// where, it is referred once by each `WindowAggr` (total of 2) in the plan.
fn get_consecutive_window_exprs(
    window: Window,
) -> (Vec<Vec<Expr>>, Vec<DFSchemaRef>, LogicalPlan) {
    let mut window_exprs = vec![];
    let mut window_schemas = vec![];
    let mut plan = LogicalPlan::Window(window);
    while let LogicalPlan::Window(Window {
        input,
        window_expr,
        schema,
    }) = plan
    {
        window_exprs.push(window_expr);
        window_schemas.push(schema);

        plan = unwrap_arc(input);
    }
    (window_exprs, window_schemas, plan)
}

impl OptimizerRule for CommonSubexprEliminate {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let original_schema = Arc::clone(plan.schema());

        let optimized_plan = match plan {
            LogicalPlan::Projection(proj) => self.try_optimize_proj(proj, config)?,
            LogicalPlan::Sort(sort) => self.try_optimize_sort(sort, config)?,
            LogicalPlan::Filter(filter) => self.try_optimize_filter(filter, config)?,
            LogicalPlan::Window(window) => self.try_optimize_window(window, config)?,
            LogicalPlan::Aggregate(agg) => self.try_optimize_aggregate(agg, config)?,
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
                // ApplyOrder::TopDown handles recursion
                Transformed::no(plan)
            }
        };

        // If we rewrote the plan, ensure the schema stays the same
        if optimized_plan.transformed && optimized_plan.data.schema() != &original_schema
        {
            optimized_plan.map_data(|optimized_plan| {
                build_recover_project_plan(&original_schema, optimized_plan)
            })
        } else {
            Ok(optimized_plan)
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

fn pop_expr(new_expr: &mut Vec<Vec<Expr>>) -> Result<Vec<Expr>> {
    new_expr
        .pop()
        .ok_or_else(|| internal_datafusion_err!("Failed to pop expression"))
}

/// Build the "intermediate" projection plan that evaluates the extracted common
/// expressions.
///
/// # Arguments
/// input: the input plan
///
/// common_exprs: which common subexpressions were used (and thus are added to
/// intermediate projection)
///
/// expr_stats: the set of common subexpressions
fn build_common_expr_project_plan(
    input: LogicalPlan,
    common_exprs: CommonExprs,
) -> Result<LogicalPlan> {
    let mut fields_set = BTreeSet::new();
    let mut project_exprs = common_exprs
        .into_values()
        .map(|(expr, expr_alias)| {
            fields_set.insert(expr_alias.clone());
            Ok(expr.alias(expr_alias))
        })
        .collect::<Result<Vec<_>>>()?;

    for (qualifier, field) in input.schema().iter() {
        if fields_set.insert(qualified_name(qualifier, field.name())) {
            project_exprs.push(Expr::from((qualifier, field)));
        }
    }

    Projection::try_new(project_exprs, Arc::new(input)).map(LogicalPlan::Projection)
}

/// Build the projection plan to eliminate unnecessary columns produced by
/// the "intermediate" projection plan built in [build_common_expr_project_plan].
///
/// This is required to keep the schema the same for plans that pass the input
/// on to the output, such as `Filter` or `Sort`.
fn build_recover_project_plan(
    schema: &DFSchema,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let col_exprs = schema.iter().map(Expr::from).collect();
    Projection::try_new(col_exprs, Arc::new(input)).map(LogicalPlan::Projection)
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

/// Go through an expression tree and generate identifiers for each subexpression.
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
struct ExprIdentifierVisitor<'a, 'n> {
    // statistics of expressions
    expr_stats: &'a mut ExprStats<'n>,
    // cache to speed up second traversal
    id_array: &'a mut IdArray<'n>,
    // inner states
    visit_stack: Vec<VisitRecord<'n>>,
    // preorder index, start from 0.
    down_index: usize,
    // postorder index, start from 0.
    up_index: usize,
    // which expression should be skipped?
    expr_mask: ExprMask,
    // a `RandomState` to generate hashes during the first traversal
    random_state: &'a RandomState,
    // a flag to indicate that common expression found
    found_common: bool,
}

/// Record item that used when traversing a expression tree.
enum VisitRecord<'n> {
    /// `usize` postorder index assigned in `f-down`(). Starts from 0.
    EnterMark(usize),
    /// the node's children were skipped => jump to f_up on same node
    JumpMark,
    /// Accumulated identifier of sub expression.
    ExprItem(Identifier<'n>),
}

impl<'n> ExprIdentifierVisitor<'_, 'n> {
    /// Find the first `EnterMark` in the stack, and accumulates every `ExprItem`
    /// before it.
    fn pop_enter_mark(&mut self) -> Option<(usize, Option<Identifier<'n>>)> {
        let mut expr_id = None;

        while let Some(item) = self.visit_stack.pop() {
            match item {
                VisitRecord::EnterMark(idx) => {
                    return Some((idx, expr_id));
                }
                VisitRecord::ExprItem(id) => {
                    expr_id = Some(id.combine(expr_id));
                }
                VisitRecord::JumpMark => return None,
            }
        }
        unreachable!("Enter mark should paired with node number");
    }
}

impl<'n> TreeNodeVisitor<'n> for ExprIdentifierVisitor<'_, 'n> {
    type Node = Expr;

    fn f_down(&mut self, expr: &'n Expr) -> Result<TreeNodeRecursion> {
        // TODO: consider non-volatile sub-expressions for CSE
        // TODO: consider surely executed children of "short circuited"s for CSE

        // If an expression can short circuit its children then don't consider it for CSE
        // (https://github.com/apache/arrow-datafusion/issues/8814).
        if expr.short_circuits() {
            self.visit_stack.push(VisitRecord::JumpMark);

            return Ok(TreeNodeRecursion::Jump);
        }

        self.id_array.push((0, None));
        self.visit_stack
            .push(VisitRecord::EnterMark(self.down_index));
        self.down_index += 1;

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, expr: &'n Expr) -> Result<TreeNodeRecursion> {
        let Some((down_index, sub_expr_id)) = self.pop_enter_mark() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        let expr_id = Identifier::new(expr, self.random_state).combine(sub_expr_id);

        self.id_array[down_index].0 = self.up_index;
        if !self.expr_mask.ignores(expr) {
            self.id_array[down_index].1 = Some(expr_id);
            let count = self.expr_stats.entry(expr_id).or_insert(0);
            *count += 1;
            if *count > 1 {
                self.found_common = true;
            }
        }
        self.visit_stack.push(VisitRecord::ExprItem(expr_id));
        self.up_index += 1;

        Ok(TreeNodeRecursion::Continue)
    }
}

/// Rewrite expression by replacing detected common sub-expression with
/// the corresponding temporary column name. That column contains the
/// evaluate result of replaced expression.
struct CommonSubexprRewriter<'a, 'n> {
    // statistics of expressions
    expr_stats: &'a ExprStats<'n>,
    // cache to speed up second traversal
    id_array: &'a IdArray<'n>,
    // common expression, that are replaced during the second traversal, are collected to
    // this map
    common_exprs: &'a mut CommonExprs<'n>,
    // preorder index, starts from 0.
    down_index: usize,
    // how many aliases have we seen so far
    alias_counter: usize,
    // alias generator for extracted common expressions
    alias_generator: &'a AliasGenerator,
}

impl TreeNodeRewriter for CommonSubexprRewriter<'_, '_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if matches!(expr, Expr::Alias(_)) {
            self.alias_counter += 1;
        }

        // The `CommonSubexprRewriter` relies on `ExprIdentifierVisitor` to generate
        // the `id_array`, which records the expr's identifier used to rewrite expr. So if we
        // skip an expr in `ExprIdentifierVisitor`, we should skip it here, too.
        if expr.short_circuits() {
            return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
        }

        let (up_index, expr_id) = self.id_array[self.down_index];
        self.down_index += 1;

        // skip `Expr`s without identifier (empty identifier).
        let Some(expr_id) = expr_id else {
            return Ok(Transformed::no(expr));
        };

        let count = self.expr_stats.get(&expr_id).unwrap();
        if *count > 1 {
            // step index to skip all sub-node (which has smaller series number).
            while self.down_index < self.id_array.len()
                && self.id_array[self.down_index].0 < up_index
            {
                self.down_index += 1;
            }

            let expr_name = expr.display_name()?;
            let (_, expr_alias) = self.common_exprs.entry(expr_id).or_insert_with(|| {
                let expr_alias = self.alias_generator.next(CSE_PREFIX);
                (expr, expr_alias)
            });

            // alias the expressions without an `Alias` ancestor node
            let rewritten = if self.alias_counter > 0 {
                col(expr_alias.clone())
            } else {
                self.alias_counter += 1;
                col(expr_alias.clone()).alias(expr_name)
            };

            Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump))
        } else {
            Ok(Transformed::no(expr))
        }
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Self::Node>> {
        if matches!(expr, Expr::Alias(_)) {
            self.alias_counter -= 1
        }

        Ok(Transformed::no(expr))
    }
}

/// Replace common sub-expression in `expr` with the corresponding temporary
/// column name, updating `common_exprs` with any replaced expressions
fn replace_common_expr<'n>(
    expr: Expr,
    id_array: &IdArray<'n>,
    expr_stats: &ExprStats<'n>,
    common_exprs: &mut CommonExprs<'n>,
    alias_generator: &AliasGenerator,
) -> Result<Transformed<Expr>> {
    if id_array.is_empty() {
        Ok(Transformed::no(expr))
    } else {
        expr.rewrite(&mut CommonSubexprRewriter {
            expr_stats,
            id_array,
            common_exprs,
            down_index: 0,
            alias_counter: 0,
            alias_generator,
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::iter;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::logical_type::field::LogicalField;
    use datafusion_expr::expr::AggregateFunction;
    use datafusion_expr::logical_plan::{table_scan, JoinType};
    use datafusion_expr::{
        grouping_set, AccumulatorFactoryFunction, AggregateUDF, BinaryExpr, Signature,
        SimpleAggregateUDF, Volatility,
    };
    use datafusion_expr::{lit, logical_plan::builder::LogicalPlanBuilder};

    use crate::optimizer::OptimizerContext;
    use crate::test::*;
    use datafusion_expr::test::function_stub::{avg, sum};

    use super::*;

    fn assert_non_optimized_plan_eq(
        expected: &str,
        plan: LogicalPlan,
        config: Option<&dyn OptimizerConfig>,
    ) {
        assert_eq!(expected, format!("{plan:?}"), "Unexpected starting plan");
        let optimizer = CommonSubexprEliminate::new();
        let default_config = OptimizerContext::new();
        let config = config.unwrap_or(&default_config);
        let optimized_plan = optimizer.rewrite(plan, config).unwrap();
        assert!(!optimized_plan.transformed, "unexpectedly optimize plan");
        let optimized_plan = optimized_plan.data;
        assert_eq!(
            expected,
            format!("{optimized_plan:?}"),
            "Unexpected optimized plan"
        );
    }

    fn assert_optimized_plan_eq(
        expected: &str,
        plan: LogicalPlan,
        config: Option<&dyn OptimizerConfig>,
    ) {
        let optimizer = CommonSubexprEliminate::new();
        let default_config = OptimizerContext::new();
        let config = config.unwrap_or(&default_config);
        let optimized_plan = optimizer.rewrite(plan, config).unwrap();
        assert!(optimized_plan.transformed, "failed to optimize plan");
        let optimized_plan = optimized_plan.data;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(expected, formatted_plan);
    }

    #[test]
    fn id_array_visitor() -> Result<()> {
        let optimizer = CommonSubexprEliminate::new();

        let a_plus_1 = col("a") + lit(1);
        let avg_c = avg(col("c"));
        let sum_a_plus_1 = sum(a_plus_1);
        let sum_a_plus_1_minus_avg_c = sum_a_plus_1 - avg_c;
        let expr = sum_a_plus_1_minus_avg_c * lit(2);

        let Expr::BinaryExpr(BinaryExpr {
            left: sum_a_plus_1_minus_avg_c,
            ..
        }) = &expr
        else {
            panic!("Cannot extract subexpression reference")
        };
        let Expr::BinaryExpr(BinaryExpr {
            left: sum_a_plus_1,
            right: avg_c,
            ..
        }) = sum_a_plus_1_minus_avg_c.as_ref()
        else {
            panic!("Cannot extract subexpression reference")
        };
        let Expr::AggregateFunction(AggregateFunction {
            args: a_plus_1_vec, ..
        }) = sum_a_plus_1.as_ref()
        else {
            panic!("Cannot extract subexpression reference")
        };
        let a_plus_1 = &a_plus_1_vec.as_slice()[0];

        // skip aggregates
        let mut id_array = vec![];
        optimizer.expr_to_identifier(
            &expr,
            &mut ExprStats::new(),
            &mut id_array,
            ExprMask::Normal,
        )?;

        // Collect distinct hashes and set them to 0 in `id_array`
        fn collect_hashes(id_array: &mut IdArray) -> HashSet<u64> {
            id_array
                .iter_mut()
                .flat_map(|(_, expr_id_option)| {
                    expr_id_option.as_mut().map(|expr_id| {
                        let hash = expr_id.hash;
                        expr_id.hash = 0;
                        hash
                    })
                })
                .collect::<HashSet<_>>()
        }

        let hashes = collect_hashes(&mut id_array);
        assert_eq!(hashes.len(), 3);

        let expected = vec![
            (
                8,
                Some(Identifier {
                    hash: 0,
                    expr: &expr,
                }),
            ),
            (
                6,
                Some(Identifier {
                    hash: 0,
                    expr: sum_a_plus_1_minus_avg_c,
                }),
            ),
            (3, None),
            (
                2,
                Some(Identifier {
                    hash: 0,
                    expr: a_plus_1,
                }),
            ),
            (0, None),
            (1, None),
            (5, None),
            (4, None),
            (7, None),
        ];
        assert_eq!(expected, id_array);

        // include aggregates
        let mut id_array = vec![];
        optimizer.expr_to_identifier(
            &expr,
            &mut ExprStats::new(),
            &mut id_array,
            ExprMask::NormalAndAggregates,
        )?;

        let hashes = collect_hashes(&mut id_array);
        assert_eq!(hashes.len(), 5);

        let expected = vec![
            (
                8,
                Some(Identifier {
                    hash: 0,
                    expr: &expr,
                }),
            ),
            (
                6,
                Some(Identifier {
                    hash: 0,
                    expr: sum_a_plus_1_minus_avg_c,
                }),
            ),
            (
                3,
                Some(Identifier {
                    hash: 0,
                    expr: sum_a_plus_1,
                }),
            ),
            (
                2,
                Some(Identifier {
                    hash: 0,
                    expr: a_plus_1,
                }),
            ),
            (0, None),
            (1, None),
            (
                5,
                Some(Identifier {
                    hash: 0,
                    expr: avg_c,
                }),
            ),
            (4, None),
            (7, None),
        ];
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

        let expected = "Aggregate: groupBy=[[]], aggr=[[sum(__common_expr_1 AS test.a * Int32(1) - test.b), sum(__common_expr_1 AS test.a * Int32(1) - test.b * (Int32(1) + test.c))]]\
        \n  Projection: test.a * (Int32(1) - test.b) AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn nested_aliases() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![
                (col("a") + col("b") - col("c")).alias("alias1") * (col("a") + col("b")),
                col("a") + col("b"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 - test.c AS alias1 * __common_expr_1 AS test.a + test.b, __common_expr_1 AS test.a + test.b\
        \n  Projection: test.a + test.b AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;

        let return_type = DataType::UInt32;
        let accumulator: AccumulatorFactoryFunction = Arc::new(|_| unimplemented!());
        let udf_agg = |inner: Expr| {
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                Arc::new(AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
                    "my_agg",
                    Signature::exact(vec![DataType::UInt32], Volatility::Stable),
                    return_type.clone(),
                    accumulator.clone(),
                    vec![Field::new("value", DataType::UInt32, true)],
                ))),
                vec![inner],
                false,
                None,
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

        let expected = "Projection: __common_expr_1 AS col1, __common_expr_1 AS col2, col3, __common_expr_3 AS avg(test.c), __common_expr_2 AS col4, __common_expr_2 AS col5, col6, __common_expr_4 AS my_agg(test.c)\
        \n  Aggregate: groupBy=[[]], aggr=[[avg(test.a) AS __common_expr_1, my_agg(test.a) AS __common_expr_2, avg(test.b) AS col3, avg(test.c) AS __common_expr_3, my_agg(test.b) AS col6, my_agg(test.c) AS __common_expr_4]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

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

        let expected = "Projection: Int32(1) + __common_expr_1 AS avg(test.a), Int32(1) - __common_expr_1 AS avg(test.a), Int32(1) + __common_expr_2 AS my_agg(test.a), Int32(1) - __common_expr_2 AS my_agg(test.a)\
        \n  Aggregate: groupBy=[[]], aggr=[[avg(test.a) AS __common_expr_1, my_agg(test.a) AS __common_expr_2]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

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

        let expected ="Aggregate: groupBy=[[]], aggr=[[avg(__common_expr_1) AS col1, my_agg(__common_expr_1) AS col2]]\
        \n  Projection: UInt32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

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

        let expected = "Aggregate: groupBy=[[__common_expr_1 AS UInt32(1) + test.a]], aggr=[[avg(__common_expr_1) AS col1, my_agg(__common_expr_1) AS col2]]\
        \n  Projection: UInt32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

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

        let expected = "Projection: UInt32(1) + test.a, UInt32(1) + __common_expr_2 AS col1, UInt32(1) - __common_expr_2 AS col2, __common_expr_4 AS avg(UInt32(1) + test.a), UInt32(1) + __common_expr_3 AS col3, UInt32(1) - __common_expr_3 AS col4, __common_expr_5 AS my_agg(UInt32(1) + test.a)\
        \n  Aggregate: groupBy=[[__common_expr_1 AS UInt32(1) + test.a]], aggr=[[avg(__common_expr_1) AS __common_expr_2, my_agg(__common_expr_1) AS __common_expr_3, avg(__common_expr_1 AS UInt32(1) + test.a) AS __common_expr_4, my_agg(__common_expr_1 AS UInt32(1) + test.a) AS __common_expr_5]]\
        \n    Projection: UInt32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn aggregate_with_relations_and_dots() -> Result<()> {
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

        let expected = "Projection: table.test.col.a, UInt32(1) + __common_expr_2 AS avg(UInt32(1) + table.test.col.a), __common_expr_2 AS avg(UInt32(1) + table.test.col.a)\
        \n  Aggregate: groupBy=[[table.test.col.a]], aggr=[[avg(__common_expr_1 AS UInt32(1) + table.test.col.a) AS __common_expr_2]]\
        \n    Projection: UInt32(1) + table.test.col.a AS __common_expr_1, table.test.col.a\
        \n      TableScan: table.test";

        assert_optimized_plan_eq(expected, plan, None);

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

        let expected = "Projection: __common_expr_1 AS first, __common_expr_1 AS second\
        \n  Projection: Int32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

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

        assert_non_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn cross_plans_subexpr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a"), col("a")])?
            .project(vec![lit(1) + col("a")])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  Projection: Int32(1) + test.a, test.a\
        \n    TableScan: test";

        assert_non_optimized_plan_eq(expected, plan, None);
        Ok(())
    }

    fn test_identifier(hash: u64, expr: &Expr) -> Identifier {
        Identifier { hash, expr }
    }

    #[test]
    fn redundant_project_fields() {
        let table_scan = test_table_scan().unwrap();
        let c_plus_a = col("c") + col("a");
        let b_plus_a = col("b") + col("a");
        let common_exprs_1 = CommonExprs::from([
            (
                test_identifier(0, &c_plus_a),
                (c_plus_a.clone(), format!("{CSE_PREFIX}_1")),
            ),
            (
                test_identifier(1, &b_plus_a),
                (b_plus_a.clone(), format!("{CSE_PREFIX}_2")),
            ),
        ]);
        let c_plus_a_2 = col(format!("{CSE_PREFIX}_1"));
        let b_plus_a_2 = col(format!("{CSE_PREFIX}_2"));
        let common_exprs_2 = CommonExprs::from([
            (
                test_identifier(3, &c_plus_a_2),
                (c_plus_a_2.clone(), format!("{CSE_PREFIX}_3")),
            ),
            (
                test_identifier(4, &b_plus_a_2),
                (b_plus_a_2.clone(), format!("{CSE_PREFIX}_4")),
            ),
        ]);
        let project = build_common_expr_project_plan(table_scan, common_exprs_1).unwrap();
        let project_2 = build_common_expr_project_plan(project, common_exprs_2).unwrap();

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
        let c_plus_a = col("test1.c") + col("test1.a");
        let b_plus_a = col("test1.b") + col("test1.a");
        let common_exprs_1 = CommonExprs::from([
            (
                test_identifier(0, &c_plus_a),
                (c_plus_a.clone(), format!("{CSE_PREFIX}_1")),
            ),
            (
                test_identifier(1, &b_plus_a),
                (b_plus_a.clone(), format!("{CSE_PREFIX}_2")),
            ),
        ]);
        let c_plus_a_2 = col(format!("{CSE_PREFIX}_1"));
        let b_plus_a_2 = col(format!("{CSE_PREFIX}_2"));
        let common_exprs_2 = CommonExprs::from([
            (
                test_identifier(3, &c_plus_a_2),
                (c_plus_a_2.clone(), format!("{CSE_PREFIX}_3")),
            ),
            (
                test_identifier(4, &b_plus_a_2),
                (b_plus_a_2.clone(), format!("{CSE_PREFIX}_4")),
            ),
        ]);
        let project = build_common_expr_project_plan(join, common_exprs_1).unwrap();
        let project_2 = build_common_expr_project_plan(project, common_exprs_2).unwrap();

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
        let rule = CommonSubexprEliminate::new();
        let optimized_plan = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        assert!(!optimized_plan.transformed);
        let optimized_plan = optimized_plan.data;

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
        \n  Filter: __common_expr_1 - Int32(10) > __common_expr_1\
        \n    Projection: Int32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_grouping_set() -> Result<()> {
        let mut result = Vec::with_capacity(3);
        let grouping = grouping_set(vec![vec![col("a"), col("b")], vec![col("c")]]);
        let schema = DFSchema::from_unqualified_fields(
            vec![
                LogicalField::new("a", DataType::Int32, false),
                LogicalField::new("b", DataType::Int32, false),
                LogicalField::new("c", DataType::Int32, false),
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
        let schema = DFSchema::from_unqualified_fields(
            vec![
                LogicalField::new("a", DataType::Int32, false),
                LogicalField::new("b", DataType::Int32, false),
            ]
            .into(),
            HashMap::default(),
        )?;
        extract_expressions(&grouping, &schema, &mut result)?;

        assert!(result.len() == 2);
        Ok(())
    }

    #[test]
    fn test_alias_collision() -> Result<()> {
        let table_scan = test_table_scan()?;

        let config = &OptimizerContext::new();
        let common_expr_1 = config.alias_generator().next(CSE_PREFIX);
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![
                (col("a") + col("b")).alias(common_expr_1.clone()),
                col("c"),
            ])?
            .project(vec![
                col(common_expr_1.clone()).alias("c1"),
                col(common_expr_1).alias("c2"),
                (col("c") + lit(2)).alias("c3"),
                (col("c") + lit(2)).alias("c4"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 AS c1, __common_expr_1 AS c2, __common_expr_2 AS c3, __common_expr_2 AS c4\
        \n  Projection: test.c + Int32(2) AS __common_expr_2, __common_expr_1, test.c\
        \n    Projection: test.a + test.b AS __common_expr_1, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, Some(config));

        let config = &OptimizerContext::new();
        let _common_expr_1 = config.alias_generator().next(CSE_PREFIX);
        let common_expr_2 = config.alias_generator().next(CSE_PREFIX);
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![
                (col("a") + col("b")).alias(common_expr_2.clone()),
                col("c"),
            ])?
            .project(vec![
                col(common_expr_2.clone()).alias("c1"),
                col(common_expr_2).alias("c2"),
                (col("c") + lit(2)).alias("c3"),
                (col("c") + lit(2)).alias("c4"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_2 AS c1, __common_expr_2 AS c2, __common_expr_3 AS c3, __common_expr_3 AS c4\
        \n  Projection: test.c + Int32(2) AS __common_expr_3, __common_expr_2, test.c\
        \n    Projection: test.a + test.b AS __common_expr_2, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, Some(config));

        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_col() -> Result<()> {
        let mut result = Vec::with_capacity(1);
        let schema = DFSchema::from_unqualified_fields(
            vec![LogicalField::new("a", DataType::Int32, false)].into(),
            HashMap::default(),
        )?;
        extract_expressions(&col("a"), &schema, &mut result)?;

        assert!(result.len() == 1);
        Ok(())
    }
}
