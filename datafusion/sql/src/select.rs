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

use std::collections::HashSet;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::{
    check_columns_satisfy_exprs, extract_aliases, rebase_expr, resolve_aliases_to_exprs,
    resolve_columns, resolve_positions_to_exprs,
};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion_common::{Column, UnnestOptions};
use datafusion_expr::expr::{Alias, Unnest};
use datafusion_expr::expr_rewriter::{
    normalize_col, normalize_col_with_schemas_and_ambiguity_check,
};
use datafusion_expr::utils::{
    expand_qualified_wildcard, expand_wildcard, expr_as_column_expr, expr_to_columns,
    find_aggregate_exprs, find_window_exprs,
};
use datafusion_expr::{
    Expr, Filter, GroupingSet, LogicalPlan, LogicalPlanBuilder, Partitioning,
};
use sqlparser::ast::{
    Distinct, Expr as SQLExpr, GroupByExpr, ReplaceSelectItem, WildcardAdditionalOptions,
    WindowType,
};
use sqlparser::ast::{NamedWindowDefinition, Select, SelectItem, TableWithJoins};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logic plan from an SQL select
    pub(super) fn select_to_plan(
        &self,
        mut select: Select,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // check for unsupported syntax first
        if !select.cluster_by.is_empty() {
            return not_impl_err!("CLUSTER BY");
        }
        if !select.lateral_views.is_empty() {
            return not_impl_err!("LATERAL VIEWS");
        }
        if select.qualify.is_some() {
            return not_impl_err!("QUALIFY");
        }
        if select.top.is_some() {
            return not_impl_err!("TOP");
        }
        if !select.sort_by.is_empty() {
            return not_impl_err!("SORT BY");
        }

        // process `from` clause
        let plan = self.plan_from_tables(select.from, planner_context)?;
        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));

        // process `where` clause
        let base_plan = self.plan_selection(select.selection, plan, planner_context)?;

        // handle named windows before processing the projection expression
        check_conflicting_windows(&select.named_window)?;
        match_window_definitions(&mut select.projection, &select.named_window)?;
        // process the SELECT expressions, with wildcards expanded.
        let select_exprs = self.prepare_select_exprs(
            &base_plan,
            select.projection,
            empty_from,
            planner_context,
        )?;

        // having and group by clause may reference aliases defined in select projection
        let projected_plan = self.project(base_plan.clone(), select_exprs.clone())?;
        // Place the fields of the base plan at the front so that when there are references
        // with the same name, the fields of the base plan will be searched first.
        // See https://github.com/apache/arrow-datafusion/issues/9162
        let mut combined_schema = base_plan.schema().as_ref().clone();
        combined_schema.merge(projected_plan.schema());

        // this alias map is resolved and looked up in both having exprs and group by exprs
        let alias_map = extract_aliases(&select_exprs);

        // Optionally the HAVING expression.
        let having_expr_opt = select
            .having
            .map::<Result<Expr>, _>(|having_expr| {
                let having_expr = self.sql_expr_to_logical_expr(
                    having_expr,
                    &combined_schema,
                    planner_context,
                )?;
                // This step "dereferences" any aliases in the HAVING clause.
                //
                // This is how we support queries with HAVING expressions that
                // refer to aliased columns.
                //
                // For example:
                //
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10;
                //
                // are rewritten as, respectively:
                //
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING MAX(c2) > 10;
                //
                let having_expr = resolve_aliases_to_exprs(&having_expr, &alias_map)?;
                normalize_col(having_expr, &projected_plan)
            })
            .transpose()?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT...
        let mut aggr_expr_haystack = select_exprs.clone();
        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr_opt {
            aggr_expr_haystack.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        // All of the group by expressions
        let group_by_exprs = if let GroupByExpr::Expressions(exprs) = select.group_by {
            exprs
                .into_iter()
                .map(|e| {
                    let group_by_expr = self.sql_expr_to_logical_expr(
                        e,
                        &combined_schema,
                        planner_context,
                    )?;
                    // aliases from the projection can conflict with same-named expressions in the input
                    let mut alias_map = alias_map.clone();
                    for f in base_plan.schema().fields() {
                        alias_map.remove(f.name());
                    }
                    let group_by_expr =
                        resolve_aliases_to_exprs(&group_by_expr, &alias_map)?;
                    let group_by_expr =
                        resolve_positions_to_exprs(&group_by_expr, &select_exprs)
                            .unwrap_or(group_by_expr);
                    let group_by_expr = normalize_col(group_by_expr, &projected_plan)?;
                    self.validate_schema_satisfies_exprs(
                        base_plan.schema(),
                        &[group_by_expr.clone()],
                    )?;
                    Ok(group_by_expr)
                })
                .collect::<Result<Vec<Expr>>>()?
        } else {
            // 'group by all' groups wrt. all select expressions except 'AggregateFunction's.
            // Filter and collect non-aggregate select expressions
            select_exprs
                .iter()
                .filter(|select_expr| match select_expr {
                    Expr::AggregateFunction(_) => false,
                    Expr::Alias(Alias { expr, name: _, .. }) => {
                        !matches!(**expr, Expr::AggregateFunction(_))
                    }
                    _ => true,
                })
                .cloned()
                .collect()
        };

        // process group by, aggregation or having
        let (plan, mut select_exprs_post_aggr, having_expr_post_aggr) = if !group_by_exprs
            .is_empty()
            || !aggr_exprs.is_empty()
        {
            self.aggregate(
                &base_plan,
                &select_exprs,
                having_expr_opt.as_ref(),
                &group_by_exprs,
                &aggr_exprs,
            )?
        } else {
            match having_expr_opt {
                Some(having_expr) => return plan_err!("HAVING clause references: {having_expr} must appear in the GROUP BY clause or be used in an aggregate function"),
                None => (base_plan.clone(), select_exprs.clone(), having_expr_opt)
            }
        };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr {
            LogicalPlanBuilder::from(plan)
                .filter(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        // process window function
        let window_func_exprs = find_window_exprs(&select_exprs_post_aggr);

        let plan = if window_func_exprs.is_empty() {
            plan
        } else {
            let plan = LogicalPlanBuilder::window_plan(plan, window_func_exprs.clone())?;

            // re-write the projection
            select_exprs_post_aggr = select_exprs_post_aggr
                .iter()
                .map(|expr| rebase_expr(expr, &window_func_exprs, &plan))
                .collect::<Result<Vec<Expr>>>()?;

            plan
        };

        // try process unnest expression or do the final projection
        let plan = self.try_process_unnest(plan, select_exprs_post_aggr)?;

        // process distinct clause
        let plan = match select.distinct {
            None => Ok(plan),
            Some(Distinct::Distinct) => {
                LogicalPlanBuilder::from(plan).distinct()?.build()
            }
            Some(Distinct::On(on_expr)) => {
                if !aggr_exprs.is_empty()
                    || !group_by_exprs.is_empty()
                    || !window_func_exprs.is_empty()
                {
                    return not_impl_err!("DISTINCT ON expressions with GROUP BY, aggregation or window functions are not supported ");
                }

                let on_expr = on_expr
                    .into_iter()
                    .map(|e| {
                        self.sql_expr_to_logical_expr(e, plan.schema(), planner_context)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Build the final plan
                return LogicalPlanBuilder::from(base_plan)
                    .distinct_on(on_expr, select_exprs, None)?
                    .build();
            }
        }?;

        // DISTRIBUTE BY
        let plan = if !select.distribute_by.is_empty() {
            let x = select
                .distribute_by
                .iter()
                .map(|e| {
                    self.sql_expr_to_logical_expr(
                        e.clone(),
                        &combined_schema,
                        planner_context,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            LogicalPlanBuilder::from(plan)
                .repartition(Partitioning::DistributeBy(x))?
                .build()?
        } else {
            plan
        };

        Ok(plan)
    }

    /// Try converting Expr(Unnest(Expr)) to Projection/Unnest/Projection
    pub(super) fn try_process_unnest(
        &self,
        input: LogicalPlan,
        select_exprs: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let mut unnest_columns = vec![];
        let mut inner_projection_exprs = vec![];

        let outer_projection_exprs = select_exprs
            .into_iter()
            .map(|expr| {
                let Transformed {
                    data: transformed_expr,
                    transformed,
                    tnr: _,
                } = expr.transform_up_mut(&mut |expr: Expr| {
                    if let Expr::Unnest(Unnest { ref exprs }) = expr {
                        let column_name = expr.display_name()?;
                        unnest_columns.push(column_name.clone());
                        // Add alias for the argument expression, to avoid naming conflicts with other expressions
                        // in the select list. For example: `select unnest(col1), col1 from t`.
                        inner_projection_exprs
                            .push(exprs[0].clone().alias(column_name.clone()));
                        Ok(Transformed::yes(Expr::Column(Column::from_name(
                            column_name,
                        ))))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })?;

                if !transformed {
                    if matches!(&transformed_expr, Expr::Column(_)) {
                        inner_projection_exprs.push(transformed_expr.clone());
                        Ok(transformed_expr)
                    } else {
                        // We need to evaluate the expr in the inner projection,
                        // outer projection just select its name
                        let column_name = transformed_expr.display_name()?;
                        inner_projection_exprs.push(transformed_expr);
                        Ok(Expr::Column(Column::from_name(column_name)))
                    }
                } else {
                    Ok(transformed_expr)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // Do the final projection
        if unnest_columns.is_empty() {
            LogicalPlanBuilder::from(input)
                .project(inner_projection_exprs)?
                .build()
        } else {
            if unnest_columns.len() > 1 {
                return not_impl_err!("Only support single unnest expression for now");
            }
            let unnest_column = unnest_columns.pop().unwrap();
            // Set preserve_nulls to false to ensure compatibility with DuckDB and PostgreSQL
            let unnest_options = UnnestOptions::new().with_preserve_nulls(false);
            LogicalPlanBuilder::from(input)
                .project(inner_projection_exprs)?
                .unnest_column_with_options(unnest_column, unnest_options)?
                .project(outer_projection_exprs)?
                .build()
        }
    }

    fn plan_selection(
        &self,
        selection: Option<SQLExpr>,
        plan: LogicalPlan,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(predicate_expr) => {
                let fallback_schemas = plan.fallback_normalize_schemas();
                let outer_query_schema = planner_context.outer_query_schema().cloned();
                let outer_query_schema_vec = outer_query_schema
                    .as_ref()
                    .map(|schema| vec![schema])
                    .unwrap_or_else(Vec::new);

                let filter_expr =
                    self.sql_to_expr(predicate_expr, plan.schema(), planner_context)?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[plan.schema()], &fallback_schemas, &outer_query_schema_vec],
                    &[using_columns],
                )?;

                Ok(LogicalPlan::Filter(Filter::try_new(
                    filter_expr,
                    Box::new(plan),
                )?))
            }
            None => Ok(plan),
        }
    }

    pub(crate) fn plan_from_tables(
        &self,
        mut from: Vec<TableWithJoins>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()?),
            1 => {
                let from = from.remove(0);
                self.plan_table_with_joins(from, planner_context)
            }
            _ => {
                let mut plans = from
                    .into_iter()
                    .map(|t| self.plan_table_with_joins(t, planner_context));

                let mut left = LogicalPlanBuilder::from(plans.next().unwrap()?);

                for right in plans {
                    left = left.cross_join(right?)?;
                }
                Ok(left.build()?)
            }
        }
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    ///
    /// Wildcards are expanded into the concrete list of columns.
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        projection
            .into_iter()
            .map(|expr| self.sql_select_to_rex(expr, plan, empty_from, planner_context))
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expr>>>()
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: SelectItem,
        plan: &LogicalPlan,
        empty_from: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        match sql {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_to_expr(expr, plan.schema(), planner_context)?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[plan.schema()]],
                    &plan.using_columns()?,
                )?;
                Ok(vec![col])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let select_expr =
                    self.sql_to_expr(expr, plan.schema(), planner_context)?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    select_expr,
                    &[&[plan.schema()]],
                    &plan.using_columns()?,
                )?;
                let name = self.normalizer.normalize(alias);
                // avoiding adding an alias if the column name is the same.
                let expr = match &col {
                    Expr::Column(column) if column.name.eq(&name) => col,
                    _ => col.alias(name),
                };
                Ok(vec![expr])
            }
            SelectItem::Wildcard(options) => {
                Self::check_wildcard_options(&options)?;

                if empty_from {
                    return plan_err!("SELECT * with no tables specified is not valid");
                }
                // do not expand from outer schema
                let expanded_exprs =
                    expand_wildcard(plan.schema().as_ref(), plan, Some(&options))?;
                // If there is a REPLACE statement, replace that column with the given
                // replace expression. Column name remains the same.
                if let Some(replace) = options.opt_replace {
                    self.replace_columns(
                        plan,
                        empty_from,
                        planner_context,
                        expanded_exprs,
                        replace,
                    )
                } else {
                    Ok(expanded_exprs)
                }
            }
            SelectItem::QualifiedWildcard(ref object_name, options) => {
                Self::check_wildcard_options(&options)?;
                let qualifier = format!("{object_name}");
                // do not expand from outer schema
                let expanded_exprs = expand_qualified_wildcard(
                    &qualifier,
                    plan.schema().as_ref(),
                    Some(&options),
                )?;
                // If there is a REPLACE statement, replace that column with the given
                // replace expression. Column name remains the same.
                if let Some(replace) = options.opt_replace {
                    self.replace_columns(
                        plan,
                        empty_from,
                        planner_context,
                        expanded_exprs,
                        replace,
                    )
                } else {
                    Ok(expanded_exprs)
                }
            }
        }
    }

    fn check_wildcard_options(options: &WildcardAdditionalOptions) -> Result<()> {
        let WildcardAdditionalOptions {
            // opt_exclude is handled
            opt_exclude: _opt_exclude,
            opt_except: _opt_except,
            opt_rename,
            opt_replace: _opt_replace,
        } = options;

        if opt_rename.is_some() {
            Err(DataFusionError::NotImplemented(
                "wildcard * with RENAME not supported ".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// If there is a REPLACE statement in the projected expression in the form of
    /// "REPLACE (some_column_within_an_expr AS some_column)", this function replaces
    /// that column with the given replace expression. Column name remains the same.
    /// Multiple REPLACEs are also possible with comma separations.
    fn replace_columns(
        &self,
        plan: &LogicalPlan,
        empty_from: bool,
        planner_context: &mut PlannerContext,
        mut exprs: Vec<Expr>,
        replace: ReplaceSelectItem,
    ) -> Result<Vec<Expr>> {
        for expr in exprs.iter_mut() {
            if let Expr::Column(Column { name, .. }) = expr {
                if let Some(item) = replace
                    .items
                    .iter()
                    .find(|item| item.column_name.value == *name)
                {
                    let new_expr = self.sql_select_to_rex(
                        SelectItem::UnnamedExpr(item.expr.clone()),
                        plan,
                        empty_from,
                        planner_context,
                    )?[0]
                        .clone();
                    *expr = Expr::Alias(Alias {
                        expr: Box::new(new_expr),
                        relation: None,
                        name: name.clone(),
                    });
                }
            }
        }
        Ok(exprs)
    }

    /// Wrap a plan in a projection
    fn project(&self, input: LogicalPlan, expr: Vec<Expr>) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(input.schema(), &expr)?;
        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Create an aggregate plan.
    ///
    /// An aggregate plan consists of grouping expressions, aggregate expressions, and an
    /// optional HAVING expression (which is a filter on the output of the aggregate).
    ///
    /// # Arguments
    ///
    /// * `input`           - The input plan that will be aggregated. The grouping, aggregate, and
    ///                       "having" expressions must all be resolvable from this plan.
    /// * `select_exprs`    - The projection expressions from the SELECT clause.
    /// * `having_expr_opt` - Optional HAVING clause.
    /// * `group_by_exprs`  - Grouping expressions from the GROUP BY clause. These can be column
    ///                       references or more complex expressions.
    /// * `aggr_exprs`      - Aggregate expressions, such as `SUM(a)` or `COUNT(1)`.
    ///
    /// # Return
    ///
    /// The return value is a triplet of the following items:
    ///
    /// * `plan`                   - A [LogicalPlan::Aggregate] plan for the newly created aggregate.
    /// * `select_exprs_post_aggr` - The projection expressions rewritten to reference columns from
    ///                              the aggregate
    /// * `having_expr_post_aggr`  - The "having" expression rewritten to reference a column from
    ///                              the aggregate
    fn aggregate(
        &self,
        input: &LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: Option<&Expr>,
        group_by_exprs: &[Expr],
        aggr_exprs: &[Expr],
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        // create the aggregate plan
        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(group_by_exprs.to_vec(), aggr_exprs.to_vec())?
            .build()?;

        let group_by_exprs = if let LogicalPlan::Aggregate(agg) = &plan {
            &agg.group_expr
        } else {
            unreachable!();
        };

        // in this next section of code we are re-writing the projection to refer to columns
        // output by the aggregate plan. For example, if the projection contains the expression
        // `SUM(a)` then we replace that with a reference to a column `SUM(a)` produced by
        // the aggregate plan.

        // combine the original grouping and aggregate expressions into one list (note that
        // we do not add the "having" expression since that is not part of the projection)
        let mut aggr_projection_exprs = vec![];
        for expr in group_by_exprs {
            match expr {
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    aggr_projection_exprs.extend_from_slice(exprs)
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                    aggr_projection_exprs.extend_from_slice(exprs)
                }
                Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                    for exprs in lists_of_exprs {
                        aggr_projection_exprs.extend_from_slice(exprs)
                    }
                }
                _ => aggr_projection_exprs.push(expr.clone()),
            }
        }
        aggr_projection_exprs.extend_from_slice(aggr_exprs);

        // now attempt to resolve columns and replace with fully-qualified columns
        let aggr_projection_exprs = aggr_projection_exprs
            .iter()
            .map(|expr| resolve_columns(expr, input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we replace any expressions that are not a column with a column referencing
        // an output column from the aggregate schema
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we re-write the projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &aggr_projection_exprs, input))
            .collect::<Result<Vec<Expr>>>()?;

        // finally, we have some validation that the re-written projection can be resolved
        // from the aggregate output columns
        check_columns_satisfy_exprs(
            &column_exprs_post_aggr,
            &select_exprs_post_aggr,
            "Projection references non-aggregate values",
        )?;

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, input)?;

            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                &[having_expr_post_aggr.clone()],
                "HAVING clause references non-aggregate values",
            )?;

            Some(having_expr_post_aggr)
        } else {
            None
        };

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr))
    }
}

// If there are any multiple-defined windows, we raise an error.
fn check_conflicting_windows(window_defs: &[NamedWindowDefinition]) -> Result<()> {
    for (i, window_def_i) in window_defs.iter().enumerate() {
        for window_def_j in window_defs.iter().skip(i + 1) {
            if window_def_i.0 == window_def_j.0 {
                return plan_err!(
                    "The window {} is defined multiple times!",
                    window_def_i.0
                );
            }
        }
    }
    Ok(())
}

// If the projection is done over a named window, that window
// name must be defined. Otherwise, it gives an error.
fn match_window_definitions(
    projection: &mut [SelectItem],
    named_windows: &[NamedWindowDefinition],
) -> Result<()> {
    for proj in projection.iter_mut() {
        if let SelectItem::ExprWithAlias {
            expr: SQLExpr::Function(f),
            alias: _,
        }
        | SelectItem::UnnamedExpr(SQLExpr::Function(f)) = proj
        {
            for NamedWindowDefinition(window_ident, window_spec) in named_windows.iter() {
                if let Some(WindowType::NamedWindow(ident)) = &f.over {
                    if ident.eq(window_ident) {
                        f.over = Some(WindowType::WindowSpec(window_spec.clone()))
                    }
                }
            }
            // All named windows must be defined with a WindowSpec.
            if let Some(WindowType::NamedWindow(ident)) = &f.over {
                return plan_err!("The window {ident} is not defined!");
            }
        }
    }
    Ok(())
}
