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

//! Filter Push Down optimizer rule ensures that filters are applied as early as possible in the plan

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_expr::TableProviderFilterPushDown;
use crate::logical_plan::plan::{Aggregate, Filter, Join, Projection, Union};
use crate::logical_plan::{
    col, replace_col, Column, CrossJoin, JoinType, Limit, LogicalPlan, TableScan,
};
use crate::logical_plan::{DFSchema, Expr};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use std::collections::{HashMap, HashSet};

/// Filter Push Down optimizer rule pushes filter clauses down the plan
/// # Introduction
/// A filter-commutative operation is an operation whose result of filter(op(data)) = op(filter(data)).
/// An example of a filter-commutative operation is a projection; a counter-example is `limit`.
///
/// The filter-commutative property is column-specific. An aggregate grouped by A on SUM(B)
/// can commute with a filter that depends on A only, but does not commute with a filter that depends
/// on SUM(B).
///
/// This optimizer commutes filters with filter-commutative operations to push the filters
/// the closest possible to the scans, re-writing the filter expressions by every
/// projection that changes the filter's expression.
///
/// Filter: #b Gt Int64(10)
///     Projection: #a AS b
///
/// is optimized to
///
/// Projection: #a AS b
///     Filter: #a Gt Int64(10)  <--- changed from #b to #a
///
/// This performs a single pass through the plan. When it passes through a filter, it stores that filter,
/// and when it reaches a node that does not commute with it, it adds the filter to that place.
/// When it passes through a projection, it re-writes the filter's expression taking into account that projection.
/// When multiple filters would have been written, it `AND` their expressions into a single expression.
#[derive(Default)]
pub struct FilterPushDown {}

#[derive(Debug, Clone, Default)]
struct State {
    // (predicate, columns on the predicate)
    filters: Vec<(Expr, HashSet<Column>)>,
}

type Predicates<'a> = (Vec<&'a Expr>, Vec<&'a HashSet<Column>>);

/// returns all predicates in `state` that depend on any of `used_columns`
fn get_predicates<'a>(
    state: &'a State,
    used_columns: &HashSet<Column>,
) -> Predicates<'a> {
    state
        .filters
        .iter()
        .filter(|(_, columns)| {
            !columns
                .intersection(used_columns)
                .collect::<HashSet<_>>()
                .is_empty()
        })
        .map(|&(ref a, ref b)| (a, b))
        .unzip()
}

/// Optimizes the plan
fn push_down(state: &State, plan: &LogicalPlan) -> Result<LogicalPlan> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input, state.clone()))
        .collect::<Result<Vec<_>>>()?;

    let expr = plan.expressions();
    utils::from_plan(plan, &expr, &new_inputs)
}

// remove all filters from `filters` that are in `predicate_columns`
fn remove_filters(
    filters: &[(Expr, HashSet<Column>)],
    predicate_columns: &[&HashSet<Column>],
) -> Vec<(Expr, HashSet<Column>)> {
    filters
        .iter()
        .filter(|(_, columns)| !predicate_columns.contains(&columns))
        .cloned()
        .collect::<Vec<_>>()
}

// keeps all filters from `filters` that are in `predicate_columns`
fn keep_filters(
    filters: &[(Expr, HashSet<Column>)],
    relevant_predicates: &Predicates,
) -> Vec<(Expr, HashSet<Column>)> {
    filters
        .iter()
        .filter(|(expr, _)| relevant_predicates.0.contains(&expr))
        .cloned()
        .collect::<Vec<_>>()
}

/// builds a new [LogicalPlan] from `plan` by issuing new [LogicalPlan::Filter] if any of the filters
/// in `state` depend on the columns `used_columns`.
fn issue_filters(
    mut state: State,
    used_columns: HashSet<Column>,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let (predicates, predicate_columns) = get_predicates(&state, &used_columns);

    if predicates.is_empty() {
        // all filters can be pushed down => optimize inputs and return new plan
        return push_down(&state, plan);
    }

    let plan = utils::filter_by_all(plan.clone(), &predicates);

    state.filters = remove_filters(&state.filters, &predicate_columns);

    // continue optimization over all input nodes by cloning the current state (i.e. each node is independent)
    push_down(&state, &plan)
}

// For a given JOIN logical plan, determine whether each side of the join is preserved.
// We say a join side is preserved if the join returns all or a subset of the rows from
// the relevant side, such that each row of the output table directly maps to a row of
// the preserved input table. If a table is not preserved, it can provide extra null rows.
// That is, there may be rows in the output table that don't directly map to a row in the
// input table.
//
// For example:
//   - In an inner join, both sides are preserved, because each row of the output
//     maps directly to a row from each side.
//   - In a left join, the left side is preserved and the right is not, because
//     there may be rows in the output that don't directly map to a row in the
//     right input (due to nulls filling where there is no match on the right).
//
// This is important because we can always push down post-join filters to a preserved
// side of the join, assuming the filter only references columns from that side. For the
// non-preserved side it can be more tricky.
//
// Returns a tuple of booleans - (left_preserved, right_preserved).
fn lr_is_preserved(plan: &LogicalPlan) -> (bool, bool) {
    match plan {
        LogicalPlan::Join(Join { join_type, .. }) => match join_type {
            JoinType::Inner => (true, true),
            JoinType::Left => (true, false),
            JoinType::Right => (false, true),
            JoinType::Full => (false, false),
            // No columns from the right side of the join can be referenced in output
            // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
            JoinType::Semi | JoinType::Anti => (true, false),
        },
        LogicalPlan::CrossJoin(_) => (true, true),
        _ => unreachable!("lr_is_preserved only valid for JOIN nodes"),
    }
}

// Determine which predicates in state can be pushed down to a given side of a join.
// To determine this, we need to know the schema of the relevant join side and whether
// or not the side's rows are preserved when joining. If the side is not preserved, we
// do not push down anything. Otherwise we can push down predicates where all of the
// relevant columns are contained on the relevant join side's schema.
fn get_pushable_join_predicates<'a>(
    state: &'a State,
    schema: &DFSchema,
    preserved: bool,
) -> Predicates<'a> {
    if !preserved {
        return (vec![], vec![]);
    }

    let schema_columns = schema
        .fields()
        .iter()
        .flat_map(|f| {
            [
                f.qualified_column(),
                // we need to push down filter using unqualified column as well
                f.unqualified_column(),
            ]
        })
        .collect::<HashSet<_>>();

    state
        .filters
        .iter()
        .filter(|(_, columns)| {
            let all_columns_in_schema = schema_columns
                .intersection(columns)
                .collect::<HashSet<_>>()
                .len()
                == columns.len();
            all_columns_in_schema
        })
        .map(|(a, b)| (a, b))
        .unzip()
}

fn optimize_join(
    mut state: State,
    plan: &LogicalPlan,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Result<LogicalPlan> {
    let (left_preserved, right_preserved) = lr_is_preserved(plan);
    let to_left = get_pushable_join_predicates(&state, left.schema(), left_preserved);
    let to_right = get_pushable_join_predicates(&state, right.schema(), right_preserved);

    let to_keep: Predicates = state
        .filters
        .iter()
        .filter(|(expr, _)| {
            let pushed_to_left = to_left.0.contains(&expr);
            let pushed_to_right = to_right.0.contains(&expr);
            !pushed_to_left && !pushed_to_right
        })
        .map(|(a, b)| (a, b))
        .unzip();

    let mut left_state = state.clone();
    left_state.filters = keep_filters(&left_state.filters, &to_left);
    let left = optimize(left, left_state)?;

    let mut right_state = state.clone();
    right_state.filters = keep_filters(&right_state.filters, &to_right);
    let right = optimize(right, right_state)?;

    // create a new Join with the new `left` and `right`
    let expr = plan.expressions();
    let plan = utils::from_plan(plan, &expr, &[left, right])?;

    if to_keep.0.is_empty() {
        Ok(plan)
    } else {
        // wrap the join on the filter whose predicates must be kept
        let plan = utils::filter_by_all(plan, &to_keep.0);
        state.filters = remove_filters(&state.filters, &to_keep.1);

        Ok(plan)
    }
}

fn optimize(plan: &LogicalPlan, mut state: State) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Explain { .. } => {
            // push the optimization to the plan of this explain
            push_down(&state, plan)
        }
        LogicalPlan::Analyze { .. } => push_down(&state, plan),
        LogicalPlan::Filter(Filter { input, predicate }) => {
            let mut predicates = vec![];
            utils::split_conjunction(predicate, &mut predicates);

            // Predicates without referencing columns (WHERE FALSE, WHERE 1=1, etc.)
            let mut no_col_predicates = vec![];

            predicates
                .into_iter()
                .try_for_each::<_, Result<()>>(|predicate| {
                    let mut columns: HashSet<Column> = HashSet::new();
                    utils::expr_to_columns(predicate, &mut columns)?;
                    if columns.is_empty() {
                        no_col_predicates.push(predicate)
                    } else {
                        // collect the predicate
                        state.filters.push((predicate.clone(), columns));
                    }
                    Ok(())
                })?;

            // Predicates without columns will not be pushed down.
            // As those contain only literals, they could be optimized using constant folding
            // and removal of WHERE TRUE / WHERE FALSE
            if !no_col_predicates.is_empty() {
                Ok(utils::filter_by_all(
                    optimize(input, state)?,
                    &no_col_predicates,
                ))
            } else {
                optimize(input, state)
            }
        }
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            alias: _,
        }) => {
            // A projection is filter-commutable, but re-writes all predicate expressions
            // collect projection.
            let projection = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    // strip alias, as they should not be part of filters
                    let expr = match &expr[i] {
                        Expr::Alias(expr, _) => expr.as_ref().clone(),
                        expr => expr.clone(),
                    };

                    (field.qualified_name(), expr)
                })
                .collect::<HashMap<_, _>>();

            // re-write all filters based on this projection
            // E.g. in `Filter: #b\n  Projection: #a > 1 as b`, we can swap them, but the filter must be "#a > 1"
            for (predicate, columns) in state.filters.iter_mut() {
                *predicate = rewrite(predicate, &projection)?;

                columns.clear();
                utils::expr_to_columns(predicate, columns)?;
            }

            // optimize inner
            let new_input = optimize(input, state)?;

            utils::from_plan(plan, expr, &[new_input])
        }
        LogicalPlan::Aggregate(Aggregate {
            aggr_expr, input, ..
        }) => {
            // An aggregate's aggreagate columns are _not_ filter-commutable => collect these:
            // * columns whose aggregation expression depends on
            // * the aggregation columns themselves

            // construct set of columns that `aggr_expr` depends on
            let mut used_columns = HashSet::new();
            utils::exprlist_to_columns(aggr_expr, &mut used_columns)?;

            let agg_columns = aggr_expr
                .iter()
                .map(|x| Ok(Column::from_name(x.name(input.schema())?)))
                .collect::<Result<HashSet<_>>>()?;
            used_columns.extend(agg_columns);

            issue_filters(state, used_columns, plan)
        }
        LogicalPlan::Sort { .. } => {
            // sort is filter-commutable
            push_down(&state, plan)
        }
        LogicalPlan::Union(Union {
            inputs: _,
            schema,
            alias: _,
        }) => {
            // union changing all qualifiers while building logical plan so we need
            // to rewrite filters to push unqualified columns to inputs
            let projection = schema
                .fields()
                .iter()
                .map(|field| (field.qualified_name(), col(field.name())))
                .collect::<HashMap<_, _>>();

            // rewriting predicate expressions using unqualified names as replacements
            if !projection.is_empty() {
                for (predicate, columns) in state.filters.iter_mut() {
                    *predicate = rewrite(predicate, &projection)?;

                    columns.clear();
                    utils::expr_to_columns(predicate, columns)?;
                }
            }

            push_down(&state, plan)
        }
        LogicalPlan::Limit(Limit { input, .. }) => {
            // limit is _not_ filter-commutable => collect all columns from its input
            let used_columns = input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<_>>();
            issue_filters(state, used_columns, plan)
        }
        LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
            optimize_join(state, plan, left, right)
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            ..
        }) => {
            if *join_type == JoinType::Inner {
                // For inner joins, duplicate filters for joined columns so filters can be pushed down
                // to both sides. Take the following query as an example:
                //
                // ```sql
                // SELECT * FROM t1 JOIN t2 on t1.id = t2.uid WHERE t1.id > 1
                // ```
                //
                // `t1.id > 1` predicate needs to be pushed down to t1 table scan, while
                // `t2.uid > 1` predicate needs to be pushed down to t2 table scan.
                //
                // Join clauses with `Using` constraints also take advantage of this logic to make sure
                // predicates reference the shared join columns are pushed to both sides.
                let join_side_filters = state
                    .filters
                    .iter()
                    .filter_map(|(predicate, columns)| {
                        let mut join_cols_to_replace = HashMap::new();
                        for col in columns.iter() {
                            for (l, r) in on {
                                if col == l {
                                    join_cols_to_replace.insert(col, r);
                                    break;
                                } else if col == r {
                                    join_cols_to_replace.insert(col, l);
                                    break;
                                }
                            }
                        }

                        if join_cols_to_replace.is_empty() {
                            return None;
                        }

                        let join_side_predicate =
                            match replace_col(predicate.clone(), &join_cols_to_replace) {
                                Ok(p) => p,
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            };

                        let join_side_columns = columns
                            .clone()
                            .into_iter()
                            // replace keys in join_cols_to_replace with values in resulting column
                            // set
                            .filter(|c| !join_cols_to_replace.contains_key(c))
                            .chain(join_cols_to_replace.iter().map(|(_, v)| (*v).clone()))
                            .collect();

                        Some(Ok((join_side_predicate, join_side_columns)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                state.filters.extend(join_side_filters);
            }
            optimize_join(state, plan, left, right)
        }
        LogicalPlan::TableScan(TableScan {
            source,
            projected_schema,
            filters,
            projection,
            table_name,
            limit,
        }) => {
            let mut used_columns = HashSet::new();
            let mut new_filters = filters.clone();

            for (filter_expr, cols) in &state.filters {
                let (preserve_filter_node, add_to_provider) =
                    match source.supports_filter_pushdown(filter_expr)? {
                        TableProviderFilterPushDown::Unsupported => (true, false),
                        TableProviderFilterPushDown::Inexact => (true, true),
                        TableProviderFilterPushDown::Exact => (false, true),
                    };

                if preserve_filter_node {
                    used_columns.extend(cols.clone());
                }

                if add_to_provider {
                    // Don't add expression again if it's already present in
                    // pushed down filters.
                    if new_filters.contains(filter_expr) {
                        continue;
                    }
                    new_filters.push(filter_expr.clone());
                }
            }

            issue_filters(
                state,
                used_columns,
                &LogicalPlan::TableScan(TableScan {
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    table_name: table_name.clone(),
                    filters: new_filters,
                    limit: *limit,
                }),
            )
        }
        _ => {
            // all other plans are _not_ filter-commutable
            let used_columns = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<_>>();
            issue_filters(state, used_columns, plan)
        }
    }
}

impl OptimizerRule for FilterPushDown {
    fn name(&self) -> &str {
        "filter_push_down"
    }

    fn optimize(&self, plan: &LogicalPlan, _: &ExecutionProps) -> Result<LogicalPlan> {
        optimize(plan, State::default())
    }
}

impl FilterPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// replaces columns by its name on the projection.
fn rewrite(expr: &Expr, projection: &HashMap<String, Expr>) -> Result<Expr> {
    let expressions = utils::expr_sub_expressions(expr)?;

    let expressions = expressions
        .iter()
        .map(|e| rewrite(e, projection))
        .collect::<Result<Vec<_>>>()?;

    if let Expr::Column(c) = expr {
        if let Some(expr) = projection.get(&c.flat_name()) {
            return Ok(expr.clone());
        }
    }

    utils::rewrite_expression(expr, &expressions)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::datasource::TableProvider;
    use crate::logical_plan::plan::provider_as_source;
    use crate::logical_plan::{
        and, col, lit, sum, union_with_alias, DFSchema, Expr, LogicalPlanBuilder,
        Operator,
    };
    use crate::physical_plan::ExecutionPlan;
    use crate::prelude::JoinType;
    use crate::test::*;

    use arrow::datatypes::SchemaRef;
    use async_trait::async_trait;

    fn optimize_plan(plan: &LogicalPlan) -> LogicalPlan {
        let rule = FilterPushDown::new();
        rule.optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan")
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize_plan(plan);
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn filter_before_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        let expected = "\
            Projection: #test.a, #test.b\
            \n  Filter: #test.a = Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(10)?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before single projection
        let expected = "\
            Filter: #test.a = Int64(1)\
            \n  Limit: 10\
            \n    Projection: #test.a, #test.b\
            \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_no_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(lit(0i64).eq(lit(1i64)))?
            .build()?;
        let expected = "\
            Filter: Int64(0) = Int64(1)\
            \n  TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_jump_2_plans() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("c"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before double projection
        let expected = "\
            Projection: #test.c, #test.b\
            \n  Projection: #test.a, #test.b, #test.c\
            \n    Filter: #test.a = Int64(1)\
            \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_move_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("total_salary")])?
            .filter(col("a").gt(lit(10i64)))?
            .build()?;
        // filter of key aggregation is commutative
        let expected = "\
            Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b) AS total_salary]]\
            \n  Filter: #test.a > Int64(10)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_keep_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("b")])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        // filter of aggregate is after aggregation since they are non-commutative
        let expected = "\
            Filter: #b > Int64(10)\
            \n  Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b) AS b]]\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that a filter is pushed to before a projection, the filter expression is correctly re-written
    #[test]
    fn alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        let expected = "\
            Projection: #test.a AS b, #test.c\
            \n  Filter: #test.a = Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    fn add(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op: Operator::Plus,
            right: Box::new(right),
        }
    }

    fn multiply(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op: Operator::Multiply,
            right: Box::new(right),
        }
    }

    /// verifies that a filter is pushed to before a projection with a complex expression, the filter expression is correctly re-written
    #[test]
    fn complex_expression() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #b = Int64(1)\
            \n  Projection: #test.a * Int32(2) + #test.c AS b, #test.c\
            \n    TableScan: test projection=None"
        );

        // filter is before projection
        let expected = "\
            Projection: #test.a * Int32(2) + #test.c AS b, #test.c\
            \n  Filter: #test.a * Int32(2) + #test.c = Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that when a filter is pushed to after 2 projections, the filter expression is correctly re-written
    #[test]
    fn complex_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            // second projection where we rename columns, just to make it difficult
            .project(vec![multiply(col("b"), lit(3)).alias("a"), col("c")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #a = Int64(1)\
            \n  Projection: #b * Int32(3) AS a, #test.c\
            \n    Projection: #test.a * Int32(2) + #test.c AS b, #test.c\
            \n      TableScan: test projection=None"
        );

        // filter is before the projections
        let expected = "\
        Projection: #b * Int32(3) AS a, #test.c\
        \n  Projection: #test.a * Int32(2) + #test.c AS b, #test.c\
        \n    Filter: #test.a * Int32(2) + #test.c * Int32(3) = Int64(1)\
        \n      TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that when two filters apply after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn multi_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(col("b").gt(lit(10i64)))?
            .filter(col("SUM(test.c)").gt(lit(10i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #SUM(test.c) > Int64(10)\
            \n  Filter: #b > Int64(10)\
            \n    Aggregate: groupBy=[[#b]], aggr=[[SUM(#test.c)]]\
            \n      Projection: #test.a AS b, #test.c\
            \n        TableScan: test projection=None"
        );

        // filter is before the projections
        let expected = "\
        Filter: #SUM(test.c) > Int64(10)\
        \n  Aggregate: groupBy=[[#b]], aggr=[[SUM(#test.c)]]\
        \n    Projection: #test.a AS b, #test.c\
        \n      Filter: #test.a > Int64(10)\
        \n        TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    /// verifies that when a filter with two predicates is applied after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn split_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(and(
                col("SUM(test.c)").gt(lit(10i64)),
                and(col("b").gt(lit(10i64)), col("SUM(test.c)").lt(lit(20i64))),
            ))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #SUM(test.c) > Int64(10) AND #b > Int64(10) AND #SUM(test.c) < Int64(20)\
            \n  Aggregate: groupBy=[[#b]], aggr=[[SUM(#test.c)]]\
            \n    Projection: #test.a AS b, #test.c\
            \n      TableScan: test projection=None"
        );

        // filter is before the projections
        let expected = "\
        Filter: #SUM(test.c) > Int64(10) AND #SUM(test.c) < Int64(20)\
        \n  Aggregate: groupBy=[[#b]], aggr=[[SUM(#test.c)]]\
        \n    Projection: #test.a AS b, #test.c\
        \n      Filter: #test.a > Int64(10)\
        \n        TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    /// verifies that when two limits are in place, we jump neither
    #[test]
    fn double_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(20)?
            .limit(10)?
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter does not just any of the limits
        let expected = "\
            Projection: #test.a, #test.b\
            \n  Filter: #test.a = Int64(1)\
            \n    Limit: 10\
            \n      Limit: 20\
            \n        Projection: #test.a, #test.b\
            \n          TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn union_all() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter appears below Union
        let expected = "\
            Union\
            \n  Filter: #a = Int64(1)\
            \n    TableScan: test projection=None\
            \n  Filter: #a = Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn union_all_with_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let union =
            union_with_alias(table_scan.clone(), table_scan, Some("t".to_string()))?;

        let plan = LogicalPlanBuilder::from(union)
            .filter(col("t.a").eq(lit(1i64)))?
            .build()?;

        // filter appears below Union without relation qualifier
        let expected = "\
            Union\
            \n  Filter: #a = Int64(1)\
            \n    TableScan: test projection=None\
            \n  Filter: #a = Int64(1)\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that filters with the same columns are correctly placed
    #[test]
    fn filter_2_breaks_limits() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(col("a").lt_eq(lit(1i64)))?
            .limit(1)?
            .project(vec![col("a")])?
            .filter(col("a").gt_eq(lit(1i64)))?
            .build()?;
        // Should be able to move both filters below the projections

        // not part of the test
        assert_eq!(
            format!("{:?}", plan),
            "Filter: #test.a >= Int64(1)\
             \n  Projection: #test.a\
             \n    Limit: 1\
             \n      Filter: #test.a <= Int64(1)\
             \n        Projection: #test.a\
             \n          TableScan: test projection=None"
        );

        let expected = "\
        Projection: #test.a\
        \n  Filter: #test.a >= Int64(1)\
        \n    Limit: 1\
        \n      Projection: #test.a\
        \n        Filter: #test.a <= Int64(1)\
        \n          TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that filters to be placed on the same depth are ANDed
    #[test]
    fn two_filters_on_same_depth() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(1)?
            .filter(col("a").lt_eq(lit(1i64)))?
            .filter(col("a").gt_eq(lit(1i64)))?
            .project(vec![col("a")])?
            .build()?;

        // not part of the test
        assert_eq!(
            format!("{:?}", plan),
            "Projection: #test.a\
            \n  Filter: #test.a >= Int64(1)\
            \n    Filter: #test.a <= Int64(1)\
            \n      Limit: 1\
            \n        TableScan: test projection=None"
        );

        let expected = "\
        Projection: #test.a\
        \n  Filter: #test.a >= Int64(1) AND #test.a <= Int64(1)\
        \n    Limit: 1\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// verifies that filters on a plan with user nodes are not lost
    /// (ARROW-10547)
    #[test]
    fn filters_user_defined_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let plan = crate::test::user_defined::new(plan);

        let expected = "\
            TestUserDefined\
             \n  Filter: #test.a <= Int64(1)\
             \n    TableScan: test projection=None";

        // not part of the test
        assert_eq!(format!("{:?}", plan), expected);

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-on-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_on_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                &right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test.a <= Int64(1)\
            \n  Inner Join: #test.a = #test2.a\
            \n    TableScan: test projection=None\
            \n    Projection: #test2.a\
            \n      TableScan: test2 projection=None"
        );

        // filter sent to side before the join
        let expected = "\
        Inner Join: #test.a = #test2.a\
        \n  Filter: #test.a <= Int64(1)\
        \n    TableScan: test projection=None\
        \n  Projection: #test2.a\
        \n    Filter: #test2.a <= Int64(1)\
        \n      TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-using-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_using_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                &right,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test.a <= Int64(1)\
            \n  Inner Join: Using #test.a = #test2.a\
            \n    TableScan: test projection=None\
            \n    Projection: #test2.a\
            \n      TableScan: test2 projection=None"
        );

        // filter sent to side before the join
        let expected = "\
        Inner Join: Using #test.a = #test2.a\
        \n  Filter: #test.a <= Int64(1)\
        \n    TableScan: test projection=None\
        \n  Projection: #test2.a\
        \n    Filter: #test2.a <= Int64(1)\
        \n      TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates with columns from both sides are not pushed
    #[test]
    fn filter_join_on_common_dependent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                &right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            )?
            // "b" and "c" are not shared by either side: they are only available together after the join
            .filter(col("c").lt_eq(col("b")))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test.c <= #test2.b\
            \n  Inner Join: #test.a = #test2.a\
            \n    Projection: #test.a, #test.c\
            \n      TableScan: test projection=None\
            \n    Projection: #test2.a, #test2.b\
            \n      TableScan: test2 projection=None"
        );

        // expected is equal: no push-down
        let expected = &format!("{:?}", plan);
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates with columns from one side of a join are pushed only to that side
    #[test]
    fn filter_join_on_one_side() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let table_scan_right = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(table_scan_right)
            .project(vec![col("a"), col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .join(
                &right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            )?
            .filter(col("b").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test.b <= Int64(1)\
            \n  Inner Join: #test.a = #test2.a\
            \n    Projection: #test.a, #test.b\
            \n      TableScan: test projection=None\
            \n    Projection: #test2.a, #test2.c\
            \n      TableScan: test2 projection=None"
        );

        let expected = "\
        Inner Join: #test.a = #test2.a\
        \n  Projection: #test.a, #test.b\
        \n    Filter: #test.b <= Int64(1)\
        \n      TableScan: test projection=None\
        \n  Projection: #test2.a, #test2.c\
        \n    TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates on the right side of a left join are not duplicated
    /// TODO: In this case we can sometimes convert the join to an INNER join
    #[test]
    fn filter_using_left_join() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                &right,
                JoinType::Left,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test2.a <= Int64(1)\
            \n  Left Join: Using #test.a = #test2.a\
            \n    TableScan: test projection=None\
            \n    Projection: #test2.a\
            \n      TableScan: test2 projection=None"
        );

        // filter not duplicated nor pushed down - i.e. noop
        let expected = "\
        Filter: #test2.a <= Int64(1)\
        \n  Left Join: Using #test.a = #test2.a\
        \n    TableScan: test projection=None\
        \n    Projection: #test2.a\
        \n      TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-join predicates on the left side of a right join are not duplicated
    /// TODO: In this case we can sometimes convert the join to an INNER join
    #[test]
    fn filter_using_right_join() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                &right,
                JoinType::Right,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test.a <= Int64(1)\
            \n  Right Join: Using #test.a = #test2.a\
            \n    TableScan: test projection=None\
            \n    Projection: #test2.a\
            \n      TableScan: test2 projection=None"
        );

        // filter not duplicated nor pushed down - i.e. noop
        let expected = "\
        Filter: #test.a <= Int64(1)\
        \n  Right Join: Using #test.a = #test2.a\
        \n    TableScan: test projection=None\
        \n    Projection: #test2.a\
        \n      TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-left-join predicate on a column common to both sides is only pushed to the left side
    /// i.e. - not duplicated to the right side
    #[test]
    fn filter_using_left_join_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                &right,
                JoinType::Left,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test.a <= Int64(1)\
            \n  Left Join: Using #test.a = #test2.a\
            \n    TableScan: test projection=None\
            \n    Projection: #test2.a\
            \n      TableScan: test2 projection=None"
        );

        // filter sent to left side of the join, not the right
        let expected = "\
        Left Join: Using #test.a = #test2.a\
        \n  Filter: #test.a <= Int64(1)\
        \n    TableScan: test projection=None\
        \n  Projection: #test2.a\
        \n    TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// post-right-join predicate on a column common to both sides is only pushed to the right side
    /// i.e. - not duplicated to the left side.
    #[test]
    fn filter_using_right_join_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                &right,
                JoinType::Right,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_eq!(
            format!("{:?}", plan),
            "\
            Filter: #test2.a <= Int64(1)\
            \n  Right Join: Using #test.a = #test2.a\
            \n    TableScan: test projection=None\
            \n    Projection: #test2.a\
            \n      TableScan: test2 projection=None"
        );

        // filter sent to right side of join, not duplicated to the left
        let expected = "\
        Right Join: Using #test.a = #test2.a\
        \n  TableScan: test projection=None\
        \n  Projection: #test2.a\
        \n    Filter: #test2.a <= Int64(1)\
        \n      TableScan: test2 projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    struct PushDownProvider {
        pub filter_support: TableProviderFilterPushDown,
    }

    #[async_trait]
    impl TableProvider for PushDownProvider {
        fn schema(&self) -> SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    "a",
                    arrow::datatypes::DataType::Int32,
                    true,
                ),
                arrow::datatypes::Field::new(
                    "b",
                    arrow::datatypes::DataType::Int32,
                    true,
                ),
            ]))
        }

        async fn scan(
            &self,
            _: &Option<Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn supports_filter_pushdown(
            &self,
            _: &Expr,
        ) -> Result<TableProviderFilterPushDown> {
            Ok(self.filter_support.clone())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn table_scan_with_pushdown_provider(
        filter_support: TableProviderFilterPushDown,
    ) -> Result<LogicalPlan> {
        let test_provider = PushDownProvider { filter_support };

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name: "test".to_string(),
            filters: vec![],
            projected_schema: Arc::new(DFSchema::try_from(
                (*test_provider.schema()).clone(),
            )?),
            projection: None,
            source: provider_as_source(Arc::new(test_provider)),
            limit: None,
        });

        LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1i64)))?
            .build()
    }

    #[test]
    fn filter_with_table_provider_exact() -> Result<()> {
        let plan = table_scan_with_pushdown_provider(TableProviderFilterPushDown::Exact)?;

        let expected = "\
        TableScan: test projection=None, full_filters=[#a = Int64(1)]";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_with_table_provider_inexact() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

        let expected = "\
        Filter: #a = Int64(1)\
        \n  TableScan: test projection=None, partial_filters=[#a = Int64(1)]";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn filter_with_table_provider_multiple_invocations() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

        let optimised_plan = optimize_plan(&plan);

        let expected = "\
        Filter: #a = Int64(1)\
        \n  TableScan: test projection=None, partial_filters=[#a = Int64(1)]";

        // Optimizing the same plan multiple times should produce the same plan
        // each time.
        assert_optimized_plan_eq(&optimised_plan, expected);
        Ok(())
    }

    #[test]
    fn filter_with_table_provider_unsupported() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Unsupported)?;

        let expected = "\
        Filter: #a = Int64(1)\
        \n  TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn multi_combined_filter() -> Result<()> {
        let test_provider = PushDownProvider {
            filter_support: TableProviderFilterPushDown::Inexact,
        };

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name: "test".to_string(),
            filters: vec![col("a").eq(lit(10i64)), col("b").gt(lit(11i64))],
            projected_schema: Arc::new(DFSchema::try_from(
                (*test_provider.schema()).clone(),
            )?),
            projection: Some(vec![0]),
            source: provider_as_source(Arc::new(test_provider)),
            limit: None,
        });

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
            .project(vec![col("a"), col("b")])?
            .build()?;

        let expected ="Projection: #a, #b\
            \n  Filter: #a = Int64(10) AND #b > Int64(11)\
            \n    TableScan: test projection=Some([0]), partial_filters=[#a = Int64(10), #b > Int64(11)]";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
