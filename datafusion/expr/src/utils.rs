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

//! Expression utilities

use crate::expr::{Sort, WindowFunction};
use crate::expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion};
use crate::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use crate::logical_plan::builder::build_join_schema;
use crate::logical_plan::{
    Aggregate, Analyze, CreateMemoryTable, CreateView, Distinct, Extension, Filter, Join,
    Limit, Partitioning, Prepare, Projection, Repartition, Sort as SortPlan, Subquery,
    SubqueryAlias, Union, Values, Window,
};
use crate::{
    BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator,
    TableScan, TryCast,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

///  The value to which `COUNT(*)` is expanded to in
///  `COUNT(<constant>)` expressions
pub const COUNT_STAR_EXPANSION: ScalarValue = ScalarValue::UInt8(Some(1));

/// Recursively walk a list of expression trees, collecting the unique set of columns
/// referenced in the expression
pub fn exprlist_to_columns(expr: &[Expr], accum: &mut HashSet<Column>) -> Result<()> {
    for e in expr {
        expr_to_columns(e, accum)?;
    }
    Ok(())
}

/// Count the number of distinct exprs in a list of group by expressions. If the
/// first element is a `GroupingSet` expression then it must be the only expr.
pub fn grouping_set_expr_count(group_expr: &[Expr]) -> Result<usize> {
    if let Some(Expr::GroupingSet(grouping_set)) = group_expr.first() {
        if group_expr.len() > 1 {
            return Err(DataFusionError::Plan(
                "Invalid group by expressions, GroupingSet must be the only expression"
                    .to_string(),
            ));
        }
        Ok(grouping_set.distinct_expr().len())
    } else {
        Ok(group_expr.len())
    }
}

/// Find all distinct exprs in a list of group by expressions. If the
/// first element is a `GroupingSet` expression then it must be the only expr.
pub fn grouping_set_to_exprlist(group_expr: &[Expr]) -> Result<Vec<Expr>> {
    if let Some(Expr::GroupingSet(grouping_set)) = group_expr.first() {
        if group_expr.len() > 1 {
            return Err(DataFusionError::Plan(
                "Invalid group by expressions, GroupingSet must be the only expression"
                    .to_string(),
            ));
        }
        Ok(grouping_set.distinct_expr())
    } else {
        Ok(group_expr.to_vec())
    }
}

/// Recursively walk an expression tree, collecting the unique set of column names
/// referenced in the expression
struct ColumnNameVisitor<'a> {
    accum: &'a mut HashSet<Column>,
}

impl ExpressionVisitor for ColumnNameVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(qc) => {
                self.accum.insert(qc.clone());
            }
            Expr::ScalarVariable(_, var_names) => {
                self.accum.insert(Column::from_name(var_names.join(".")));
            }
            Expr::Alias(_, _)
            | Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Sort { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarUDF { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::GroupingSet(_)
            | Expr::AggregateUDF { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::GetIndexedField { .. }
            | Expr::Placeholder { .. } => {}
        }
        Ok(Recursion::Continue(self))
    }
}

/// Recursively walk an expression tree, collecting the unique set of columns
/// referenced in the expression
pub fn expr_to_columns(expr: &Expr, accum: &mut HashSet<Column>) -> Result<()> {
    expr.accept(ColumnNameVisitor { accum })?;
    Ok(())
}

/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub fn expand_wildcard(schema: &DFSchema, plan: &LogicalPlan) -> Result<Vec<Expr>> {
    let using_columns = plan.using_columns()?;
    let columns_to_skip = using_columns
        .into_iter()
        // For each USING JOIN condition, only expand to one of each join column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            let mut out_column_names: HashSet<String> = HashSet::new();
            cols.into_iter()
                .filter_map(|c| {
                    if out_column_names.contains(&c.name) {
                        Some(c)
                    } else {
                        out_column_names.insert(c.name);
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>();

    if columns_to_skip.is_empty() {
        Ok(schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect::<Vec<Expr>>())
    } else {
        Ok(schema
            .fields()
            .iter()
            .filter_map(|f| {
                let col = f.qualified_column();
                if !columns_to_skip.contains(&col) {
                    Some(Expr::Column(col))
                } else {
                    None
                }
            })
            .collect::<Vec<Expr>>())
    }
}

/// Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.
pub fn expand_qualified_wildcard(
    qualifier: &str,
    schema: &DFSchema,
) -> Result<Vec<Expr>> {
    let qualified_fields: Vec<DFField> = schema
        .fields_with_qualified(qualifier)
        .into_iter()
        .cloned()
        .collect();
    if qualified_fields.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid qualifier {qualifier}"
        )));
    }
    let qualified_schema =
        DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    // if qualified, allow all columns in output (i.e. ignore using column check)
    Ok(qualified_schema
        .fields()
        .iter()
        .map(|f| Expr::Column(f.qualified_column()))
        .collect::<Vec<Expr>>())
}

/// (expr, "is the SortExpr for window (either comes from PARTITION BY or ORDER BY columns)")
/// if bool is true SortExpr comes from `PARTITION BY` column, if false comes from `ORDER BY` column
type WindowSortKey = Vec<(Expr, bool)>;

/// Generate a sort key for a given window expr's partition_by and order_bu expr
pub fn generate_sort_key(
    partition_by: &[Expr],
    order_by: &[Expr],
) -> Result<WindowSortKey> {
    let normalized_order_by_keys = order_by
        .iter()
        .map(|e| match e {
            Expr::Sort(Sort { expr, .. }) => {
                Ok(Expr::Sort(Sort::new(expr.clone(), true, false)))
            }
            _ => Err(DataFusionError::Plan(
                "Order by only accepts sort expressions".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;

    let mut final_sort_keys = vec![];
    let mut is_partition_flag = vec![];
    partition_by.iter().for_each(|e| {
        // By default, create sort key with ASC is true and NULLS LAST to be consistent with
        // PostgreSQL's rule: https://www.postgresql.org/docs/current/queries-order.html
        let e = e.clone().sort(true, false);
        if let Some(pos) = normalized_order_by_keys.iter().position(|key| key.eq(&e)) {
            let order_by_key = &order_by[pos];
            if !final_sort_keys.contains(order_by_key) {
                final_sort_keys.push(order_by_key.clone());
                is_partition_flag.push(true);
            }
        } else if !final_sort_keys.contains(&e) {
            final_sort_keys.push(e);
            is_partition_flag.push(true);
        }
    });

    order_by.iter().for_each(|e| {
        if !final_sort_keys.contains(e) {
            final_sort_keys.push(e.clone());
            is_partition_flag.push(false);
        }
    });
    let res = final_sort_keys
        .into_iter()
        .zip(is_partition_flag)
        .map(|(lhs, rhs)| (lhs, rhs))
        .collect::<Vec<_>>();
    Ok(res)
}

/// Compare the sort expr as PostgreSQL's common_prefix_cmp():
/// <https://github.com/postgres/postgres/blob/master/src/backend/optimizer/plan/planner.c>
pub fn compare_sort_expr(
    sort_expr_a: &Expr,
    sort_expr_b: &Expr,
    schema: &DFSchemaRef,
) -> Ordering {
    match (sort_expr_a, sort_expr_b) {
        (
            Expr::Sort(Sort {
                expr: expr_a,
                asc: asc_a,
                nulls_first: nulls_first_a,
            }),
            Expr::Sort(Sort {
                expr: expr_b,
                asc: asc_b,
                nulls_first: nulls_first_b,
            }),
        ) => {
            let ref_indexes_a = find_column_indexes_referenced_by_expr(expr_a, schema);
            let ref_indexes_b = find_column_indexes_referenced_by_expr(expr_b, schema);
            for (idx_a, idx_b) in ref_indexes_a.iter().zip(ref_indexes_b.iter()) {
                match idx_a.cmp(idx_b) {
                    Ordering::Less => {
                        return Ordering::Less;
                    }
                    Ordering::Greater => {
                        return Ordering::Greater;
                    }
                    Ordering::Equal => {}
                }
            }
            match ref_indexes_a.len().cmp(&ref_indexes_b.len()) {
                Ordering::Less => return Ordering::Greater,
                Ordering::Greater => {
                    return Ordering::Less;
                }
                Ordering::Equal => {}
            }
            match (asc_a, asc_b) {
                (true, false) => {
                    return Ordering::Greater;
                }
                (false, true) => {
                    return Ordering::Less;
                }
                _ => {}
            }
            match (nulls_first_a, nulls_first_b) {
                (true, false) => {
                    return Ordering::Less;
                }
                (false, true) => {
                    return Ordering::Greater;
                }
                _ => {}
            }
            Ordering::Equal
        }
        _ => Ordering::Equal,
    }
}

/// group a slice of window expression expr by their order by expressions
pub fn group_window_expr_by_sort_keys(
    window_expr: &[Expr],
) -> Result<Vec<(WindowSortKey, Vec<&Expr>)>> {
    let mut result = vec![];
    window_expr.iter().try_for_each(|expr| match expr {
        Expr::WindowFunction(WindowFunction{ partition_by, order_by, .. }) => {
            let sort_key = generate_sort_key(partition_by, order_by)?;
            if let Some((_, values)) = result.iter_mut().find(
                |group: &&mut (WindowSortKey, Vec<&Expr>)| matches!(group, (key, _) if *key == sort_key),
            ) {
                values.push(expr);
            } else {
                result.push((sort_key, vec![expr]))
            }
            Ok(())
        }
        other => Err(DataFusionError::Internal(format!(
            "Impossibly got non-window expr {other:?}",
        ))),
    })?;
    Ok(result)
}

/// Collect all deeply nested `Expr::AggregateFunction` and
/// `Expr::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::AggregateFunction { .. } | Expr::AggregateUDF { .. }
        )
    })
}

/// Collect all deeply nested `Expr::Sort`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub fn find_sort_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::Sort { .. })
    })
}

/// Collect all deeply nested `Expr::WindowFunction`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub fn find_window_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::WindowFunction { .. })
    })
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");
    exprs
}

// Visitor that find expressions that match a particular predicate
struct Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    test_fn: &'a F,
    exprs: Vec<Expr>,
}

impl<'a, F> Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        if (self.test_fn)(expr) {
            if !(self.exprs.contains(expr)) {
                self.exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

/// Returns a new logical plan based on the original one with inputs
/// and expressions replaced.
///
/// The exprs correspond to the same order of expressions returned by
/// `LogicalPlan::expressions`. This function is used in optimizers in
/// the following way:
///
/// ```text
/// let new_inputs = optimize_children(..., plan, props);
///
/// // get the plans expressions to optimize
/// let exprs = plan.expressions();
///
/// // potentially rewrite plan expressions
/// let rewritten_exprs = rewrite_exprs(exprs);
///
/// // create new plan using rewritten_exprs in same position
/// let new_plan = from_plan(&plan, rewritten_exprs, new_inputs);
/// ```
pub fn from_plan(
    plan: &LogicalPlan,
    expr: &[Expr],
    inputs: &[LogicalPlan],
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(Projection { schema, .. }) => {
            Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                expr.to_vec(),
                Arc::new(inputs[0].clone()),
                schema.clone(),
            )?))
        }
        LogicalPlan::Values(Values { schema, .. }) => Ok(LogicalPlan::Values(Values {
            schema: schema.clone(),
            values: expr
                .chunks_exact(schema.fields().len())
                .map(|s| s.to_vec())
                .collect::<Vec<_>>(),
        })),
        LogicalPlan::Filter { .. } => {
            assert_eq!(1, expr.len());
            let predicate = expr[0].clone();

            // filter predicates should not contain aliased expressions so we remove any aliases
            // before this logic was added we would have aliases within filters such as for
            // benchmark q6:
            //
            // lineitem.l_shipdate >= Date32(\"8766\")
            // AND lineitem.l_shipdate < Date32(\"9131\")
            // AND CAST(lineitem.l_discount AS Decimal128(30, 15)) AS lineitem.l_discount >=
            // Decimal128(Some(49999999999999),30,15)
            // AND CAST(lineitem.l_discount AS Decimal128(30, 15)) AS lineitem.l_discount <=
            // Decimal128(Some(69999999999999),30,15)
            // AND lineitem.l_quantity < Decimal128(Some(2400),15,2)

            struct RemoveAliases {}

            impl ExprRewriter for RemoveAliases {
                fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
                    match expr {
                        Expr::Exists { .. }
                        | Expr::ScalarSubquery(_)
                        | Expr::InSubquery { .. } => {
                            // subqueries could contain aliases so we don't recurse into those
                            Ok(RewriteRecursion::Stop)
                        }
                        Expr::Alias(_, _) => Ok(RewriteRecursion::Mutate),
                        _ => Ok(RewriteRecursion::Continue),
                    }
                }

                fn mutate(&mut self, expr: Expr) -> Result<Expr> {
                    Ok(expr.unalias())
                }
            }

            let mut remove_aliases = RemoveAliases {};
            let predicate = predicate.rewrite(&mut remove_aliases)?;

            Ok(LogicalPlan::Filter(Filter::try_new(
                predicate,
                Arc::new(inputs[0].clone()),
            )?))
        }
        LogicalPlan::Repartition(Repartition {
            partitioning_scheme,
            ..
        }) => match partitioning_scheme {
            Partitioning::RoundRobinBatch(n) => {
                Ok(LogicalPlan::Repartition(Repartition {
                    partitioning_scheme: Partitioning::RoundRobinBatch(*n),
                    input: Arc::new(inputs[0].clone()),
                }))
            }
            Partitioning::Hash(_, n) => Ok(LogicalPlan::Repartition(Repartition {
                partitioning_scheme: Partitioning::Hash(expr.to_owned(), *n),
                input: Arc::new(inputs[0].clone()),
            })),
            Partitioning::DistributeBy(_) => Ok(LogicalPlan::Repartition(Repartition {
                partitioning_scheme: Partitioning::DistributeBy(expr.to_owned()),
                input: Arc::new(inputs[0].clone()),
            })),
        },
        LogicalPlan::Window(Window {
            window_expr,
            schema,
            ..
        }) => Ok(LogicalPlan::Window(Window {
            input: Arc::new(inputs[0].clone()),
            window_expr: expr[0..window_expr.len()].to_vec(),
            schema: schema.clone(),
        })),
        LogicalPlan::Aggregate(Aggregate {
            group_expr, schema, ..
        }) => Ok(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
            Arc::new(inputs[0].clone()),
            expr[0..group_expr.len()].to_vec(),
            expr[group_expr.len()..].to_vec(),
            schema.clone(),
        )?)),
        LogicalPlan::Sort(SortPlan { fetch, .. }) => Ok(LogicalPlan::Sort(SortPlan {
            expr: expr.to_vec(),
            input: Arc::new(inputs[0].clone()),
            fetch: *fetch,
        })),
        LogicalPlan::Join(Join {
            join_type,
            join_constraint,
            on,
            null_equals_null,
            ..
        }) => {
            let schema =
                build_join_schema(inputs[0].schema(), inputs[1].schema(), join_type)?;

            let equi_expr_count = on.len();
            assert!(expr.len() >= equi_expr_count);

            // The preceding part of expr is equi-exprs,
            // and the struct of each equi-expr is like `left-expr = right-expr`.
            let new_on:Vec<(Expr,Expr)> = expr.iter().take(equi_expr_count).map(|equi_expr| {
                    // SimplifyExpression rule may add alias to the equi_expr.
                    let unalias_expr = equi_expr.clone().unalias();
                    if let Expr::BinaryExpr(BinaryExpr { left, op:Operator::Eq, right }) = unalias_expr {
                        Ok((*left, *right))
                    } else {
                        Err(DataFusionError::Internal(format!(
                            "The front part expressions should be an binary equiality expression, actual:{equi_expr}"
                        )))
                    }
                }).collect::<Result<Vec<(Expr, Expr)>>>()?;

            // Assume that the last expr, if any,
            // is the filter_expr (non equality predicate from ON clause)
            let filter_expr =
                (expr.len() > equi_expr_count).then(|| expr[expr.len() - 1].clone());

            Ok(LogicalPlan::Join(Join {
                left: Arc::new(inputs[0].clone()),
                right: Arc::new(inputs[1].clone()),
                join_type: *join_type,
                join_constraint: *join_constraint,
                on: new_on,
                filter: filter_expr,
                schema: DFSchemaRef::new(schema),
                null_equals_null: *null_equals_null,
            }))
        }
        LogicalPlan::CrossJoin(_) => {
            let left = inputs[0].clone();
            let right = inputs[1].clone();
            LogicalPlanBuilder::from(left).cross_join(right)?.build()
        }
        LogicalPlan::Subquery(_) => {
            let subquery = LogicalPlanBuilder::from(inputs[0].clone()).build()?;
            Ok(LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(subquery),
            }))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
            let schema = inputs[0].schema().as_ref().clone().into();
            let schema =
                DFSchemaRef::new(DFSchema::try_from_qualified_schema(alias, &schema)?);
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias {
                alias: alias.clone(),
                input: Arc::new(inputs[0].clone()),
                schema,
            }))
        }
        LogicalPlan::Limit(Limit { skip, fetch, .. }) => Ok(LogicalPlan::Limit(Limit {
            skip: *skip,
            fetch: *fetch,
            input: Arc::new(inputs[0].clone()),
        })),
        LogicalPlan::CreateMemoryTable(CreateMemoryTable {
            name,
            if_not_exists,
            or_replace,
            ..
        }) => Ok(LogicalPlan::CreateMemoryTable(CreateMemoryTable {
            input: Arc::new(inputs[0].clone()),
            name: name.clone(),
            if_not_exists: *if_not_exists,
            or_replace: *or_replace,
        })),
        LogicalPlan::CreateView(CreateView {
            name,
            or_replace,
            definition,
            ..
        }) => Ok(LogicalPlan::CreateView(CreateView {
            input: Arc::new(inputs[0].clone()),
            name: name.clone(),
            or_replace: *or_replace,
            definition: definition.clone(),
        })),
        LogicalPlan::Extension(e) => Ok(LogicalPlan::Extension(Extension {
            node: e.node.from_template(expr, inputs),
        })),
        LogicalPlan::Union(Union { schema, .. }) => Ok(LogicalPlan::Union(Union {
            inputs: inputs.iter().cloned().map(Arc::new).collect(),
            schema: schema.clone(),
        })),
        LogicalPlan::Distinct(Distinct { .. }) => Ok(LogicalPlan::Distinct(Distinct {
            input: Arc::new(inputs[0].clone()),
        })),
        LogicalPlan::Analyze(a) => {
            assert!(expr.is_empty());
            assert_eq!(inputs.len(), 1);
            Ok(LogicalPlan::Analyze(Analyze {
                verbose: a.verbose,
                schema: a.schema.clone(),
                input: Arc::new(inputs[0].clone()),
            }))
        }
        LogicalPlan::Explain(_) => {
            // Explain should be handled specially in the optimizers;
            // If this check cannot pass it means some optimizer pass is
            // trying to optimize Explain directly
            if expr.is_empty() {
                return Err(DataFusionError::Plan(
                    "Invalid EXPLAIN command. Expression is empty".to_string(),
                ));
            }

            if inputs.is_empty() {
                return Err(DataFusionError::Plan(
                    "Invalid EXPLAIN command. Inputs are empty".to_string(),
                ));
            }

            Ok(plan.clone())
        }
        LogicalPlan::Prepare(Prepare {
            name, data_types, ..
        }) => Ok(LogicalPlan::Prepare(Prepare {
            name: name.clone(),
            data_types: data_types.clone(),
            input: Arc::new(inputs[0].clone()),
        })),
        LogicalPlan::TableScan(ts) => {
            assert!(inputs.is_empty(), "{plan:?}  should have no inputs");
            Ok(LogicalPlan::TableScan(TableScan {
                filters: expr.to_vec(),
                ..ts.clone()
            }))
        }
        LogicalPlan::EmptyRelation(_)
        | LogicalPlan::CreateExternalTable(_)
        | LogicalPlan::DropTable(_)
        | LogicalPlan::DropView(_)
        | LogicalPlan::SetVariable(_)
        | LogicalPlan::CreateCatalogSchema(_)
        | LogicalPlan::CreateCatalog(_) => {
            // All of these plan types have no inputs / exprs so should not be called
            assert!(expr.is_empty(), "{plan:?} should have no exprs");
            assert!(inputs.is_empty(), "{plan:?}  should have no inputs");
            Ok(plan.clone())
        }
    }
}

/// Find all columns referenced from an aggregate query
fn agg_cols(agg: &Aggregate) -> Result<Vec<Column>> {
    Ok(agg
        .aggr_expr
        .iter()
        .chain(&agg.group_expr)
        .flat_map(find_columns_referenced_by_expr)
        .collect())
}

fn exprlist_to_fields_aggregate(
    exprs: &[Expr],
    plan: &LogicalPlan,
    agg: &Aggregate,
) -> Result<Vec<DFField>> {
    let agg_cols = agg_cols(agg)?;
    let mut fields = vec![];
    for expr in exprs {
        match expr {
            Expr::Column(c) if agg_cols.iter().any(|x| x == c) => {
                // resolve against schema of input to aggregate
                fields.push(expr.to_field(agg.input.schema())?);
            }
            _ => fields.push(expr.to_field(plan.schema())?),
        }
    }
    Ok(fields)
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields<'a>(
    expr: impl IntoIterator<Item = &'a Expr>,
    plan: &LogicalPlan,
) -> Result<Vec<DFField>> {
    let exprs: Vec<Expr> = expr.into_iter().cloned().collect();
    // when dealing with aggregate plans we cannot simply look in the aggregate output schema
    // because it will contain columns representing complex expressions (such a column named
    // `GROUPING(person.state)` so in order to resolve `person.state` in this case we need to
    // look at the input to the aggregate instead.
    let fields = match plan {
        LogicalPlan::Aggregate(agg) => {
            Some(exprlist_to_fields_aggregate(&exprs, plan, agg))
        }
        LogicalPlan::Window(window) => match window.input.as_ref() {
            LogicalPlan::Aggregate(agg) => {
                Some(exprlist_to_fields_aggregate(&exprs, plan, agg))
            }
            _ => None,
        },
        _ => None,
    };
    if let Some(fields) = fields {
        fields
    } else {
        // look for exact match in plan's output schema
        let input_schema = &plan.schema();
        exprs.iter().map(|e| e.to_field(input_schema)).collect()
    }
}

/// Convert an expression into Column expression if it's already provided as input plan.
///
/// For example, it rewrites:
///
/// ```text
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
/// .project(vec![col("c1"), sum(col("c2"))?
/// ```
///
/// Into:
///
/// ```text
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
/// .project(vec![col("c1"), col("SUM(c2)")?
/// ```
pub fn columnize_expr(e: Expr, input_schema: &DFSchema) -> Expr {
    match e {
        Expr::Column(_) => e,
        Expr::Alias(inner_expr, name) => {
            columnize_expr(*inner_expr, input_schema).alias(name)
        }
        Expr::Cast(Cast { expr, data_type }) => Expr::Cast(Cast {
            expr: Box::new(columnize_expr(*expr, input_schema)),
            data_type,
        }),
        Expr::TryCast(TryCast { expr, data_type }) => Expr::TryCast(TryCast::new(
            Box::new(columnize_expr(*expr, input_schema)),
            data_type,
        )),
        Expr::ScalarSubquery(_) => e.clone(),
        _ => match e.display_name() {
            Ok(name) => match input_schema.field_with_unqualified_name(&name) {
                Ok(field) => Expr::Column(field.qualified_column()),
                // expression not provided as input, do not convert to a column reference
                Err(_) => e,
            },
            Err(_) => e,
        },
    }
}

/// Collect all deeply nested `Expr::Column`'s. They are returned in order of
/// appearance (depth first), and may contain duplicates.
pub fn find_column_exprs(exprs: &[Expr]) -> Vec<Expr> {
    exprs
        .iter()
        .flat_map(find_columns_referenced_by_expr)
        .map(Expr::Column)
        .collect()
}

/// Recursively find all columns referenced by an expression
#[derive(Debug, Default)]
struct ColumnCollector {
    exprs: Vec<Column>,
}

impl ExpressionVisitor for ColumnCollector {
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        if let Expr::Column(c) = expr {
            self.exprs.push(c.clone())
        }
        Ok(Recursion::Continue(self))
    }
}

pub(crate) fn find_columns_referenced_by_expr(e: &Expr) -> Vec<Column> {
    // As the `ExpressionVisitor` impl above always returns Ok, this
    // "can't" error
    let ColumnCollector { exprs } = e
        .accept(ColumnCollector::default())
        .expect("Unexpected error");
    exprs
}

/// Convert any `Expr` to an `Expr::Column`.
pub fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    match expr {
        Expr::Column(col) => {
            let field = plan.schema().field_from_column(col)?;
            Ok(Expr::Column(field.qualified_column()))
        }
        _ => Ok(Expr::Column(Column::from_name(expr.display_name()?))),
    }
}

/// Recursively walk an expression tree, collecting the column indexes
/// referenced in the expression
struct ColumnIndexesCollector<'a> {
    schema: &'a DFSchemaRef,
    indexes: Vec<usize>,
}

impl ExpressionVisitor for ColumnIndexesCollector<'_> {
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor,
    {
        match expr {
            Expr::Column(qc) => {
                if let Ok(idx) = self.schema.index_of_column(qc) {
                    self.indexes.push(idx);
                }
            }
            Expr::Literal(_) => {
                self.indexes.push(std::usize::MAX);
            }
            _ => {}
        }
        Ok(Recursion::Continue(self))
    }
}

pub(crate) fn find_column_indexes_referenced_by_expr(
    e: &Expr,
    schema: &DFSchemaRef,
) -> Vec<usize> {
    // As the `ExpressionVisitor` impl above always returns Ok, this
    // "can't" error
    let ColumnIndexesCollector { indexes, .. } = e
        .accept(ColumnIndexesCollector {
            schema,
            indexes: vec![],
        })
        .expect("Unexpected error");
    indexes
}

/// can this data type be used in hash join equal conditions??
/// data types here come from function 'equal_rows', if more data types are supported
/// in equal_rows(hash join), add those data types here to generate join logical plan.
pub fn can_hash(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::Boolean => true,
        DataType::Int8 => true,
        DataType::Int16 => true,
        DataType::Int32 => true,
        DataType::Int64 => true,
        DataType::UInt8 => true,
        DataType::UInt16 => true,
        DataType::UInt32 => true,
        DataType::UInt64 => true,
        DataType::Float32 => true,
        DataType::Float64 => true,
        DataType::Timestamp(time_unit, None) => match time_unit {
            TimeUnit::Second => true,
            TimeUnit::Millisecond => true,
            TimeUnit::Microsecond => true,
            TimeUnit::Nanosecond => true,
        },
        DataType::Utf8 => true,
        DataType::LargeUtf8 => true,
        DataType::Decimal128(_, _) => true,
        DataType::Date32 => true,
        DataType::Date64 => true,
        DataType::Dictionary(key_type, value_type)
            if *value_type.as_ref() == DataType::Utf8 =>
        {
            DataType::is_dictionary_key_type(key_type)
        }
        _ => false,
    }
}

/// Check whether all columns are from the schema.
fn check_all_column_from_schema(columns: &HashSet<Column>, schema: DFSchemaRef) -> bool {
    columns
        .iter()
        .all(|column| schema.index_of_column(column).is_ok())
}

/// Check whether all columns are from the schemas.
pub fn is_valid_join_predicate(expr: &Expr, schemas: &[DFSchemaRef]) -> Result<bool> {
    let cols = expr.to_columns()?;
    let is_valid = cols.iter().all(|col| {
        schemas
            .iter()
            .any(|schema| schema.index_of_column(col).is_ok())
    });

    Ok(is_valid)
}

/// Give two sides of the equijoin predicate, return a valid join key pair.
/// If there is no valid join key pair, return None.
///
/// A valid join means:
/// 1. All referenced column of the left side is from the left schema, and
///    all referenced column of the right side is from the right schema.
/// 2. Or opposite. All referenced column of the left side is from the right schema,
///    and the right side is from the left schema.
///
pub fn find_valid_equijoin_key_pair(
    left_key: &Expr,
    right_key: &Expr,
    left_schema: DFSchemaRef,
    right_schema: DFSchemaRef,
) -> Result<Option<(Expr, Expr)>> {
    let left_using_columns = left_key.to_columns()?;
    let right_using_columns = right_key.to_columns()?;

    // Conditions like a = 10, will be added to non-equijoin.
    if left_using_columns.is_empty() || right_using_columns.is_empty() {
        return Ok(None);
    }

    let l_is_left =
        check_all_column_from_schema(&left_using_columns, left_schema.clone());
    let r_is_right =
        check_all_column_from_schema(&right_using_columns, right_schema.clone());

    let r_is_left_and_l_is_right = || {
        check_all_column_from_schema(&right_using_columns, left_schema.clone())
            && check_all_column_from_schema(&left_using_columns, right_schema.clone())
    };

    let join_key_pair = match (l_is_left, r_is_right) {
        (true, true) => Some((left_key.clone(), right_key.clone())),
        (_, _) if r_is_left_and_l_is_right() => {
            Some((right_key.clone(), left_key.clone()))
        }
        _ => None,
    };

    Ok(join_key_pair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, expr, AggregateFunction, WindowFrame, WindowFunction};

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_case() -> Result<()> {
        let result = group_window_expr_by_sort_keys(&[])?;
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_window() -> Result<()> {
        let max1 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("name")],
            vec![],
            vec![],
            WindowFrame::new(false),
        ));
        let max2 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("name")],
            vec![],
            vec![],
            WindowFrame::new(false),
        ));
        let min3 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Min),
            vec![col("name")],
            vec![],
            vec![],
            WindowFrame::new(false),
        ));
        let sum4 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Sum),
            vec![col("age")],
            vec![],
            vec![],
            WindowFrame::new(false),
        ));
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;
        let key = vec![];
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> =
            vec![(key, vec![&max1, &max2, &min3, &sum4])];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys() -> Result<()> {
        let age_asc = Expr::Sort(expr::Sort::new(Box::new(col("age")), true, true));
        let name_desc = Expr::Sort(expr::Sort::new(Box::new(col("name")), false, true));
        let created_at_desc =
            Expr::Sort(expr::Sort::new(Box::new(col("created_at")), false, true));
        let max1 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("name")],
            vec![],
            vec![age_asc.clone(), name_desc.clone()],
            WindowFrame::new(true),
        ));
        let max2 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("name")],
            vec![],
            vec![],
            WindowFrame::new(false),
        ));
        let min3 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Min),
            vec![col("name")],
            vec![],
            vec![age_asc.clone(), name_desc.clone()],
            WindowFrame::new(true),
        ));
        let sum4 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Sum),
            vec![col("age")],
            vec![],
            vec![name_desc.clone(), age_asc.clone(), created_at_desc.clone()],
            WindowFrame::new(true),
        ));
        // FIXME use as_ref
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;

        let key1 = vec![(age_asc.clone(), false), (name_desc.clone(), false)];
        let key2 = vec![];
        let key3 = vec![
            (name_desc, false),
            (age_asc, false),
            (created_at_desc, false),
        ];

        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![
            (key1, vec![&max1, &min3]),
            (key2, vec![&max2]),
            (key3, vec![&sum4]),
        ];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_find_sort_exprs() -> Result<()> {
        let exprs = &[
            Expr::WindowFunction(expr::WindowFunction::new(
                WindowFunction::AggregateFunction(AggregateFunction::Max),
                vec![col("name")],
                vec![],
                vec![
                    Expr::Sort(expr::Sort::new(Box::new(col("age")), true, true)),
                    Expr::Sort(expr::Sort::new(Box::new(col("name")), false, true)),
                ],
                WindowFrame::new(true),
            )),
            Expr::WindowFunction(expr::WindowFunction::new(
                WindowFunction::AggregateFunction(AggregateFunction::Sum),
                vec![col("age")],
                vec![],
                vec![
                    Expr::Sort(expr::Sort::new(Box::new(col("name")), false, true)),
                    Expr::Sort(expr::Sort::new(Box::new(col("age")), true, true)),
                    Expr::Sort(expr::Sort::new(Box::new(col("created_at")), false, true)),
                ],
                WindowFrame::new(true),
            )),
        ];
        let expected = vec![
            Expr::Sort(expr::Sort::new(Box::new(col("age")), true, true)),
            Expr::Sort(expr::Sort::new(Box::new(col("name")), false, true)),
            Expr::Sort(expr::Sort::new(Box::new(col("created_at")), false, true)),
        ];
        let result = find_sort_exprs(exprs);
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn avoid_generate_duplicate_sort_keys() -> Result<()> {
        let asc_or_desc = [true, false];
        let nulls_first_or_last = [true, false];
        let partition_by = &[col("age"), col("name"), col("created_at")];
        for asc_ in asc_or_desc {
            for nulls_first_ in nulls_first_or_last {
                let order_by = &[
                    Expr::Sort(Sort {
                        expr: Box::new(col("age")),
                        asc: asc_,
                        nulls_first: nulls_first_,
                    }),
                    Expr::Sort(Sort {
                        expr: Box::new(col("name")),
                        asc: asc_,
                        nulls_first: nulls_first_,
                    }),
                ];

                let expected = vec![
                    (
                        Expr::Sort(Sort {
                            expr: Box::new(col("age")),
                            asc: asc_,
                            nulls_first: nulls_first_,
                        }),
                        true,
                    ),
                    (
                        Expr::Sort(Sort {
                            expr: Box::new(col("name")),
                            asc: asc_,
                            nulls_first: nulls_first_,
                        }),
                        true,
                    ),
                    (
                        Expr::Sort(Sort {
                            expr: Box::new(col("created_at")),
                            asc: true,
                            nulls_first: false,
                        }),
                        true,
                    ),
                ];
                let result = generate_sort_key(partition_by, order_by)?;
                assert_eq!(expected, result);
            }
        }
        Ok(())
    }
}
