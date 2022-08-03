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

use crate::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use crate::logical_plan::builder::build_join_schema;
use crate::logical_plan::{
    Aggregate, Analyze, CreateMemoryTable, CreateView, Distinct, Extension, Filter, Join,
    Limit, Partitioning, Projection, Repartition, Sort, Subquery, SubqueryAlias, Union,
    Values, Window,
};
use crate::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
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
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
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
            | Expr::GetIndexedField { .. } => {}
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
        // For each USING JOIN condition, only expand to one column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            cols.into_iter().skip(1)
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
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let qualified_fields: Vec<DFField> = schema
        .fields_with_qualified(qualifier)
        .into_iter()
        .cloned()
        .collect();
    if qualified_fields.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid qualifier {}",
            qualifier
        )));
    }
    let qualifier_schema =
        DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    expand_wildcard(&qualifier_schema, plan)
}

type WindowSortKey = Vec<Expr>;

/// Generate a sort key for a given window expr's partition_by and order_bu expr
pub fn generate_sort_key(partition_by: &[Expr], order_by: &[Expr]) -> WindowSortKey {
    let mut sort_key = vec![];
    partition_by.iter().for_each(|e| {
        let e = e.clone().sort(true, true);
        if !sort_key.contains(&e) {
            sort_key.push(e);
        }
    });
    order_by.iter().for_each(|e| {
        if !sort_key.contains(e) {
            sort_key.push(e.clone());
        }
    });
    sort_key
}

/// group a slice of window expression expr by their order by expressions
pub fn group_window_expr_by_sort_keys(
    window_expr: &[Expr],
) -> Result<Vec<(WindowSortKey, Vec<&Expr>)>> {
    let mut result = vec![];
    window_expr.iter().try_for_each(|expr| match expr {
        Expr::WindowFunction { partition_by, order_by, .. } => {
            let sort_key = generate_sort_key(partition_by, order_by);
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
            "Impossibly got non-window expr {:?}",
            other,
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
        LogicalPlan::Projection(Projection { schema, alias, .. }) => {
            Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                expr.to_vec(),
                Arc::new(inputs[0].clone()),
                schema.clone(),
                alias.clone(),
            )?))
        }
        LogicalPlan::Values(Values { schema, .. }) => Ok(LogicalPlan::Values(Values {
            schema: schema.clone(),
            values: expr
                .chunks_exact(schema.fields().len())
                .map(|s| s.to_vec())
                .collect::<Vec<_>>(),
        })),
        LogicalPlan::Filter { .. } => Ok(LogicalPlan::Filter(Filter {
            predicate: expr[0].clone(),
            input: Arc::new(inputs[0].clone()),
        })),
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
        }) => Ok(LogicalPlan::Aggregate(Aggregate {
            group_expr: expr[0..group_expr.len()].to_vec(),
            aggr_expr: expr[group_expr.len()..].to_vec(),
            input: Arc::new(inputs[0].clone()),
            schema: schema.clone(),
        })),
        LogicalPlan::Sort(Sort { .. }) => Ok(LogicalPlan::Sort(Sort {
            expr: expr.to_vec(),
            input: Arc::new(inputs[0].clone()),
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
            // Assume that the last expr, if any,
            // is the filter_expr (non equality predicate from ON clause)
            let filter_expr = if on.len() * 2 == expr.len() {
                None
            } else {
                Some(expr[expr.len() - 1].clone())
            };

            Ok(LogicalPlan::Join(Join {
                left: Arc::new(inputs[0].clone()),
                right: Arc::new(inputs[1].clone()),
                join_type: *join_type,
                join_constraint: *join_constraint,
                on: on.clone(),
                filter: filter_expr,
                schema: DFSchemaRef::new(schema),
                null_equals_null: *null_equals_null,
            }))
        }
        LogicalPlan::CrossJoin(_) => {
            let left = inputs[0].clone();
            let right = &inputs[1];
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
        LogicalPlan::Union(Union { schema, alias, .. }) => {
            Ok(LogicalPlan::Union(Union {
                inputs: inputs.iter().cloned().map(Arc::new).collect(),
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
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
            // If this assert fails it means some optimizer pass is
            // trying to optimize Explain directly
            assert!(
                expr.is_empty(),
                "Explain can not be created from utils::from_expr"
            );
            assert!(
                inputs.is_empty(),
                "Explain can not be created from utils::from_expr"
            );
            Ok(plan.clone())
        }
        LogicalPlan::EmptyRelation(_)
        | LogicalPlan::TableScan { .. }
        | LogicalPlan::CreateExternalTable(_)
        | LogicalPlan::DropTable(_)
        | LogicalPlan::CreateCatalogSchema(_)
        | LogicalPlan::CreateCatalog(_) => {
            // All of these plan types have no inputs / exprs so should not be called
            assert!(expr.is_empty(), "{:?} should have no exprs", plan);
            assert!(inputs.is_empty(), "{:?}  should have no inputs", plan);
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
    // `#GROUPING(person.state)` so in order to resolve `person.state` in this case we need to
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
/// .project(vec![col("c1"), col("SUM(#c2)")?
/// ```
pub fn columnize_expr(e: Expr, input_schema: &DFSchema) -> Expr {
    match e {
        Expr::Column(_) => e,
        Expr::Alias(inner_expr, name) => {
            Expr::Alias(Box::new(columnize_expr(*inner_expr, input_schema)), name)
        }
        Expr::ScalarSubquery(_) => e.clone(),
        _ => match e.name(input_schema) {
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
        _ => {
            // we should not be trying to create a name for the expression
            // based on the input schema but this is the current behavior
            // see https://github.com/apache/arrow-datafusion/issues/2456
            Ok(Expr::Column(Column::from_name(expr.name(plan.schema())?)))
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, AggregateFunction, WindowFunction};

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_case() -> Result<()> {
        let result = group_window_expr_by_sort_keys(&[])?;
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_window() -> Result<()> {
        let max1 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let max2 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let min3 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Min),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let sum4 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
            args: vec![col("age")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
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
        let age_asc = Expr::Sort {
            expr: Box::new(col("age")),
            asc: true,
            nulls_first: true,
        };
        let name_desc = Expr::Sort {
            expr: Box::new(col("name")),
            asc: false,
            nulls_first: true,
        };
        let created_at_desc = Expr::Sort {
            expr: Box::new(col("created_at")),
            asc: false,
            nulls_first: true,
        };
        let max1 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![age_asc.clone(), name_desc.clone()],
            window_frame: None,
        };
        let max2 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let min3 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Min),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![age_asc.clone(), name_desc.clone()],
            window_frame: None,
        };
        let sum4 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
            args: vec![col("age")],
            partition_by: vec![],
            order_by: vec![name_desc.clone(), age_asc.clone(), created_at_desc.clone()],
            window_frame: None,
        };
        // FIXME use as_ref
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;

        let key1 = vec![age_asc.clone(), name_desc.clone()];
        let key2 = vec![];
        let key3 = vec![name_desc, age_asc, created_at_desc];

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
            Expr::WindowFunction {
                fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
                args: vec![col("name")],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort {
                        expr: Box::new(col("age")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("name")),
                        asc: false,
                        nulls_first: true,
                    },
                ],
                window_frame: None,
            },
            Expr::WindowFunction {
                fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
                args: vec![col("age")],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort {
                        expr: Box::new(col("name")),
                        asc: false,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("age")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("created_at")),
                        asc: false,
                        nulls_first: true,
                    },
                ],
                window_frame: None,
            },
        ];
        let expected = vec![
            Expr::Sort {
                expr: Box::new(col("age")),
                asc: true,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("name")),
                asc: false,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("created_at")),
                asc: false,
                nulls_first: true,
            },
        ];
        let result = find_sort_exprs(exprs);
        assert_eq!(expected, result);
        Ok(())
    }
}
