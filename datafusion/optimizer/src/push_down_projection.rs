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

//! Projection Push Down optimizer rule ensures that only referenced columns are
//! loaded into memory

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::eliminate_project::can_eliminate;
use crate::merge_projection::merge_projection;
use crate::optimizer::ApplyOrder;
use crate::push_down_filter::replace_cols_by_name;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::error::Result as ArrowResult;
use datafusion_common::ScalarValue::UInt8;
use datafusion_common::{
    plan_err, Column, DFSchema, DFSchemaRef, DataFusionError, Result,
};
use datafusion_expr::expr::{AggregateFunction, Alias};
use datafusion_expr::{
    logical_plan::{Aggregate, LogicalPlan, Projection, TableScan, Union},
    utils::{expr_to_columns, exprlist_to_columns, exprlist_to_fields},
    Expr, LogicalPlanBuilder, SubqueryAlias,
};

// if projection is empty return projection-new_plan, else return new_plan.
#[macro_export]
macro_rules! generate_plan {
    ($projection_is_empty:expr, $plan:expr, $new_plan:expr) => {
        if $projection_is_empty {
            $new_plan
        } else {
            $plan.with_new_inputs(&[$new_plan])?
        }
    };
}

/// Optimizer that removes unused projections and aggregations from plans
/// This reduces both scans and
#[derive(Default)]
pub struct PushDownProjection {}

impl OptimizerRule for PushDownProjection {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let projection = match plan {
            LogicalPlan::Projection(projection) => projection,
            LogicalPlan::Aggregate(agg) => {
                let mut required_columns = HashSet::new();
                for e in agg.aggr_expr.iter().chain(agg.group_expr.iter()) {
                    expr_to_columns(e, &mut required_columns)?
                }
                let new_expr = get_expr(&required_columns, agg.input.schema())?;
                let projection = LogicalPlan::Projection(Projection::try_new(
                    new_expr,
                    agg.input.clone(),
                )?);
                let optimized_child = self
                    .try_optimize(&projection, _config)?
                    .unwrap_or(projection);
                return Ok(Some(plan.with_new_inputs(&[optimized_child])?));
            }
            LogicalPlan::TableScan(scan) if scan.projection.is_none() => {
                return Ok(Some(push_down_scan(&HashSet::new(), scan, false)?));
            }
            _ => return Ok(None),
        };

        let child_plan = &*projection.input;
        let projection_is_empty = projection.expr.is_empty();

        let new_plan = match child_plan {
            LogicalPlan::Projection(child_projection) => {
                let new_plan = merge_projection(projection, child_projection)?;
                self.try_optimize(&new_plan, _config)?.unwrap_or(new_plan)
            }
            LogicalPlan::Join(join) => {
                // collect column in on/filter in join and projection.
                let mut push_columns: HashSet<Column> = HashSet::new();
                for e in projection.expr.iter() {
                    expr_to_columns(e, &mut push_columns)?;
                }
                for (l, r) in join.on.iter() {
                    expr_to_columns(l, &mut push_columns)?;
                    expr_to_columns(r, &mut push_columns)?;
                }
                if let Some(expr) = &join.filter {
                    expr_to_columns(expr, &mut push_columns)?;
                }

                let new_left = generate_projection(
                    &push_columns,
                    join.left.schema(),
                    join.left.clone(),
                )?;
                let new_right = generate_projection(
                    &push_columns,
                    join.right.schema(),
                    join.right.clone(),
                )?;
                let new_join = child_plan.with_new_inputs(&[new_left, new_right])?;

                generate_plan!(projection_is_empty, plan, new_join)
            }
            LogicalPlan::CrossJoin(join) => {
                // collect column in on/filter in join and projection.
                let mut push_columns: HashSet<Column> = HashSet::new();
                for e in projection.expr.iter() {
                    expr_to_columns(e, &mut push_columns)?;
                }
                let new_left = generate_projection(
                    &push_columns,
                    join.left.schema(),
                    join.left.clone(),
                )?;
                let new_right = generate_projection(
                    &push_columns,
                    join.right.schema(),
                    join.right.clone(),
                )?;
                let new_join = child_plan.with_new_inputs(&[new_left, new_right])?;

                generate_plan!(projection_is_empty, plan, new_join)
            }
            LogicalPlan::TableScan(scan)
                if !scan.projected_schema.fields().is_empty() =>
            {
                let mut used_columns: HashSet<Column> = HashSet::new();
                if projection_is_empty {
                    push_down_scan(&used_columns, scan, true)?
                } else {
                    for expr in projection.expr.iter() {
                        expr_to_columns(expr, &mut used_columns)?;
                    }
                    let new_scan = push_down_scan(&used_columns, scan, true)?;

                    plan.with_new_inputs(&[new_scan])?
                }
            }
            LogicalPlan::Union(union) => {
                let mut required_columns = HashSet::new();
                exprlist_to_columns(&projection.expr, &mut required_columns)?;
                // When there is no projection, we need to add the first column to the projection
                // Because if push empty down, children may output different columns.
                if required_columns.is_empty() {
                    required_columns.insert(union.schema.fields()[0].qualified_column());
                }
                // we don't push down projection expr, we just prune columns, so we just push column
                // because push expr may cause more cost.
                let projection_column_exprs = get_expr(&required_columns, &union.schema)?;
                let mut inputs = Vec::with_capacity(union.inputs.len());
                for input in &union.inputs {
                    let mut replace_map = HashMap::new();
                    for (i, field) in input.schema().fields().iter().enumerate() {
                        replace_map.insert(
                            union.schema.fields()[i].qualified_name(),
                            Expr::Column(field.qualified_column()),
                        );
                    }

                    let exprs = projection_column_exprs
                        .iter()
                        .map(|expr| replace_cols_by_name(expr.clone(), &replace_map))
                        .collect::<Result<Vec<_>>>()?;

                    inputs.push(Arc::new(LogicalPlan::Projection(Projection::try_new(
                        exprs,
                        input.clone(),
                    )?)))
                }
                // create schema of all used columns
                let schema = DFSchema::new_with_metadata(
                    exprlist_to_fields(&projection_column_exprs, child_plan)?,
                    union.schema.metadata().clone(),
                )?;
                let new_union = LogicalPlan::Union(Union {
                    inputs,
                    schema: Arc::new(schema),
                });

                generate_plan!(projection_is_empty, plan, new_union)
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let replace_map = generate_column_replace_map(subquery_alias);
                let mut required_columns = HashSet::new();
                exprlist_to_columns(&projection.expr, &mut required_columns)?;

                let new_required_columns = required_columns
                    .iter()
                    .map(|c| {
                        replace_map.get(c).cloned().ok_or_else(|| {
                            DataFusionError::Internal("replace column failed".to_string())
                        })
                    })
                    .collect::<Result<HashSet<_>>>()?;

                let new_expr =
                    get_expr(&new_required_columns, subquery_alias.input.schema())?;
                let new_projection = LogicalPlan::Projection(Projection::try_new(
                    new_expr,
                    subquery_alias.input.clone(),
                )?);
                let new_alias = child_plan.with_new_inputs(&[new_projection])?;

                generate_plan!(projection_is_empty, plan, new_alias)
            }
            LogicalPlan::Aggregate(agg) => {
                let mut required_columns = HashSet::new();
                exprlist_to_columns(&projection.expr, &mut required_columns)?;
                // Gather all columns needed for expressions in this Aggregate
                let mut new_aggr_expr = vec![];
                for e in agg.aggr_expr.iter() {
                    let column = Column::from_name(e.display_name()?);
                    if required_columns.contains(&column) {
                        new_aggr_expr.push(e.clone());
                    }
                }

                // if new_aggr_expr emtpy and aggr is COUNT(UInt8(1)), push it
                if new_aggr_expr.is_empty() && agg.aggr_expr.len() == 1 {
                    if let Expr::AggregateFunction(AggregateFunction {
                        fun, args, ..
                    }) = &agg.aggr_expr[0]
                    {
                        if matches!(fun, datafusion_expr::AggregateFunction::Count)
                            && args.len() == 1
                            && args[0] == Expr::Literal(UInt8(Some(1)))
                        {
                            new_aggr_expr.push(agg.aggr_expr[0].clone());
                        }
                    }
                }

                let new_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                    agg.input.clone(),
                    agg.group_expr.clone(),
                    new_aggr_expr,
                )?);

                generate_plan!(projection_is_empty, plan, new_agg)
            }
            LogicalPlan::Window(window) => {
                let mut required_columns = HashSet::new();
                exprlist_to_columns(&projection.expr, &mut required_columns)?;
                // Gather all columns needed for expressions in this Window
                let mut new_window_expr = vec![];
                for e in window.window_expr.iter() {
                    let column = Column::from_name(e.display_name()?);
                    if required_columns.contains(&column) {
                        new_window_expr.push(e.clone());
                    }
                }

                if new_window_expr.is_empty() {
                    // none columns in window expr are needed, remove the window expr
                    let input = window.input.clone();
                    let new_window = restrict_outputs(input.clone(), &required_columns)?
                        .unwrap_or((*input).clone());

                    generate_plan!(projection_is_empty, plan, new_window)
                } else {
                    let mut referenced_inputs = HashSet::new();
                    exprlist_to_columns(&new_window_expr, &mut referenced_inputs)?;
                    window
                        .input
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| required_columns.contains(&f.qualified_column()))
                        .for_each(|f| {
                            referenced_inputs.insert(f.qualified_column());
                        });

                    let input = window.input.clone();
                    let new_input = restrict_outputs(input.clone(), &referenced_inputs)?
                        .unwrap_or((*input).clone());
                    let new_window = LogicalPlanBuilder::from(new_input)
                        .window(new_window_expr)?
                        .build()?;

                    generate_plan!(projection_is_empty, plan, new_window)
                }
            }
            LogicalPlan::Filter(filter) => {
                if can_eliminate(projection, child_plan.schema()) {
                    // when projection schema == filter schema, we can commute directly.
                    let new_proj =
                        plan.with_new_inputs(&[filter.input.as_ref().clone()])?;
                    child_plan.with_new_inputs(&[new_proj])?
                } else {
                    let mut required_columns = HashSet::new();
                    exprlist_to_columns(&projection.expr, &mut required_columns)?;
                    exprlist_to_columns(
                        &[filter.predicate.clone()],
                        &mut required_columns,
                    )?;

                    let new_expr = get_expr(&required_columns, filter.input.schema())?;
                    let new_projection = LogicalPlan::Projection(Projection::try_new(
                        new_expr,
                        filter.input.clone(),
                    )?);
                    let new_filter = child_plan.with_new_inputs(&[new_projection])?;

                    generate_plan!(projection_is_empty, plan, new_filter)
                }
            }
            LogicalPlan::Sort(sort) => {
                if can_eliminate(projection, child_plan.schema()) {
                    // can commute
                    let new_proj = plan.with_new_inputs(&[(*sort.input).clone()])?;
                    child_plan.with_new_inputs(&[new_proj])?
                } else {
                    let mut required_columns = HashSet::new();
                    exprlist_to_columns(&projection.expr, &mut required_columns)?;
                    exprlist_to_columns(&sort.expr, &mut required_columns)?;

                    let new_expr = get_expr(&required_columns, sort.input.schema())?;
                    let new_projection = LogicalPlan::Projection(Projection::try_new(
                        new_expr,
                        sort.input.clone(),
                    )?);
                    let new_sort = child_plan.with_new_inputs(&[new_projection])?;

                    generate_plan!(projection_is_empty, plan, new_sort)
                }
            }
            LogicalPlan::Limit(limit) => {
                // can commute
                let new_proj = plan.with_new_inputs(&[limit.input.as_ref().clone()])?;
                child_plan.with_new_inputs(&[new_proj])?
            }
            _ => return Ok(None),
        };

        Ok(Some(new_plan))
    }

    fn name(&self) -> &str {
        "push_down_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

impl PushDownProjection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn generate_column_replace_map(
    subquery_alias: &SubqueryAlias,
) -> HashMap<Column, Column> {
    subquery_alias
        .input
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            (
                subquery_alias.schema.fields()[i].qualified_column(),
                field.qualified_column(),
            )
        })
        .collect()
}

pub fn collect_projection_expr(projection: &Projection) -> HashMap<String, Expr> {
    projection
        .schema
        .fields()
        .iter()
        .enumerate()
        .flat_map(|(i, field)| {
            // strip alias, as they should not be part of filters
            let expr = match &projection.expr[i] {
                Expr::Alias(Alias { expr, .. }) => expr.as_ref().clone(),
                expr => expr.clone(),
            };

            // Convert both qualified and unqualified fields
            [
                (field.name().clone(), expr.clone()),
                (field.qualified_name(), expr),
            ]
        })
        .collect::<HashMap<_, _>>()
}

/// Get the projection exprs from columns in the order of the schema
fn get_expr(columns: &HashSet<Column>, schema: &DFSchemaRef) -> Result<Vec<Expr>> {
    let expr = schema
        .fields()
        .iter()
        .flat_map(|field| {
            let qc = field.qualified_column();
            let uqc = field.unqualified_column();
            if columns.contains(&qc) || columns.contains(&uqc) {
                Some(Expr::Column(qc))
            } else {
                None
            }
        })
        .collect::<Vec<Expr>>();
    if columns.len() != expr.len() {
        plan_err!("required columns can't push down, columns: {columns:?}")
    } else {
        Ok(expr)
    }
}

fn generate_projection(
    used_columns: &HashSet<Column>,
    schema: &DFSchemaRef,
    input: Arc<LogicalPlan>,
) -> Result<LogicalPlan> {
    let expr = schema
        .fields()
        .iter()
        .flat_map(|field| {
            let column = field.qualified_column();
            if used_columns.contains(&column) {
                Some(Expr::Column(column))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(LogicalPlan::Projection(Projection::try_new(expr, input)?))
}

fn push_down_scan(
    used_columns: &HashSet<Column>,
    scan: &TableScan,
    has_projection: bool,
) -> Result<LogicalPlan> {
    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    //
    // Use BTreeSet to remove potential duplicates (e.g. union) as
    // well as to sort the projection to ensure deterministic behavior
    let schema = scan.source.schema();
    let mut projection: BTreeSet<usize> = used_columns
        .iter()
        .filter(|c| {
            c.relation.is_none() || c.relation.as_ref().unwrap() == &scan.table_name
        })
        .map(|c| schema.index_of(&c.name))
        .filter_map(ArrowResult::ok)
        .collect();

    if !has_projection && projection.is_empty() {
        // for table scan without projection, we default to return all columns
        projection = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect::<BTreeSet<usize>>();
    }

    // Building new projection from BTreeSet
    // preserving source projection order if it exists
    let projection = if let Some(original_projection) = &scan.projection {
        original_projection
            .clone()
            .into_iter()
            .filter(|idx| projection.contains(idx))
            .collect::<Vec<_>>()
    } else {
        projection.into_iter().collect::<Vec<_>>()
    };

    TableScan::try_new(
        scan.table_name.clone(),
        scan.source.clone(),
        Some(projection),
        scan.filters.clone(),
        scan.fetch,
    )
    .map(LogicalPlan::TableScan)
}

fn restrict_outputs(
    plan: Arc<LogicalPlan>,
    permitted_outputs: &HashSet<Column>,
) -> Result<Option<LogicalPlan>> {
    let schema = plan.schema();
    if permitted_outputs.len() == schema.fields().len() {
        return Ok(None);
    }
    Ok(Some(generate_projection(
        permitted_outputs,
        schema,
        plan.clone(),
    )?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::vec;

    use super::*;
    use crate::eliminate_project::EliminateProjection;
    use crate::optimizer::Optimizer;
    use crate::test::*;
    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DFField, DFSchema};
    use datafusion_expr::builder::table_scan_with_filters;
    use datafusion_expr::expr::{self, Cast};
    use datafusion_expr::logical_plan::{
        builder::LogicalPlanBuilder, table_scan, JoinType,
    };
    use datafusion_expr::{
        col, count, lit, max, min, AggregateFunction, Expr, WindowFrame, WindowFunction,
    };

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]]\
        \n  TableScan: test projection=[b]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[MAX(test.b)]]\
        \n  TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn aggregate_group_by_with_table_alias() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[a.c]], aggr=[[MAX(a.b)]]\
        \n  SubqueryAlias: a\
        \n    TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn aggregate_no_group_by_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("c").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(test.b)]]\
        \n  Projection: test.b\
        \n    Filter: test.c > Int32(1)\
        \n      TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn redundant_project() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.c, test.b\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn reorder_scan() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let plan = table_scan(Some("test"), &schema, Some(vec![1, 0, 2]))?.build()?;
        let expected = "TableScan: test projection=[b, a, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn reorder_scan_projection() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let plan = table_scan(Some("test"), &schema, Some(vec![1, 0, 2]))?
            .project(vec![col("a"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.b\
        \n  TableScan: test projection=[b, a]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn reorder_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .build()?;
        let expected = "Projection: test.c, test.b, test.a\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn noncontinuous_redundant_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("b").gt(lit(1)))?
            .filter(col("a").gt(lit(1)))?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: test.a, test.c, test.b\
        \n  Filter: test.a > Int32(1)\
        \n    Filter: test.b > Int32(1)\
        \n      Projection: test.c, test.a, test.b\
        \n        Filter: test.c > Int32(1)\
        \n          Projection: test.c, test.b, test.a\
        \n            TableScan: test projection=[a, b, c]";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn join_schema_trim_full_join_column_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            .project(vec![col("a"), col("b"), col("c1")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Left Join: test.a = test2.c1\
        \n  TableScan: test projection=[a, b]\
        \n  TableScan: test2 projection=[c1]";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan;
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", LogicalType::UInt32, false),
                    DFField::new(Some("test"), "b", LogicalType::UInt32, false),
                    DFField::new(Some("test2"), "c1", LogicalType::UInt32, true),
                ],
                HashMap::new(),
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_partial_join_column_projection() -> Result<()> {
        // test join column push down without explicit column projections

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]), None)?
            // projecting joined column `a` should push the right side column `c1` projection as
            // well into test2 table even though `c1` is not referenced in projection.
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Projection: test.a, test.b\
        \n  Left Join: test.a = test2.c1\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[c1]";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", LogicalType::UInt32, false),
                    DFField::new(Some("test"), "b", LogicalType::UInt32, false),
                    DFField::new(Some("test2"), "c1", LogicalType::UInt32, true),
                ],
                HashMap::new(),
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_using_join() -> Result<()> {
        // shared join columns from using join should be pushed to both sides

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let table2_scan = scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join_using(table2_scan, JoinType::Left, vec!["a"])?
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to table scan
        let expected = "Projection: test.a, test.b\
        \n  Left Join: Using test.a = test2.a\
        \n    TableScan: test projection=[a, b]\
        \n    TableScan: test2 projection=[a]";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", LogicalType::UInt32, false),
                    DFField::new(Some("test"), "b", LogicalType::UInt32, false),
                    DFField::new(Some("test2"), "a", LogicalType::UInt32, true),
                ],
                HashMap::new(),
            )?,
        );

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(table_scan)
            .project(vec![Expr::Cast(Cast::new(
                Box::new(col("c")),
                LogicalType::Float64,
            ))])?
            .build()?;

        let expected = "Projection: CAST(test.c AS Float64)\
        \n  TableScan: test projection=[c]";

        assert_optimized_plan_eq(&projection, expected)
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);
        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "TableScan: test projection=[a, b]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn table_scan_projected_schema_non_qualified_relation() -> Result<()> {
        let table_scan = test_table_scan()?;
        let input_schema = table_scan.schema();
        assert_eq!(3, input_schema.fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // Build the LogicalPlan directly (don't use PlanBuilder), so
        // that the Column references are unqualified (e.g. their
        // relation is `None`). PlanBuilder resolves the expressions
        let expr = vec![col("a"), col("b")];
        let plan =
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(table_scan))?);

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "TableScan: test projection=[a, b]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a")])?
            .limit(0, Some(5))?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: skip=0, fetch=5\
        \n  Projection: test.c, test.a\
        \n    TableScan: test projection=[a, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;
        // should expand projection to all columns without projection
        let expected = "TableScan: test projection=[a, b, c]";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1_i64), lit(2_i64)])?
            .build()?;
        let expected = "Projection: Int64(1), Int64(2)\
                      \n  TableScan: test projection=[]";
        assert_optimized_plan_eq(&plan, expected)
    }

    /// tests that it removes unused columns in projections
    #[test]
    fn table_unused_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "b" in the first projection => remove it
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("c").gt(lit(1)))?
            .aggregate(vec![col("c")], vec![max(col("a"))])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "MAX(test.a)"]);

        let plan = optimize(&plan).expect("failed to optimize plan");
        let expected = "\
        Aggregate: groupBy=[[test.c]], aggr=[[MAX(test.a)]]\
        \n  Filter: test.c > Int32(1)\
        \n    Projection: test.c, test.a\
        \n      TableScan: test projection=[a, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    /// tests that it removes un-needed projections
    #[test]
    fn table_unused_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        let expected = "\
        Projection: Int32(1) AS a\
        \n  TableScan: test projection=[]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn table_full_filter_pushdown() -> Result<()> {
        let schema = Schema::new(test_table_scan_fields());

        let table_scan = table_scan_with_filters(
            Some("test"),
            &schema,
            None,
            vec![col("b").eq(lit(1))],
        )?
        .build()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // there is no need for the first projection
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a"]);

        let expected = "\
        Projection: Int32(1) AS a\
        \n  TableScan: test projection=[], full_filters=[b = Int32(1)]";

        assert_optimized_plan_eq(&plan, expected)
    }

    /// tests that optimizing twice yields same plan
    #[test]
    fn test_double_optimization() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b")])?
            .project(vec![lit(1).alias("a")])?
            .build()?;

        let optimized_plan1 = optimize(&plan).expect("failed to optimize plan");
        let optimized_plan2 =
            optimize(&optimized_plan1).expect("failed to optimize plan");

        let formatted_plan1 = format!("{optimized_plan1:?}");
        let formatted_plan2 = format!("{optimized_plan2:?}");
        assert_eq!(formatted_plan1, formatted_plan2);
        Ok(())
    }

    /// tests that it removes an aggregate is never used downstream
    #[test]
    fn table_unused_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        // we never use "min(b)" => remove it
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("c")], vec![max(col("b")), min(col("b"))])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("MAX(test.b)")])?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a", "MAX(test.b)"]);

        let expected = "Projection: test.c, test.a, MAX(test.b)\
        \n  Filter: test.c > Int32(1)\
        \n    Aggregate: groupBy=[[test.a, test.c]], aggr=[[MAX(test.b)]]\
        \n      TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn aggregate_filter_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;

        let aggr_with_filter = Expr::AggregateFunction(expr::AggregateFunction::new(
            AggregateFunction::Count,
            vec![col("b")],
            false,
            Some(Box::new(col("c").gt(lit(42)))),
            None,
        ));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count(col("b")), aggr_with_filter.alias("count2")],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b), COUNT(test.b) FILTER (WHERE test.c > Int32(42)) AS count2]]\
        \n  TableScan: test projection=[a, b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn pushdown_through_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .project(vec![col("a")])?
            .build()?;

        let expected = "Projection: test.a\
        \n  Distinct:\
        \n    TableScan: test projection=[a, b]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let max1 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("test.a")],
            vec![col("test.b")],
            vec![],
            WindowFrame::new(false),
        ));

        let max2 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("test.b")],
            vec![],
            vec![],
            WindowFrame::new(false),
        ));
        let col1 = col(max1.display_name()?);
        let col2 = col(max2.display_name()?);

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![max1])?
            .window(vec![max2])?
            .project(vec![col1, col2])?
            .build()?;

        let expected = "Projection: MAX(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MAX(test.b) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(test.b) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    Projection: test.b, MAX(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n      WindowAggr: windowExpr=[[MAX(test.a) PARTITION BY [test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n        TableScan: test projection=[a, b]";

        assert_optimized_plan_eq(&plan, expected)
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);
        Ok(())
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(PushDownProjection::new()),
            Arc::new(EliminateProjection::new()),
        ]);
        let mut optimized_plan = optimizer
            .optimize_recursively(
                optimizer.rules.get(0).unwrap(),
                plan,
                &OptimizerContext::new(),
            )?
            .unwrap_or_else(|| plan.clone());
        optimized_plan = optimizer
            .optimize_recursively(
                optimizer.rules.get(1).unwrap(),
                &optimized_plan,
                &OptimizerContext::new(),
            )?
            .unwrap_or(optimized_plan);
        Ok(optimized_plan)
    }
}
