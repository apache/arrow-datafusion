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

use crate::error::{DataFusionError, Result};
use crate::execution::context::ExecutionProps;
use crate::logical_plan::plan::{
    Aggregate, Analyze, Join, Projection, SubqueryAlias, TableScan, Window,
};
use crate::logical_plan::{
    build_join_schema, Column, DFField, DFSchema, DFSchemaRef, LogicalPlan,
    LogicalPlanBuilder, ToDFSchema, Union,
};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::sql::utils::find_sort_exprs;
use arrow::datatypes::{Field, Schema};
use arrow::error::Result as ArrowResult;
use datafusion_expr::logical_plan::Filter;
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

/// Optimizer that removes unused projections and aggregations from plans
/// This reduces both scans and
#[derive(Default)]
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        // set of all columns refered by the plan (and thus considered required by the root)
        let required_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect::<HashSet<Column>>();
        optimize_plan(self, plan, &required_columns, false, execution_props)
    }

    fn name(&self) -> &str {
        "projection_push_down"
    }
}

impl ProjectionPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn get_projected_schema(
    table_name: Option<&String>,
    schema: &Schema,
    required_columns: &HashSet<Column>,
    has_projection: bool,
) -> Result<(Vec<usize>, DFSchemaRef)> {
    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    //
    // Use BTreeSet to remove potential duplicates (e.g. union) as
    // well as to sort the projection to ensure deterministic behavior
    let mut projection: BTreeSet<usize> = required_columns
        .iter()
        .filter(|c| c.relation.is_none() || c.relation.as_ref() == table_name)
        .map(|c| schema.index_of(&c.name))
        .filter_map(ArrowResult::ok)
        .collect();

    if projection.is_empty() {
        if has_projection && !schema.fields().is_empty() {
            // Ensure that we are reading at least one column from the table in case the query
            // does not reference any columns directly such as "SELECT COUNT(1) FROM table",
            // except when the table is empty (no column)
            projection.insert(0);
        } else {
            // for table scan without projection, we default to return all columns
            projection = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<BTreeSet<usize>>();
        }
    }

    // create the projected schema
    let projected_fields: Vec<DFField> = match table_name {
        Some(qualifer) => projection
            .iter()
            .map(|i| DFField::from_qualified(qualifer, schema.fields()[*i].clone()))
            .collect(),
        None => projection
            .iter()
            .map(|i| DFField::from(schema.fields()[*i].clone()))
            .collect(),
    };

    let projection = projection.into_iter().collect::<Vec<_>>();
    Ok((projection, projected_fields.to_dfschema_ref()?))
}

/// Recursively transverses the logical plan removing expressions and that are not needed.
fn optimize_plan(
    _optimizer: &ProjectionPushDown,
    plan: &LogicalPlan,
    required_columns: &HashSet<Column>, // set of columns required up to this step
    has_projection: bool,
    _execution_props: &ExecutionProps,
) -> Result<LogicalPlan> {
    let mut new_required_columns = required_columns.clone();
    match plan {
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            alias,
        }) => {
            // projection:
            // * remove any expression that is not required
            // * construct the new set of required columns

            let mut new_expr = Vec::new();
            let mut new_fields = Vec::new();

            // Gather all columns needed for expressions in this Projection
            schema
                .fields()
                .iter()
                .enumerate()
                .try_for_each(|(i, field)| {
                    if required_columns.contains(&field.qualified_column()) {
                        new_expr.push(expr[i].clone());
                        new_fields.push(field.clone());

                        // gather the new set of required columns
                        utils::expr_to_columns(&expr[i], &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;

            let new_input = optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                true,
                _execution_props,
            )?;

            let new_required_columns_optimized = new_input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<Column>>();

            if new_fields.is_empty()
                || (has_projection && &new_required_columns_optimized == required_columns)
            {
                // no need for an expression at all
                Ok(new_input)
            } else {
                let metadata = new_input.schema().metadata().clone();
                Ok(LogicalPlan::Projection(Projection {
                    expr: new_expr,
                    input: Arc::new(new_input),
                    schema: DFSchemaRef::new(DFSchema::new_with_metadata(
                        new_fields, metadata,
                    )?),
                    alias: alias.clone(),
                }))
            }
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            join_constraint,
            null_equals_null,
            ..
        }) => {
            for (l, r) in on {
                new_required_columns.insert(l.clone());
                new_required_columns.insert(r.clone());
            }

            let optimized_left = Arc::new(optimize_plan(
                _optimizer,
                left,
                &new_required_columns,
                true,
                _execution_props,
            )?);

            let optimized_right = Arc::new(optimize_plan(
                _optimizer,
                right,
                &new_required_columns,
                true,
                _execution_props,
            )?);

            let schema = build_join_schema(
                optimized_left.schema(),
                optimized_right.schema(),
                join_type,
            )?;

            Ok(LogicalPlan::Join(Join {
                left: optimized_left,
                right: optimized_right,
                join_type: *join_type,
                join_constraint: *join_constraint,
                on: on.clone(),
                schema: DFSchemaRef::new(schema),
                null_equals_null: *null_equals_null,
            }))
        }
        LogicalPlan::Window(Window {
            schema,
            window_expr,
            input,
            ..
        }) => {
            // Gather all columns needed for expressions in this Window
            let mut new_window_expr = Vec::new();
            {
                window_expr.iter().try_for_each(|expr| {
                    let name = &expr.name(schema)?;
                    let column = Column::from_name(name);
                    if required_columns.contains(&column) {
                        new_window_expr.push(expr.clone());
                        new_required_columns.insert(column);
                        // add to the new set of required columns
                        utils::expr_to_columns(expr, &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;
            }

            // for all the retained window expr, find their sort expressions if any, and retain these
            utils::exprlist_to_columns(
                &find_sort_exprs(&new_window_expr),
                &mut new_required_columns,
            )?;

            LogicalPlanBuilder::from(optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                true,
                _execution_props,
            )?)
            .window(new_window_expr)?
            .build()
        }
        LogicalPlan::Aggregate(Aggregate {
            group_expr,
            aggr_expr,
            schema,
            input,
        }) => {
            // aggregate:
            // * remove any aggregate expression that is not required
            // * construct the new set of required columns

            utils::exprlist_to_columns(group_expr, &mut new_required_columns)?;

            // Gather all columns needed for expressions in this Aggregate
            let mut new_aggr_expr = Vec::new();
            aggr_expr.iter().try_for_each(|expr| {
                let name = &expr.name(schema)?;
                let column = Column::from_name(name);

                if required_columns.contains(&column) {
                    new_aggr_expr.push(expr.clone());
                    new_required_columns.insert(column);

                    // add to the new set of required columns
                    utils::expr_to_columns(expr, &mut new_required_columns)
                } else {
                    Ok(())
                }
            })?;

            let new_schema = DFSchema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .filter(|x| new_required_columns.contains(&x.qualified_column()))
                    .cloned()
                    .collect(),
                schema.metadata().clone(),
            )?;

            Ok(LogicalPlan::Aggregate(Aggregate {
                group_expr: group_expr.clone(),
                aggr_expr: new_aggr_expr,
                input: Arc::new(optimize_plan(
                    _optimizer,
                    input,
                    &new_required_columns,
                    true,
                    _execution_props,
                )?),
                schema: DFSchemaRef::new(new_schema),
            }))
        }
        // scans:
        // * remove un-used columns from the scan projection
        LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            filters,
            limit,
            ..
        }) => {
            let (projection, projected_schema) = get_projected_schema(
                Some(table_name),
                &source.schema(),
                required_columns,
                has_projection,
            )?;
            // return the table scan with projection
            Ok(LogicalPlan::TableScan(TableScan {
                table_name: table_name.clone(),
                source: source.clone(),
                projection: Some(projection),
                projected_schema,
                filters: filters.clone(),
                limit: *limit,
            }))
        }
        LogicalPlan::Explain { .. } => Err(DataFusionError::Internal(
            "Unsupported logical plan: Explain must be root of the plan".to_string(),
        )),
        LogicalPlan::Analyze(a) => {
            // make sure we keep all the columns from the input plan
            let required_columns = a
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<Column>>();

            Ok(LogicalPlan::Analyze(Analyze {
                input: Arc::new(optimize_plan(
                    _optimizer,
                    &a.input,
                    &required_columns,
                    false,
                    _execution_props,
                )?),
                verbose: a.verbose,
                schema: a.schema.clone(),
            }))
        }
        LogicalPlan::Union(Union { inputs, schema }) => {
            // UNION inputs will reference the same column with different identifiers, so we need
            // to populate new_required_columns by unqualified column name based on required fields
            // from the resulting UNION output
            let union_required_fields = schema
                .fields()
                .iter()
                .filter(|f| new_required_columns.contains(&f.qualified_column()))
                .map(|f| f.field())
                .collect::<HashSet<&Field>>();
            let new_inputs = inputs
                .iter()
                .map(|input_plan| {
                    input_plan
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| union_required_fields.contains(f.field()))
                        .for_each(|f| {
                            new_required_columns.insert(f.qualified_column());
                        });
                    optimize_plan(
                        _optimizer,
                        input_plan,
                        &new_required_columns,
                        has_projection,
                        _execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let new_schema = DFSchema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .filter(|f| union_required_fields.contains(f.field()))
                    .cloned()
                    .collect(),
                schema.metadata().clone(),
            )?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                schema: Arc::new(new_schema),
            }))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            println!("input: {:?}", *input);
            match input.as_ref() {
                LogicalPlan::TableScan(TableScan { table_name, .. }) => {
                    let new_required_columns = new_required_columns
                        .iter()
                        .map(|c| match &c.relation {
                            Some(q) if q == alias => Column {
                                relation: Some(table_name.clone()),
                                name: c.name.clone(),
                            },
                            _ => c.clone(),
                        })
                        .collect();
                    let new_inputs = vec![optimize_plan(
                        _optimizer,
                        input,
                        &new_required_columns,
                        has_projection,
                        _execution_props,
                    )?];
                    let expr = vec![];
                    utils::from_plan(plan, &expr, &new_inputs)
                }
                LogicalPlan::Union(Union { .. }) => {
                    // Scope of alias is outside, inside we should remove it.
                    // Here we remove the alias in `new_required_columns`.
                    let new_required_columns = new_required_columns
                        .iter()
                        .map(|c| match &c.relation {
                            Some(q) if q == alias => Column {
                                relation: None,
                                name: c.name.clone(),
                            },
                            _ => c.clone(),
                        })
                        .collect();
                    let new_inputs = vec![optimize_plan(
                        _optimizer,
                        input,
                        &new_required_columns,
                        has_projection,
                        _execution_props,
                    )?];
                    let expr = vec![];

                    utils::from_plan(plan, &expr, &new_inputs)
                }
                LogicalPlan::Filter(Filter { input, .. }) => {
                    // let new_inputs = vec![optimize_plan(
                    //     _optimizer,
                    //     input,
                    //     &new_required_columns,
                    //     has_projection,
                    //     _execution_props,
                    // )?];
                    // let expr = vec![];

                    // utils::from_plan(plan, &expr, &new_inputs)
                    let expr = plan.expressions();
                    // collect all required columns by this plan
                    utils::exprlist_to_columns(&expr, &mut new_required_columns)?;

                    // apply the optimization to all inputs of the plan
                    let inputs = plan.inputs();
                    let new_inputs = inputs
                        .iter()
                        .map(|input_plan| {
                            optimize_plan(
                                _optimizer,
                                input_plan,
                                &new_required_columns,
                                has_projection,
                                _execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    utils::from_plan(plan, &expr, &new_inputs)
                }
                _ => Err(DataFusionError::Plan(
                    "SubqueryAlias should only wrap TableScan".to_string(),
                )),
            }
        }
        // all other nodes: Add any additional columns used by
        // expressions in this node to the list of required columns
        LogicalPlan::Limit(_)
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Repartition(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Sort { .. }
        | LogicalPlan::CreateExternalTable(_)
        | LogicalPlan::CreateMemoryTable(_)
        | LogicalPlan::CreateView(_)
        | LogicalPlan::CreateCatalogSchema(_)
        | LogicalPlan::CreateCatalog(_)
        | LogicalPlan::DropTable(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Extension { .. } => {
            let expr = plan.expressions();
            // collect all required columns by this plan
            utils::exprlist_to_columns(&expr, &mut new_required_columns)?;

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|input_plan| {
                    optimize_plan(
                        _optimizer,
                        input_plan,
                        &new_required_columns,
                        has_projection,
                        _execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::logical_plan::{
        col, exprlist_to_fields, lit, max, min, Expr, JoinType, LogicalPlanBuilder,
    };
    use crate::test::*;
    use arrow::datatypes::DataType;

    #[test]
    fn aggregate_no_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#test.b)]]\
        \n  TableScan: test projection=Some([1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[#test.c]], aggr=[[MAX(#test.b)]]\
        \n  TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_group_by_with_table_alias() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .aggregate(vec![col("c")], vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[#a.c]], aggr=[[MAX(#a.b)]]\
        \n  SubqueryAlias: a\
        \n    TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn aggregate_no_group_by_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("c"))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#test.b)]]\
        \n  Filter: #test.c\
        \n    TableScan: test projection=Some([1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn redundunt_project() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: #test.a, #test.c, #test.b\
        \n  TableScan: test projection=Some([0, 1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reorder_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .build()?;
        let expected = "Projection: #test.c, #test.b, #test.a\
        \n  TableScan: test projection=Some([0, 1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn noncontiguous_redundunt_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("b"), col("a")])?
            .filter(col("c").gt(lit(1)))?
            .project(vec![col("c"), col("a"), col("b")])?
            .filter(col("b").gt(lit(1)))?
            .filter(col("a").gt(lit(1)))?
            .project(vec![col("a"), col("c"), col("b")])?
            .build()?;
        let expected = "Projection: #test.a, #test.c, #test.b\
        \n  Filter: #test.a > Int32(1)\
        \n    Filter: #test.b > Int32(1)\
        \n      Filter: #test.c > Int32(1)\
        \n        TableScan: test projection=Some([0, 1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn join_schema_trim_full_join_column_projection() -> Result<()> {
        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan =
            LogicalPlanBuilder::scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(&table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]))?
            .project(vec![col("a"), col("b"), col("c1")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Projection: #test.a, #test.b, #test2.c1\
        \n  Left Join: #test.a = #test2.c1\
        \n    TableScan: test projection=Some([0, 1])\
        \n    TableScan: test2 projection=Some([0])";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "c1", DataType::UInt32, false),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_partial_join_column_projection() -> Result<()> {
        // test join column push down without explicit column projections

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("c1", DataType::UInt32, false)]);
        let table2_scan =
            LogicalPlanBuilder::scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join(&table2_scan, JoinType::Left, (vec!["a"], vec!["c1"]))?
            // projecting joined column `a` should push the right side column `c1` projection as
            // well into test2 table even though `c1` is not referenced in projection.
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to both table scans
        let expected = "Projection: #test.a, #test.b\
        \n  Left Join: #test.a = #test2.c1\
        \n    TableScan: test projection=Some([0, 1])\
        \n    TableScan: test2 projection=Some([0])";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "c1", DataType::UInt32, false),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn join_schema_trim_using_join() -> Result<()> {
        // shared join colums from using join should be pushed to both sides

        let table_scan = test_table_scan()?;

        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);
        let table2_scan =
            LogicalPlanBuilder::scan_empty(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .join_using(&table2_scan, JoinType::Left, vec!["a"])?
            .project(vec![col("a"), col("b")])?
            .build()?;

        // make sure projections are pushed down to table scan
        let expected = "Projection: #test.a, #test.b\
        \n  Left Join: Using #test.a = #test2.a\
        \n    TableScan: test projection=Some([0, 1])\
        \n    TableScan: test2 projection=Some([0])";

        let optimized_plan = optimize(&plan)?;
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);

        // make sure schema for join node include both join columns
        let optimized_join = optimized_plan.inputs()[0];
        assert_eq!(
            **optimized_join.schema(),
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "a", DataType::UInt32, false),
                ],
                HashMap::new()
            )?,
        );

        Ok(())
    }

    #[test]
    fn cast() -> Result<()> {
        let table_scan = test_table_scan()?;

        let projection = LogicalPlanBuilder::from(table_scan)
            .project(vec![Expr::Cast {
                expr: Box::new(col("c")),
                data_type: DataType::Float64,
            }])?
            .build()?;

        let expected = "Projection: CAST(#test.c AS Float64)\
        \n  TableScan: test projection=Some([2])";

        assert_optimized_plan_eq(&projection, expected);

        Ok(())
    }

    #[test]
    fn table_scan_projected_schema() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: #test.a, #test.b\
        \n  TableScan: test projection=Some([0, 1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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
        let projected_fields = exprlist_to_fields(&expr, &table_scan).unwrap();
        let projected_schema = DFSchema::new_with_metadata(
            projected_fields,
            input_schema.metadata().clone(),
        )
        .unwrap();
        let plan = LogicalPlan::Projection(Projection {
            expr,
            input: Arc::new(table_scan),
            schema: Arc::new(projected_schema),
            alias: None,
        });

        assert_fields_eq(&plan, vec!["a", "b"]);

        let expected = "Projection: #a, #b\
        \n  TableScan: test projection=Some([0, 1])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        assert_eq!(3, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a", "b", "c"]);

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c"), col("a")])?
            .limit(5)?
            .build()?;

        assert_fields_eq(&plan, vec!["c", "a"]);

        let expected = "Limit: 5\
        \n  Projection: #test.c, #test.a\
        \n    TableScan: test projection=Some([0, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn table_scan_without_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;
        // should expand projection to all columns without projection
        let expected = "TableScan: test projection=Some([0, 1, 2])";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn table_scan_with_literal_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1_i64), lit(2_i64)])?
            .build()?;
        let expected = "Projection: Int64(1), Int64(2)\
                      \n  TableScan: test projection=Some([0])";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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

        let expected = "\
        Aggregate: groupBy=[[#test.c]], aggr=[[MAX(#test.a)]]\
        \n  Filter: #test.c > Int32(1)\
        \n    Projection: #test.c, #test.a\
        \n      TableScan: test projection=Some([0, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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
        \n  TableScan: test projection=Some([0])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        let formatted_plan1 = format!("{:?}", optimized_plan1);
        let formatted_plan2 = format!("{:?}", optimized_plan2);
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

        let expected = "Projection: #test.c, #test.a, #MAX(test.b)\
        \n  Filter: #test.c > Int32(1)\
        \n    Aggregate: groupBy=[[#test.a, #test.c]], aggr=[[MAX(#test.b)]]\
        \n      TableScan: test projection=Some([0, 1, 2])";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimized_plan = optimize(plan).expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rule = ProjectionPushDown::new();
        rule.optimize(plan, &ExecutionProps::new())
    }
}
