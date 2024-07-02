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

//! [`EliminateNestedUnion`]: flattens nested `Union` to a single `Union`
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion_expr::{Distinct, LogicalPlan, Union};
use std::sync::Arc;

#[derive(Default)]
/// An optimization rule that replaces nested unions with a single union.
pub struct EliminateNestedUnion;

impl EliminateNestedUnion {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateNestedUnion {
    fn name(&self) -> &str {
        "eliminate_nested_union"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Union(Union { inputs, schema }) => {
                let inputs = inputs
                    .iter()
                    .flat_map(extract_plans_from_union)
                    .collect::<Vec<_>>();

                Ok(Transformed::yes(LogicalPlan::Union(Union {
                    inputs,
                    schema,
                })))
            }
            LogicalPlan::Distinct(Distinct::All(ref nested_plan)) => {
                match nested_plan.as_ref() {
                    LogicalPlan::Union(Union { inputs, schema }) => {
                        let inputs = inputs
                            .iter()
                            .map(extract_plan_from_distinct)
                            .flat_map(extract_plans_from_union)
                            .collect::<Vec<_>>();

                        Ok(Transformed::yes(LogicalPlan::Distinct(Distinct::All(
                            Arc::new(LogicalPlan::Union(Union {
                                inputs,
                                schema: Arc::clone(schema),
                            })),
                        ))))
                    }
                    _ => Ok(Transformed::no(plan)),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn extract_plans_from_union(plan: &Arc<LogicalPlan>) -> Vec<Arc<LogicalPlan>> {
    match plan.as_ref() {
        LogicalPlan::Union(Union { inputs, schema }) => inputs
            .iter()
            .map(|plan| Arc::new(coerce_plan_expr_for_schema(plan, schema).unwrap()))
            .collect::<Vec<_>>(),
        _ => vec![Arc::clone(plan)],
    }
}

fn extract_plan_from_distinct(plan: &Arc<LogicalPlan>) -> &Arc<LogicalPlan> {
    match plan.as_ref() {
        LogicalPlan::Distinct(Distinct::All(plan)) => plan,
        _ => plan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{col, logical_plan::table_scan};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
    }

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateNestedUnion::new()), plan, expected)
    }

    #[test]
    fn eliminate_nothing() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .build()?;

        let expected = "\
        Union\
        \n  TableScan: table\
        \n  TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_distinct_nothing() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(plan_builder.clone().build()?)?
            .build()?;

        let expected = "Distinct:\
        \n  Union\
        \n    TableScan: table\
        \n    TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .build()?;

        let expected = "\
        Union\
        \n  TableScan: table\
        \n  TableScan: table\
        \n  TableScan: table\
        \n  TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_union_with_distinct_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .build()?;

        let expected = "Union\
        \n  Distinct:\
        \n    Union\
        \n      TableScan: table\
        \n      TableScan: table\
        \n  TableScan: table\
        \n  TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_distinct_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .union_distinct(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union_distinct(plan_builder.clone().build()?)?
            .build()?;

        let expected = "Distinct:\
        \n  Union\
        \n    TableScan: table\
        \n    TableScan: table\
        \n    TableScan: table\
        \n    TableScan: table\
        \n    TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_distinct_union_with_distinct_table() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(plan_builder.clone().distinct()?.build()?)?
            .union(plan_builder.clone().distinct()?.build()?)?
            .union_distinct(plan_builder.clone().build()?)?
            .build()?;

        let expected = "Distinct:\
        \n  Union\
        \n    TableScan: table\
        \n    TableScan: table\
        \n    TableScan: table\
        \n    TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    // We don't need to use project_with_column_index in logical optimizer,
    // after LogicalPlanBuilder::union, we already have all equal expression aliases
    #[test]
    fn eliminate_nested_union_with_projection() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(
                plan_builder
                    .clone()
                    .project(vec![col("id").alias("table_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .union(
                plan_builder
                    .clone()
                    .project(vec![col("id").alias("_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .build()?;

        let expected = "Union\
        \n  TableScan: table\
        \n  Projection: table.id AS id, table.key, table.value\
        \n    TableScan: table\
        \n  Projection: table.id AS id, table.key, table.value\
        \n    TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_distinct_union_with_projection() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union_distinct(
                plan_builder
                    .clone()
                    .project(vec![col("id").alias("table_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .union_distinct(
                plan_builder
                    .clone()
                    .project(vec![col("id").alias("_id"), col("key"), col("value")])?
                    .build()?,
            )?
            .build()?;

        let expected = "Distinct:\
        \n  Union\
        \n    TableScan: table\
        \n    Projection: table.id AS id, table.key, table.value\
        \n      TableScan: table\
        \n    Projection: table.id AS id, table.key, table.value\
        \n      TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_union_with_type_cast_projection() -> Result<()> {
        let table_1 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]),
            None,
        )?;

        let table_2 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let table_3 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int16, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let plan = table_1
            .union(table_2.build()?)?
            .union(table_3.build()?)?
            .build()?;

        let expected = "Union\
        \n  TableScan: table_1\
        \n  Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value\
        \n    TableScan: table_1\
        \n  Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value\
        \n    TableScan: table_1";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_nested_distinct_union_with_type_cast_projection() -> Result<()> {
        let table_1 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]),
            None,
        )?;

        let table_2 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let table_3 = table_scan(
            Some("table_1"),
            &Schema::new(vec![
                Field::new("id", DataType::Int16, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float32, false),
            ]),
            None,
        )?;

        let plan = table_1
            .union_distinct(table_2.build()?)?
            .union_distinct(table_3.build()?)?
            .build()?;

        let expected = "Distinct:\
        \n  Union\
        \n    TableScan: table_1\
        \n    Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value\
        \n      TableScan: table_1\
        \n    Projection: CAST(table_1.id AS Int64) AS id, table_1.key, CAST(table_1.value AS Float64) AS value\
        \n      TableScan: table_1";
        assert_optimized_plan_equal(plan, expected)
    }
}
