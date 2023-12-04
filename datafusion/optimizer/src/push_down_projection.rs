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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::vec;

    use crate::optimize_projections::OptimizeProjections;
    use crate::optimizer::Optimizer;
    use crate::test::*;
    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Column, DFField, DFSchema, Result};
    use datafusion_expr::builder::table_scan_with_filters;
    use datafusion_expr::expr::{self, Cast};
    use datafusion_expr::logical_plan::{
        builder::LogicalPlanBuilder, table_scan, JoinType,
    };
    use datafusion_expr::{
        col, count, lit, max, min, AggregateFunction, Expr, LogicalPlan, Projection,
        WindowFrame, WindowFunction,
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
        \n  Filter: test.c > Int32(1), projection=[b]\
        \n    TableScan: test projection=[b, c]";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn aggregate_with_periods() -> Result<()> {
        let schema = Schema::new(vec![Field::new("tag.one", DataType::Utf8, false)]);

        // Build a plan that looks as follows (note "tag.one" is a column named
        // "tag.one", not a column named "one" in a table named "tag"):
        //
        // Projection: tag.one
        //   Aggregate: groupBy=[], aggr=[MAX("tag.one") AS "tag.one"]
        //    TableScan
        let plan = table_scan(Some("m4"), &schema, None)?
            .aggregate(
                Vec::<Expr>::new(),
                vec![max(col(Column::new_unqualified("tag.one"))).alias("tag.one")],
            )?
            .project([col(Column::new_unqualified("tag.one"))])?
            .build()?;

        let expected = "\
        Aggregate: groupBy=[[]], aggr=[[MAX(m4.tag.one) AS tag.one]]\
        \n  TableScan: m4 projection=[tag.one]";

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
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "c1", DataType::UInt32, true),
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
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "c1", DataType::UInt32, true),
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
                    DFField::new(Some("test"), "a", DataType::UInt32, false),
                    DFField::new(Some("test"), "b", DataType::UInt32, false),
                    DFField::new(Some("test2"), "a", DataType::UInt32, true),
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
                DataType::Float64,
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
        let expr = vec![col("test.a"), col("test.b")];
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
        let optimizer = Optimizer::with_rules(vec![Arc::new(OptimizeProjections::new())]);
        let optimized_plan = optimizer
            .optimize_recursively(
                optimizer.rules.get(0).unwrap(),
                plan,
                &OptimizerContext::new(),
            )?
            .unwrap_or_else(|| plan.clone());
        Ok(optimized_plan)
    }
}
