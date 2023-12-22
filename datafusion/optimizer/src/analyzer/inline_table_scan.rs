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

//! Analyzed rule to replace TableScan references
//! such as DataFrames and Views and inlines the LogicalPlan.
use std::sync::Arc;

use crate::analyzer::AnalyzerRule;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_expr::expr::Exists;
use datafusion_expr::expr::InSubquery;
use datafusion_expr::{
    logical_plan::LogicalPlan, Expr, Filter, LogicalPlanBuilder, TableScan,
};

use crate::analyzer::AnalyzerConfig;

/// Analyzed rule that inlines TableScan that provide a [`LogicalPlan`]
/// (DataFrame / ViewTable)
#[derive(Default)]
pub struct InlineTableScan;

impl InlineTableScan {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for InlineTableScan {
    fn analyze(&self, plan: LogicalPlan, _: &dyn AnalyzerConfig) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal)
    }

    fn name(&self) -> &str {
        "inline_table_scan"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    Ok(match plan {
        // Match only on scans without filter / projection / fetch
        // Views and DataFrames won't have those added
        // during the early stage of planning
        LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            projection,
            filters,
            ..
        }) if filters.is_empty() && source.get_logical_plan().is_some() => {
            let sub_plan = source.get_logical_plan().unwrap();
            let projection_exprs = generate_projection_expr(&projection, sub_plan)?;
            let plan = LogicalPlanBuilder::from(sub_plan.clone())
                .project(projection_exprs)?
                // Ensures that the reference to the inlined table remains the
                // same, meaning we don't have to change any of the parent nodes
                // that reference this table.
                .alias(table_name)?
                .build()?;
            Transformed::Yes(plan)
        }
        LogicalPlan::Filter(filter) => {
            let new_expr = filter.predicate.transform(&rewrite_subquery)?;
            Transformed::Yes(LogicalPlan::Filter(Filter::try_new(
                new_expr,
                filter.input,
            )?))
        }
        _ => Transformed::No(plan),
    })
}

fn rewrite_subquery(expr: Expr) -> Result<Transformed<Expr>> {
    match expr {
        Expr::Exists(Exists { subquery, negated }) => {
            let plan = subquery.subquery.as_ref().clone();
            let new_plan = plan.transform_up(&analyze_internal)?;
            let subquery = subquery.with_plan(Arc::new(new_plan));
            Ok(Transformed::Yes(Expr::Exists(Exists { subquery, negated })))
        }
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => {
            let plan = subquery.subquery.as_ref().clone();
            let new_plan = plan.transform_up(&analyze_internal)?;
            let subquery = subquery.with_plan(Arc::new(new_plan));
            Ok(Transformed::Yes(Expr::InSubquery(InSubquery::new(
                expr, subquery, negated,
            ))))
        }
        Expr::ScalarSubquery(subquery) => {
            let plan = subquery.subquery.as_ref().clone();
            let new_plan = plan.transform_up(&analyze_internal)?;
            let subquery = subquery.with_plan(Arc::new(new_plan));
            Ok(Transformed::Yes(Expr::ScalarSubquery(subquery)))
        }
        _ => Ok(Transformed::No(expr)),
    }
}

fn generate_projection_expr(
    projection: &Option<Vec<usize>>,
    sub_plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let mut exprs = vec![];
    if let Some(projection) = projection {
        for i in projection {
            exprs.push(Expr::Column(
                sub_plan.schema().fields()[*i].qualified_column(),
            ));
        }
    } else {
        exprs.push(Expr::Wildcard { qualifier: None });
    }
    Ok(exprs)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};

    use arrow::datatypes::{DataType, Field, Schema};

    use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder, TableSource};

    use crate::analyzer::inline_table_scan::InlineTableScan;
    use crate::test::assert_analyzed_plan_eq;

    pub struct RawTableSource {}

    impl TableSource for RawTableSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]))
        }

        fn supports_filter_pushdown(
            &self,
            _filter: &datafusion_expr::Expr,
        ) -> datafusion_common::Result<datafusion_expr::TableProviderFilterPushDown>
        {
            Ok(datafusion_expr::TableProviderFilterPushDown::Inexact)
        }
    }

    pub struct CustomSource {
        plan: LogicalPlan,
    }

    impl CustomSource {
        fn new() -> Self {
            Self {
                plan: LogicalPlanBuilder::scan("y", Arc::new(RawTableSource {}), None)
                    .unwrap()
                    .build()
                    .unwrap(),
            }
        }
    }

    impl TableSource for CustomSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn supports_filter_pushdown(
            &self,
            _filter: &datafusion_expr::Expr,
        ) -> datafusion_common::Result<datafusion_expr::TableProviderFilterPushDown>
        {
            Ok(datafusion_expr::TableProviderFilterPushDown::Exact)
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
        }

        fn get_logical_plan(&self) -> Option<&LogicalPlan> {
            Some(&self.plan)
        }
    }

    #[test]
    fn inline_table_scan() -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(CustomSource::new()),
            None,
        )?;
        let plan = scan.filter(col("x.a").eq(lit(1)))?.build()?;
        let expected = "Filter: x.a = Int32(1)\
        \n  SubqueryAlias: x\
        \n    Projection: y.a, y.b\
        \n      TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), &plan, expected)
    }

    #[test]
    fn inline_table_scan_with_projection() -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(CustomSource::new()),
            Some(vec![0]),
        )?;

        let plan = scan.build()?;
        let expected = "SubqueryAlias: x\
        \n  Projection: y.a\
        \n    TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), &plan, expected)
    }
}
