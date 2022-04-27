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
//! This module contains the  `LogicalPlan` enum that describes queries
//! via a logical query plan.

use super::expr::Expr;
use crate::arrow::datatypes::SchemaRef;
use crate::datasource::TableProvider;
use crate::error::DataFusionError;
pub use crate::logical_expr::{
    logical_plan::{
        display::{GraphvizVisitor, IndentVisitor},
        Aggregate, Analyze, CreateCatalog, CreateCatalogSchema, CreateExternalTable,
        CreateMemoryTable, CrossJoin, DropTable, EmptyRelation, Explain, Extension,
        FileType, Filter, Join, JoinConstraint, JoinType, Limit, LogicalPlan,
        Partitioning, PlanType, PlanVisitor, Projection, Repartition, Sort,
        StringifiedPlan, Subquery, SubqueryAlias, TableScan, TableUDFs,
        ToStringifiedPlan, Union, UserDefinedLogicalNode, Values, Window,
    },
    TableProviderFilterPushDown, TableSource,
};
use std::any::Any;
use std::sync::Arc;

/// DataFusion default table source, wrapping TableProvider
///
/// This structure adapts a `TableProvider` (physical plan trait) to the `TableSource`
/// (logical plan trait)
pub struct DefaultTableSource {
    /// table provider
    pub table_provider: Arc<dyn TableProvider>,
}

impl DefaultTableSource {
    /// Create a new DefaultTableSource to wrap a TableProvider
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self { table_provider }
    }
}

impl TableSource for DefaultTableSource {
    /// Returns the table source as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.table_provider.schema()
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> datafusion_common::Result<TableProviderFilterPushDown> {
        self.table_provider.supports_filter_pushdown(filter)
    }
}

/// Wrap TableProvider in TableSource
pub fn provider_as_source(
    table_provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableSource> {
    Arc::new(DefaultTableSource::new(table_provider))
}

/// Attempt to downcast a TableSource to DefaultTableSource and access the
/// TableProvider. This will only work with a TableSource created by DataFusion.
pub fn source_as_provider(
    source: &Arc<dyn TableSource>,
) -> datafusion_common::Result<Arc<dyn TableProvider>> {
    match source
        .as_ref()
        .as_any()
        .downcast_ref::<DefaultTableSource>()
    {
        Some(source) => Ok(source.table_provider.clone()),
        _ => Err(DataFusionError::Internal(
            "TableSource was not DefaultTableSource".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{col, lit, LogicalPlanBuilder};
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    fn display_plan() -> LogicalPlan {
        LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            Some(vec![0, 3]),
        )
        .unwrap()
        .filter(col("state").eq(lit("CO")))
        .unwrap()
        .project(vec![col("id")])
        .unwrap()
        .build()
        .unwrap()
    }

    #[test]
    fn test_display_indent() {
        let plan = display_plan();

        let expected = "Projection: #employee_csv.id\
        \n  Filter: #employee_csv.state = Utf8(\"CO\")\
        \n    TableScan: employee_csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{}", plan.display_indent()));
    }

    #[test]
    fn test_display_indent_schema() {
        let plan = display_plan();

        let expected = "Projection: #employee_csv.id [id:Int32]\
                        \n  Filter: #employee_csv.state = Utf8(\"CO\") [id:Int32, state:Utf8]\
                        \n    TableScan: employee_csv projection=Some([0, 3]) [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
    }

    #[test]
    fn test_display_graphviz() {
        let plan = display_plan();

        // just test for a few key lines in the output rather than the
        // whole thing to make test mainteance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert!(
            graphviz.contains(
                r#"// Begin DataFusion GraphViz Plan (see https://graphviz.org)"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(
            graphviz.contains(
                r#"[shape=box label="TableScan: employee_csv projection=Some([0, 3])"]"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(graphviz.contains(r#"[shape=box label="TableScan: employee_csv projection=Some([0, 3])\nSchema: [id:Int32, state:Utf8]"]"#),
                "\n{}", plan.display_graphviz());
        assert!(
            graphviz.contains(r#"// End DataFusion GraphViz Plan"#),
            "\n{}",
            plan.display_graphviz()
        );
    }

    /// Tests for the Visitor trait and walking logical plan nodes
    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }

    impl PlanVisitor for OkVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
                "post_visit Filter",
                "post_visit Projection",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Counter than counts to zero and returns true when it gets there
    struct OptionalCounter {
        val: Option<usize>,
    }

    impl OptionalCounter {
        fn new(val: usize) -> Self {
            Self { val: Some(val) }
        }
        // Decrements the counter by 1, if any, returning true if it hits zero
        fn dec(&mut self) -> bool {
            if Some(0) == self.val {
                true
            } else {
                self.val = self.val.take().map(|i| i - 1);
                false
            }
        }
    }

    #[derive(Debug, Default)]
    /// Visitor that returns false after some number of visits
    struct StoppingVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_false_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_false_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for StoppingVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_pre_in.dec() {
                return Ok(false);
            }
            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_post_in.dec() {
                return Ok(false);
            }

            self.inner.post_visit(plan)
        }
    }

    /// test early stopping in pre-visit
    #[test]
    fn early_stopping_pre_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter"]
        );
    }

    #[test]
    fn early_stopping_post_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Visitor that returns an error after some number of visits
    struct ErrorVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_error_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_error_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for ErrorVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_pre_in.dec() {
                return Err("Error in pre_visit".into());
            }

            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_post_in.dec() {
                return Err("Error in post_visit".into());
            }

            self.inner.post_visit(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);

        if let Err(e) = res {
            assert_eq!("Error in pre_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter"]
        );
    }

    #[test]
    fn error_post_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        if let Err(e) = res {
            assert_eq!("Error in post_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("state", DataType::Utf8, false),
        ]);

        LogicalPlanBuilder::scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }
}
