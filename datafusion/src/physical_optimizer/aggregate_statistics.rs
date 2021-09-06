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

//! Utilizing exact statistics from sources to avoid scanning data
use std::{sync::Arc, vec};

use arrow::datatypes::Schema;

use crate::execution::context::ExecutionConfig;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{expressions, AggregateExpr, ExecutionPlan, Statistics};
use crate::scalar::ScalarValue;

use super::optimizer::PhysicalOptimizerRule;
use super::utils::optimize_children;
use crate::error::Result;

/// Optimizer that uses available statistics for aggregate functions
pub struct AggregateStatistics {}

impl AggregateStatistics {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AggregateStatistics {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        execution_config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some((partial_agg_exec, stats)) = take_optimizable(&*plan) {
            let partial_agg_exec = partial_agg_exec
                .as_any()
                .downcast_ref::<HashAggregateExec>()
                .expect("take_optimizable() ensures that this is a HashAggregateExec");
            let mut projections = vec![];
            for expr in partial_agg_exec.aggr_expr() {
                if let Some((num_rows, name)) = take_optimizable_count(&**expr, &stats) {
                    projections.push((expressions::lit(num_rows), name.to_owned()));
                } else if let Some((min, name)) = take_optimizable_min(&**expr, &stats) {
                    projections.push((expressions::lit(min), name.to_owned()));
                } else if let Some((max, name)) = take_optimizable_max(&**expr, &stats) {
                    projections.push((expressions::lit(max), name.to_owned()));
                } else {
                    // TODO: we need all aggr_expr to be resolved (cf TODO fullres)
                    break;
                }
            }

            // TODO fullres: use statistics even if not all aggr_expr could be resolved
            if projections.len() == partial_agg_exec.aggr_expr().len() {
                // input can be entirely removed
                Ok(Arc::new(ProjectionExec::try_new(
                    projections,
                    Arc::new(EmptyExec::new(true, Arc::new(Schema::empty()))),
                )?))
            } else {
                optimize_children(self, plan, execution_config)
            }
        } else {
            optimize_children(self, plan, execution_config)
        }
    }

    fn name(&self) -> &str {
        "aggregate_statistics"
    }
}

/// assert if the node passed as argument is a final `HashAggregateExec` node that can be optimized:
/// - its child (with posssible intermediate layers) is a partial `HashAggregateExec` node
/// - they both have no grouping expression
/// - the statistics are exact
/// If this is the case, return a ref to the partial `HashAggregateExec` and the stats, else `None`.
/// We would have prefered to return a casted ref to HashAggregateExec but the recursion requires
/// the `ExecutionPlan.children()` method that returns an owned reference.
fn take_optimizable(
    node: &dyn ExecutionPlan,
) -> Option<(Arc<dyn ExecutionPlan>, Statistics)> {
    if let Some(final_agg_exec) = node.as_any().downcast_ref::<HashAggregateExec>() {
        if final_agg_exec.mode() == &AggregateMode::Final
            && final_agg_exec.group_expr().is_empty()
        {
            let mut child = Arc::clone(final_agg_exec.input());
            loop {
                if let Some(partial_agg_exec) =
                    child.as_any().downcast_ref::<HashAggregateExec>()
                {
                    if partial_agg_exec.mode() == &AggregateMode::Partial
                        && partial_agg_exec.group_expr().is_empty()
                    {
                        let stats = partial_agg_exec.input().statistics();
                        if stats.is_exact {
                            return Some((child, stats));
                        }
                    }
                }
                if let [ref childrens_child] = child.children().as_slice() {
                    child = Arc::clone(childrens_child);
                } else {
                    break;
                }
            }
        }
    }
    None
}

/// If this agg_expr is a count that is defined in the statistics, return it
fn take_optimizable_count(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, &'static str)> {
    if let (Some(num_rows), Some(count_expr)) = (
        stats.num_rows,
        agg_expr.as_any().downcast_ref::<expressions::Count>(),
    ) {
        // TODO implementing Eq on PhysicalExpr would help a lot here
        if count_expr.expressions().len() == 1 {
            if let Some(lit_expr) = count_expr.expressions()[0]
                .as_any()
                .downcast_ref::<expressions::Literal>()
            {
                if lit_expr.value() == &ScalarValue::UInt8(Some(1)) {
                    return Some((
                        ScalarValue::UInt64(Some(num_rows as u64)),
                        "COUNT(Uint8(1))",
                    ));
                }
            }
        }
    }
    None
}

/// If this agg_expr is a min that is defined in the statistics, return it
fn take_optimizable_min(
    _agg_expr: &dyn AggregateExpr,
    _stats: &Statistics,
) -> Option<(ScalarValue, &'static str)> {
    // TODO
    None
}

/// If this agg_expr is a max that is defined in the statistics, return it
fn take_optimizable_max(
    _agg_expr: &dyn AggregateExpr,
    _stats: &Statistics,
) -> Option<(ScalarValue, &'static str)> {
    // TODO
    None
}

#[cfg(test)]
mod tests {
    // use std::sync::Arc;

    // use arrow::datatypes::{DataType, Field, Schema};

    // use crate::error::Result;
    // use crate::execution::context::ExecutionProps;
    // use crate::logical_plan::LogicalPlan;
    // use crate::optimizer::aggregate_statistics::AggregateStatistics;
    // use crate::optimizer::optimizer::OptimizerRule;
    // use crate::scalar::ScalarValue;
    // use crate::{
    //     datasource::{
    //         datasource::{ColumnStatistics, Statistics},
    //         TableProvider,
    //     },
    //     logical_plan::Expr,
    // };

    // struct TestTableProvider {
    //     num_rows: usize,
    //     column_statistics: Vec<ColumnStatistics>,
    //     is_exact: bool,
    // }

    // impl TableProvider for TestTableProvider {
    //     fn as_any(&self) -> &dyn std::any::Any {
    //         unimplemented!()
    //     }
    //     fn schema(&self) -> arrow::datatypes::SchemaRef {
    //         Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    //     }

    //     fn scan(
    //         &self,
    //         _projection: &Option<Vec<usize>>,
    //         _batch_size: usize,
    //         _filters: &[Expr],
    //         _limit: Option<usize>,
    //     ) -> Result<std::sync::Arc<dyn crate::physical_plan::ExecutionPlan>> {
    //         unimplemented!()
    //     }
    //     fn statistics(&self) -> Statistics {
    //         Statistics {
    //             num_rows: Some(self.num_rows),
    //             total_byte_size: None,
    //             column_statistics: Some(self.column_statistics.clone()),
    //         }
    //     }
    //     fn has_exact_statistics(&self) -> bool {
    //         self.is_exact
    //     }
    // }

    // #[test]
    // fn optimize_count_using_statistics() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();
    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics: Vec::new(),
    //             is_exact: true,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("select count(*) from test")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #COUNT(UInt8(1))\
    //         \n  Projection: UInt64(100) AS COUNT(Uint8(1))\
    //         \n    EmptyRelation";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // #[test]
    // fn optimize_count_not_exact() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();
    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics: Vec::new(),
    //             is_exact: false,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("select count(*) from test")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #COUNT(UInt8(1))\
    //         \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
    //         \n    TableScan: test projection=None";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // #[test]
    // fn optimize_count_sum() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();
    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics: Vec::new(),
    //             is_exact: true,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("select sum(a)/count(*) from test")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #SUM(test.a) Divide #COUNT(UInt8(1))\
    //         \n  Projection: UInt64(100) AS COUNT(Uint8(1)), #SUM(test.a)\
    //         \n    Aggregate: groupBy=[[]], aggr=[[SUM(#test.a)]]\
    //         \n      TableScan: test projection=None";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // #[test]
    // fn optimize_count_group_by() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();
    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics: Vec::new(),
    //             is_exact: true,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("SELECT count(*), a FROM test GROUP BY a")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #COUNT(UInt8(1)), #test.a\
    //         \n  Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(UInt8(1))]]\
    //         \n    TableScan: test projection=None";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // #[test]
    // fn optimize_count_filter() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();
    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics: Vec::new(),
    //             is_exact: true,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("SELECT count(*) FROM test WHERE a < 5")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #COUNT(UInt8(1))\
    //         \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
    //         \n    Filter: #test.a Lt Int64(5)\
    //         \n      TableScan: test projection=None";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // #[test]
    // fn optimize_max_min_using_statistics() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();

    //     let column_statistic = ColumnStatistics {
    //         null_count: None,
    //         max_value: Some(ScalarValue::from(100_i64)),
    //         min_value: Some(ScalarValue::from(1_i64)),
    //         distinct_count: None,
    //     };
    //     let column_statistics = vec![column_statistic];

    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics,
    //             is_exact: true,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("select max(a), min(a) from test")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #MAX(test.a), #MIN(test.a)\
    //         \n  Projection: Int64(100) AS MAX(a), Int64(1) AS MIN(a)\
    //         \n    EmptyRelation";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // #[test]
    // fn optimize_max_min_not_using_statistics() -> Result<()> {
    //     use crate::execution::context::ExecutionContext;
    //     let mut ctx = ExecutionContext::new();
    //     ctx.register_table(
    //         "test",
    //         Arc::new(TestTableProvider {
    //             num_rows: 100,
    //             column_statistics: Vec::new(),
    //             is_exact: true,
    //         }),
    //     )
    //     .unwrap();

    //     let plan = ctx
    //         .create_logical_plan("select max(a), min(a) from test")
    //         .unwrap();
    //     let expected = "\
    //         Projection: #MAX(test.a), #MIN(test.a)\
    //         \n  Aggregate: groupBy=[[]], aggr=[[MAX(#test.a), MIN(#test.a)]]\
    //         \n    TableScan: test projection=None";

    //     assert_optimized_plan_eq(&plan, expected);
    //     Ok(())
    // }

    // fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
    //     let opt = AggregateStatistics::new();
    //     let optimized_plan = opt.optimize(plan, &ExecutionProps::new()).unwrap();
    //     let formatted_plan = format!("{:?}", optimized_plan);
    //     assert_eq!(formatted_plan, expected);
    //     assert_eq!(plan.schema(), plan.schema());
    // }
}
