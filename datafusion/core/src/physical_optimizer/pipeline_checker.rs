// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The [PipelineChecker] rule ensures that a given plan can accommodate its
//! infinite sources, if there are any. It will reject non-runnable query plans
//! that use pipeline-breaking operators on infinite input(s).

use std::sync::Arc;

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::ExecutionPlan;

use datafusion_common::config::OptimizerOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{plan_err, DataFusionError};
use datafusion_physical_expr::intervals::utils::{check_support, is_datatype_supported};
use datafusion_physical_plan::joins::SymmetricHashJoinExec;

/// The PipelineChecker rule rejects non-runnable query plans that use
/// pipeline-breaking operators on infinite input(s).
#[derive(Default)]
pub struct PipelineChecker {}

impl PipelineChecker {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for PipelineChecker {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (plan, _) =
            plan.transform_up_with_payload(&mut |p, children_unbounded| {
                check_finiteness_requirements(p, children_unbounded, &config.optimizer)
            })?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "PipelineChecker"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function propagates finiteness information and rejects any plan with
/// pipeline-breaking operators acting on infinite inputs.
pub fn check_finiteness_requirements(
    plan: Arc<dyn ExecutionPlan>,
    children_unbounded: Vec<bool>,
    optimizer_options: &OptimizerOptions,
) -> Result<(Transformed<Arc<dyn ExecutionPlan>>, bool)> {
    if let Some(exec) = plan.as_any().downcast_ref::<SymmetricHashJoinExec>() {
        if !(optimizer_options.allow_symmetric_joins_without_pruning
            || (exec.check_if_order_information_available()? && is_prunable(exec)))
        {
            return plan_err!("Join operation cannot operate on a non-prunable stream without enabling \
                              the 'allow_symmetric_joins_without_pruning' configuration flag");
        }
    }
    plan.unbounded_output(children_unbounded.as_slice())
        .map(|value| (Transformed::No(plan), value))
}

/// This function returns whether a given symmetric hash join is amenable to
/// data pruning. For this to be possible, it needs to have a filter where
/// all involved [`PhysicalExpr`]s, [`Operator`]s and data types support
/// interval calculations.
///
/// [`PhysicalExpr`]: crate::physical_plan::PhysicalExpr
/// [`Operator`]: datafusion_expr::Operator
fn is_prunable(join: &SymmetricHashJoinExec) -> bool {
    join.filter().map_or(false, |filter| {
        check_support(filter.expression(), &join.schema())
            && filter
                .schema()
                .fields()
                .iter()
                .all(|f| is_datatype_supported(f.data_type()))
    })
}

#[cfg(test)]
mod sql_tests {
    use super::*;
    use crate::physical_optimizer::test_utils::{
        BinaryTestCase, QueryCase, SourceType, UnaryTestCase,
    };

    #[tokio::test]
    async fn test_hash_left_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: false,
        };

        let test2 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 LEFT JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_right_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: true,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: false,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 RIGHT JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_inner_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: false,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: false,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_full_outer_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: true,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 FULL JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2), Arc::new(test3)],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "SELECT c1, MIN(c4) FROM test GROUP BY c1".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Aggregate Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_hash_partition() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "SELECT
                    c9,
                    SUM(c9) OVER(PARTITION BY c1 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) as sum1
                  FROM test
                  LIMIT 5".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Sort Error".to_string()
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_single_partition() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "SELECT
                        c9,
                        SUM(c9) OVER(ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) as sum1
                  FROM test".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Sort Error".to_string()
        };
        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_cross_join() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: true,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test4 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 CROSS JOIN right as t2".to_string(),
            cases: vec![
                Arc::new(test1),
                Arc::new(test2),
                Arc::new(test3),
                Arc::new(test4),
            ],
            error_operator: "Cross Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_analyzer() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "EXPLAIN ANALYZE SELECT * FROM test".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Analyze Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }
}
