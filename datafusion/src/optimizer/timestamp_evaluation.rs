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

//! Optimizer rule to replace timestamp expressions to constants.
//! This saves time in planning and executing the query.
use crate::error::Result;
use crate::logical_plan::{Expr, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;

use super::utils;
use crate::physical_plan::functions::BuiltinScalarFunction;
use crate::scalar::ScalarValue;
use chrono::{DateTime, Utc};

/// Optimization rule that replaces timestamp expressions with their values evaluated
pub struct TimestampEvaluation {
    timestamp: DateTime<Utc>,
}

impl TimestampEvaluation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn optimize_now(&self, exp: &Expr) -> Expr {
        match exp {
            Expr::ScalarFunction { fun, .. } => match fun {
                BuiltinScalarFunction::Now => {
                    Expr::Literal(ScalarValue::TimestampNanosecond(Some(
                        self.timestamp.timestamp_nanos(),
                    )))
                }
                _ => exp.clone(),
            },
            Expr::Alias(inner_exp, alias) => {
                Expr::Alias(Box::new(self.optimize_now(inner_exp)), alias.clone())
            }
            _ => {
                exp.clone()
            }
        }
    }
}

impl OptimizerRule for TimestampEvaluation {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { .. } => {
                let exprs = plan
                    .expressions()
                    .iter()
                    .map(|exp| self.optimize_now(exp))
                    .collect::<Vec<_>>();

                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(*plan))
                    .collect::<Result<Vec<_>>>()?;

                println!("plan is {:?}", &plan);

                utils::from_plan(plan, &exprs, &new_inputs)
            }
            _ => {
                let expr = plan.expressions();

                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(*plan))
                    .collect::<Result<Vec<_>>>()?;

                println!("plan is {:?}", &plan);
                utils::from_plan(plan, &expr, &new_inputs)
            }
        }
    }

    fn name(&self) -> &str {
        "timestamp_evaluation"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::LogicalPlanBuilder;
    use crate::test::*;

    fn get_optimized_plan_formatted(plan: &LogicalPlan) -> String {
        let rule = TimestampEvaluation::new();
        let optimized_plan = rule.optimize(plan).expect("failed to optimize plan");
        return format!("{:?}", optimized_plan);
    }

    #[test]
    fn single_now() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }];
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: TimestampNanosecond(";
        assert!(get_optimized_plan_formatted(&plan).starts_with(expected));
    }

    #[test]
    fn double_now() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }, Expr::Alias(Box::new(
            Expr::ScalarFunction {
                args: vec![],
                fun: BuiltinScalarFunction::Now,
            }
        ), "t2".to_string())];
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let actual = get_optimized_plan_formatted(&plan);
        println!("output is {}", &actual);
        let expected_start = "Projection: TimestampNanosecond(";
        assert!(actual.starts_with(expected_start));

        let expected_end = ") AS t2\
             \n  TableScan: test projection=None";
        assert!(actual.ends_with(expected_end));
    }
}
