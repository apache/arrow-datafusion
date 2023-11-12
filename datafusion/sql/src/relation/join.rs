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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{not_impl_err, Column, DataFusionError, Result};
use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableWithJoins};
use std::collections::HashSet;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // From clause may exist CTEs, we should separate them from global CTEs.
        // CTEs in from clause are allowed to be duplicated.
        // Such as `select * from (WITH source AS (select 1 as e) SELECT * FROM source) t1, (WITH source AS (select 1 as e) SELECT * FROM source) t2;` which is valid.
        // So always use original global CTEs to plan CTEs in from clause.
        // Btw, don't need to add CTEs in from to global CTEs.
        let origin_planner_context = planner_context.clone();
        let left = self.create_relation(t.relation, planner_context)?;
        match t.joins.len() {
            0 => {
                *planner_context = origin_planner_context;
                Ok(left)
            }
            _ => {
                let mut joins = t.joins.into_iter();
                *planner_context = origin_planner_context.clone();
                let mut left = self.parse_relation_join(
                    left,
                    joins.next().unwrap(), // length of joins > 0
                    planner_context,
                )?;
                for join in joins {
                    *planner_context = origin_planner_context.clone();
                    left = self.parse_relation_join(left, join, planner_context)?;
                }
                *planner_context = origin_planner_context;
                Ok(left)
            }
        }
    }

    fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let right = self.create_relation(join.relation, planner_context)?;
        match join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::LeftSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftSemi,
                planner_context,
            ),
            JoinOperator::RightSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightSemi,
                planner_context,
            ),
            JoinOperator::LeftAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftAnti,
                planner_context,
            ),
            JoinOperator::RightAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightAnti,
                planner_context,
            ),
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin => self.parse_cross_join(left, right),
            other => not_impl_err!("Unsupported JOIN operator {other:?}"),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let join_schema = left.schema().join(right.schema())?;
                // parse ON expression
                let expr = self.sql_to_expr(sql_expr, &join_schema, planner_context)?;
                LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(expr))?
                    .build()
            }
            JoinConstraint::Using(idents) => {
                let keys: Vec<Column> = idents
                    .into_iter()
                    .map(|x| Column::from_name(self.normalizer.normalize(x)))
                    .collect();
                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                let left_cols: HashSet<&String> =
                    left.schema().fields().iter().map(|f| f.name()).collect();
                let keys: Vec<Column> = right
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .filter(|f| left_cols.contains(f))
                    .map(Column::from_name)
                    .collect();
                if keys.is_empty() {
                    self.parse_cross_join(left, right)
                } else {
                    LogicalPlanBuilder::from(left)
                        .join_using(right, join_type, keys)?
                        .build()
                }
            }
            JoinConstraint::None => not_impl_err!("NONE constraint is not supported"),
        }
    }
}
