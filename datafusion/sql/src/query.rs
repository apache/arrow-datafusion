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

use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use datafusion_common::{
    plan_err, sql_err, Constraints, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::ast::{
    Expr as SQLExpr, Offset as SQLOffset, OrderByExpr, Query, SetExpr, SetOperator,
    SetQuantifier, Value,
};

use sqlparser::parser::ParserError::ParserError;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logical plan from an SQL query
    pub(crate) fn query_to_plan(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.query_to_plan_with_schema(query, planner_context)
    }

    /// Generate a logic plan from an SQL query.
    /// It's implementation of `subquery_to_plan` and `query_to_plan`.
    /// It shouldn't be invoked directly.
    fn query_to_plan_with_schema(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let set_expr = query.body;
        if let Some(with) = query.with {
            // Process CTEs from top to bottom
            let is_recursive = with.recursive;

            for cte in with.cte_tables {
                // A `WITH` block can't use the same name more than once
                let cte_name = self.normalizer.normalize(cte.alias.name.clone());
                if planner_context.contains_cte(&cte_name) {
                    return sql_err!(ParserError(format!(
                        "WITH query name {cte_name:?} specified more than once"
                    )));
                }
                let cte_query = cte.query;
                if is_recursive {
                    match *cte_query.body {
                        SetExpr::SetOperation {
                            op: SetOperator::Union,
                            left,
                            right,
                            set_quantifier,
                        } => {
                            let distinct = set_quantifier != SetQuantifier::All;

                            // Each recursive CTE consists from two parts in the logical plan:
                            //   1. A static term   (the left hand side on the SQL, where the
                            //                       referencing to the same CTE is not allowed)
                            //
                            //   2. A recursive term (the right hand side, and the recursive
                            //                       part)

                            // Since static term does not have any specific properties, it can
                            // be compiled as if it was a regular expression. This will
                            // allow us to infer the schema to be used in the recursive term.

                            // ---------- Step 1: Compile the static term ------------------
                            let static_plan = self
                                .set_expr_to_plan(*left, &mut planner_context.clone())?;

                            // Since the recursive CTEs include a component that references a
                            // table with its name, like the example below:
                            //
                            // WITH RECURSIVE values(n) AS (
                            //      SELECT 1 as n -- static term
                            //    UNION ALL
                            //      SELECT n + 1
                            //      FROM values -- self reference
                            //      WHERE n < 100
                            // )
                            //
                            // We need a temporary 'relation' to be referenced and used. PostgreSQL
                            // calls this a 'working table', but it is entirely an implementation
                            // detail and a 'real' table with that name might not even exist (as
                            // in the case of DataFusion).
                            //
                            // Since we can't simply register a table during planning stage (it is
                            // an execution problem), we'll use a relation object that preserves the
                            // schema of the input perfectly and also knows which recursive CTE it is
                            // bound to.

                            // ---------- Step 2: Create a temporary relation ------------------
                            // Step 2.1: Create a schema for the temporary relation
                            let static_fields = static_plan.schema().fields().clone();
                            let static_metadata = static_plan.schema().metadata().clone();

                            let named_relation_schema = DFSchema::new_with_metadata(
                                // take the fields from the static plan
                                // but add the cte_name as the qualifier
                                // so that we can access the fields in the recursive term using
                                // the cte_name as the qualifier (e.g. table.id)
                                static_fields
                                    .into_iter()
                                    .map(|field| {
                                        if field.qualifier().is_some() {
                                            field
                                        } else {
                                            field.with_qualifier(cte_name.clone())
                                        }
                                    })
                                    .collect(),
                                static_metadata,
                            )?;

                            // Step 2.2: Create a temporary relation logical plan that will be used
                            // as the input to the recursive term
                            let named_relation = LogicalPlanBuilder::named_relation(
                                cte_name.as_str(),
                                Arc::new(named_relation_schema),
                            )
                            .build()?;

                            // Step 2.3: Register the temporary relation in the planning context
                            // For all the self references in the variadic term, we'll replace it
                            // with the temporary relation we created above by temporarily registering
                            // it as a CTE. This temporary relation in the planning context will be
                            // replaced by the actual CTE plan once we're done with the planning.
                            planner_context.insert_cte(cte_name.clone(), named_relation);

                            // ---------- Step 3: Compile the recursive term ------------------
                            // this uses the named_relation we inserted above to resolve the
                            // relation. This ensures that the recursive term uses the named relation logical plan
                            // and thus the 'continuance' physical plan as its input and source
                            let recursive_plan = self
                                .set_expr_to_plan(*right, &mut planner_context.clone())?;

                            // ---------- Step 4: Create the final plan ------------------
                            // Step 4.1: Compile the final plan
                            let final_plan = LogicalPlanBuilder::from(static_plan)
                                .to_recursive_query(
                                    cte_name.clone(),
                                    recursive_plan,
                                    distinct,
                                )?
                                .build()?;

                            // Step 4.2: Remove the temporary relation from the planning context and replace it
                            // with the final plan.
                            planner_context.insert_cte(cte_name.clone(), final_plan);
                        }
                        _ => {
                            return Err(DataFusionError::SQL(ParserError(
                                "Invalid recursive CTE".to_string(),
                            )));
                        }
                    };
                } else {
                    // create logical plan & pass backreferencing CTEs
                    // CTE expr don't need extend outer_query_schema
                    let logical_plan =
                        self.query_to_plan(*cte_query, &mut planner_context.clone())?;

                    // Each `WITH` block can change the column names in the last
                    // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
                    let logical_plan = self.apply_table_alias(logical_plan, cte.alias)?;

                    planner_context.insert_cte(cte_name, logical_plan);
                }
            }
        }
        let plan = self.set_expr_to_plan(*(set_expr.clone()), planner_context)?;
        let plan = self.order_by(plan, query.order_by, planner_context)?;
        let plan = self.limit(plan, query.offset, query.limit)?;

        let plan = match *set_expr {
            SetExpr::Select(select) if select.into.is_some() => {
                let select_into = select.into.unwrap();
                LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name: self.object_name_to_table_reference(select_into.name)?,
                    constraints: Constraints::empty(),
                    input: Arc::new(plan),
                    if_not_exists: false,
                    or_replace: false,
                }))
            }
            _ => plan,
        };

        Ok(plan)
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        skip: Option<SQLOffset>,
        fetch: Option<SQLExpr>,
    ) -> Result<LogicalPlan> {
        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        let skip = match skip {
            Some(skip_expr) => match self.sql_to_expr(
                skip_expr.value,
                input.schema(),
                &mut PlannerContext::new(),
            )? {
                Expr::Literal(ScalarValue::Int64(Some(s))) => {
                    if s < 0 {
                        return plan_err!("Offset must be >= 0, '{s}' was provided.");
                    }
                    Ok(s as usize)
                }
                _ => plan_err!("Unexpected expression in OFFSET clause"),
            }?,
            _ => 0,
        };

        let fetch = match fetch {
            Some(limit_expr)
                if limit_expr != sqlparser::ast::Expr::Value(Value::Null) =>
            {
                let n = match self.sql_to_expr(
                    limit_expr,
                    input.schema(),
                    &mut PlannerContext::new(),
                )? {
                    Expr::Literal(ScalarValue::Int64(Some(n))) if n >= 0 => {
                        Ok(n as usize)
                    }
                    _ => plan_err!("LIMIT must not be negative"),
                }?;
                Some(n)
            }
            _ => None,
        };

        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<OrderByExpr>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        let order_by_rex =
            self.order_by_to_sort_expr(&order_by, plan.schema(), planner_context)?;

        if let LogicalPlan::Distinct(Distinct::On(ref distinct_on)) = plan {
            // In case of `DISTINCT ON` we must capture the sort expressions since during the plan
            // optimization we're effectively doing a `first_value` aggregation according to them.
            let distinct_on = distinct_on.clone().with_sort_expr(order_by_rex)?;
            Ok(LogicalPlan::Distinct(Distinct::On(distinct_on)))
        } else {
            LogicalPlanBuilder::from(plan).sort(order_by_rex)?.build()
        }
    }
}
