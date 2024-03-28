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

use arrow::datatypes::Schema;
use datafusion_common::{
    not_impl_err, plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
    Result,
};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, TableSource};
use sqlparser::ast::{Query, SetExpr, SetOperator, With};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn plan_with_clause(
        &self,
        with: With,
        planner_context: &mut PlannerContext,
    ) -> Result<()> {
        let is_recursive = with.recursive;
        // Process CTEs from top to bottom
        for cte in with.cte_tables {
            // A `WITH` block can't use the same name more than once
            let cte_name = self.normalizer.normalize(cte.alias.name.clone());
            if planner_context.contains_cte(&cte_name) {
                return plan_err!(
                    "WITH query name {cte_name:?} specified more than once"
                );
            }

            // Create a logical plan for the CTE
            let cte_plan = if is_recursive {
                self.recursive_cte(cte_name.clone(), *cte.query, planner_context)?
            } else {
                self.non_recursive_cte(*cte.query, planner_context)?
            };

            // Each `WITH` block can change the column names in the last
            // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
            let final_plan = self.apply_table_alias(cte_plan, cte.alias)?;
            // Export the CTE to the outer query
            planner_context.insert_cte(cte_name, final_plan);
        }
        Ok(())
    }

    fn non_recursive_cte(
        &self,
        cte_query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // CTE expr don't need extend outer_query_schema,
        // so we clone a new planner_context here.
        let mut cte_planner_context = planner_context.clone();
        self.query_to_plan(cte_query, &mut cte_planner_context)
    }

    fn recursive_cte(
        &self,
        cte_name: String,
        mut cte_query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if !self
            .context_provider
            .options()
            .execution
            .enable_recursive_ctes
        {
            return not_impl_err!("Recursive CTEs are not enabled");
        }

        let (left_expr, right_expr, set_quantifier) = match *cte_query.body {
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                set_quantifier,
            } => (left, right, set_quantifier),
            other => {
                // If the query is not a UNION, then it is not a recursive CTE
                cte_query.body = Box::new(other);
                return self.non_recursive_cte(cte_query, planner_context);
            }
        };

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
        let static_plan =
            self.set_expr_to_plan(*left_expr, &mut planner_context.clone())?;

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
        // Step 2.1: Create a table source for the temporary relation
        let work_table_source = self.context_provider.create_cte_work_table(
            &cte_name,
            Arc::new(Schema::from(static_plan.schema().as_ref())),
        )?;

        // Step 2.2: Create a temporary relation logical plan that will be used
        // as the input to the recursive term
        let work_table_plan = LogicalPlanBuilder::scan(
            cte_name.to_string(),
            work_table_source.clone(),
            None,
        )?
        .build()?;

        let name = cte_name.clone();

        // Step 2.3: Register the temporary relation in the planning context
        // For all the self references in the variadic term, we'll replace it
        // with the temporary relation we created above by temporarily registering
        // it as a CTE. This temporary relation in the planning context will be
        // replaced by the actual CTE plan once we're done with the planning.
        planner_context.insert_cte(cte_name.clone(), work_table_plan);

        // ---------- Step 3: Compile the recursive term ------------------
        // this uses the named_relation we inserted above to resolve the
        // relation. This ensures that the recursive term uses the named relation logical plan
        // and thus the 'continuance' physical plan as its input and source
        let recursive_plan =
            self.set_expr_to_plan(*right_expr, &mut planner_context.clone())?;

        // Check if the recursive term references the CTE itself,
        // if not, it is a non-recursive CTE
        if !has_work_table_reference(&recursive_plan, &work_table_source) {
            // Remove the work table plan from the context
            planner_context.remove_cte(&cte_name);
            // Compile it as a non-recursive CTE
            return self.set_operation_to_plan(
                SetOperator::Union,
                static_plan,
                recursive_plan,
                set_quantifier,
            );
        }

        // ---------- Step 4: Create the final plan ------------------
        // Step 4.1: Compile the final plan
        let distinct = !Self::is_union_all(set_quantifier)?;
        LogicalPlanBuilder::from(static_plan)
            .to_recursive_query(name, recursive_plan, distinct)?
            .build()
    }
}

fn has_work_table_reference(
    plan: &LogicalPlan,
    work_table_source: &Arc<dyn TableSource>,
) -> bool {
    let mut has_reference = false;
    plan.apply(&mut |node| {
        if let LogicalPlan::TableScan(scan) = node {
            if Arc::ptr_eq(&scan.source, work_table_source) {
                has_reference = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    // Closure always return Ok
    .unwrap();
    has_reference
}
