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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::analyzer::AnalyzerRule;
use std::sync::{Arc, Mutex};

/// This example demonstrates how to add your own [`AnalyzerRule`] to
/// DataFusion.
///
/// [`AnalyzerRule`]s transform [`LogicalPlan`]s prior to the DataFusion
/// optimization process, and can be used to change the plan's semantics (e.g.
/// output types).
///
/// This example shows an `AnalyzerRule` which implements a simplistic of row
/// level access control scheme by introducing a filter to the query.
///
/// See [optimizer_rule.rs] for an example of a optimizer rule
#[tokio::main]
pub async fn main() -> Result<()> {
    // AnalyzerRules run before OptimizerRules.
    //
    // DataFusion includes several built in AnalyzerRules for tasks such as type
    // coercion which change the types of expressions in the plan. Add our new
    // rule to the context to run it during the analysis phase.
    let rule = Arc::new(RowLevelAccessControl::new());
    let ctx = SessionContext::new();
    ctx.add_analyzer_rule(Arc::clone(&rule) as _);

    ctx.register_batch("employee", employee_batch())?;

    // Now, planning any SQL statement also invokes the AnalyzerRule
    let plan = ctx
        .sql("SELECT * FROM employee")
        .await?
        .into_optimized_plan()?;

    // Printing the query plan shows a filter has been added
    //
    // Filter: employee.position = Utf8("Engineer")
    //   TableScan: employee projection=[name, age, position]
    println!("Logical Plan:\n\n{}\n", plan.display_indent());

    // Execute the query, and indeed no Manager's are returned
    //
    // +-----------+-----+----------+
    // | name      | age | position |
    // +-----------+-----+----------+
    // | Andy      | 11  | Engineer |
    // | Oleks     | 33  | Engineer |
    // | Xiangpeng | 55  | Engineer |
    // +-----------+-----+----------+
    ctx.sql("SELECT * FROM employee").await?.show().await?;

    // We can now change the access level to "Manager" and see the results
    //
    // +----------+-----+----------+
    // | name     | age | position |
    // +----------+-----+----------+
    // | Andrew   | 22  | Manager  |
    // | Chunchun | 44  | Manager  |
    // +----------+-----+----------+
    rule.set_show_position("Manager");
    ctx.sql("SELECT * FROM employee").await?.show().await?;

    // The filters introduced by our AanalyzerRule are treated the same as any
    // other filter by the DataFusion optimizer, including predicate push down
    // (including into scans), simplifications, and similar optimizations.
    //
    // For example adding another predicate to the query
    let plan = ctx
        .sql("SELECT * FROM employee WHERE age > 30")
        .await?
        .into_optimized_plan()?;

    // We can see the DataFusion Optimizer has combined the filters together
    // when we print out the plan
    //
    // Filter: employee.age > Int32(30) AND employee.position = Utf8("Manager")
    //   TableScan: employee projection=[name, age, position]
    println!("Logical Plan:\n\n{}\n", plan.display_indent());

    Ok(())
}

/// Example AnalyzerRulw ule that implements a very basic "row level access
/// control"
///
/// In this case, it adds a filter to the plan that removes all managers from
/// the result set.
#[derive(Debug)]
struct RowLevelAccessControl {
    /// Models the current access level of the session
    ///
    /// This is value of the position column which should be included in the
    /// result set. It is wrapped in a `Mutex` so we can change it during query
    show_position: Mutex<String>,
}

impl RowLevelAccessControl {
    fn new() -> Self {
        Self {
            show_position: Mutex::new("Engineer".to_string()),
        }
    }

    /// return the current position to show, as an expression
    fn show_position(&self) -> Expr {
        lit(self.show_position.lock().unwrap().clone())
    }

    /// specifies a different position to show in the result set
    fn set_show_position(&self, access_level: impl Into<String>) {
        *self.show_position.lock().unwrap() = access_level.into();
    }
}

impl AnalyzerRule for RowLevelAccessControl {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // use the TreeNode API to recursively walk the LogicalPlan tree
        // and all of its children (inputs)
        let transfomed_plan = plan.transform(|plan| {
            // This closure is called for each LogicalPlan node
            // if it is a Scan node, add a filter to remove all managers
            if is_employee_table_scan(&plan) {
                // Use the LogicalPlanBuilder to add a filter to the plan
                let filter = LogicalPlanBuilder::from(plan)
                    // Filter Expression: position = <access level>
                    .filter(col("position").eq(self.show_position()))?
                    .build()?;

                // `Transformed::yes` signals the plan was changed
                Ok(Transformed::yes(filter))
            } else {
                // `Transformed::no`
                // signals the plan was not changed
                Ok(Transformed::no(plan))
            }
        })?;

        // the result of calling transform is a `Transformed` structure which
        // contains
        //
        // 1. a flag signaling if any rewrite took place
        // 2. a flag if the recursion stopped early
        // 3. The actual transformed data (a LogicalPlan in this case)
        //
        // This example does not need the value of either flag, so simply
        // extract the LogicalPlan "data"
        Ok(transfomed_plan.data)
    }

    fn name(&self) -> &str {
        "table_access"
    }
}

fn is_employee_table_scan(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::TableScan(scan) = plan {
        scan.table_name.table() == "employee"
    } else {
        false
    }
}

/// Return a RecordBatch with made up data about fictional employees
fn employee_batch() -> RecordBatch {
    let name: ArrayRef = Arc::new(StringArray::from_iter_values([
        "Andy",
        "Andrew",
        "Oleks",
        "Chunchun",
        "Xiangpeng",
    ]));
    let age: ArrayRef = Arc::new(Int32Array::from(vec![11, 22, 33, 44, 55]));
    let position = Arc::new(StringArray::from_iter_values([
        "Engineer", "Manager", "Engineer", "Manager", "Engineer",
    ]));
    RecordBatch::try_from_iter(vec![("name", name), ("age", age), ("position", position)])
        .unwrap()
}
