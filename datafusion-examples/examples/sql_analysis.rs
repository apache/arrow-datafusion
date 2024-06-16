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

//! This example shows how we can use the structures that DataFusion provide to perform
//! SQL Analysis.
//! We'll show how we can count the amount of join's in a query as well as how many
//! join tree's there are

use std::{fs, sync::Arc};

use datafusion::common::Result;
use datafusion::{
    datasource::MemTable,
    execution::context::{SessionConfig, SessionContext},
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::LogicalPlan;
use test_utils::tpcds::tpcds_schemas;

/// Counts the total amount of joins in a plan
fn total_join_count(plan: &LogicalPlan) -> usize {
    let mut total = 0;

    // We can use the TreeNode API to walk over a LogicalPlan.
    plan.apply(|node| {
        // if we encouter a join we update the total
        if matches!(node, LogicalPlan::Join(_) | LogicalPlan::CrossJoin(_)) {
            total += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    total
}

/// Counts the total amount of joins and collects every join group with it's join count
/// The list of groupts summes to the total count
fn count_trees(plan: &LogicalPlan) -> (usize, Vec<usize>) {
    // this works the same way as `total_count`, but now when we encounter a Join
    // we try to collect it's entire tree
    let mut to_visit = vec![plan];
    let mut total = 0;
    let mut groups = vec![];

    while let Some(node) = to_visit.pop() {
        // if we encouter a join, we know were at the root of the tree
        // so count this group and later start recursing on it's children
        if matches!(node, LogicalPlan::Join(_) | LogicalPlan::CrossJoin(_)) {
            let (group_count, inputs) = count_tree(node);
            total += group_count;
            groups.push(group_count);
            to_visit.extend(inputs);
        } else {
            to_visit.extend(node.inputs());
        }
    }

    (total, groups)
}

// count the entire join tree and return it's inputs using TreeNode API
fn count_tree(join: &LogicalPlan) -> (usize, Vec<&LogicalPlan>) {
    let mut inputs = Vec::new();
    let mut total = 0;

    join.apply(|node| {
        // Some extra knowledge:
        // optimized plans have their projections pushed down as far as possible, which sometimes results in a projection going in between 2 subsequent joins
        // giving the illusion these joins are not "related", when in fact they are.
        // just continue the recursion in this case
        if let LogicalPlan::Projection(_) = node {
            return Ok(TreeNodeRecursion::Continue);
        }

        // any join we count
        if matches!(node, LogicalPlan::Join(_) | LogicalPlan::CrossJoin(_)) {
            total += 1;
            Ok(TreeNodeRecursion::Continue)
        } else {
            inputs.push(node);
            // skip children of input node
            Ok(TreeNodeRecursion::Jump)
        }
    })
    .unwrap();

    (total, inputs)
}

#[tokio::main]
async fn main() -> Result<()> {
    // To show how we can count the joins in a sql query we'll be using query 88 from the
    // TPC-DS benchmark. It has a lot of joins, cross-joins and multiple join-trees, perfect for our example

    let config = SessionConfig::default();
    let ctx = SessionContext::new_with_config(config);

    // register the tables of the TPC-DS query
    let tables = tpcds_schemas();
    for table in tables {
        ctx.register_table(
            table.name,
            Arc::new(MemTable::try_new(Arc::new(table.schema.clone()), vec![])?),
        )?;
    }

    let query_88 = "datafusion/core/tests/tpc-ds/88.sql";
    let sql = fs::read_to_string(query_88).expect("Could not read query");

    // We can create a logicalplan from a SQL query like this
    let logical_plan = ctx.sql(&sql).await?.into_optimized_plan()?;

    println!(
        "Optimized Logical Plan:\n\n{}\n",
        logical_plan.display_indent()
    );
    // we can get the total count (query 88 has 31 joins: 7 CROSS joins and 24 INNER joins => 40 input relations)
    let total_join_count = total_join_count(&logical_plan);
    assert_eq!(31, total_join_count);

    println!("The plan has {total_join_count} joins.");

    // Furthermore the 24 inner joins are 8 groups of 3 joins with the 7 cross-joins combining them
    // we can get these groups using the `count_trees` method
    let (total_join_count, trees) = count_trees(&logical_plan);
    assert_eq!(
        (total_join_count, &trees),
        // query 88 is very straightforward, we know the cross-join group is at the top of the plan followed by the INNER joins
        (31, &vec![7, 3, 3, 3, 3, 3, 3, 3, 3])
    );

    println!(
        "And following join-trees (number represents join amount in tree): {trees:?}"
    );

    Ok(())
}
