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

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
};

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_expr::LogicalPlan;

/// Non-unique identifier of a [`LogicalPlan`].
///
/// See [`LogicalPlanSignature::new`] for details.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogicalPlanSignature {
    node_number: NonZeroUsize,
    plan_hash: u64,
}

impl LogicalPlanSignature {
    /// Returns [`LogicalPlanSignature`] of the given [`LogicalPlan`].
    ///
    /// It is a kind of [`LogicalPlan`] hashing with stronger guarantees.
    ///
    /// # Guarantees
    ///
    /// Consider two [`LogicalPlan`]s `p1` and `p2`.
    ///
    /// If `p1` and `p2` have a different number of [`LogicalPlan`]s, then
    /// they will have different [`LogicalPlanSignature`]s.
    ///
    /// If `p1` and `p2` have a different [`Hash`], then
    /// they will have different [`LogicalPlanSignature`]s.
    ///
    /// # Caveats
    ///
    /// The intention of [`LogicalPlanSignature`] is to have a lower chance
    /// of hash collisions.
    ///
    /// There exist different [`LogicalPlan`]s with the same
    /// [`LogicalPlanSignature`].
    ///
    /// When two [`LogicalPlan`]s differ only in metadata, then they will have
    /// the same [`LogicalPlanSignature`]s (due to hash implementation in
    /// [`LogicalPlan`]).
    pub fn new(plan: &LogicalPlan) -> Self {
        let mut hasher = DefaultHasher::new();
        plan.hash(&mut hasher);

        Self {
            node_number: get_node_number(plan),
            plan_hash: hasher.finish(),
        }
    }
}

/// Get total number of [`LogicalPlan`]s in the plan.
fn get_node_number(plan: &LogicalPlan) -> NonZeroUsize {
    let mut node_number = 0;
    plan.apply_with_subqueries(|_plan| {
        node_number += 1;
        Ok(TreeNodeRecursion::Continue)
    })
    // Closure always return Ok
    .unwrap();
    // Visitor must have at least visited the root,
    // so v.node_number is at least 1.
    NonZeroUsize::new(node_number).unwrap()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{lit, LogicalPlan};

    use crate::plan_signature::get_node_number;

    #[test]
    fn node_number_for_some_plan() -> Result<()> {
        let schema = Arc::new(DFSchema::empty());

        let one_node_plan =
            Arc::new(LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&schema),
            }));

        assert_eq!(1, get_node_number(&one_node_plan).get());

        let two_node_plan = Arc::new(LogicalPlan::Projection(
            datafusion_expr::Projection::try_new(vec![lit(1), lit(2)], one_node_plan)?,
        ));

        assert_eq!(2, get_node_number(&two_node_plan).get());

        let five_node_plan = Arc::new(LogicalPlan::Union(datafusion_expr::Union {
            inputs: vec![Arc::clone(&two_node_plan), two_node_plan],
            schema,
        }));

        assert_eq!(5, get_node_number(&five_node_plan).get());

        Ok(())
    }
}
