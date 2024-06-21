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

//! [`EliminateOneUnion`]  eliminates single element `Union`
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{internal_err, tree_node::Transformed, Result};
use datafusion_expr::logical_plan::{tree_node::unwrap_arc, LogicalPlan, Union};

use crate::optimizer::ApplyOrder;

#[derive(Default)]
/// An optimization rule that eliminates union with one element.
pub struct EliminateOneUnion;

impl EliminateOneUnion {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateOneUnion {
    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        internal_err!("Should have called EliminateOneUnion::rewrite")
    }

    fn name(&self) -> &str {
        "eliminate_one_union"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Union(Union { mut inputs, .. }) if inputs.len() == 1 => {
                Ok(Transformed::yes(unwrap_arc(inputs.pop().unwrap())))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{
        expr_rewriter::coerce_plan_expr_for_schema, logical_plan::table_scan,
    };
    use std::sync::Arc;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ])
    }

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_with_rules(
            vec![Arc::new(EliminateOneUnion::new())],
            plan,
            expected,
            true,
        )
    }

    #[test]
    fn eliminate_nothing() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .build()?;

        let expected = "\
        Union\
        \n  TableScan: table\
        \n  TableScan: table";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_one_union() -> Result<()> {
        let table_plan = coerce_plan_expr_for_schema(
            &table_scan(Some("table"), &schema(), None)?.build()?,
            &schema().to_dfschema()?,
        )?;
        let schema = table_plan.schema().clone();
        let single_union_plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(table_plan)],
            schema,
        });

        let expected = "TableScan: table";
        assert_optimized_plan_equal(single_union_plan, expected)
    }
}
