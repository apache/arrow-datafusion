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

//! [`ApplyFunctionRewrites`] to replace `Expr`s with function calls (e.g `||` to array_concat`)

use super::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result};

use crate::utils::NamePreserver;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::LogicalPlan;
use std::sync::Arc;

/// Analyzer rule that invokes [`FunctionRewrite`]s on expressions
#[derive(Default)]
pub struct ApplyFunctionRewrites {
    /// Expr --> Function writes to apply
    function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

impl ApplyFunctionRewrites {
    pub fn new(function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>) -> Self {
        Self { function_rewrites }
    }

    /// Rewrite a single plan, and all its expressions using the provided rewriters
    fn rewrite_plan(
        &self,
        plan: LogicalPlan,
        options: &ConfigOptions,
    ) -> Result<Transformed<LogicalPlan>> {
        // get schema representing all available input fields. This is used for data type
        // resolution only, so order does not matter here
        let mut schema = merge_schema(plan.inputs());

        if let LogicalPlan::TableScan(ts) = &plan {
            let source_schema = DFSchema::try_from_qualified_schema(
                ts.table_name.clone(),
                &ts.source.schema().as_ref().clone().into(),
            )?;
            schema.merge(&source_schema);
        }

        let name_preserver = NamePreserver::new(&plan);

        plan.map_expressions(|expr| {
            let original_name = name_preserver.save(&expr)?;

            // recursively transform the expression, applying the rewrites at each step
            let transformed_expr = expr.transform_up(|expr| {
                let mut result = Transformed::no(expr);
                for rewriter in self.function_rewrites.iter() {
                    result = result.transform_data(|expr| {
                        rewriter.rewrite(expr, &schema, options)
                    })?;
                }
                Ok(result)
            })?;

            transformed_expr.map_data(|expr| original_name.restore(expr))
        })
    }
}

impl AnalyzerRule for ApplyFunctionRewrites {
    fn name(&self) -> &str {
        "apply_function_rewrites"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| self.rewrite_plan(plan, options))
            .map(|res| res.data)
    }
}
