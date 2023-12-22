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

use crate::analyzer::AnalyzerRule;

use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::DataFusionError;
use datafusion_common::{internal_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::rewrite_preserving_name;
use datafusion_expr::{BuiltinScalarFunction, ScalarFunctionDefinition};

use datafusion_execution::FunctionRegistry;
use datafusion_expr::{logical_plan::LogicalPlan, Expr};
use std::str::FromStr;

use crate::analyzer::AnalyzerConfig;

/// Resolves `ScalarFunctionDefinition::Name` at execution time.
///

pub struct ResolveFunctionByName {}

impl ResolveFunctionByName {
    pub fn new() -> Self {
        ResolveFunctionByName {}
    }
}

impl Default for ResolveFunctionByName {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzerRule for ResolveFunctionByName {
    fn analyze(
        &self,
        plan: LogicalPlan,
        config: &dyn AnalyzerConfig,
    ) -> Result<LogicalPlan> {
        analyze_internal(&plan, config.function_registry())
    }

    fn name(&self) -> &str {
        "resolve_function_by_name"
    }
}

fn analyze_internal(
    plan: &LogicalPlan,
    registry: &dyn FunctionRegistry,
) -> Result<LogicalPlan> {
    // optimize child plans first
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|p| analyze_internal(p, registry))
        .collect::<Result<Vec<_>>>()?;

    let mut expr_rewrite = FunctionResolverRewriter { registry };

    let new_expr = plan
        .expressions()
        .into_iter()
        .map(|expr| rewrite_preserving_name(expr, &mut expr_rewrite))
        .collect::<Result<Vec<_>>>()?;
    plan.with_new_exprs(new_expr, &new_inputs)
}

struct FunctionResolverRewriter<'a> {
    registry: &'a dyn FunctionRegistry,
}

impl<'a> TreeNodeRewriter for FunctionResolverRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, old_expr: Expr) -> Result<Expr> {
        match old_expr.clone() {
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::Name(name),
                args,
            }) => {
                // user-defined function (UDF) should have precedence in case it has the same name as a scalar built-in function
                if let Ok(fm) = self.registry.udf(&name) {
                    return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fm, args)));
                }

                // next, scalar built-in
                if let Ok(fun) = BuiltinScalarFunction::from_str(&name) {
                    return Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)));
                }
                internal_err!("Unknown scalar function")
            }
            _ => Ok(old_expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
        Volatility,
    };
    use std::sync::Arc;

    use super::*;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockRegistry {}

    impl FunctionRegistry for MockRegistry {
        fn udfs(&self) -> std::collections::HashSet<String> {
            todo!()
        }

        fn udf(&self, name: &str) -> Result<Arc<datafusion_expr::ScalarUDF>> {
            if name == "my-udf" {
                let return_type: ReturnTypeFunction =
                    Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
                let fun: ScalarFunctionImplementation = Arc::new(move |_| {
                    Ok(ColumnarValue::Scalar(ScalarValue::new_utf8("a")))
                });
                return Ok(Arc::new(datafusion_expr::ScalarUDF::new(
                    "my-udf",
                    &Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
                    &return_type,
                    &fun,
                )));
            }
            internal_err!("function not found")
        }

        fn udaf(&self, _name: &str) -> Result<Arc<datafusion_expr::AggregateUDF>> {
            todo!()
        }

        fn udwf(&self, _name: &str) -> Result<Arc<datafusion_expr::WindowUDF>> {
            todo!()
        }
    }

    fn rewrite(function: ScalarFunction) -> Result<Expr, DataFusionError> {
        let registry = MockRegistry {};
        let mut rewriter = FunctionResolverRewriter {
            registry: &registry,
        };
        rewriter.mutate(Expr::ScalarFunction(function))
    }

    #[test]
    fn rewriter_rewrites_builtin_correctly() {
        let function = datafusion_expr::expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::Name(Arc::from("log10")),
            args: vec![],
        };
        let result = rewrite(function);
        assert!(matches!(
            result,
            Ok(Expr::ScalarFunction(
                datafusion_expr::expr::ScalarFunction {
                    func_def: ScalarFunctionDefinition::BuiltIn(
                        BuiltinScalarFunction::Log
                    ),
                    ..
                }
            ))
        ));
    }
    #[test]
    fn rewriter_rewrites_udf_correctly() {
        let function = datafusion_expr::expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::Name(Arc::from("my-udf")),
            args: vec![],
        };
        let result = rewrite(function);
        if let Ok(Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(udf),
            ..
        })) = result
        {
            assert_eq!(udf.name(), "my-udf");
        } else {
            panic!("Pattern did not match");
        }
    }
    #[test]
    fn rewriter_fails_unknown_function() {
        let function = datafusion_expr::expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::Name(Arc::from("my-udf-invalid")),
            args: vec![],
        };
        let result = rewrite(function);
        assert!(result.is_err());
    }
}
