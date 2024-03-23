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

use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, Float32Array, Float64Array},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    execution::{
        context::{QueryPlanner, SessionState},
        runtime_env::RuntimeEnv,
        TaskContext,
    },
    logical_expr::Volatility,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayFormatType, Distribution, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

use datafusion::prelude::*;
use datafusion::{error::Result, physical_plan::functions::make_scalar_function};
use datafusion_common::{
    cast::{as_float32_array, as_float64_array},
    tree_node::{Transformed, TreeNode},
    DFSchemaRef, DataFusionError,
};
use datafusion_expr::{
    expr::ScalarUDF, Extension, LogicalPlan, Subquery, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion_optimizer::{optimize_children, OptimizerConfig, OptimizerRule};
use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use futures::{FutureExt, StreamExt};

// This example demonstrates executing an async user defined function (pow).
// Async UDFs aren't supported at the moment. This is a workaround example.
// In this example a user defined `QueryPlanner` (PowQueryPlanner) and an `OptimizerRule` (PowOptimizerRule) is provided to a SessionState.
// The `QueryPlanner` registers an `ExtensionPlanner` (PowPlanner).
// The `OptimizerRule` (PowOptimizerRule) replaces a `LogicalPlan::Projection` with a `LogicalPlan::Extension`.
// The extension hosts a user defined node (PowNode).
// When creating a physical plan for the extension, the node is casted to a user defined execution plan (PowExec) by the `ExtensionPlanner`.
// On plan execution the user defined async function (pow) is called with a `RecordBatch`.

// create local execution context with an in-memory table and
// register an user defined QueryPlanner and a OptimizerRule.
fn create_context() -> Result<SessionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float32Array::from(vec![2.1, 3.1, 4.1, 5.1])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        ],
    )?;

    // declare a state with a query planner and an optimizer rule
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::with_config_rt(config, runtime)
        .with_query_planner(Arc::new(PowQueryPlanner {}))
        .add_optimizer_rule(Arc::new(PowOptimizerRule {}));

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::with_state(state);

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

// pow is similar to the pow function in the simple_udf example
// in this example the function is async. It's not registered directly in the SessionContext,
// but a placeholder UDF is replaced with a custom Extension.
// The custom Extension manages executing this function on plan execution.
//
// The input type depends on the placeholder UDF types.
// The output type is not from the placeholder UDF, but returned from this function.
//
// Pow calculates a^b in an async context.
async fn pow(batch: Result<RecordBatch>) -> Result<RecordBatch> {
    batch.map(|b| {
        // 1. get columns from batch and cast them
        let args = b.columns();
        let base = as_float32_array(&args[0]).expect("cast failed");
        let exponent = as_float64_array(&args[1]).expect("cast failed");

        // 2. perform the computation
        let array = base
            .iter()
            .zip(exponent.iter())
            .map(|(base, exponent)| {
                match (base, exponent) {
                    // in arrow, any value can be null.
                    // Here we decide to make our UDF to return null when either base or exponent is null.
                    (Some(base), Some(exponent)) => Some((base as f64).powf(exponent)),
                    _ => None,
                }
            })
            .collect::<Float64Array>();

        // 3. Define a new schema and construct a new RecordBatch with the computed data
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // First, declare the placeholder UDF
    let placeholder_pow = |_args: &[ArrayRef]| {
        Err(datafusion_common::DataFusionError::NotImplemented(
            "Not supposed to be executed.".to_string(),
        ))
    };

    // the function above expects an `ArrayRef`, but DataFusion may pass a scalar to a UDF.
    // thus, we use `make_scalar_function` to decorare the closure so that it can handle both Arrays and Scalar values.
    let placeholder_pow = make_scalar_function(placeholder_pow);

    // Next:
    // * give it a name so that it can be recognised
    // * the input type is checked and is important
    // * the output type isn't used in this example, the real function provides the type via a
    // schema
    let placeholder_pow = create_udf(
        "pow",
        vec![DataType::Float64, DataType::Float64],
        Arc::new(DataType::Null),
        // Needs to be volatile to be optimized to the async pow function
        Volatility::Volatile,
        placeholder_pow,
    );

    // at this point, we register the place holder
    ctx.register_udf(placeholder_pow);

    const QUERY: &str = "SELECT pow(a, b) FROM t";

    let df = ctx.sql(QUERY).await?;

    // print the results
    df.show().await?;

    Ok(())
}

struct PowQueryPlanner {}

#[async_trait]
impl QueryPlanner for PowQueryPlanner {
    // Given a `LogicalPlan` created from above, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan Pow nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                PowPlanner {},
            )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

// Physical planner for Pow nodes
struct PowPlanner {}

#[async_trait]
impl ExtensionPlanner for PowPlanner {
    // Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(pow_node) = node.as_any().downcast_ref::<PowNode>() {
                Some(Arc::new(PowExec {
                    schema: pow_node.schema.clone(),
                    inputs: physical_inputs.to_vec(),
                }))
            } else {
                None
            },
        )
    }
}

struct PowOptimizerRule {}

impl OptimizerRule for PowOptimizerRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // recurse down and optimize children first
        let optimized_plan = optimize_children(self, plan, config)?;
        match optimized_plan {
            Some(LogicalPlan::Projection(proj)) => {
                Ok(Some(LogicalPlan::Extension(Extension {
                    node: Arc::new(PowNode {
                        schema: proj.schema.clone(),
                        input: (*proj.input).clone(),
                        expr: proj.expr.clone(),
                    }),
                })))
            }
            Some(optimized_plan) => Ok(Some(optimized_plan)),
            None => match plan {
                LogicalPlan::Projection(proj) => {
                    Ok(Some(LogicalPlan::Extension(Extension {
                        node: Arc::new(PowNode {
                            schema: proj.schema.clone(),
                            input: (*proj.input).clone(),
                            expr: proj.expr.clone(),
                        }),
                    })))
                }
                _ => Ok(None),
            },
        }
    }

    fn name(&self) -> &str {
        "pow"
    }
}

// use rewrite_expr to modify the expression tree.
fn rewrite_expr(expr: Expr, schema: DFSchemaRef, input: &LogicalPlan) -> Result<Expr> {
    expr.transform(&|expr| {
        // closure is invoked for all sub expressions
        Ok(match expr {
            Expr::ScalarUDF(ScalarUDF { fun: _, ref args }) => {
                let subplan = LogicalPlan::Extension(Extension {
                    node: Arc::new(PowNode {
                        schema: schema.clone(),
                        input: input.clone(),
                        expr: vec![args[0].clone()],
                    }),
                });
                // Expr::ScalarSubquery doesn't support LogicalPlan::Extension as a subquery
                Transformed::Yes(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(subplan),
                    outer_ref_columns: vec![],
                }))
            }
            _ => Transformed::No(expr),
        })
    })
}

#[derive(PartialEq, Eq, Hash)]
struct PowNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    expr: Vec<Expr>,
}

impl Debug for PowNode {
    // For PowNode, use explain format for the Debug format. Other types of nodes may
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for PowNode {
    fn name(&self) -> &str {
        "pow"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expr.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "pow expr.len({})", self.expr.len())
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            schema: self.schema.clone(),
            input: inputs[0].clone(),
            expr: exprs.to_vec(),
        }
    }
}

struct PowExec {
    schema: DFSchemaRef,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
}

impl Debug for PowExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PowExec")
    }
}

#[async_trait]
impl ExecutionPlan for PowExec {
    // Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        (*self.schema).clone().into()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PowExec {
            schema: self.schema.clone(),
            inputs: children,
        }))
    }

    // Execute one partition and return an iterator over `RecordBatch`.
    // The iterator calls the user defined async function `pow`.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "PowExec invalid partition {partition}"
            )));
        }
        let s = self.inputs[0]
            .execute(partition, context)?
            .flat_map(|b| pow(b).into_stream());
        let s = RecordBatchStreamAdapter::new((*self.schema).clone().into(), s);
        Ok(Box::pin(s))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "PowExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // to improve the optimizability of this plan
        // better statistics inference could be provided
        Statistics::default()
    }
}
