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

//! Physical query planner

use super::analyze::AnalyzeExec;
use super::{
    aggregates, empty::EmptyExec, expressions::binary, functions,
    hash_join::PartitionMode, udaf, union::UnionExec, values::ValuesExec, windows,
};
use crate::execution::context::{ExecutionProps, SessionState};
use crate::logical_plan::plan::{
    source_as_provider, Aggregate, EmptyRelation, Filter, Join, Projection, Sort,
    SubqueryAlias, TableScan, Window,
};
use crate::logical_plan::{
    unalias, unnormalize_cols, CrossJoin, DFSchema, Expr, LogicalPlan, Operator,
    Partitioning as LogicalPartitioning, PlanType, Repartition, ToStringifiedPlan, Union,
    UserDefinedLogicalNode,
};
use crate::logical_plan::{Limit, Values};
use crate::physical_optimizer::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
use crate::physical_plan::cross_join::CrossJoinExec;
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions;
use crate::physical_plan::expressions::{
    CaseExpr, Column, GetIndexedFieldExpr, Literal, PhysicalSortExpr,
};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::hash_join::HashJoinExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::udf;
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::{join_utils, Partitioning};
use crate::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, WindowExpr};
use crate::scalar::ScalarValue;
use crate::sql::utils::{generate_sort_key, window_expr_common_partition_keys};
use crate::variable::VarType;
use crate::{
    error::{DataFusionError, Result},
    physical_plan::displayable,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::{compute::can_cast_types, datatypes::DataType};
use async_trait::async_trait;
use datafusion_expr::expr::GroupingSet;
use datafusion_physical_expr::expressions::DateIntervalExpr;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use log::{debug, trace};
use std::sync::Arc;

fn create_function_physical_name(
    fun: &str,
    distinct: bool,
    args: &[Expr],
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_physical_name(e, false))
        .collect::<Result<_>>()?;

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

fn physical_name(e: &Expr) -> Result<String> {
    create_physical_name(e, true)
}

fn create_physical_name(e: &Expr, is_first_expr: bool) -> Result<String> {
    match e {
        Expr::Column(c) => {
            if is_first_expr {
                Ok(c.name.clone())
            } else {
                Ok(c.flat_name())
            }
        }
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::ScalarVariable(_, variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{} {:?} {}", left, op, right))
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let mut name = "CASE ".to_string();
            if let Some(e) = expr {
                name += &format!("{:?} ", e);
            }
            for (w, t) in when_then_expr {
                name += &format!("WHEN {:?} THEN {:?} ", w, t);
            }
            if let Some(e) = else_expr {
                name += &format!("ELSE {:?} ", e);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast { expr, data_type } => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("CAST({} AS {:?})", expr, data_type))
        }
        Expr::TryCast { expr, data_type } => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("TRY_CAST({} AS {:?})", expr, data_type))
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {}", expr))
        }
        Expr::Negative(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("(- {})", expr))
        }
        Expr::IsNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{} IS NULL", expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{} IS NOT NULL", expr))
        }
        Expr::GetIndexedField { expr, key } => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{}[{}]", expr, key))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_physical_name(&fun.name, false, args)
        }
        Expr::WindowFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => create_function_physical_name(&fun.to_string(), *distinct, args),
        Expr::AggregateUDF { fun, args } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_physical_name(e, false)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => Ok(format!(
                "ROLLUP ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::Cube(exprs) => Ok(format!(
                "CUBE ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::GroupingSets(lists_of_exprs) => {
                let mut strings = vec![];
                for exprs in lists_of_exprs {
                    let exprs_str = exprs
                        .iter()
                        .map(|e| create_physical_name(e, false))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    strings.push(format!("({})", exprs_str));
                }
                Ok(format!("GROUPING SETS ({})", strings.join(", ")))
            }
        },

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = create_physical_name(expr, false)?;
            let list = list.iter().map(|expr| create_physical_name(expr, false));
            if *negated {
                Ok(format!("{} NOT IN ({:?})", expr, list))
            } else {
                Ok(format!("{} IN ({:?})", expr, list))
            }
        }
        Expr::Exists { .. } => Err(DataFusionError::NotImplemented(
            "EXISTS is not yet supported in the physical plan".to_string(),
        )),
        Expr::InSubquery { .. } => Err(DataFusionError::NotImplemented(
            "IN subquery is not yet supported in the physical plan".to_string(),
        )),
        Expr::ScalarSubquery(_) => Err(DataFusionError::NotImplemented(
            "Scalar subqueries are not yet supported in the physical plan".to_string(),
        )),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = create_physical_name(expr, false)?;
            let low = create_physical_name(low, false)?;
            let high = create_physical_name(high, false)?;
            if *negated {
                Ok(format!("{} NOT BETWEEN {} AND {}", expr, low, high))
            } else {
                Ok(format!("{} BETWEEN {} AND {}", expr, low, high))
            }
        }
        Expr::Sort { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support sort expression".to_string(),
        )),
        Expr::Wildcard => Err(DataFusionError::Internal(
            "Create physical name does not support wildcard".to_string(),
        )),
        Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support qualified wildcard".to_string(),
        )),
    }
}

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
#[async_trait]
pub trait PhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    ///
    /// `input_schema`: the physical schema for evaluating `expr`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
pub trait ExtensionPlanner {
    /// Create a physical plan for a [`UserDefinedLogicalNode`].
    ///
    /// `input_dfschema`: the logical plan schema for the inputs to this node
    ///
    /// Returns an error when the planner knows how to plan the concrete
    /// implementation of `node` but errors while doing so.
    ///
    /// Returns `None` when the planner does not know how to plan the
    /// `node` and wants to delegate the planning to another
    /// [`ExtensionPlanner`].
    fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
#[derive(Default)]
pub struct DefaultPhysicalPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.handle_explain(logical_plan, session_state).await? {
            Some(plan) => Ok(plan),
            None => {
                let plan = self
                    .create_initial_plan(logical_plan, session_state)
                    .await?;
                self.optimize_internal(plan, session_state, |_, _| {})
            }
        }
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `e`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `e`
    ///
    /// `input_schema`: the physical schema for evaluating `e`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            &session_state.execution_props,
        )
    }
}

impl DefaultPhysicalPlanner {
    /// Create a physical planner that uses `extension_planners` to
    /// plan user-defined logical nodes [`LogicalPlan::Extension`].
    /// The planner uses the first [`ExtensionPlanner`] to return a non-`None`
    /// plan.
    pub fn with_extension_planners(
        extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        Self { extension_planners }
    }

    /// Create a physical plan from a logical plan
    fn create_initial_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        async move {
            let exec_plan: Result<Arc<dyn ExecutionPlan>> = match logical_plan {
                LogicalPlan::TableScan (TableScan {
                    source,
                    projection,
                    filters,
                    limit,
                    ..
                }) => {
                    let source = source_as_provider(source)?;
                    // Remove all qualifiers from the scan as the provider
                    // doesn't know (nor should care) how the relation was
                    // referred to in the query
                    let filters = unnormalize_cols(filters.iter().cloned());
                    let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                    source.scan(projection, &unaliased, *limit).await
                }
                LogicalPlan::Values(Values {
                    values,
                    schema,
                }) => {
                    let exec_schema = schema.as_ref().to_owned().into();
                    let exprs = values.iter()
                        .map(|row| {
                            row.iter().map(|expr|{
                                self.create_physical_expr(
                                    expr,
                                    schema,
                                    &exec_schema,
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let value_exec = ValuesExec::try_new(
                        SchemaRef::new(exec_schema),
                        exprs
                    )?;
                    Ok(Arc::new(value_exec))
                }
                LogicalPlan::Window(Window {
                    input, window_expr, ..
                }) => {
                    if window_expr.is_empty() {
                        return Err(DataFusionError::Internal(
                            "Impossibly got empty window expression".to_owned(),
                        ));
                    }

                    let input_exec = self.create_initial_plan(input, session_state).await?;

                    // at this moment we are guaranteed by the logical planner
                    // to have all the window_expr to have equal sort key
                    let partition_keys = window_expr_common_partition_keys(window_expr)?;

                    let can_repartition = !partition_keys.is_empty()
                        && session_state.config.target_partitions > 1
                        && session_state.config.repartition_windows;

                    let input_exec = if can_repartition {
                        let partition_keys = partition_keys
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(
                                    e,
                                    input.schema(),
                                    &input_exec.schema(),
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;
                        Arc::new(RepartitionExec::try_new(
                            input_exec,
                            Partitioning::Hash(
                                partition_keys,
                                session_state.config.target_partitions,
                            ),
                        )?)
                    } else {
                        input_exec
                    };

                    // add a sort phase
                    let get_sort_keys = |expr: &Expr| match expr {
                        Expr::WindowFunction {
                            ref partition_by,
                            ref order_by,
                            ..
                        } => generate_sort_key(partition_by, order_by),
                        _ => unreachable!(),
                    };
                    let sort_keys = get_sort_keys(&window_expr[0]);
                    if window_expr.len() > 1 {
                        debug_assert!(
                            window_expr[1..]
                                .iter()
                                .all(|expr| get_sort_keys(expr) == sort_keys),
                            "all window expressions shall have the same sort keys, as guaranteed by logical planning"
                        );
                    }

                    let logical_input_schema = input.schema();

                    let input_exec = if sort_keys.is_empty() {
                        input_exec
                    } else {
                        let physical_input_schema = input_exec.schema();
                        let sort_keys = sort_keys
                            .iter()
                            .map(|e| match e {
                                Expr::Sort {
                                    expr,
                                    asc,
                                    nulls_first,
                                } => create_physical_sort_expr(
                                    expr,
                                    logical_input_schema,
                                    &physical_input_schema,
                                    SortOptions {
                                        descending: !*asc,
                                        nulls_first: *nulls_first,
                                    },
                                    &session_state.execution_props,
                                ),
                                _ => unreachable!(),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Arc::new(if can_repartition {
                            SortExec::new_with_partitioning(sort_keys, input_exec, true)
                        } else {
                            SortExec::try_new(sort_keys, input_exec)?
                        })
                    };

                    let physical_input_schema = input_exec.schema();
                    let window_expr = window_expr
                        .iter()
                        .map(|e| {
                            create_window_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                &session_state.execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(Arc::new(WindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        physical_input_schema,
                    )?) )
                }
                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    ..
                }) => {
                    // Initially need to perform the aggregate and then merge the partitions
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let physical_input_schema = input_exec.schema();
                    let logical_input_schema = input.as_ref().schema();

                    let groups = group_expr
                        .iter()
                        .map(|e| {
                            tuple_err((
                                self.create_physical_expr(
                                    e,
                                    logical_input_schema,
                                    &physical_input_schema,
                                    session_state,
                                ),
                                physical_name(e),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let aggregates = aggr_expr
                        .iter()
                        .map(|e| {
                            create_aggregate_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                &session_state.execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let initial_aggr = Arc::new(AggregateExec::try_new(
                        AggregateMode::Partial,
                        groups.clone(),
                        aggregates.clone(),
                        input_exec,
                        physical_input_schema.clone(),
                    )?);

                    // update group column indices based on partial aggregate plan evaluation
                    let final_group: Vec<Arc<dyn PhysicalExpr>> = initial_aggr.output_group_expr();

                    // TODO: dictionary type not yet supported in Hash Repartition
                    let contains_dict = groups
                        .iter()
                        .flat_map(|x| x.0.data_type(physical_input_schema.as_ref()))
                        .any(|x| matches!(x, DataType::Dictionary(_, _)));

                    let can_repartition = !groups.is_empty()
                        && session_state.config.target_partitions > 1
                        && session_state.config.repartition_aggregations
                        && !contains_dict;

                    let (initial_aggr, next_partition_mode): (
                        Arc<dyn ExecutionPlan>,
                        AggregateMode,
                    ) = if can_repartition {
                        // Divide partial hash aggregates into multiple partitions by hash key
                        let hash_repartition = Arc::new(RepartitionExec::try_new(
                            initial_aggr,
                            Partitioning::Hash(
                                final_group.clone(),
                                session_state.config.target_partitions,
                            ),
                        )?);
                        // Combine hash aggregates within the partition
                        (hash_repartition, AggregateMode::FinalPartitioned)
                    } else {
                        // construct a second aggregation, keeping the final column name equal to the
                        // first aggregation and the expressions corresponding to the respective aggregate
                        (initial_aggr, AggregateMode::Final)
                    };

                    Ok(Arc::new(AggregateExec::try_new(
                        next_partition_mode,
                        final_group
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                            .collect(),
                        aggregates,
                        initial_aggr,
                        physical_input_schema.clone(),
                    )?) )
                }
                LogicalPlan::Projection(Projection { input, expr, .. }) => {
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let input_schema = input.as_ref().schema();

                    let physical_exprs = expr
                        .iter()
                        .map(|e| {
                            // For projections, SQL planner and logical plan builder may convert user
                            // provided expressions into logical Column expressions if their results
                            // are already provided from the input plans. Because we work with
                            // qualified columns in logical plane, derived columns involve operators or
                            // functions will contain qualifers as well. This will result in logical
                            // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                            //
                            // If we run these logical columns through physical_name function, we will
                            // get physical names with column qualifiers, which violates DataFusion's
                            // field name semantics. To account for this, we need to derive the
                            // physical name from physical input instead.
                            //
                            // This depends on the invariant that logical schema field index MUST match
                            // with physical schema field index.
                            let physical_name = if let Expr::Column(col) = e {
                                match input_schema.index_of_column(col) {
                                    Ok(idx) => {
                                        // index physical field using logical field index
                                        Ok(input_exec.schema().field(idx).name().to_string())
                                    }
                                    // logical column is not a derived column, safe to pass along to
                                    // physical_name
                                    Err(_) => physical_name(e),
                                }
                            } else {
                                physical_name(e)
                            };

                            tuple_err((
                                self.create_physical_expr(
                                    e,
                                    input_schema,
                                    &input_exec.schema(),
                                    session_state,
                                ),
                                physical_name,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(Arc::new(ProjectionExec::try_new(
                        physical_exprs,
                        input_exec,
                    )?) )
                }
                LogicalPlan::Filter(Filter {
                    input, predicate, ..
                }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = input.as_ref().schema();

                    let runtime_expr = self.create_physical_expr(
                        predicate,
                        input_dfschema,
                        &input_schema,
                        session_state,
                    )?;
                    Ok(Arc::new(FilterExec::try_new(runtime_expr, physical_input)?) )
                }
                LogicalPlan::Union(Union { inputs, .. }) => {
                    let physical_plans = futures::stream::iter(inputs)
                        .then(|lp| self.create_initial_plan(lp, session_state))
                        .try_collect::<Vec<_>>()
                        .await?;
                    Ok(Arc::new(UnionExec::new(physical_plans)) )
                }
                LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme,
                }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.schema();
                    let input_dfschema = input.as_ref().schema();
                    let physical_partitioning = match partitioning_scheme {
                        LogicalPartitioning::RoundRobinBatch(n) => {
                            Partitioning::RoundRobinBatch(*n)
                        }
                        LogicalPartitioning::Hash(expr, n) => {
                            let runtime_expr = expr
                                .iter()
                                .map(|e| {
                                    self.create_physical_expr(
                                        e,
                                        input_dfschema,
                                        &input_schema,
                                        session_state,
                                    )
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Partitioning::Hash(runtime_expr, *n)
                        }
                    };
                    Ok(Arc::new(RepartitionExec::try_new(
                        physical_input,
                        physical_partitioning,
                    )?) )
                }
                LogicalPlan::Sort(Sort { expr, input, .. }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = input.as_ref().schema();
                    let sort_expr = expr
                        .iter()
                        .map(|e| match e {
                            Expr::Sort {
                                expr,
                                asc,
                                nulls_first,
                            } => create_physical_sort_expr(
                                expr,
                                input_dfschema,
                                &input_schema,
                                SortOptions {
                                    descending: !*asc,
                                    nulls_first: *nulls_first,
                                },
                                &session_state.execution_props,
                            ),
                            _ => Err(DataFusionError::Plan(
                                "Sort only accepts sort expressions".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(SortExec::try_new(sort_expr, physical_input)?) )
                }
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on: keys,
                    join_type,
                    null_equals_null,
                    ..
                }) => {
                    let left_df_schema = left.schema();
                    let physical_left = self.create_initial_plan(left, session_state).await?;
                    let right_df_schema = right.schema();
                    let physical_right = self.create_initial_plan(right, session_state).await?;
                    let join_on = keys
                        .iter()
                        .map(|(l, r)| {
                            Ok((
                                Column::new(&l.name, left_df_schema.index_of_column(l)?),
                                Column::new(&r.name, right_df_schema.index_of_column(r)?),
                            ))
                        })
                        .collect::<Result<join_utils::JoinOn>>()?;

                    if session_state.config.target_partitions > 1
                        && session_state.config.repartition_joins
                    {
                        let (left_expr, right_expr) = join_on
                            .iter()
                            .map(|(l, r)| {
                                (
                                    Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                                    Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                                )
                            })
                            .unzip();

                        // Use hash partition by default to parallelize hash joins
                        Ok(Arc::new(HashJoinExec::try_new(
                            Arc::new(RepartitionExec::try_new(
                                physical_left,
                                Partitioning::Hash(
                                    left_expr,
                                    session_state.config.target_partitions,
                                ),
                            )?),
                            Arc::new(RepartitionExec::try_new(
                                physical_right,
                                Partitioning::Hash(
                                    right_expr,
                                    session_state.config.target_partitions,
                                ),
                            )?),
                            join_on,
                            join_type,
                            PartitionMode::Partitioned,
                            null_equals_null,
                        )?))
                    } else {
                        Ok(Arc::new(HashJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_on,
                            join_type,
                            PartitionMode::CollectLeft,
                            null_equals_null,
                        )?))
                    }
                }
                LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                    let left = self.create_initial_plan(left, session_state).await?;
                    let right = self.create_initial_plan(right, session_state).await?;
                    Ok(Arc::new(CrossJoinExec::try_new(left, right)?))
                }
                LogicalPlan::Subquery(_) => todo!(),
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row,
                    schema,
                }) => Ok(Arc::new(EmptyExec::new(
                    *produce_one_row,
                    SchemaRef::new(schema.as_ref().to_owned().into()),
                ))),
                LogicalPlan::SubqueryAlias(SubqueryAlias { input,.. }) => {
                    match input.as_ref() {
                        LogicalPlan::TableScan(..) => {
                            self.create_initial_plan(input, session_state).await
                        }
                        LogicalPlan::Union(..) => {
                            self.create_initial_plan(input, session_state).await
                        }
                        LogicalPlan::Filter(..) => {
                            self.create_initial_plan(input, session_state).await
                        }
                        _ => Err(DataFusionError::Plan("SubqueryAlias should only wrap TableScan".to_string()))
                    }
                }
                LogicalPlan::Limit(Limit { input, n, .. }) => {
                    let limit = *n;
                    let input = self.create_initial_plan(input, session_state).await?;

                    // GlobalLimitExec requires a single partition for input
                    let input = if input.output_partitioning().partition_count() == 1 {
                        input
                    } else {
                        // Apply a LocalLimitExec to each partition. The optimizer will also insert
                        // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                        Arc::new(LocalLimitExec::new(input, limit))
                    };

                    Ok(Arc::new(GlobalLimitExec::new(input, limit)))
                }
                LogicalPlan::CreateExternalTable(_) => {
                    // There is no default plan for "CREATE EXTERNAL
                    // TABLE" -- it must be handled at a higher level (so
                    // that the appropriate table can be registered with
                    // the context)
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: CreateExternalTable".to_string(),
                    ))
                }
                LogicalPlan::CreateCatalogSchema(_) => {
                    // There is no default plan for "CREATE SCHEMA".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: CreateCatalogSchema".to_string(),
                    ))
                }
                LogicalPlan::CreateCatalog(_) => {
                    // There is no default plan for "CREATE DATABASE".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: CreateCatalog".to_string(),
                    ))
                }
                | LogicalPlan::CreateMemoryTable(_) | LogicalPlan::DropTable (_) | LogicalPlan::CreateView(_) => {
                    // Create a dummy exec.
                    Ok(Arc::new(EmptyExec::new(
                        false,
                        SchemaRef::new(Schema::empty()),
                    )))
                }
                LogicalPlan::Explain (_) => Err(DataFusionError::Internal(
                    "Unsupported logical plan: Explain must be root of the plan".to_string(),
                )),
                LogicalPlan::Analyze(a) => {
                    let input = self.create_initial_plan(&a.input, session_state).await?;
                    let schema = SchemaRef::new((*a.schema).clone().into());
                    Ok(Arc::new(AnalyzeExec::new(a.verbose, input, schema)))
                }
                LogicalPlan::Extension(e) => {
                    let physical_inputs = futures::stream::iter(e.node.inputs())
                        .then(|lp| self.create_initial_plan(lp, session_state))
                        .try_collect::<Vec<_>>()
                        .await?;

                    let maybe_plan = self.extension_planners.iter().try_fold(
                        None,
                        |maybe_plan, planner| {
                            if let Some(plan) = maybe_plan {
                                Ok(Some(plan))
                            } else {
                                planner.plan_extension(
                                    self,
                                    e.node.as_ref(),
                                    &e.node.inputs(),
                                    &physical_inputs,
                                    session_state,
                                )
                            }
                        },
                    )?;
                    let plan = maybe_plan.ok_or_else(|| DataFusionError::Plan(format!(
                        "No installed planner was able to convert the custom node to an execution plan: {:?}", e.node
                    )))?;

                    // Ensure the ExecutionPlan's schema matches the
                    // declared logical schema to catch and warn about
                    // logic errors when creating user defined plans.
                    if !e.node.schema().matches_arrow_schema(&plan.schema()) {
                        Err(DataFusionError::Plan(format!(
                            "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                            LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                            e.node, e.node.schema(), plan.schema()
                        )))
                    } else {
                        Ok(plan)
                    }
                }
            };
            exec_plan
        }.boxed()
    }
}

/// Create a physical expression from a logical expression ([Expr])
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::Alias(expr, ..) => Ok(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Column(c) => {
            let idx = input_dfschema.index_of_column(c)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::ScalarVariable(_, variable_names) => {
            if &variable_names[0][0..2] == "@@" {
                match execution_props.get_var_provider(VarType::System) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => Err(DataFusionError::Plan(
                        "No system variable provider found".to_string(),
                    )),
                }
            } else {
                match execution_props.get_var_provider(VarType::UserDefined) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => Err(DataFusionError::Plan(
                        "No user defined variable provider found".to_string(),
                    )),
                }
            }
        }
        Expr::BinaryExpr { left, op, right } => {
            let lhs = create_physical_expr(
                left,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let rhs = create_physical_expr(
                right,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            match (
                lhs.data_type(input_schema)?,
                op,
                rhs.data_type(input_schema)?,
            ) {
                (
                    DataType::Date32 | DataType::Date64,
                    Operator::Plus | Operator::Minus,
                    DataType::Interval(_),
                ) => Ok(Arc::new(DateIntervalExpr::try_new(
                    lhs,
                    *op,
                    rhs,
                    input_schema,
                )?)),
                _ => {
                    // assume that we can coerce both sides into a common type
                    // and then perform a binary operation
                    binary(lhs, *op, rhs, input_schema)
                }
            }
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
            ..
        } => {
            let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            let when_expr = when_then_expr
                .iter()
                .map(|(w, _)| {
                    create_physical_expr(
                        w.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let then_expr = when_then_expr
                .iter()
                .map(|(_, t)| {
                    create_physical_expr(
                        t.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                when_expr
                    .iter()
                    .zip(then_expr.iter())
                    .map(|(w, t)| (w.clone(), t.clone()))
                    .collect();
            let else_expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = else_expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            Ok(Arc::new(CaseExpr::try_new(
                expr,
                &when_then_expr,
                else_expr,
            )?))
        }
        Expr::Cast { expr, data_type } => expressions::cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::TryCast { expr, data_type } => expressions::try_cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::Not(expr) => expressions::not(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Negative(expr) => expressions::negative(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
        ),
        Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::GetIndexedField { expr, key } => Ok(Arc::new(GetIndexedFieldExpr::new(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            key.clone(),
        ))),

        Expr::ScalarFunction { fun, args } => {
            let physical_args = args
                .iter()
                .map(|e| {
                    create_physical_expr(e, input_dfschema, input_schema, execution_props)
                })
                .collect::<Result<Vec<_>>>()?;
            functions::create_physical_expr(
                fun,
                &physical_args,
                input_schema,
                execution_props,
            )
        }
        Expr::ScalarUDF { fun, args } => {
            let mut physical_args = vec![];
            for e in args {
                physical_args.push(create_physical_expr(
                    e,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?);
            }

            udf::create_physical_expr(fun.clone().as_ref(), &physical_args, input_schema)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value_expr = create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let low_expr =
                create_physical_expr(low, input_dfschema, input_schema, execution_props)?;
            let high_expr = create_physical_expr(
                high,
                input_dfschema,
                input_schema,
                execution_props,
            )?;

            // rewrite the between into the two binary operators
            let binary_expr = binary(
                binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                Operator::And,
                binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                input_schema,
            );

            if *negated {
                expressions::not(binary_expr?)
            } else {
                binary_expr
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => match expr.as_ref() {
            Expr::Literal(ScalarValue::Utf8(None)) => {
                Ok(expressions::lit(ScalarValue::Boolean(None)))
            }
            _ => {
                let value_expr = create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?;
                let value_expr_data_type = value_expr.data_type(input_schema)?;

                let list_exprs = list
                    .iter()
                    .map(|expr| match expr {
                        Expr::Literal(ScalarValue::Utf8(None)) => create_physical_expr(
                            expr,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        ),
                        _ => {
                            let list_expr = create_physical_expr(
                                expr,
                                input_dfschema,
                                input_schema,
                                execution_props,
                            )?;
                            let list_expr_data_type =
                                list_expr.data_type(input_schema)?;

                            if list_expr_data_type == value_expr_data_type {
                                Ok(list_expr)
                            } else if can_cast_types(
                                &list_expr_data_type,
                                &value_expr_data_type,
                            ) {
                                expressions::cast(
                                    list_expr,
                                    input_schema,
                                    value_expr.data_type(input_schema)?,
                                )
                            } else {
                                Err(DataFusionError::Plan(format!(
                                    "Unsupported CAST from {:?} to {:?}",
                                    list_expr_data_type, value_expr_data_type
                                )))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                expressions::in_list(value_expr, list_exprs, negated)
            }
        },
        other => Err(DataFusionError::NotImplemented(format!(
            "Physical plan does not support logical expression {:?}",
            other
        ))),
    }
}

/// Create a window expression with a name from a logical expression
pub fn create_window_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    let name = name.into();
    match e {
        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        } => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let partition_by = partition_by
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let order_by = order_by
                .iter()
                .map(|e| match e {
                    Expr::Sort {
                        expr,
                        asc,
                        nulls_first,
                    } => create_physical_sort_expr(
                        expr,
                        logical_input_schema,
                        physical_input_schema,
                        SortOptions {
                            descending: !*asc,
                            nulls_first: *nulls_first,
                        },
                        execution_props,
                    ),
                    _ => Err(DataFusionError::Plan(
                        "Sort only accepts sort expressions".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            if window_frame.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "window expression with window frame definition is not yet supported"
                        .to_owned(),
                ));
            }
            windows::create_window_expr(
                fun,
                name,
                &args,
                &partition_by,
                &order_by,
                *window_frame,
                physical_input_schema,
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Invalid window expression '{:?}'",
            other
        ))),
    }
}

/// Create a window expression from a logical expression or an alias
pub fn create_window_expr(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    // unpack aliased logical expressions, e.g. "sum(col) over () as total"
    let (name, e) = match e {
        Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
        _ => (physical_name(e)?, e),
    };
    create_window_expr_with_name(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

/// Create an aggregate expression with a name from a logical expression
pub fn create_aggregate_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn AggregateExpr>> {
    match e {
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            aggregates::create_aggregate_expr(
                fun,
                *distinct,
                &args,
                physical_input_schema,
                name,
            )
        }
        Expr::AggregateUDF { fun, args, .. } => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            udaf::create_aggregate_expr(fun, &args, physical_input_schema, name)
        }
        other => Err(DataFusionError::Internal(format!(
            "Invalid aggregate expression '{:?}'",
            other
        ))),
    }
}

/// Create an aggregate expression from a logical expression or an alias
pub fn create_aggregate_expr(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn AggregateExpr>> {
    // unpack (nested) aliased logical expressions, e.g. "sum(col) as total"
    let (name, e) = match e {
        Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
        _ => (physical_name(e)?, e),
    };

    create_aggregate_expr_with_name(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    options: SortOptions,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    Ok(PhysicalSortExpr {
        expr: create_physical_expr(e, input_dfschema, input_schema, execution_props)?,
        options,
    })
}

impl DefaultPhysicalPlanner {
    /// Handles capturing the various plans for EXPLAIN queries
    ///
    /// Returns
    /// Some(plan) if optimized, and None if logical_plan was not an
    /// explain (and thus needs to be optimized as normal)
    async fn handle_explain(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let LogicalPlan::Explain(e) = logical_plan {
            use PlanType::*;
            let mut stringified_plans = e.stringified_plans.clone();

            stringified_plans.push(e.plan.to_stringified(FinalLogicalPlan));

            let input = self
                .create_initial_plan(e.plan.as_ref(), session_state)
                .await?;

            stringified_plans
                .push(displayable(input.as_ref()).to_stringified(InitialPhysicalPlan));

            let input =
                self.optimize_internal(input, session_state, |plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = OptimizedPhysicalPlan { optimizer_name };
                    stringified_plans.push(displayable(plan).to_stringified(plan_type));
                })?;

            stringified_plans
                .push(displayable(input.as_ref()).to_stringified(FinalPhysicalPlan));

            Ok(Some(Arc::new(ExplainExec::new(
                SchemaRef::new(e.schema.as_ref().to_owned().into()),
                stringified_plans,
                e.verbose,
            ))))
        } else {
            Ok(None)
        }
    }

    /// Optimize a physical plan by applying each physical optimizer,
    /// calling observer(plan, optimizer after each one)
    fn optimize_internal<F>(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
        mut observer: F,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        F: FnMut(&dyn ExecutionPlan, &dyn PhysicalOptimizerRule),
    {
        let optimizers = &session_state.physical_optimizers;
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent()
        );
        trace!("Detailed input physical plan:\n{:?}", plan);

        let mut new_plan = plan;
        for optimizer in optimizers {
            new_plan = optimizer.optimize(new_plan, &session_state.config)?;
            observer(new_plan.as_ref(), optimizer.as_ref())
        }
        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent()
        );
        trace!("Detailed optimized physical plan:\n{:?}", new_plan);
        Ok(new_plan)
    }
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion_data_access::object_store::local::LocalFileSystem;
    use crate::execution::context::TaskContext;
    use crate::execution::options::CsvReadOptions;
    use crate::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use crate::logical_plan::plan::Extension;
    use crate::physical_plan::{
        expressions, DisplayFormatType, Partitioning, Statistics,
    };
    use crate::prelude::SessionConfig;
    use crate::scalar::ScalarValue;
    use crate::{
        logical_plan::LogicalPlanBuilder, physical_plan::SendableRecordBatchStream,
    };
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use datafusion_common::{DFField, DFSchema, DFSchemaRef};
    use datafusion_expr::sum;
    use datafusion_expr::{col, lit};
    use fmt::Debug;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::{any::Any, fmt};

    fn make_session_state() -> SessionState {
        let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap());
        SessionState::with_config_rt(SessionConfig::new(), runtime)
    }

    async fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let mut session_state = make_session_state();
        session_state.config.target_partitions = 4;
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, &session_state)
            .await
    }

    #[tokio::test]
    async fn test_all_operators() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            path,
            options,
            None,
            1,
        )
        .await?
        // filter clause needs the type coercion rule applied
        .filter(col("c7").lt(lit(5_u8)))?
        .project(vec![col("c1"), col("c2")])?
        .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
        .sort(vec![col("c1").sort(true, true)])?
        .limit(10)?
        .build()?;

        let plan = plan(&logical_plan).await?;

        // verify that the plan correctly casts u8 to i64
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "BinaryExpr { left: Column { name: \"c7\", index: 6 }, op: Lt, right: TryCastExpr { expr: Literal { value: UInt8(5) }, cast_type: Int64 } }";
        assert!(format!("{:?}", plan).contains(expected));

        Ok(())
    }

    #[tokio::test]
    async fn test_create_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let dfschema = DFSchema::try_from(schema.clone())?;

        let planner = DefaultPhysicalPlanner::default();

        let expr = planner.create_physical_expr(
            &col("a").not(),
            &dfschema,
            &schema,
            &make_session_state(),
        )?;
        let expected = expressions::not(expressions::col("a", &schema)?)?;

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected));

        Ok(())
    }

    #[tokio::test]
    async fn test_with_csv_plan() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            path,
            options,
            None,
            1,
        )
        .await?
        .filter(col("c7").lt(col("c12")))?
        .build()?;

        let plan = plan(&logical_plan).await?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "predicate: BinaryExpr { left: TryCastExpr { expr: Column { name: \"c7\", index: 6 }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\", index: 11 } }";
        assert!(format!("{:?}", plan).contains(expected));
        Ok(())
    }

    #[tokio::test]
    async fn errors() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);

        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 < u32
            col("c1").lt(col("c2")),
            // utf8 AND utf8
            col("c1").and(col("c1")),
            // u8 AND u8
            col("c3").and(col("c3")),
            // utf8 = u32
            col("c1").eq(col("c2")),
            // utf8 = bool
            col("c1").eq(bool_expr.clone()),
            // u32 AND bool
            col("c2").and(bool_expr),
            // utf8 LIKE u32
            col("c1").like(col("c2")),
        ];
        for case in cases {
            let logical_plan = LogicalPlanBuilder::scan_csv(
                Arc::new(LocalFileSystem {}),
                &path,
                options.clone(),
                None,
                1,
            )
            .await?
            .project(vec![case.clone()]);
            let message = format!(
                "Expression {:?} expected to error due to impossible coercion",
                case
            );
            assert!(logical_plan.is_err(), "{}", message);
        }
        Ok(())
    }

    #[tokio::test]
    async fn default_extension_planner() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::default();
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error =
            "No installed planner was able to convert the custom node to an execution plan: NoOp";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e,
                expected_error
            ),
        }
    }

    #[tokio::test]
    async fn bad_extension_planner() {
        // Test that creating an execution plan whose schema doesn't
        // match the logical plan's schema generates an error.
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            BadExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error: &str = "Error during planning: \
        Extension planner for NoOp created an ExecutionPlan with mismatched schema. \
        LogicalPlan schema: DFSchema { fields: [\
            DFField { qualifier: None, field: Field { \
                name: \"a\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, \
                metadata: None } }\
        ], metadata: {} }, \
        ExecutionPlan schema: Schema { fields: [\
            Field { \
                name: \"b\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, \
                metadata: None }\
        ], metadata: {} }";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e,
                expected_error
            ),
        }
    }

    #[tokio::test]
    async fn in_list_types() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);

        // expression: "a in ('a', 1)"
        let list = vec![
            Expr::Literal(ScalarValue::Utf8(Some("a".to_string()))),
            Expr::Literal(ScalarValue::Int64(Some(1))),
        ];
        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            &path,
            options.clone(),
            None,
            1,
        )
        .await?
        // filter clause needs the type coercion rule applied
        .filter(col("c12").lt(lit(0.05)))?
        .project(vec![col("c1").in_list(list, false)])?
        .build()?;
        let execution_plan = plan(&logical_plan).await?;
        // verify that the plan correctly adds cast from Int64(1) to Utf8
        let expected = "InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [Literal { value: Utf8(\"a\") }, CastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }], negated: false, set: None }";
        assert!(format!("{:?}", execution_plan).contains(expected));

        // expression: "a in (true, 'a')"
        let list = vec![
            Expr::Literal(ScalarValue::Boolean(Some(true))),
            Expr::Literal(ScalarValue::Utf8(Some("a".to_string()))),
        ];
        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            &path,
            options.clone(),
            None,
            1,
        )
        .await?
        // filter clause needs the type coercion rule applied
        .filter(col("c12").lt(lit(0.05)))?
        .project(vec![col("c12").lt_eq(lit(0.025)).in_list(list, false)])?
        .build()?;
        let execution_plan = plan(&logical_plan).await;

        let expected_error = "Unsupported CAST from Utf8 to Boolean";
        match execution_plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e,
                expected_error
            ),
        }

        Ok(())
    }

    #[tokio::test]
    async fn in_set_test() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);

        // OPTIMIZER_INSET_THRESHOLD = 10
        // expression: "a in ('a', 1, 2, ..30)"
        let mut list = vec![Expr::Literal(ScalarValue::Utf8(Some("a".to_string())))];
        for i in 1..31 {
            list.push(Expr::Literal(ScalarValue::Int64(Some(i))));
        }

        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            &path,
            options,
            None,
            1,
        )
        .await?
        .filter(col("c12").lt(lit(0.05)))?
        .project(vec![col("c1").in_list(list, false)])?
        .build()?;
        let execution_plan = plan(&logical_plan).await?;
        let expected = "expr: [(InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [Literal { value: Utf8(\"a\") }, CastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(2) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(3) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(4) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(5) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(6) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(7) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(8) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(9) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(10) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(11) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(12) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(13) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(14) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(15) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(16) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(17) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(18) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(19) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(20) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(21) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(22) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(23) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(24) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(25) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(26) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(27) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(28) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(29) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(30) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }], negated: false, set: Some(InSet { set:";
        assert!(format!("{:?}", execution_plan).contains(expected));
        Ok(())
    }

    #[tokio::test]
    async fn in_set_null_test() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        // test NULL
        let mut list = vec![Expr::Literal(ScalarValue::Int64(None))];
        for i in 1..31 {
            list.push(Expr::Literal(ScalarValue::Int64(Some(i))));
        }

        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            &path,
            options,
            None,
            1,
        )
        .await?
        .filter(col("c12").lt(lit(0.05)))?
        .project(vec![col("c1").in_list(list, false)])?
        .build()?;
        let execution_plan = plan(&logical_plan).await?;
        let expected = "expr: [(InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [CastExpr { expr: Literal { value: Int64(NULL) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(2) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(3) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(4) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(5) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(6) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(7) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(8) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(9) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(10) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(11) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(12) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(13) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(14) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(15) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(16) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(17) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(18) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(19) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(20) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(21) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(22) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(23) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(24) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(25) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(26) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(27) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(28) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(29) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }, CastExpr { expr: Literal { value: Int64(30) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }], negated: false, set: Some(InSet { set: ";
        assert!(format!("{:?}", execution_plan).contains(expected));
        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_input_schema() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv_with_name(
            Arc::new(LocalFileSystem {}),
            &path,
            options,
            None,
            "aggregate_test_100",
            1,
        )
        .await?
        .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
        .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "SUM(aggregate_test_100.c2)",
            final_hash_agg.schema().field(1).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c2", final_hash_agg.input_schema().field(1).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            &path,
            options,
            None,
            1,
        )
        .await?
        .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
        .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{:?}", execution_plan);

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn test_explain() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan =
            LogicalPlanBuilder::scan_empty(Some("employee"), &schema, None)
                .unwrap()
                .explain(true, false)
                .unwrap()
                .build()
                .unwrap();

        let plan = plan(&logical_plan).await.unwrap();
        if let Some(plan) = plan.as_any().downcast_ref::<ExplainExec>() {
            let stringified_plans = plan.stringified_plans();
            assert!(stringified_plans.len() >= 4);
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalLogicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::InitialPhysicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::OptimizedPhysicalPlan { .. })));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalPhysicalPlan)));
        } else {
            panic!(
                "Plan was not an explain plan: {}",
                displayable(plan.as_ref()).indent()
            );
        }
    }

    /// An example extension node that doesn't do anything
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::new_with_metadata(
                        vec![DFField::new(None, "a", DataType::Int32, false)],
                        HashMap::new(),
                    )
                    .unwrap(),
                ),
            }
        }
    }

    impl Debug for NoOpExtensionNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }
    }

    impl UserDefinedLogicalNode for NoOpExtensionNode {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }

        fn from_template(
            &self,
            _exprs: &[Expr],
            _inputs: &[LogicalPlan],
        ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
            unimplemented!("NoOp");
        }
    }

    #[derive(Debug)]
    struct NoOpExecutionPlan {
        schema: SchemaRef,
    }

    impl ExecutionPlan for NoOpExecutionPlan {
        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn relies_on_input_order(&self) -> bool {
            false
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("NoOpExecutionPlan::with_new_children");
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("NoOpExecutionPlan::execute");
        }

        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default => {
                    write!(f, "NoOpExecutionPlan")
                }
            }
        }

        fn statistics(&self) -> Statistics {
            unimplemented!("NoOpExecutionPlan::statistics");
        }
    }

    //  Produces an execution plan where the schema is mismatched from
    //  the logical plan node.
    struct BadExtensionPlanner {}

    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(Some(Arc::new(NoOpExecutionPlan {
                schema: SchemaRef::new(Schema::new(vec![Field::new(
                    "b",
                    DataType::Int32,
                    false,
                )])),
            })))
        }
    }
}
