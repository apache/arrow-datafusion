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
use super::unnest::UnnestExec;
use super::{
    aggregates, empty::EmptyExec, joins::PartitionMode, udaf, union::UnionExec,
    values::ValuesExec, windows,
};
use crate::datasource::source_as_provider;
use crate::execution::context::{ExecutionProps, SessionState};
use crate::logical_expr::utils::generate_sort_key;
use crate::logical_expr::{
    Aggregate, EmptyRelation, Join, Projection, Sort, SubqueryAlias, TableScan, Unnest,
    Window,
};
use crate::logical_expr::{
    CrossJoin, Expr, LogicalPlan, Partitioning as LogicalPartitioning, PlanType,
    Repartition, ToStringifiedPlan, Union, UserDefinedLogicalNode,
};
use crate::logical_expr::{Limit, Values};
use crate::physical_expr::create_physical_expr;
use crate::physical_optimizer::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions::{Column, PhysicalSortExpr};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::HashJoinExec;
use crate::physical_plan::joins::SortMergeJoinExec;
use crate::physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{joins::utils as join_utils, Partitioning};
use crate::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, WindowExpr};
use crate::{
    error::{DataFusionError, Result},
    physical_plan::displayable,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::expr::{
    self, AggregateFunction, Between, BinaryExpr, Cast, GetIndexedField, GroupingSet,
    Like, TryCast, WindowFunction,
};
use datafusion_expr::expr_rewriter::unnormalize_cols;
use datafusion_expr::logical_plan::builder::wrap_projection_for_join_if_necessary;
use datafusion_expr::{logical_plan, StringifiedPlan};
use datafusion_expr::{WindowFrame, WindowFrameBound};
use datafusion_optimizer::utils::unalias;
use datafusion_physical_expr::expressions::{HiddenColumn, Literal};
use datafusion_sql::utils::window_expr_common_partition_keys;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use log::{debug, trace};
use std::collections::HashMap;
use std::fmt::Write;
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
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{left} {op} {right}"))
        }
        Expr::Case(case) => {
            let mut name = "CASE ".to_string();
            if let Some(e) = &case.expr {
                let _ = write!(name, "{e:?} ");
            }
            for (w, t) in &case.when_then_expr {
                let _ = write!(name, "WHEN {w:?} THEN {t:?} ");
            }
            if let Some(e) = &case.else_expr {
                let _ = write!(name, "ELSE {e:?} ");
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast(Cast { expr, .. }) => {
            // CAST does not change the expression name
            create_physical_name(expr, false)
        }
        Expr::TryCast(TryCast { expr, .. }) => {
            // CAST does not change the expression name
            create_physical_name(expr, false)
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::Negative(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("(- {expr})"))
        }
        Expr::IsNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsUnknown(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS UNKNOWN"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT TRUE"))
        }
        Expr::IsNotFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::IsNotUnknown(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT UNKNOWN"))
        }
        Expr::GetIndexedField(GetIndexedField { key, expr }) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr}[{key}]"))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_physical_name(&fun.name, false, args)
        }
        Expr::WindowFunction(WindowFunction { fun, args, .. }) => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            ..
        }) => create_function_physical_name(&fun.to_string(), *distinct, args),
        Expr::AggregateUDF { fun, args, filter } => {
            if filter.is_some() {
                return Err(DataFusionError::Execution(
                    "aggregate expression with filter is not supported".to_string(),
                ));
            }
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
                    strings.push(format!("({exprs_str})"));
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
                Ok(format!("{expr} NOT IN ({list:?})"))
            } else {
                Ok(format!("{expr} IN ({list:?})"))
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
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let low = create_physical_name(low, false)?;
            let high = create_physical_name(high, false)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} AND {high}"))
            } else {
                Ok(format!("{expr} BETWEEN {low} AND {high}"))
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT LIKE {pattern}{escape}"))
            } else {
                Ok(format!("{expr} LIKE {pattern}{escape}"))
            }
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT ILIKE {pattern}{escape}"))
            } else {
                Ok(format!("{expr} ILIKE {pattern}{escape}"))
            }
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT SIMILAR TO {pattern}{escape}"))
            } else {
                Ok(format!("{expr} SIMILAR TO {pattern}{escape}"))
            }
        }
        Expr::HiddenColumn(_dt, c) => Ok(format!("#{}", c)),
        Expr::HiddenExpr(expr, _) => Ok(create_physical_name(expr, false)?),
        Expr::Sort { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support sort expression".to_string(),
        )),
        Expr::Wildcard => Err(DataFusionError::Internal(
            "Create physical name does not support wildcard".to_string(),
        )),
        Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support qualified wildcard".to_string(),
        )),
        Expr::Placeholder { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support placeholder".to_string(),
        )),
        Expr::OuterReferenceColumn(_, _) => Err(DataFusionError::Internal(
            "Create physical name does not support OuterReferenceColumn".to_string(),
        )),
    }
}

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
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
#[async_trait]
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
    async fn plan_extension(
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
            session_state.execution_props(),
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
                LogicalPlan::TableScan(TableScan {
                    source,
                    projection,
                    filters,
                    fetch,
                    ..
                }) => {
                    let source = source_as_provider(source)?;
                    // Remove all qualifiers from the scan as the provider
                    // doesn't know (nor should care) how the relation was
                    // referred to in the query
                    let filters = unnormalize_cols(filters.iter().cloned());
                    let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                    source.scan(session_state, projection.as_ref(), &unaliased, *fetch).await
                }
                LogicalPlan::Values(Values {
                    values,
                    schema,
                }) => {
                    let exec_schema = schema.as_ref().to_owned().into();
                    let exprs = values.iter()
                        .map(|row| {
                            row.iter().map(|expr| {
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
                        exprs,
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
                        && session_state.config().target_partitions() > 1
                        && session_state.config().repartition_window_functions();

                    let physical_partition_keys = if can_repartition
                    {
                        partition_keys
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(
                                    e,
                                    input.schema(),
                                    &input_exec.schema(),
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?
                    } else {
                        vec![]
                    };

                    let get_sort_keys = |expr: &Expr| match expr {
                        Expr::WindowFunction(WindowFunction{
                            ref partition_by,
                            ref order_by,
                            ..
                        }) => generate_sort_key(partition_by, order_by),
                        Expr::Alias(expr, _) => {
                            // Convert &Box<T> to &T
                            match &**expr {
                                Expr::WindowFunction(WindowFunction{
                                    ref partition_by,
                                    ref order_by,
                                    ..}) => generate_sort_key(partition_by, order_by),
                                _ => unreachable!(),
                            }
                        }
                        _ => unreachable!(),
                    };
                    let sort_keys = get_sort_keys(&window_expr[0])?;
                    if window_expr.len() > 1 {
                        debug_assert!(
                            window_expr[1..]
                                .iter()
                                .all(|expr| get_sort_keys(expr).unwrap() == sort_keys),
                            "all window expressions shall have the same sort keys, as guaranteed by logical planning"
                        );
                    }

                    let logical_input_schema = input.schema();
                    let physical_input_schema = input_exec.schema();
                    let window_expr = window_expr
                        .iter()
                        .map(|e| {
                            create_window_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                session_state.execution_props(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let uses_bounded_memory = window_expr
                        .iter()
                        .all(|e| e.uses_bounded_memory());
                    // If all window expressions can run with bounded memory,
                    // choose the bounded window variant:
                    Ok(if uses_bounded_memory {
                        Arc::new(BoundedWindowAggExec::try_new(
                            window_expr,
                            input_exec,
                            physical_input_schema,
                            physical_partition_keys,
                        )?)
                    } else {
                        Arc::new(WindowAggExec::try_new(
                            window_expr,
                            input_exec,
                            physical_input_schema,
                            physical_partition_keys,
                        )?)
                    })
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

                    let groups = self.create_grouping_physical_expr(
                        group_expr,
                        logical_input_schema,
                        &physical_input_schema,
                        session_state)?;

                    let aggregates = aggr_expr
                        .iter()
                        .map(|e| {
                            create_aggregate_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                session_state.execution_props(),
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

                    let can_repartition = !groups.is_empty()
                        && session_state.config().target_partitions() > 1
                        && session_state.config().repartition_aggregations();

                    let (initial_aggr, next_partition_mode): (
                        Arc<dyn ExecutionPlan>,
                        AggregateMode,
                    ) = if can_repartition {
                        // construct a second aggregation with 'AggregateMode::FinalPartitioned'
                        (initial_aggr, AggregateMode::FinalPartitioned)
                    } else {
                        // construct a second aggregation, keeping the final column name equal to the
                        // first aggregation and the expressions corresponding to the respective aggregate
                        (initial_aggr, AggregateMode::Final)
                    };

                    let final_grouping_set = PhysicalGroupBy::new_single(
                        final_group
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| (expr.clone(), groups.result_expr()[i].1.clone()))
                            .collect()
                    );

                    Ok(Arc::new(AggregateExec::try_new(
                        next_partition_mode,
                        final_grouping_set,
                        aggregates,
                        initial_aggr,
                        physical_input_schema.clone(),
                    )?))
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
                            // functions will contain qualifiers as well. This will result in logical
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
                    )?))
                }
                LogicalPlan::Filter(filter) => {
                    let physical_input = self.create_initial_plan(&filter.input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = filter.input.schema();

                    let runtime_expr = self.create_physical_expr(
                        &filter.predicate,
                        input_dfschema,
                        &input_schema,
                        session_state,
                    )?;
                    Ok(Arc::new(FilterExec::try_new(runtime_expr, physical_input)?))
                }
                LogicalPlan::Union(Union { inputs, schema }) => {
                    let physical_plans = futures::stream::iter(inputs)
                        .then(|lp| self.create_initial_plan(lp, session_state))
                        .try_collect::<Vec<_>>()
                        .await?;
                    if schema.fields().len() < physical_plans[0].schema().fields().len() {
                        // `schema` could be a subset of the child schema. For example
                        // for query "select count(*) from (select a from t union all select a from t)"
                        // `schema` is empty but child schema contains one field `a`.
                        Ok(Arc::new(UnionExec::try_new_with_schema(physical_plans, schema.clone())?))
                    } else {
                        Ok(Arc::new(UnionExec::new(physical_plans)))
                    }
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
                        LogicalPartitioning::DistributeBy(_) => {
                            return Err(DataFusionError::NotImplemented("Physical plan does not support DistributeBy partitioning".to_string()));
                        }
                    };
                    Ok(Arc::new(RepartitionExec::try_new(
                        physical_input,
                        physical_partitioning,
                    )?))
                }
                LogicalPlan::Sort(Sort { expr, input, fetch, .. }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = input.as_ref().schema();
                    let sort_expr = expr
                        .iter()
                        .map(|e| match e {
                            Expr::Sort(expr::Sort {
                                expr,
                                asc,
                                nulls_first,
                            }) => create_physical_sort_expr(
                                expr,
                                input_dfschema,
                                &input_schema,
                                SortOptions {
                                    descending: !*asc,
                                    nulls_first: *nulls_first,
                                },
                                session_state.execution_props(),
                            ),
                            _ => Err(DataFusionError::Plan(
                                "Sort only accepts sort expressions".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(SortExec::try_new(sort_expr, physical_input, *fetch)?))
                }
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on: keys,
                    filter,
                    join_type,
                    null_equals_null,
                    schema: join_schema,
                    ..
                }) => {
                    let null_equals_null = *null_equals_null;

                    // If join has expression equijoin keys, add physical projecton.
                    let has_expr_join_key = keys.iter().any(|(l, r)| {
                        !(matches!(l, Expr::Column(_))
                            && matches!(r, Expr::Column(_)))
                    });
                    if has_expr_join_key {
                        let left_keys = keys
                            .iter()
                            .map(|(l, _r)| l)
                            .cloned()
                            .collect::<Vec<_>>();
                        let right_keys = keys
                            .iter()
                            .map(|(_l, r)| r)
                            .cloned()
                            .collect::<Vec<_>>();
                        let (left, right, column_on, added_project) = {
                            let (left, left_col_keys, left_projected) =
                                wrap_projection_for_join_if_necessary(
                                    left_keys.as_slice(),
                                    left.as_ref().clone(),
                                )?;
                            let (right, right_col_keys, right_projected) =
                                wrap_projection_for_join_if_necessary(
                                    &right_keys,
                                    right.as_ref().clone(),
                                )?;
                            (
                                left,
                                right,
                                (left_col_keys, right_col_keys),
                                left_projected || right_projected,
                            )
                        };

                        let join_plan =
                            LogicalPlan::Join(Join::try_new_with_project_input(
                                logical_plan,
                                Arc::new(left),
                                Arc::new(right),
                                column_on,
                            )?);

                        // Remove temporary projected columns
                        let join_plan = if added_project {
                            let final_join_result = join_schema
                                .fields()
                                .iter()
                                .map(|field| {
                                    Expr::Column(field.qualified_column())
                                })
                                .collect::<Vec<_>>();
                            let projection =
                                logical_plan::Projection::try_new_with_schema(
                                    final_join_result,
                                    Arc::new(join_plan),
                                    join_schema.clone(),
                                )?;
                            LogicalPlan::Projection(projection)
                        } else {
                            join_plan
                        };

                        return self
                            .create_initial_plan(&join_plan, session_state)
                            .await;
                    }

                    // All equi-join keys are columns now, create physical join plan
                    let left_df_schema = left.schema();
                    let physical_left = self.create_initial_plan(left, session_state).await?;
                    let right_df_schema = right.schema();
                    let physical_right = self.create_initial_plan(right, session_state).await?;
                    let join_on = keys
                        .iter()
                        .map(|(l, r)| {
                            let l = l.try_into_col()?;
                            let r = r.try_into_col()?;
                            Ok((
                                Column::new(&l.name, left_df_schema.index_of_column(&l)?),
                                Column::new(&r.name, right_df_schema.index_of_column(&r)?),
                            ))
                        })
                        .collect::<Result<join_utils::JoinOn>>()?;

                    let join_filter = match filter {
                        Some(expr) => {
                            // Extract columns from filter expression and saved in a HashSet
                            let cols = expr.to_columns()?;

                            // Collect left & right field indices, the field indices are sorted in ascending order
                            let left_field_indices = cols.iter()
                                .filter_map(|c| match left_df_schema.index_of_column(c) {
                                    Ok(idx) => Some(idx),
                                    _ => None,
                                }).sorted()
                                .collect::<Vec<_>>();
                            let right_field_indices = cols.iter()
                                .filter_map(|c| match right_df_schema.index_of_column(c) {
                                    Ok(idx) => Some(idx),
                                    _ => None,
                                }).sorted()
                                .collect::<Vec<_>>();

                            // Collect DFFields and Fields required for intermediate schemas
                            let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) = left_field_indices.clone()
                                .into_iter()
                                .map(|i| (
                                    left_df_schema.field(i).clone(),
                                    physical_left.schema().field(i).clone(),
                                ))
                                .chain(
                                    right_field_indices.clone()
                                        .into_iter()
                                        .map(|i| (
                                            right_df_schema.field(i).clone(),
                                            physical_right.schema().field(i).clone(),
                                        ))
                                )
                                .unzip();

                            // Construct intermediate schemas used for filtering data and
                            // convert logical expression to physical according to filter schema
                            let filter_df_schema = DFSchema::new_with_metadata(filter_df_fields, HashMap::new())?;
                            let filter_schema = Schema::new_with_metadata(filter_fields, HashMap::new());
                            let filter_expr = create_physical_expr(
                                expr,
                                &filter_df_schema,
                                &filter_schema,
                                session_state.execution_props(),
                            )?;
                            let column_indices = join_utils::JoinFilter::build_column_indices(left_field_indices, right_field_indices);

                            Some(join_utils::JoinFilter::new(
                                filter_expr,
                                column_indices,
                                filter_schema,
                            ))
                        }
                        _ => None
                    };

                    let prefer_hash_join = session_state.config_options().optimizer.prefer_hash_join;
                    if join_on.is_empty() {
                        // there is no equal join condition, use the nested loop join
                        // TODO optimize the plan, and use the config of `target_partitions` and `repartition_joins`
                        Ok(Arc::new(NestedLoopJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_filter,
                            join_type,
                        )?))
                    } else if session_state.config().target_partitions() > 1
                        && session_state.config().repartition_joins()
                        && !prefer_hash_join
                    {
                        // Use SortMergeJoin if hash join is not preferred
                        // Sort-Merge join support currently is experimental
                        if join_filter.is_some() {
                            // TODO SortMergeJoinExec need to support join filter
                            Err(DataFusionError::NotImplemented("SortMergeJoinExec does not support join_filter now.".to_string()))
                        } else {
                            let join_on_len = join_on.len();
                            Ok(Arc::new(SortMergeJoinExec::try_new(
                                physical_left,
                                physical_right,
                                join_on,
                                *join_type,
                                vec![SortOptions::default(); join_on_len],
                                null_equals_null,
                            )?))
                        }
                    } else if session_state.config().target_partitions() > 1
                        && session_state.config().repartition_joins()
                        && prefer_hash_join {
                         let partition_mode = {
                            if session_state.config().collect_statistics() {
                                PartitionMode::Auto
                            } else {
                                PartitionMode::Partitioned
                            }
                         };
                        Ok(Arc::new(HashJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_on,
                            join_filter,
                            join_type,
                            partition_mode,
                            null_equals_null,
                        )?))
                    } else {
                        Ok(Arc::new(HashJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_on,
                            join_filter,
                            join_type,
                            PartitionMode::CollectLeft,
                            null_equals_null,
                        )?))
                    }
                }
                LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                    let left = self.create_initial_plan(left, session_state).await?;
                    let right = self.create_initial_plan(right, session_state).await?;
                    Ok(Arc::new(CrossJoinExec::new(left, right)))
                }
                LogicalPlan::Subquery(_) => todo!(),
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row,
                    schema,
                }) => Ok(Arc::new(EmptyExec::new(
                    *produce_one_row,
                    SchemaRef::new(schema.as_ref().to_owned().into()),
                ))),
                LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                    self.create_initial_plan(input, session_state).await
                }
                LogicalPlan::Limit(Limit { input, skip, fetch, .. }) => {
                    let input = self.create_initial_plan(input, session_state).await?;

                    // GlobalLimitExec requires a single partition for input
                    let input = if input.output_partitioning().partition_count() == 1 {
                        input
                    } else {
                        // Apply a LocalLimitExec to each partition. The optimizer will also insert
                        // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                        if let Some(fetch) = fetch {
                            Arc::new(LocalLimitExec::new(input, *fetch + skip))
                        } else {
                            input
                        }
                    };

                    Ok(Arc::new(GlobalLimitExec::new(input, *skip, *fetch)))
                }
                LogicalPlan::Unnest(Unnest { input, column, schema }) => {
                    let input = self.create_initial_plan(input, session_state).await?;
                    let column_exec = schema.index_of_column(column)
                        .map(|idx| Column::new(&column.name, idx))?;
                    let schema = SchemaRef::new(schema.as_ref().to_owned().into());
                    Ok(Arc::new(UnnestExec::new(input, column_exec, schema)))
                }
                LogicalPlan::CreateExternalTable(_) => {
                    // There is no default plan for "CREATE EXTERNAL
                    // TABLE" -- it must be handled at a higher level (so
                    // that the appropriate table can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: CreateExternalTable".to_string(),
                    ))
                }
                LogicalPlan::Prepare(_) => {
                    // There is no default plan for "PREPARE" -- it must be
                    // handled at a higher level (so that the appropriate
                    // statement can be prepared)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: Prepare".to_string(),
                    ))
                }
                LogicalPlan::CreateCatalogSchema(_) => {
                    // There is no default plan for "CREATE SCHEMA".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: CreateCatalogSchema".to_string(),
                    ))
                }
                LogicalPlan::CreateCatalog(_) => {
                    // There is no default plan for "CREATE DATABASE".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: CreateCatalog".to_string(),
                    ))
                }
                LogicalPlan::CreateMemoryTable(_) => {
                    // There is no default plan for "CREATE MEMORY TABLE".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: CreateMemoryTable".to_string(),
                    ))
                }
                LogicalPlan::DropTable(_) => {
                    // There is no default plan for "DROP TABLE".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: DropTable".to_string(),
                    ))
                }
                LogicalPlan::DropView(_) => {
                    // There is no default plan for "DROP VIEW".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: DropView".to_string(),
                    ))
                }
                LogicalPlan::CreateView(_) => {
                    // There is no default plan for "CREATE VIEW".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: CreateView".to_string(),
                    ))
                }
                LogicalPlan::Dml(_) => {
                    // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                    Err(DataFusionError::NotImplemented(
                        "Unsupported logical plan: Dml".to_string(),
                    ))
                }
                LogicalPlan::Statement(statement) => {
                    // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                    let name = statement.name();
                    Err(DataFusionError::NotImplemented(
                        format!("Unsupported logical plan: Statement({name})")
                    ))
                }
                LogicalPlan::DescribeTable(_) => {
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: DescribeTable must be root of the plan".to_string(),
                    ))
                }
                LogicalPlan::Explain(_) => Err(DataFusionError::Internal(
                    "Unsupported logical plan: Explain must be root of the plan".to_string(),
                )),
                LogicalPlan::Distinct(_) => {
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: Distinct should be replaced to Aggregate".to_string(),
                    ))
                }
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

                    let mut maybe_plan = None;
                    for planner in &self.extension_planners {
                        if maybe_plan.is_some() {
                            break;
                        }

                        let logical_input = e.node.inputs();
                        maybe_plan = planner.plan_extension(
                            self,
                            e.node.as_ref(),
                            &logical_input,
                            &physical_inputs,
                            session_state,
                        ).await?;
                    }

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

    fn create_grouping_physical_expr(
        &self,
        group_expr: &[Expr],
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<PhysicalGroupBy> {
        if group_expr.len() == 1 {
            match &group_expr[0] {
                Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)) => {
                    merge_grouping_set_physical_expr(
                        grouping_sets,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                Expr::GroupingSet(GroupingSet::Cube(_exprs)) => Err(DataFusionError::Internal(
                    "Unsupported logical plan: GroupingSet::Cube should be replaced to GroupingSet::GroupingSets".to_string(),
                )),
                Expr::GroupingSet(GroupingSet::Rollup(_exprs)) => Err(DataFusionError::Internal(
                    "Unsupported logical plan: GroupingSet::Rollup should be replaced to GroupingSet::GroupingSets".to_string(),
                )),
                expr => Ok(PhysicalGroupBy::new_single(vec![tuple_err((
                    self.create_physical_expr(
                        expr,
                        input_dfschema,
                        input_schema,
                        session_state,
                    ),
                    physical_name(expr),
                ))?])),
            }
        } else {
            Ok(PhysicalGroupBy::new_single(
                group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                input_dfschema,
                                input_schema,
                                session_state,
                            ),
                            physical_name(e),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
    }
}

/// Expand and align  a GROUPING SET expression.
/// (see https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS)
///
/// This will take a list of grouping sets and ensure that each group is
/// properly aligned for the physical execution plan. We do this by
/// identifying all unique expression in each group and conforming each
/// group to the same set of expression types and ordering.
/// For example, if we have something like `GROUPING SETS ((a,b,c),(a),(b),(b,c))`
/// we would expand this to `GROUPING SETS ((a,b,c),(a,NULL,NULL),(NULL,b,NULL),(NULL,b,c))
/// (see https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS)
fn merge_grouping_set_physical_expr(
    grouping_sets: &[Vec<Expr>],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_groups = grouping_sets.len();
    let mut all_normal_exprs: Vec<Expr> = vec![];
    let mut all_hidden_result_exprs: Vec<Expr> = vec![];

    let mut grouping_set_phy_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut hidden_grouping_set_phy_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut null_phy_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    let mut hidden_grouping_set_result_phy_expr: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![];
    let mut grouping_set_result_phy_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    for expr in grouping_sets.iter().flatten() {
        if let Expr::HiddenExpr(first, second) = expr {
            if let Expr::HiddenColumn(dt, _) = second.as_ref() {
                hidden_grouping_set_phy_expr.push(get_physical_expr_pair(
                    first,
                    input_dfschema,
                    input_schema,
                    session_state,
                )?);

                if !all_hidden_result_exprs.contains(second) {
                    all_hidden_result_exprs.push(*second.clone());
                    let hidden_column_name = second.display_name()?;
                    // The second element in the hidden expr should be converted to a physic HiddenColumn
                    hidden_grouping_set_result_phy_expr.push((
                        Arc::new(HiddenColumn::new(&hidden_column_name, dt)),
                        hidden_column_name,
                    ));
                }
            } else {
                return Err(DataFusionError::Internal(
                    "The second part of the Expr::HiddenExpr should be a Expr::HiddenColumn"
                        .to_string(),
                ));
            }
        } else if !all_normal_exprs.contains(expr) {
            all_normal_exprs.push(expr.clone());

            let phy_expr = get_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?;
            grouping_set_phy_expr.push(phy_expr.clone());
            null_phy_exprs.push(get_null_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);
            grouping_set_result_phy_expr.push(phy_expr);
        }
    }
    grouping_set_result_phy_expr.append(&mut hidden_grouping_set_result_phy_expr);

    let mut merged_sets: Vec<Vec<bool>> = Vec::with_capacity(num_groups);
    for expr_group in grouping_sets.iter() {
        let group: Vec<bool> = all_normal_exprs
            .iter()
            .map(|expr| !expr_group.contains(expr))
            .collect();
        merged_sets.push(group)
    }

    Ok(PhysicalGroupBy::new(
        grouping_set_phy_expr,
        hidden_grouping_set_phy_expr,
        grouping_set_result_phy_expr,
        null_phy_exprs,
        merged_sets,
    ))
}

/// For a given logical expr, get a properly typed NULL ScalarValue physical expression
fn get_null_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr = create_physical_expr(
        expr,
        input_dfschema,
        input_schema,
        session_state.execution_props(),
    )?;
    let physical_name = physical_name(&expr.clone())?;

    let data_type = physical_expr.data_type(input_schema)?;
    let null_value: ScalarValue = (&data_type).try_into()?;

    let null_value = Literal::new(null_value);
    Ok((Arc::new(null_value), physical_name))
}

fn get_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr = create_physical_expr(
        expr,
        input_dfschema,
        input_schema,
        session_state.execution_props(),
    )?;
    let physical_name = physical_name(expr)?;
    Ok((physical_expr, physical_name))
}

/// Check if window bounds are valid after schema information is available, and
/// window_frame bounds are casted to the corresponding column type.
/// queries like:
/// OVER (ORDER BY a RANGES BETWEEN 3 PRECEDING AND 5 PRECEDING)
/// OVER (ORDER BY a RANGES BETWEEN INTERVAL '3 DAY' PRECEDING AND '5 DAY' PRECEDING)  are rejected
pub fn is_window_valid(window_frame: &WindowFrame) -> bool {
    match (&window_frame.start_bound, &window_frame.end_bound) {
        (WindowFrameBound::Following(_), WindowFrameBound::Preceding(_))
        | (WindowFrameBound::Following(_), WindowFrameBound::CurrentRow)
        | (WindowFrameBound::CurrentRow, WindowFrameBound::Preceding(_)) => false,
        (WindowFrameBound::Preceding(lhs), WindowFrameBound::Preceding(rhs)) => {
            !rhs.is_null() && (lhs.is_null() || (lhs >= rhs))
        }
        (WindowFrameBound::Following(lhs), WindowFrameBound::Following(rhs)) => {
            !lhs.is_null() && (rhs.is_null() || (lhs <= rhs))
        }
        _ => true,
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
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        }) => {
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
                    Expr::Sort(expr::Sort {
                        expr,
                        asc,
                        nulls_first,
                    }) => create_physical_sort_expr(
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
            if !is_window_valid(window_frame) {
                return Err(DataFusionError::Plan(format!(
                        "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
                        window_frame.start_bound, window_frame.end_bound
                    )));
            }

            let window_frame = Arc::new(window_frame.clone());
            windows::create_window_expr(
                fun,
                name,
                &args,
                &partition_by,
                &order_by,
                window_frame,
                physical_input_schema,
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Invalid window expression '{other:?}'"
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
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            ..
        }) => {
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
            "Invalid aggregate expression '{other:?}'"
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
            let mut stringified_plans = vec![];

            let config = &session_state.config_options().explain;

            if !config.physical_plan_only {
                stringified_plans = e.stringified_plans.clone();
                if e.logical_optimization_succeeded {
                    stringified_plans.push(e.plan.to_stringified(FinalLogicalPlan));
                }
            }

            if !config.logical_plan_only && e.logical_optimization_succeeded {
                match self
                    .create_initial_plan(e.plan.as_ref(), session_state)
                    .await
                {
                    Ok(input) => {
                        stringified_plans.push(
                            displayable(input.as_ref())
                                .to_stringified(InitialPhysicalPlan),
                        );

                        match self.optimize_internal(
                            input,
                            session_state,
                            |plan, optimizer| {
                                let optimizer_name = optimizer.name().to_string();
                                let plan_type = OptimizedPhysicalPlan { optimizer_name };
                                stringified_plans
                                    .push(displayable(plan).to_stringified(plan_type));
                            },
                        ) {
                            Ok(input) => stringified_plans.push(
                                displayable(input.as_ref())
                                    .to_stringified(FinalPhysicalPlan),
                            ),
                            Err(DataFusionError::Context(optimizer_name, e)) => {
                                let plan_type = OptimizedPhysicalPlan { optimizer_name };
                                stringified_plans
                                    .push(StringifiedPlan::new(plan_type, e.to_string()))
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => stringified_plans
                        .push(StringifiedPlan::new(InitialPhysicalPlan, e.to_string())),
                }
            }

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
        let optimizers = session_state.physical_optimizers();
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent()
        );
        trace!("Detailed input physical plan:\n{:?}", plan);

        let mut new_plan = plan;
        for optimizer in optimizers {
            let before_schema = new_plan.schema();
            new_plan = optimizer
                .optimize(new_plan, session_state.config_options())
                .map_err(|e| {
                    DataFusionError::Context(optimizer.name().to_string(), Box::new(e))
                })?;
            if optimizer.schema_check() && new_plan.schema() != before_schema {
                let e = DataFusionError::Internal(format!(
                    "PhysicalOptimizer rule '{}' failed, due to generate a different schema, original schema: {:?}, new schema: {:?}",
                    optimizer.name(),
                    before_schema,
                    new_plan.schema()
                ));
                return Err(DataFusionError::Context(
                    optimizer.name().to_string(),
                    Box::new(e),
                ));
            }
            trace!(
                "Optimized physical plan by {}:\n{}\n",
                optimizer.name(),
                displayable(new_plan.as_ref()).indent()
            );
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
    use crate::datasource::MemTable;
    use crate::execution::context::TaskContext;
    use crate::execution::options::CsvReadOptions;
    use crate::execution::runtime_env::RuntimeEnv;
    use crate::physical_plan::SendableRecordBatchStream;
    use crate::physical_plan::{
        expressions, DisplayFormatType, Partitioning, PhysicalPlanner, Statistics,
    };
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::scalar::ScalarValue;
    use crate::test_util::{scan_empty, scan_empty_with_partitions};
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{assert_contains, TableReference};
    use datafusion_common::{DFField, DFSchema, DFSchemaRef};
    use datafusion_expr::{
        col, lit, sum, Extension, GroupingSet, LogicalPlanBuilder,
        UserDefinedLogicalNodeCore,
    };
    use fmt::Debug;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::{any::Any, fmt};

    fn make_session_state() -> SessionState {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(4);
        let config = config.set_bool("datafusion.optimizer.skip_failed_rules", false);
        SessionState::with_config_rt(config, runtime)
    }

    async fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let session_state = make_session_state();
        // optimize the logical plan
        let logical_plan = session_state.optimize(logical_plan)?;
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
    }

    #[tokio::test]
    async fn test_all_operators() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .sort(vec![col("c1").sort(true, true)])?
            .limit(3, Some(10))?
            .build()?;

        let exec_plan = plan(&logical_plan).await?;

        // verify that the plan correctly casts u8 to i64
        // the cast from u8 to i64 for literal will be simplified, and get lit(int64(5))
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "BinaryExpr { left: Column { name: \"c7\", index: 2 }, op: Lt, right: Literal { value: Int64(5) } }";
        assert!(format!("{exec_plan:?}").contains(expected));
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

        assert_eq!(format!("{expr:?}"), format!("{expected:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn test_with_csv_plan() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .filter(col("c7").lt(col("c12")))?
            .limit(3, None)?
            .build()?;

        let plan = plan(&logical_plan).await?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        // the cast here is implicit so has CastOptions with safe=true
        let _expected = "predicate: BinaryExpr { left: TryCastExpr { expr: Column { name: \"c7\", index: 6 }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\", index: 11 } }";
        let plan_debug_str = format!("{plan:?}");
        assert!(plan_debug_str.contains("GlobalLimitExec"));
        assert!(plan_debug_str.contains("skip: 3"));
        Ok(())
    }

    #[tokio::test]
    async fn error_during_extension_planning() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            ErrorExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        match planner
            .create_physical_plan(&logical_plan, &session_state)
            .await
        {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(e.to_string().contains("BOOM"),),
        }
    }

    #[tokio::test]
    async fn test_with_zero_offset_plan() -> Result<()> {
        let logical_plan = test_csv_scan().await?.limit(0, None)?.build()?;
        let plan = plan(&logical_plan).await?;
        assert!(format!("{plan:?}").contains("limit: None"));
        Ok(())
    }

    #[tokio::test]
    async fn test_limit_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty_with_partitions(Some("test"), &schema, None, 2)?
            .limit(3, Some(5))?
            .build()?;
        let plan = plan(&logical_plan).await?;

        assert!(format!("{plan:?}").contains("GlobalLimitExec"));
        assert!(format!("{plan:?}").contains("skip: 3, fetch: Some(5)"));

        // LocalLimitExec adjusts the `fetch`
        assert!(format!("{plan:?}").contains("LocalLimitExec"));
        assert!(format!("{plan:?}").contains("fetch: 8"));
        Ok(())
    }

    #[tokio::test]
    async fn errors() -> Result<()> {
        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 AND utf8
            col("c1").and(col("c1")),
            // u8 AND u8
            col("c3").and(col("c3")),
            // utf8 = bool
            col("c1").eq(bool_expr.clone()),
            // u32 AND bool
            col("c2").and(bool_expr),
        ];
        for case in cases {
            let logical_plan = test_csv_scan().await?.project(vec![case.clone()]);
            let message = format!(
                "Expression {case:?} expected to error due to impossible coercion"
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
                "Error '{e}' did not contain expected error '{expected_error}'"
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
                metadata: {} } }\
        ], metadata: {} }, \
        ExecutionPlan schema: Schema { fields: [\
            Field { \
                name: \"b\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, \
                metadata: {} }\
        ], metadata: {} }";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{e}' did not contain expected error '{expected_error}'"
            ),
        }
    }

    #[tokio::test]
    async fn in_list_types() -> Result<()> {
        // expression: "a in ('a', 1)"
        let list = vec![lit("a"), lit(1i64)];
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await?;
        // verify that the plan correctly adds cast from Int64(1) to Utf8, and the const will be evaluated.

        let expected = "expr: [(BinaryExpr { left: BinaryExpr { left: Column { name: \"c1\", index: 0 }, op: Eq, right: Literal { value: Utf8(\"1\") } }, op: Or, right: BinaryExpr { left: Column { name: \"c1\", index: 0 }, op: Eq, right: Literal { value: Utf8(\"a\") } } }";

        let actual = format!("{execution_plan:?}");
        assert!(actual.contains(expected), "{}", actual);

        Ok(())
    }

    #[tokio::test]
    async fn in_list_types_struct_literal() -> Result<()> {
        // expression: "a in (struct::null, 'a')"
        let list = vec![struct_literal(), lit("a")];

        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c12").lt_eq(lit(0.025)).in_list(list, false)])?
            .build()?;
        let e = plan(&logical_plan).await.unwrap_err().to_string();

        assert_contains!(
            &e,
            r#"type_coercion
caused by
Internal error: Optimizer rule 'type_coercion' failed due to unexpected error: Error during planning: Can not find compatible types to compare Boolean with [Struct([Field { name: "foo", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), Utf8]. This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker"#
        );

        Ok(())
    }

    /// Return a `null` literal representing a struct type like: `{ a: bool }`
    fn struct_literal() -> Expr {
        let struct_literal = ScalarValue::Struct(
            None,
            Box::new(vec![Field::new("foo", DataType::Boolean, false)]),
        );
        lit(struct_literal)
    }

    #[tokio::test]
    async fn hash_agg_input_schema() -> Result<()> {
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
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
    async fn hash_agg_grouping_set_input_schema() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "SUM(aggregate_test_100.c3)",
            final_hash_agg.schema().field(2).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c3", final_hash_agg.input_schema().field(2).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned_on_dicts() -> Result<()> {
        let dict_array: DictionaryArray<Int32Type> =
            vec!["A", "B", "A", "A", "C", "A"].into_iter().collect();
        let val_array: Int32Array = vec![1, 2, 2, 4, 1, 1].into();

        let batch = RecordBatch::try_from_iter(vec![
            ("d1", Arc::new(dict_array) as ArrayRef),
            ("d2", Arc::new(val_array) as ArrayRef),
        ])
        .unwrap();

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let ctx = SessionContext::new();

        let logical_plan = LogicalPlanBuilder::from(
            ctx.read_table(Arc::new(table))?.into_optimized_plan()?,
        )
        .aggregate(vec![col("d1")], vec![sum(col("d2"))])?
        .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));
        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_by_partitioned() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{execution_plan:?}");

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn test_explain() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
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
    struct ErrorExtensionPlanner {}

    #[async_trait]
    impl ExtensionPlanner for ErrorExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Err(DataFusionError::Internal("BOOM".to_string()))
        }
    }
    /// An example extension node that doesn't do anything
    #[derive(PartialEq, Eq, Hash)]
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::new_with_metadata(
                        vec![DFField::new_unqualified("a", DataType::Int32, false)],
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

    impl UserDefinedLogicalNodeCore for NoOpExtensionNode {
        fn name(&self) -> &str {
            "NoOp"
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

        fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
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

        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
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

    #[async_trait]
    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
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

    async fn test_csv_scan_with_name(name: &str) -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{testdata}/csv/aggregate_test_100.csv");
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan =
            match ctx.read_csv(path, options).await?.into_optimized_plan()? {
                LogicalPlan::TableScan(ref scan) => {
                    let mut scan = scan.clone();
                    let table_reference = TableReference::from(name).to_owned_reference();
                    scan.table_name = table_reference;
                    let new_schema = scan
                        .projected_schema
                        .as_ref()
                        .clone()
                        .replace_qualifier(name.to_string());
                    scan.projected_schema = Arc::new(new_schema);
                    LogicalPlan::TableScan(scan)
                }
                _ => unimplemented!(),
            };
        Ok(LogicalPlanBuilder::from(logical_plan))
    }

    async fn test_csv_scan() -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{testdata}/csv/aggregate_test_100.csv");
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        Ok(LogicalPlanBuilder::from(
            ctx.read_csv(path, options).await?.into_optimized_plan()?,
        ))
    }
}
