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

//! Planner for [`LogicalPlan`] to [`ExecutionPlan`]

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use crate::datasource::file_format::arrow::ArrowFormat;
use crate::datasource::file_format::avro::AvroFormat;
use crate::datasource::file_format::csv::CsvFormat;
use crate::datasource::file_format::json::JsonFormat;
#[cfg(feature = "parquet")]
use crate::datasource::file_format::parquet::ParquetFormat;
use crate::datasource::file_format::FileFormat;
use crate::datasource::listing::ListingTableUrl;
use crate::datasource::physical_plan::FileSinkConfig;
use crate::datasource::source_as_provider;
use crate::error::{DataFusionError, Result};
use crate::execution::context::{ExecutionProps, SessionState};
use crate::logical_expr::utils::generate_sort_key;
use crate::logical_expr::{
    Aggregate, EmptyRelation, Join, Projection, Sort, SubqueryAlias, TableScan, Unnest,
    Window,
};
use crate::logical_expr::{
    CrossJoin, Expr, LogicalPlan, Partitioning as LogicalPartitioning, PlanType,
    Repartition, Union, UserDefinedLogicalNode,
};
use crate::logical_expr::{Limit, Values};
use crate::physical_expr::create_physical_expr;
use crate::physical_optimizer::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::analyze::AnalyzeExec;
use crate::physical_plan::continuance::ContinuanceExec;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions::{Column, PhysicalSortExpr};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils as join_utils;
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::recursive_query::RecursiveQueryExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::unnest::UnnestExec;
use crate::physical_plan::values::ValuesExec;
use crate::physical_plan::windows::{
    BoundedWindowAggExec, PartitionSearchMode, WindowAggExec,
};
use crate::physical_plan::{
    aggregates, displayable, udaf, windows, AggregateExpr, ExecutionPlan, Partitioning,
    PhysicalExpr, WindowExpr,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use datafusion_common::display::ToStringifiedPlan;
use datafusion_common::file_options::FileTypeWriterOptions;
use datafusion_common::{
    exec_err, internal_err, not_impl_err, plan_err, DFSchema, FileType, ScalarValue,
};
use datafusion_expr::dml::{CopyOptions, CopyTo};
use datafusion_expr::expr::{
    self, AggregateFunction, AggregateUDF, Alias, Between, BinaryExpr, Cast,
    GetFieldAccess, GetIndexedField, GroupingSet, InList, Like, ScalarUDF, TryCast,
    WindowFunction,
};
use datafusion_expr::expr_rewriter::{unalias, unnormalize_cols};
use datafusion_expr::logical_plan::builder::wrap_projection_for_join_if_necessary;
use datafusion_expr::{
    DescribeTable, DmlStatement, NamedRelation, RecursiveQuery, StringifiedPlan,
    WindowFrame, WindowFrameBound, WriteOp,
};
use datafusion_physical_expr::expressions::Literal;
use datafusion_sql::utils::window_expr_common_partition_keys;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::{multiunzip, Itertools};
use log::{debug, trace};

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
        Expr::Alias(Alias { name, .. }) => Ok(name.clone()),
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
                let _ = write!(name, "{e} ");
            }
            for (w, t) in &case.when_then_expr {
                let _ = write!(name, "WHEN {w} THEN {t} ");
            }
            if let Some(e) = &case.else_expr {
                let _ = write!(name, "ELSE {e} ");
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
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let expr = create_physical_name(expr, false)?;
            let name = match field {
                GetFieldAccess::NamedStructField { name } => format!("{expr}[{name}]"),
                GetFieldAccess::ListIndex { key } => {
                    let key = create_physical_name(key, false)?;
                    format!("{expr}[{key}]")
                }
                GetFieldAccess::ListRange { start, stop } => {
                    let start = create_physical_name(start, false)?;
                    let stop = create_physical_name(stop, false)?;
                    format!("{expr}[{start}:{stop}]")
                }
            };

            Ok(name)
        }
        Expr::ScalarFunction(func) => {
            create_function_physical_name(&func.fun.to_string(), false, &func.args)
        }
        Expr::ScalarUDF(ScalarUDF { fun, args }) => {
            create_function_physical_name(fun.name(), false, args)
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
        Expr::AggregateUDF(AggregateUDF {
            fun,
            args,
            filter,
            order_by,
        }) => {
            // TODO: Add support for filter and order by in AggregateUDF
            if filter.is_some() {
                return exec_err!("aggregate expression with filter is not supported");
            }
            if order_by.is_some() {
                return exec_err!("aggregate expression with order_by is not supported");
            }
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_physical_name(e, false)?);
            }
            Ok(format!("{}({})", fun.name(), names.join(",")))
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

        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let list = list.iter().map(|expr| create_physical_name(expr, false));
            if *negated {
                Ok(format!("{expr} NOT IN ({list:?})"))
            } else {
                Ok(format!("{expr} IN ({list:?})"))
            }
        }
        Expr::Exists { .. } => {
            not_impl_err!("EXISTS is not yet supported in the physical plan")
        }
        Expr::InSubquery(_) => {
            not_impl_err!("IN subquery is not yet supported in the physical plan")
        }
        Expr::ScalarSubquery(_) => {
            not_impl_err!("Scalar subqueries are not yet supported in the physical plan")
        }
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
            case_insensitive,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT {op_name} {pattern}{escape}"))
            } else {
                Ok(format!("{expr} {op_name} {pattern}{escape}"))
            }
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive: _,
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
        Expr::Sort { .. } => {
            internal_err!("Create physical name does not support sort expression")
        }
        Expr::Wildcard { .. } => {
            internal_err!("Create physical name does not support wildcard")
        }
        Expr::Placeholder(_) => {
            internal_err!("Create physical name does not support placeholder")
        }
        Expr::OuterReferenceColumn(_, _) => {
            internal_err!("Create physical name does not support OuterReferenceColumn")
        }
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

    /// Create a physical plans for multiple logical plans.
    ///
    /// This is the same as [`create_initial_plan`](Self::create_initial_plan) but runs the planning concurrently.
    ///
    /// The result order is the same as the input order.
    fn create_initial_plan_multi<'a>(
        &'a self,
        logical_plans: impl IntoIterator<Item = &'a LogicalPlan> + Send + 'a,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Vec<Arc<dyn ExecutionPlan>>>> {
        async move {
            // First build futures with as little references as possible, then performing some stream magic.
            // Otherwise rustc bails out w/:
            //
            //   error: higher-ranked lifetime error
            //   ...
            //   note: could not prove `[async block@...]: std::marker::Send`
            let futures = logical_plans
                .into_iter()
                .enumerate()
                .map(|(idx, lp)| async move {
                    let plan = self.create_initial_plan(lp, session_state).await?;
                    Ok((idx, plan)) as Result<_>
                })
                .collect::<Vec<_>>();

            let mut physical_plans = futures::stream::iter(futures)
                .buffer_unordered(
                    session_state
                        .config_options()
                        .execution
                        .planning_concurrency,
                )
                .try_collect::<Vec<(usize, Arc<dyn ExecutionPlan>)>>()
                .await?;
            physical_plans.sort_by_key(|(idx, _plan)| *idx);
            let physical_plans = physical_plans
                .into_iter()
                .map(|(_idx, plan)| plan)
                .collect::<Vec<_>>();
            Ok(physical_plans)
        }
        .boxed()
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
                LogicalPlan::Copy(CopyTo{
                    input,
                    output_url,
                    file_format,
                    single_file_output,
                    copy_options,
                }) => {
                    let input_exec = self.create_initial_plan(input, session_state).await?;

                    // TODO: make this behavior configurable via options (should copy to create path/file as needed?)
                    // TODO: add additional configurable options for if existing files should be overwritten or
                    // appended to
                    let parsed_url = ListingTableUrl::parse_create_local_if_not_exists(output_url, !*single_file_output)?;
                    let object_store_url = parsed_url.object_store();

                    let schema: Schema = (**input.schema()).clone().into();

                    let file_type_writer_options = match copy_options{
                        CopyOptions::SQLOptions(statement_options) => {
                            FileTypeWriterOptions::build(
                                file_format,
                                session_state.config_options(),
                                statement_options)?
                        },
                        CopyOptions::WriterOptions(writer_options) => *writer_options.clone()
                    };

                    // Set file sink related options
                    let config = FileSinkConfig {
                        object_store_url,
                        table_paths: vec![parsed_url],
                        file_groups: vec![],
                        output_schema: Arc::new(schema),
                        table_partition_cols: vec![],
                        unbounded_input: false,
                        single_file_output: *single_file_output,
                        overwrite: false,
                        file_type_writer_options
                    };

                    let sink_format: Arc<dyn FileFormat> = match file_format {
                        FileType::CSV => Arc::new(CsvFormat::default()),
                        #[cfg(feature = "parquet")]
                        FileType::PARQUET => Arc::new(ParquetFormat::default()),
                        FileType::JSON => Arc::new(JsonFormat::default()),
                        FileType::AVRO => Arc::new(AvroFormat {} ),
                        FileType::ARROW => Arc::new(ArrowFormat {}),
                    };

                    sink_format.create_writer_physical_plan(input_exec, session_state, config, None).await
                }
                LogicalPlan::Dml(DmlStatement {
                    table_name,
                    op: WriteOp::InsertInto,
                    input,
                    ..
                }) => {
                    let name = table_name.table();
                    let schema = session_state.schema_for_ref(table_name)?;
                    if let Some(provider) = schema.table(name).await {
                        let input_exec = self.create_initial_plan(input, session_state).await?;
                        provider.insert_into(session_state, input_exec, false).await
                    } else {
                        return exec_err!(
                            "Table '{table_name}' does not exist"
                        );
                    }
                }
                LogicalPlan::Dml(DmlStatement {
                    table_name,
                    op: WriteOp::InsertOverwrite,
                    input,
                    ..
                }) => {
                    let name = table_name.table();
                    let schema = session_state.schema_for_ref(table_name)?;
                    if let Some(provider) = schema.table(name).await {
                        let input_exec = self.create_initial_plan(input, session_state).await?;
                        provider.insert_into(session_state, input_exec, true).await
                    } else {
                        return exec_err!(
                            "Table '{table_name}' does not exist"
                        );
                    }
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
                        return internal_err!(
                            "Impossibly got empty window expression"
                        );
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
                        Expr::Alias(Alias{expr,..}) => {
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
                            physical_partition_keys,
                            PartitionSearchMode::Sorted,
                        )?)
                    } else {
                        Arc::new(WindowAggExec::try_new(
                            window_expr,
                            input_exec,
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

                    let agg_filter = aggr_expr
                        .iter()
                        .map(|e| {
                            create_aggregate_expr_and_maybe_filter(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                session_state.execution_props(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let (aggregates, filters, order_bys) : (Vec<_>, Vec<_>, Vec<_>) = multiunzip(agg_filter);

                    let initial_aggr = Arc::new(AggregateExec::try_new(
                        AggregateMode::Partial,
                        groups.clone(),
                        aggregates.clone(),
                        filters.clone(),
                        order_bys,
                        input_exec,
                        physical_input_schema.clone(),
                    )?);

                    // update group column indices based on partial aggregate plan evaluation
                    let final_group: Vec<Arc<dyn PhysicalExpr>> = initial_aggr.output_group_expr();

                    let can_repartition = !groups.is_empty()
                        && session_state.config().target_partitions() > 1
                        && session_state.config().repartition_aggregations();

                    // Some aggregators may be modified during initialization for
                    // optimization purposes. For example, a FIRST_VALUE may turn
                    // into a LAST_VALUE with the reverse ordering requirement.
                    // To reflect such changes to subsequent stages, use the updated
                    // `AggregateExpr`/`PhysicalSortExpr` objects.
                    let updated_aggregates = initial_aggr.aggr_expr().to_vec();
                    let updated_order_bys = initial_aggr.order_by_expr().to_vec();

                    let next_partition_mode = if can_repartition {
                        // construct a second aggregation with 'AggregateMode::FinalPartitioned'
                        AggregateMode::FinalPartitioned
                    } else {
                        // construct a second aggregation, keeping the final column name equal to the
                        // first aggregation and the expressions corresponding to the respective aggregate
                        AggregateMode::Final
                    };

                    let final_grouping_set = PhysicalGroupBy::new_single(
                        final_group
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| (expr.clone(), groups.expr()[i].1.clone()))
                            .collect()
                    );

                    Ok(Arc::new(AggregateExec::try_new(
                        next_partition_mode,
                        final_grouping_set,
                        updated_aggregates,
                        filters,
                        updated_order_bys,
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
                    let physical_plans = self.create_initial_plan_multi(inputs.iter().map(|lp| lp.as_ref()), session_state).await?;

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
                            return not_impl_err!("Physical plan does not support DistributeBy partitioning");
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
                        .map(|e| create_physical_sort_expr(
                            e,
                            input_dfschema,
                            &input_schema,
                            session_state.execution_props(),
                        ))
                        .collect::<Result<Vec<_>>>()?;
                    let new_sort = SortExec::new(sort_expr, physical_input)
                        .with_fetch(*fetch);
                    Ok(Arc::new(new_sort))
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
                                Projection::try_new(
                                    final_join_result,
                                    Arc::new(join_plan),
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
                    let left_right = self.create_initial_plan_multi([left.as_ref(), right.as_ref()], session_state).await?;
                    let [physical_left, physical_right]: [Arc<dyn ExecutionPlan>; 2] = left_right.try_into().map_err(|_| DataFusionError::Internal("`create_initial_plan_multi` is broken".to_string()))?;
                    let left_df_schema = left.schema();
                    let right_df_schema = right.schema();
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
                            not_impl_err!("SortMergeJoinExec does not support join_filter now.")
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
                    let left_right = self.create_initial_plan_multi([left.as_ref(), right.as_ref()], session_state).await?;
                    let [left, right]: [Arc<dyn ExecutionPlan>; 2] = left_right.try_into().map_err(|_| DataFusionError::Internal("`create_initial_plan_multi` is broken".to_string()))?;
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
                LogicalPlan::Unnest(Unnest { input, column, schema, options }) => {
                    let input = self.create_initial_plan(input, session_state).await?;
                    let column_exec = schema.index_of_column(column)
                        .map(|idx| Column::new(&column.name, idx))?;
                    let schema = SchemaRef::new(schema.as_ref().to_owned().into());
                    Ok(Arc::new(UnnestExec::new(input, column_exec, schema, options.clone())))
                }
                LogicalPlan::Ddl(ddl) => {
                    // There is no default plan for DDl statements --
                    // it must be handled at a higher level (so that
                    // the appropriate table can be registered with
                    // the context)
                    let name = ddl.name();
                    not_impl_err!(
                        "Unsupported logical plan: {name}"
                    )
                }
                LogicalPlan::Prepare(_) => {
                    // There is no default plan for "PREPARE" -- it must be
                    // handled at a higher level (so that the appropriate
                    // statement can be prepared)
                    not_impl_err!(
                        "Unsupported logical plan: Prepare"
                    )
                }
                LogicalPlan::Dml(dml) => {
                    // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                    not_impl_err!(
                        "Unsupported logical plan: Dml({0})", dml.op
                    )
                }
                LogicalPlan::Statement(statement) => {
                    // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                    let name = statement.name();
                    not_impl_err!(
                        "Unsupported logical plan: Statement({name})"
                    )
                }
                LogicalPlan::DescribeTable(DescribeTable { schema, output_schema}) => {
                    let output_schema: Schema = output_schema.as_ref().into();
                    self.plan_describe(schema.clone(), Arc::new(output_schema))
                }
                LogicalPlan::Explain(_) => internal_err!(
                    "Unsupported logical plan: Explain must be root of the plan"
                ),
                LogicalPlan::Distinct(_) => {
                    internal_err!(
                        "Unsupported logical plan: Distinct should be replaced to Aggregate"
                    )
                }
                LogicalPlan::Analyze(_) => internal_err!(
                    "Unsupported logical plan: Analyze must be root of the plan"
                ),
                LogicalPlan::Extension(e) => {
                    let physical_inputs = self.create_initial_plan_multi(e.node.inputs(), session_state).await?;

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

                    let plan = match maybe_plan {
                        Some(v) => Ok(v),
                        _ => plan_err!("No installed planner was able to convert the custom node to an execution plan: {:?}", e.node)
                    }?;

                    // Ensure the ExecutionPlan's schema matches the
                    // declared logical schema to catch and warn about
                    // logic errors when creating user defined plans.
                    if !e.node.schema().matches_arrow_schema(&plan.schema()) {
                        plan_err!(
                            "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                            LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                            e.node, e.node.schema(), plan.schema()
                        )
                    } else {
                        Ok(plan)
                    }
                }
                LogicalPlan::RecursiveQuery(RecursiveQuery { name, static_term, recursive_term, is_distinct }) => {
                    let static_term = self.create_initial_plan(static_term, session_state).await?;
                    let recursive_term = self.create_initial_plan(recursive_term, session_state).await?;

                    Ok(Arc::new(RecursiveQueryExec::new(name.clone(), static_term, recursive_term, *is_distinct)))
                }
                LogicalPlan::NamedRelation(NamedRelation {name, schema}) => {
                    // Named relations is how we represent access to any sort of dynamic data provider. They
                    // differ from tables in the sense that they can start existing dynamically during the
                    // execution of a query and then disappear before it even finishes.
                    //
                    // This system allows us to replicate the tricky behavior of classical databases where a
                    // temporary "working table" (as it is called in Postgres) can be used when dealing with
                    // complex operations (such as recursive CTEs) and then can be dropped. Since DataFusion
                    // at its core is heavily stream-based and vectorized, we try to avoid using 'real' tables
                    // and let the streams take care of the data flow in this as well.

                    // Since the actual "input"'s will be only available to us at runtime (through task context)
                    // we can't really do any sort of meaningful validation here.
                    let schema = SchemaRef::new(schema.as_ref().to_owned().into());
                    Ok(Arc::new(ContinuanceExec::new(name.clone(), schema)))
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
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => create_cube_physical_expr(
                    exprs,
                    input_dfschema,
                    input_schema,
                    session_state,
                ),
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    create_rollup_physical_expr(
                        exprs,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
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

/// Expand and align a GROUPING SET expression.
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
///
/// This will take a list of grouping sets and ensure that each group is
/// properly aligned for the physical execution plan. We do this by
/// identifying all unique expression in each group and conforming each
/// group to the same set of expression types and ordering.
/// For example, if we have something like `GROUPING SETS ((a,b,c),(a),(b),(b,c))`
/// we would expand this to `GROUPING SETS ((a,b,c),(a,NULL,NULL),(NULL,b,NULL),(NULL,b,c))
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn merge_grouping_set_physical_expr(
    grouping_sets: &[Vec<Expr>],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_groups = grouping_sets.len();
    let mut all_exprs: Vec<Expr> = vec![];
    let mut grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    for expr in grouping_sets.iter().flatten() {
        if !all_exprs.contains(expr) {
            all_exprs.push(expr.clone());

            grouping_set_expr.push(get_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);

            null_exprs.push(get_null_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);
        }
    }

    let mut merged_sets: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    for expr_group in grouping_sets.iter() {
        let group: Vec<bool> = all_exprs
            .iter()
            .map(|expr| !expr_group.contains(expr))
            .collect();

        merged_sets.push(group)
    }

    Ok(PhysicalGroupBy::new(
        grouping_set_expr,
        null_exprs,
        merged_sets,
    ))
}

/// Expand and align a CUBE expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn create_cube_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();
    let num_groups = num_of_exprs * num_of_exprs;

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?)
    }

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    groups.push(vec![false; num_of_exprs]);

    for null_count in 1..=num_of_exprs {
        for null_idx in (0..num_of_exprs).combinations(null_count) {
            let mut next_group: Vec<bool> = vec![false; num_of_exprs];
            null_idx.into_iter().for_each(|i| next_group[i] = true);
            groups.push(next_group);
        }
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// Expand and align a ROLLUP expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
fn create_rollup_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_of_exprs + 1);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?)
    }

    for total in 0..=num_of_exprs {
        let mut group: Vec<bool> = Vec::with_capacity(num_of_exprs);

        for index in 0..num_of_exprs {
            if index < total {
                group.push(false);
            } else {
                group.push(true);
            }
        }

        groups.push(group)
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
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
                .map(|e| {
                    create_physical_sort_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            if !is_window_valid(window_frame) {
                return plan_err!(
                        "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
                        window_frame.start_bound, window_frame.end_bound
                    );
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
        other => plan_err!("Invalid window expression '{other:?}'"),
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
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        _ => (e.display_name()?, e),
    };
    create_window_expr_with_name(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

type AggregateExprWithOptionalArgs = (
    Arc<dyn AggregateExpr>,
    // The filter clause, if any
    Option<Arc<dyn PhysicalExpr>>,
    // Ordering requirements, if any
    Option<Vec<PhysicalSortExpr>>,
);

/// Create an aggregate expression with a name from a logical expression
pub fn create_aggregate_expr_with_name_and_maybe_filter(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    match e {
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            filter,
            order_by,
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
            let filter = match filter {
                Some(e) => Some(create_physical_expr(
                    e,
                    logical_input_schema,
                    physical_input_schema,
                    execution_props,
                )?),
                None => None,
            };
            let order_by = match order_by {
                Some(e) => Some(
                    e.iter()
                        .map(|expr| {
                            create_physical_sort_expr(
                                expr,
                                logical_input_schema,
                                physical_input_schema,
                                execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                None => None,
            };
            let ordering_reqs = order_by.clone().unwrap_or(vec![]);
            let agg_expr = aggregates::create_aggregate_expr(
                fun,
                *distinct,
                &args,
                &ordering_reqs,
                physical_input_schema,
                name,
            )?;
            Ok((agg_expr, filter, order_by))
        }
        Expr::AggregateUDF(AggregateUDF {
            fun,
            args,
            filter,
            order_by,
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

            let filter = match filter {
                Some(e) => Some(create_physical_expr(
                    e,
                    logical_input_schema,
                    physical_input_schema,
                    execution_props,
                )?),
                None => None,
            };
            let order_by = match order_by {
                Some(e) => Some(
                    e.iter()
                        .map(|expr| {
                            create_physical_sort_expr(
                                expr,
                                logical_input_schema,
                                physical_input_schema,
                                execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                None => None,
            };

            let agg_expr =
                udaf::create_aggregate_expr(fun, &args, physical_input_schema, name);
            Ok((agg_expr?, filter, order_by))
        }
        other => internal_err!("Invalid aggregate expression '{other:?}'"),
    }
}

/// Create an aggregate expression from a logical expression or an alias
pub fn create_aggregate_expr_and_maybe_filter(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    // unpack (nested) aliased logical expressions, e.g. "sum(col) as total"
    let (name, e) = match e {
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        _ => (physical_name(e)?, e),
    };

    create_aggregate_expr_with_name_and_maybe_filter(
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
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    if let Expr::Sort(expr::Sort {
        expr,
        asc,
        nulls_first,
    }) = e
    {
        Ok(PhysicalSortExpr {
            expr: create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?,
            options: SortOptions {
                descending: !asc,
                nulls_first: *nulls_first,
            },
        })
    } else {
        internal_err!("Expects a sort expression")
    }
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
                        // This plan will includes statistics if show_statistics is on
                        stringified_plans.push(
                            displayable(input.as_ref())
                                .set_show_statistics(config.show_statistics)
                                .to_stringified(e.verbose, InitialPhysicalPlan),
                        );

                        // If the show_statisitcs is off, add another line to show statsitics in the case of explain verbose
                        if e.verbose && !config.show_statistics {
                            stringified_plans.push(
                                displayable(input.as_ref())
                                    .set_show_statistics(true)
                                    .to_stringified(
                                        e.verbose,
                                        InitialPhysicalPlanWithStats,
                                    ),
                            );
                        }

                        match self.optimize_internal(
                            input,
                            session_state,
                            |plan, optimizer| {
                                let optimizer_name = optimizer.name().to_string();
                                let plan_type = OptimizedPhysicalPlan { optimizer_name };
                                stringified_plans.push(
                                    displayable(plan)
                                        .set_show_statistics(config.show_statistics)
                                        .to_stringified(e.verbose, plan_type),
                                );
                            },
                        ) {
                            Ok(input) => {
                                // This plan will includes statistics if show_statistics is on
                                stringified_plans.push(
                                    displayable(input.as_ref())
                                        .set_show_statistics(config.show_statistics)
                                        .to_stringified(e.verbose, FinalPhysicalPlan),
                                );

                                // If the show_statisitcs is off, add another line to show statsitics in the case of explain verbose
                                if e.verbose && !config.show_statistics {
                                    stringified_plans.push(
                                        displayable(input.as_ref())
                                            .set_show_statistics(true)
                                            .to_stringified(
                                                e.verbose,
                                                FinalPhysicalPlanWithStats,
                                            ),
                                    );
                                }
                            }
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
        } else if let LogicalPlan::Analyze(a) = logical_plan {
            let input = self.create_physical_plan(&a.input, session_state).await?;
            let schema = SchemaRef::new((*a.schema).clone().into());
            let show_statistics = session_state.config_options().explain.show_statistics;
            Ok(Some(Arc::new(AnalyzeExec::new(
                a.verbose,
                show_statistics,
                input,
                schema,
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
            displayable(plan.as_ref()).indent(false)
        );
        trace!(
            "Detailed input physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

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
                displayable(new_plan.as_ref()).indent(false)
            );
            observer(new_plan.as_ref(), optimizer.as_ref())
        }
        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent(false)
        );
        trace!("Detailed optimized physical plan:\n{:?}", new_plan);
        Ok(new_plan)
    }

    // return an record_batch which describes a table's schema.
    fn plan_describe(
        &self,
        table_schema: Arc<Schema>,
        output_schema: Arc<Schema>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut column_names = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut is_nullables = StringBuilder::new();
        for (_, field) in table_schema.fields().iter().enumerate() {
            column_names.append_value(field.name());

            // "System supplied type" --> Use debug format of the datatype
            let data_type = field.data_type();
            data_types.append_value(format!("{data_type:?}"));

            // "YES if the column is possibly nullable, NO if it is known not nullable. "
            let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
            is_nullables.append_value(nullable_str);
        }

        let record_batch = RecordBatch::try_new(
            output_schema,
            vec![
                Arc::new(column_names.finish()),
                Arc::new(data_types.finish()),
                Arc::new(is_nullables.finish()),
            ],
        )?;

        let schema = record_batch.schema();
        let partitions = vec![vec![record_batch]];
        let projection = None;
        let mem_exec = MemoryExec::try_new(&partitions, schema, projection)?;
        Ok(Arc::new(mem_exec))
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
    use crate::datasource::file_format::options::CsvReadOptions;
    use crate::datasource::MemTable;
    use crate::physical_plan::{expressions, DisplayFormatType, Partitioning};
    use crate::physical_plan::{DisplayAs, SendableRecordBatchStream};
    use crate::physical_planner::PhysicalPlanner;
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::scalar::ScalarValue;
    use crate::test_util::{scan_empty, scan_empty_with_partitions};
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{assert_contains, TableReference};
    use datafusion_common::{DFField, DFSchema, DFSchemaRef};
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_execution::TaskContext;
    use datafusion_expr::{
        col, lit, sum, Extension, GroupingSet, LogicalPlanBuilder,
        UserDefinedLogicalNodeCore,
    };
    use fmt::Debug;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::ops::{BitAnd, Not};
    use std::{any::Any, fmt};

    fn make_session_state() -> SessionState {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(4);
        let config = config.set_bool("datafusion.optimizer.skip_failed_rules", false);
        SessionState::new_with_config_rt(config, runtime)
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
    async fn test_create_cube_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let cube = create_cube_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        let expected = r#"Ok(PhysicalGroupBy { expr: [(Column { name: "c1", index: 0 }, "c1"), (Column { name: "c2", index: 1 }, "c2"), (Column { name: "c3", index: 2 }, "c3")], null_expr: [(Literal { value: Utf8(NULL) }, "c1"), (Literal { value: Int64(NULL) }, "c2"), (Literal { value: Int64(NULL) }, "c3")], groups: [[false, false, false], [true, false, false], [false, true, false], [false, false, true], [true, true, false], [true, false, true], [false, true, true], [true, true, true]] })"#;

        assert_eq!(format!("{cube:?}"), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_rollup_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let rollup = create_rollup_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        let expected = r#"Ok(PhysicalGroupBy { expr: [(Column { name: "c1", index: 0 }, "c1"), (Column { name: "c2", index: 1 }, "c2"), (Column { name: "c3", index: 2 }, "c3")], null_expr: [(Literal { value: Utf8(NULL) }, "c1"), (Literal { value: Int64(NULL) }, "c2"), (Literal { value: Int64(NULL) }, "c3")], groups: [[true, true, true], [false, true, true], [false, false, true], [false, false, false]] })"#;

        assert_eq!(format!("{rollup:?}"), expected);

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
        assert!(!format!("{plan:?}").contains("limit="));
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
            // utf8 = utf8
            col("c1").eq(col("c1")),
            // u8 AND u8
            col("c3").bitand(col("c3")),
            // utf8 = u8
            col("c1").eq(col("c3")),
            // bool AND bool
            bool_expr.clone().and(bool_expr),
        ];
        for case in cases {
            test_csv_scan().await?.project(vec![case.clone()]).unwrap();
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
        ], metadata: {}, functional_dependencies: FunctionalDependencies { deps: [] } }, \
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

        let expected = "expr: [(BinaryExpr { left: BinaryExpr { left: Column { name: \"c1\", index: 0 }, op: Eq, right: Literal { value: Utf8(\"a\") } }, op: Or, right: BinaryExpr { left: Column { name: \"c1\", index: 0 }, op: Eq, right: Literal { value: Utf8(\"1\") } } }";

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
            r#"Error during planning: Can not find compatible types to compare Boolean with [Struct([Field { name: "foo", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), Utf8]"#
        );

        Ok(())
    }

    /// Return a `null` literal representing a struct type like: `{ a: bool }`
    fn struct_literal() -> Expr {
        let struct_literal = ScalarValue::Struct(
            None,
            vec![Field::new("foo", DataType::Boolean, false)].into(),
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
                displayable(plan.as_ref()).indent(true)
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
            internal_err!("BOOM")
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

    impl DisplayAs for NoOpExecutionPlan {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "NoOpExecutionPlan")
                }
            }
        }
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

    #[tokio::test]
    async fn test_display_plan_in_graphviz_format() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .project(vec![col("id") + lit(2)])
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();

        let expected_graph = r#"
// Begin DataFusion GraphViz Plan,
// display it online here: https://dreampuf.github.io/GraphvizOnline

digraph {
    1[shape=box label="ProjectionExec: expr=[id@0 + 2 as employee.id + Int32(2)]", tooltip=""]
    2[shape=box label="EmptyExec: produce_one_row=false", tooltip=""]
    1 -> 2 [arrowhead=none, arrowtail=normal, dir=back]
}
// End DataFusion GraphViz Plan
"#;

        let generated_graph = format!("{}", displayable(&*plan).graphviz());

        assert_eq!(expected_graph, generated_graph);
    }

    #[tokio::test]
    async fn test_display_graphviz_with_statistics() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .project(vec![col("id") + lit(2)])
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();

        let expected_tooltip = ", tooltip=\"statistics=[";

        let generated_graph = format!(
            "{}",
            displayable(&*plan).set_show_statistics(true).graphviz()
        );

        assert_contains!(generated_graph, expected_tooltip);
    }
}
