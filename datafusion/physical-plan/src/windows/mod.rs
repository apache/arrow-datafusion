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

//! Physical expressions for window functions

use std::borrow::Borrow;
use std::sync::Arc;

use crate::{
    aggregates,
    expressions::{
        cume_dist, dense_rank, lag, lead, percent_rank, rank, Literal, NthValue, Ntile,
        PhysicalSortExpr, RowNumber,
    },
    udaf, ExecutionPlan, ExecutionPlanProperties, InputOrderMode, PhysicalExpr,
};

use arrow::datatypes::Schema;
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion_common::{exec_err, Column, DataFusionError, Result, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::{
    BuiltInWindowFunction, PartitionEvaluator, SortExpr, WindowFrame,
    WindowFunctionDefinition, WindowUDF,
};
use datafusion_physical_expr::equivalence::collapse_lex_req;
use datafusion_physical_expr::{
    reverse_order_bys,
    window::{BuiltInWindowFunctionExpr, SlidingAggregateWindowExpr},
    AggregateExpr, ConstExpr, EquivalenceProperties, LexOrdering,
    PhysicalSortRequirement,
};
use itertools::Itertools;

mod bounded_window_agg_exec;
mod window_agg_exec;

pub use bounded_window_agg_exec::BoundedWindowAggExec;
pub use datafusion_physical_expr::window::{
    BuiltInWindowExpr, PlainAggregateWindowExpr, WindowExpr,
};
pub use window_agg_exec::WindowAggExec;

/// Build field from window function and add it into schema
pub fn schema_add_window_field(
    args: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    window_fn: &WindowFunctionDefinition,
    fn_name: &str,
) -> Result<Arc<Schema>> {
    let data_types = args
        .iter()
        .map(|e| e.clone().as_ref().data_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let nullability = args
        .iter()
        .map(|e| e.clone().as_ref().nullable(schema))
        .collect::<Result<Vec<_>>>()?;
    let window_expr_return_type = window_fn.return_type(&data_types, &nullability)?;
    let mut window_fields = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect_vec();
    // Skip extending schema for UDAF
    if let WindowFunctionDefinition::AggregateUDF(_) = window_fn {
        Ok(Arc::new(Schema::new(window_fields)))
    } else {
        window_fields.extend_from_slice(&[Field::new(
            fn_name,
            window_expr_return_type,
            false,
        )]);
        Ok(Arc::new(Schema::new(window_fields)))
    }
}

/// Create a physical expression for window function
#[allow(clippy::too_many_arguments)]
pub fn create_window_expr(
    fun: &WindowFunctionDefinition,
    name: String,
    args: &[Arc<dyn PhysicalExpr>],
    logical_args: &[Expr],
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Arc<WindowFrame>,
    input_schema: &Schema,
    ignore_nulls: bool,
) -> Result<Arc<dyn WindowExpr>> {
    Ok(match fun {
        WindowFunctionDefinition::AggregateFunction(fun) => {
            let aggregate = aggregates::create_aggregate_expr(
                fun,
                false,
                args,
                &[],
                input_schema,
                name,
                ignore_nulls,
            )?;
            window_expr_from_aggregate_expr(
                partition_by,
                order_by,
                window_frame,
                aggregate,
            )
        }
        WindowFunctionDefinition::BuiltInWindowFunction(fun) => {
            Arc::new(BuiltInWindowExpr::new(
                create_built_in_window_expr(fun, args, input_schema, name, ignore_nulls)?,
                partition_by,
                order_by,
                window_frame,
            ))
        }
        WindowFunctionDefinition::AggregateUDF(fun) => {
            // TODO: Ordering not supported for Window UDFs yet
            // Convert `Vec<PhysicalSortExpr>` into `Vec<Expr::Sort>`
            let sort_exprs = order_by
                .iter()
                .map(|PhysicalSortExpr { expr, options }| {
                    let field_name = expr.to_string();
                    let field_name = field_name.split('@').next().unwrap_or(&field_name);
                    Expr::Sort(SortExpr {
                        expr: Box::new(Expr::Column(Column::new(
                            None::<String>,
                            field_name,
                        ))),
                        asc: !options.descending,
                        nulls_first: options.nulls_first,
                    })
                })
                .collect::<Vec<_>>();

            let aggregate = udaf::create_aggregate_expr(
                fun.as_ref(),
                args,
                logical_args,
                &sort_exprs,
                order_by,
                input_schema,
                name,
                ignore_nulls,
                false,
            )?;
            window_expr_from_aggregate_expr(
                partition_by,
                order_by,
                window_frame,
                aggregate,
            )
        }
        WindowFunctionDefinition::WindowUDF(fun) => Arc::new(BuiltInWindowExpr::new(
            create_udwf_window_expr(fun, args, input_schema, name)?,
            partition_by,
            order_by,
            window_frame,
        )),
    })
}

/// Creates an appropriate [`WindowExpr`] based on the window frame and
fn window_expr_from_aggregate_expr(
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Arc<WindowFrame>,
    aggregate: Arc<dyn AggregateExpr>,
) -> Arc<dyn WindowExpr> {
    // Is there a potentially unlimited sized window frame?
    let unbounded_window = window_frame.start_bound.is_unbounded();

    if !unbounded_window {
        Arc::new(SlidingAggregateWindowExpr::new(
            aggregate,
            partition_by,
            order_by,
            window_frame,
        ))
    } else {
        Arc::new(PlainAggregateWindowExpr::new(
            aggregate,
            partition_by,
            order_by,
            window_frame,
        ))
    }
}

fn get_scalar_value_from_args(
    args: &[Arc<dyn PhysicalExpr>],
    index: usize,
) -> Result<Option<ScalarValue>> {
    Ok(if let Some(field) = args.get(index) {
        let tmp = field
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or_else(|| DataFusionError::NotImplemented(
                format!("There is only support Literal types for field at idx: {index} in Window Function"),
            ))?
            .value()
            .clone();
        Some(tmp)
    } else {
        None
    })
}

fn get_casted_value(
    default_value: Option<ScalarValue>,
    dtype: &DataType,
) -> Result<ScalarValue> {
    match default_value {
        Some(v) if !v.data_type().is_null() => v.cast_to(dtype),
        // If None or Null datatype
        _ => ScalarValue::try_from(dtype),
    }
}

fn create_built_in_window_expr(
    fun: &BuiltInWindowFunction,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
    ignore_nulls: bool,
) -> Result<Arc<dyn BuiltInWindowFunctionExpr>> {
    // derive the output datatype from incoming schema
    let out_data_type: &DataType = input_schema.field_with_name(&name)?.data_type();

    Ok(match fun {
        BuiltInWindowFunction::RowNumber => Arc::new(RowNumber::new(name, out_data_type)),
        BuiltInWindowFunction::Rank => Arc::new(rank(name, out_data_type)),
        BuiltInWindowFunction::DenseRank => Arc::new(dense_rank(name, out_data_type)),
        BuiltInWindowFunction::PercentRank => Arc::new(percent_rank(name, out_data_type)),
        BuiltInWindowFunction::CumeDist => Arc::new(cume_dist(name, out_data_type)),
        BuiltInWindowFunction::Ntile => {
            let n = get_scalar_value_from_args(args, 0)?.ok_or_else(|| {
                DataFusionError::Execution(
                    "NTILE requires a positive integer".to_string(),
                )
            })?;

            if n.is_null() {
                return exec_err!("NTILE requires a positive integer, but finds NULL");
            }

            if n.is_unsigned() {
                let n: u64 = n.try_into()?;
                Arc::new(Ntile::new(name, n, out_data_type))
            } else {
                let n: i64 = n.try_into()?;
                if n <= 0 {
                    return exec_err!("NTILE requires a positive integer");
                }
                Arc::new(Ntile::new(name, n as u64, out_data_type))
            }
        }
        BuiltInWindowFunction::Lag => {
            let arg = args[0].clone();
            let shift_offset = get_scalar_value_from_args(args, 1)?
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value =
                get_casted_value(get_scalar_value_from_args(args, 2)?, out_data_type)?;
            Arc::new(lag(
                name,
                out_data_type.clone(),
                arg,
                shift_offset,
                default_value,
                ignore_nulls,
            ))
        }
        BuiltInWindowFunction::Lead => {
            let arg = args[0].clone();
            let shift_offset = get_scalar_value_from_args(args, 1)?
                .map(|v| v.try_into())
                .and_then(|v| v.ok());
            let default_value =
                get_casted_value(get_scalar_value_from_args(args, 2)?, out_data_type)?;
            Arc::new(lead(
                name,
                out_data_type.clone(),
                arg,
                shift_offset,
                default_value,
                ignore_nulls,
            ))
        }
        BuiltInWindowFunction::NthValue => {
            let arg = args[0].clone();
            let n = args[1].as_any().downcast_ref::<Literal>().unwrap().value();
            let n: i64 = n
                .clone()
                .try_into()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            Arc::new(NthValue::nth(
                name,
                arg,
                out_data_type.clone(),
                n,
                ignore_nulls,
            )?)
        }
        BuiltInWindowFunction::FirstValue => {
            let arg = args[0].clone();
            Arc::new(NthValue::first(
                name,
                arg,
                out_data_type.clone(),
                ignore_nulls,
            ))
        }
        BuiltInWindowFunction::LastValue => {
            let arg = args[0].clone();
            Arc::new(NthValue::last(
                name,
                arg,
                out_data_type.clone(),
                ignore_nulls,
            ))
        }
    })
}

/// Creates a `BuiltInWindowFunctionExpr` suitable for a user defined window function
fn create_udwf_window_expr(
    fun: &Arc<WindowUDF>,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn BuiltInWindowFunctionExpr>> {
    // need to get the types into an owned vec for some reason
    let input_types: Vec<_> = args
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<_>>()?;

    // figure out the output type
    let data_type = fun.return_type(&input_types)?;
    Ok(Arc::new(WindowUDFExpr {
        fun: Arc::clone(fun),
        args: args.to_vec(),
        name,
        data_type,
    }))
}

/// Implements [`BuiltInWindowFunctionExpr`] for [`WindowUDF`]
#[derive(Clone, Debug)]
struct WindowUDFExpr {
    fun: Arc<WindowUDF>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Display name
    name: String,
    /// result type
    data_type: DataType,
}

impl BuiltInWindowFunctionExpr for WindowUDFExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        self.fun.partition_evaluator_factory()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn BuiltInWindowFunctionExpr>> {
        None
    }
}

pub(crate) fn calc_requirements<
    T: Borrow<Arc<dyn PhysicalExpr>>,
    S: Borrow<PhysicalSortExpr>,
>(
    partition_by_exprs: impl IntoIterator<Item = T>,
    orderby_sort_exprs: impl IntoIterator<Item = S>,
) -> Option<Vec<PhysicalSortRequirement>> {
    let mut sort_reqs = partition_by_exprs
        .into_iter()
        .map(|partition_by| {
            PhysicalSortRequirement::new(partition_by.borrow().clone(), None)
        })
        .collect::<Vec<_>>();
    for element in orderby_sort_exprs.into_iter() {
        let PhysicalSortExpr { expr, options } = element.borrow();
        if !sort_reqs.iter().any(|e| e.expr.eq(expr)) {
            sort_reqs.push(PhysicalSortRequirement::new(expr.clone(), Some(*options)));
        }
    }
    // Convert empty result to None. Otherwise wrap result inside Some()
    (!sort_reqs.is_empty()).then_some(sort_reqs)
}

/// This function calculates the indices such that when partition by expressions reordered with the indices
/// resulting expressions define a preset for existing ordering.
/// For instance, if input is ordered by a, b, c and PARTITION BY b, a is used,
/// this vector will be [1, 0]. It means that when we iterate b, a columns with the order [1, 0]
/// resulting vector (a, b) is a preset of the existing ordering (a, b, c).
pub fn get_ordered_partition_by_indices(
    partition_by_exprs: &[Arc<dyn PhysicalExpr>],
    input: &Arc<dyn ExecutionPlan>,
) -> Vec<usize> {
    let (_, indices) = input
        .equivalence_properties()
        .find_longest_permutation(partition_by_exprs);
    indices
}

pub(crate) fn get_partition_by_sort_exprs(
    input: &Arc<dyn ExecutionPlan>,
    partition_by_exprs: &[Arc<dyn PhysicalExpr>],
    ordered_partition_by_indices: &[usize],
) -> Result<LexOrdering> {
    let ordered_partition_exprs = ordered_partition_by_indices
        .iter()
        .map(|idx| partition_by_exprs[*idx].clone())
        .collect::<Vec<_>>();
    // Make sure ordered section doesn't move over the partition by expression
    assert!(ordered_partition_by_indices.len() <= partition_by_exprs.len());
    let (ordering, _) = input
        .equivalence_properties()
        .find_longest_permutation(&ordered_partition_exprs);
    if ordering.len() == ordered_partition_exprs.len() {
        Ok(ordering)
    } else {
        exec_err!("Expects PARTITION BY expression to be ordered")
    }
}

pub(crate) fn window_equivalence_properties(
    schema: &SchemaRef,
    input: &Arc<dyn ExecutionPlan>,
    window_expr: &[Arc<dyn WindowExpr>],
) -> EquivalenceProperties {
    // We need to update the schema, so we can not directly use
    // `input.equivalence_properties()`.
    let mut window_eq_properties = EquivalenceProperties::new(schema.clone())
        .extend(input.equivalence_properties().clone());

    for expr in window_expr {
        if let Some(builtin_window_expr) =
            expr.as_any().downcast_ref::<BuiltInWindowExpr>()
        {
            builtin_window_expr.add_equal_orderings(&mut window_eq_properties);
        }
    }
    window_eq_properties
}

/// Constructs the best-fitting windowing operator (a `WindowAggExec` or a
/// `BoundedWindowExec`) for the given `input` according to the specifications
/// of `window_exprs` and `physical_partition_keys`. Here, best-fitting means
/// not requiring additional sorting and/or partitioning for the given input.
/// - A return value of `None` represents that there is no way to construct a
///   windowing operator that doesn't need additional sorting/partitioning for
///   the given input. Existing ordering should be changed to run the given
///   windowing operation.
/// - A `Some(window exec)` value contains the optimal windowing operator (a
///   `WindowAggExec` or a `BoundedWindowExec`) for the given input.
pub fn get_best_fitting_window(
    window_exprs: &[Arc<dyn WindowExpr>],
    input: &Arc<dyn ExecutionPlan>,
    // These are the partition keys used during repartitioning.
    // They are either the same with `window_expr`'s PARTITION BY columns,
    // or it is empty if partitioning is not desirable for this windowing operator.
    physical_partition_keys: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Contains at least one window expr and all of the partition by and order by sections
    // of the window_exprs are same.
    let partitionby_exprs = window_exprs[0].partition_by();
    let orderby_keys = window_exprs[0].order_by();
    let (should_reverse, input_order_mode) =
        if let Some((should_reverse, input_order_mode)) =
            get_window_mode(partitionby_exprs, orderby_keys, input)
        {
            (should_reverse, input_order_mode)
        } else {
            return Ok(None);
        };
    let is_unbounded = input.execution_mode().is_unbounded();
    if !is_unbounded && input_order_mode != InputOrderMode::Sorted {
        // Executor has bounded input and `input_order_mode` is not `InputOrderMode::Sorted`
        // in this case removing the sort is not helpful, return:
        return Ok(None);
    };

    let window_expr = if should_reverse {
        if let Some(reversed_window_expr) = window_exprs
            .iter()
            .map(|e| e.get_reverse_expr())
            .collect::<Option<Vec<_>>>()
        {
            reversed_window_expr
        } else {
            // Cannot take reverse of any of the window expr
            // In this case, with existing ordering window cannot be run
            return Ok(None);
        }
    } else {
        window_exprs.to_vec()
    };

    // If all window expressions can run with bounded memory, choose the
    // bounded window variant:
    if window_expr.iter().all(|e| e.uses_bounded_memory()) {
        Ok(Some(Arc::new(BoundedWindowAggExec::try_new(
            window_expr,
            input.clone(),
            physical_partition_keys.to_vec(),
            input_order_mode,
        )?) as _))
    } else if input_order_mode != InputOrderMode::Sorted {
        // For `WindowAggExec` to work correctly PARTITION BY columns should be sorted.
        // Hence, if `input_order_mode` is not `Sorted` we should convert
        // input ordering such that it can work with `Sorted` (add `SortExec`).
        // Effectively `WindowAggExec` works only in `Sorted` mode.
        Ok(None)
    } else {
        Ok(Some(Arc::new(WindowAggExec::try_new(
            window_expr,
            input.clone(),
            physical_partition_keys.to_vec(),
        )?) as _))
    }
}

/// Compares physical ordering (output ordering of the `input` operator) with
/// `partitionby_exprs` and `orderby_keys` to decide whether existing ordering
/// is sufficient to run the current window operator.
/// - A `None` return value indicates that we can not remove the sort in question
///   (input ordering is not sufficient to run current window executor).
/// - A `Some((bool, InputOrderMode))` value indicates that the window operator
///   can run with existing input ordering, so we can remove `SortExec` before it.
/// The `bool` field in the return value represents whether we should reverse window
/// operator to remove `SortExec` before it. The `InputOrderMode` field represents
/// the mode this window operator should work in to accommodate the existing ordering.
pub fn get_window_mode(
    partitionby_exprs: &[Arc<dyn PhysicalExpr>],
    orderby_keys: &[PhysicalSortExpr],
    input: &Arc<dyn ExecutionPlan>,
) -> Option<(bool, InputOrderMode)> {
    let input_eqs = input.equivalence_properties().clone();
    let mut partition_by_reqs: Vec<PhysicalSortRequirement> = vec![];
    let (_, indices) = input_eqs.find_longest_permutation(partitionby_exprs);
    partition_by_reqs.extend(indices.iter().map(|&idx| PhysicalSortRequirement {
        expr: partitionby_exprs[idx].clone(),
        options: None,
    }));
    // Treat partition by exprs as constant. During analysis of requirements are satisfied.
    let const_exprs = partitionby_exprs
        .iter()
        .map(|expr| ConstExpr::new(expr.clone()));
    let partition_by_eqs = input_eqs.add_constants(const_exprs);
    let order_by_reqs = PhysicalSortRequirement::from_sort_exprs(orderby_keys);
    let reverse_order_by_reqs =
        PhysicalSortRequirement::from_sort_exprs(&reverse_order_bys(orderby_keys));
    for (should_swap, order_by_reqs) in
        [(false, order_by_reqs), (true, reverse_order_by_reqs)]
    {
        let req = [partition_by_reqs.clone(), order_by_reqs].concat();
        let req = collapse_lex_req(req);
        if partition_by_eqs.ordering_satisfy_requirement(&req) {
            // Window can be run with existing ordering
            let mode = if indices.len() == partitionby_exprs.len() {
                InputOrderMode::Sorted
            } else if indices.is_empty() {
                InputOrderMode::Linear
            } else {
                InputOrderMode::PartiallySorted(indices)
            };
            return Some((should_swap, mode));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::expressions::col;
    use crate::streaming::StreamingTableExec;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};

    use arrow::compute::SortOptions;
    use datafusion_execution::TaskContext;

    use datafusion_functions_aggregate::count::count_udaf;
    use futures::FutureExt;

    use InputOrderMode::{Linear, PartiallySorted, Sorted};

    fn create_test_schema() -> Result<SchemaRef> {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));

        Ok(schema)
    }

    fn create_test_schema2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema3() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    /// make PhysicalSortExpr with default options
    pub fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        sort_expr_options(name, schema, SortOptions::default())
    }

    /// PhysicalSortExpr with specified options
    pub fn sort_expr_options(
        name: &str,
        schema: &Schema,
        options: SortOptions,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options,
        }
    }

    /// Created a sorted Streaming Table exec
    pub fn streaming_table_exec(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        infinite_source: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sort_exprs = sort_exprs.into_iter().collect();

        Ok(Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![],
            None,
            Some(sort_exprs),
            infinite_source,
            None,
        )?))
    }

    #[tokio::test]
    async fn test_calc_requirements() -> Result<()> {
        let schema = create_test_schema2()?;
        let test_data = vec![
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", true, true)],
                vec![("a", None), ("b", Some((true, true)))],
            ),
            // PARTITION BY a, ORDER BY a ASC NULLS FIRST
            (vec!["a"], vec![("a", true, true)], vec![("a", None)]),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST
            (
                vec!["a"],
                vec![("b", true, true), ("c", false, false)],
                vec![
                    ("a", None),
                    ("b", Some((true, true))),
                    ("c", Some((false, false))),
                ],
            ),
            // PARTITION BY a, c, ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST
            (
                vec!["a", "c"],
                vec![("b", true, true), ("c", false, false)],
                vec![("a", None), ("c", None), ("b", Some((true, true)))],
            ),
        ];
        for (pb_params, ob_params, expected_params) in test_data {
            let mut partitionbys = vec![];
            for col_name in pb_params {
                partitionbys.push(col(col_name, &schema)?);
            }

            let mut orderbys = vec![];
            for (col_name, descending, nulls_first) in ob_params {
                let expr = col(col_name, &schema)?;
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                orderbys.push(PhysicalSortExpr { expr, options });
            }

            let mut expected: Option<Vec<PhysicalSortRequirement>> = None;
            for (col_name, reqs) in expected_params {
                let options = reqs.map(|(descending, nulls_first)| SortOptions {
                    descending,
                    nulls_first,
                });
                let expr = col(col_name, &schema)?;
                let res = PhysicalSortRequirement::new(expr, options);
                if let Some(expected) = &mut expected {
                    expected.push(res);
                } else {
                    expected = Some(vec![res]);
                }
            }
            assert_eq!(calc_requirements(partitionbys, orderbys), expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let window_agg_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunctionDefinition::AggregateUDF(count_udaf()),
                "count".to_owned(),
                &[col("a", &schema)?],
                &[],
                &[],
                &[],
                Arc::new(WindowFrame::new(None)),
                schema.as_ref(),
                false,
            )?],
            blocking_exec,
            vec![],
        )?);

        let fut = collect(window_agg_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_satisfiy_nullable() -> Result<()> {
        let schema = create_test_schema()?;
        let params = vec![
            ((true, true), (false, false), false),
            ((true, true), (false, true), false),
            ((true, true), (true, false), false),
            ((true, false), (false, true), false),
            ((true, false), (false, false), false),
            ((true, false), (true, true), false),
            ((true, false), (true, false), true),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            expected,
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let res = physical_ordering.satisfy(&required_ordering.into(), &schema);
            assert_eq!(res, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_satisfy_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), false),
            ((true, true), (false, true), false),
            ((true, true), (true, false), true),
            ((true, false), (false, true), false),
            ((true, false), (false, false), false),
            ((true, false), (true, true), true),
            ((true, false), (true, false), true),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            expected,
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let res = physical_ordering.satisfy(&required_ordering.into(), &schema);
            assert_eq!(res, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_window_mode_exhaustive() -> Result<()> {
        let test_schema = create_test_schema3()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let sort_exprs = vec![
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ];
        let exec_unbounded = streaming_table_exec(&test_schema, sort_exprs, true)?;

        // test cases consists of vector of tuples. Where each tuple represents a single test case.
        // First field in the tuple is Vec<str> where each element in the vector represents PARTITION BY columns
        // For instance `vec!["a", "b"]` corresponds to PARTITION BY a, b
        // Second field in the tuple is Vec<str> where each element in the vector represents ORDER BY columns
        // For instance, vec!["c"], corresponds to ORDER BY c ASC NULLS FIRST, (ordering is default ordering. We do not check
        // for reversibility in this test).
        // Third field in the tuple is Option<InputOrderMode>, which corresponds to expected algorithm mode.
        // None represents that existing ordering is not sufficient to run executor with any one of the algorithms
        // (We need to add SortExec to be able to run it).
        // Some(InputOrderMode) represents, we can run algorithm with existing ordering; and algorithm should work in
        // InputOrderMode.
        let test_cases = vec![
            (vec!["a"], vec!["a"], Some(Sorted)),
            (vec!["a"], vec!["b"], Some(Sorted)),
            (vec!["a"], vec!["c"], None),
            (vec!["a"], vec!["a", "b"], Some(Sorted)),
            (vec!["a"], vec!["b", "c"], Some(Sorted)),
            (vec!["a"], vec!["a", "c"], None),
            (vec!["a"], vec!["a", "b", "c"], Some(Sorted)),
            (vec!["b"], vec!["a"], Some(Linear)),
            (vec!["b"], vec!["b"], Some(Linear)),
            (vec!["b"], vec!["c"], None),
            (vec!["b"], vec!["a", "b"], Some(Linear)),
            (vec!["b"], vec!["b", "c"], None),
            (vec!["b"], vec!["a", "c"], Some(Linear)),
            (vec!["b"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["c"], vec!["a"], Some(Linear)),
            (vec!["c"], vec!["b"], None),
            (vec!["c"], vec!["c"], Some(Linear)),
            (vec!["c"], vec!["a", "b"], Some(Linear)),
            (vec!["c"], vec!["b", "c"], None),
            (vec!["c"], vec!["a", "c"], Some(Linear)),
            (vec!["c"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["b", "a"], vec!["a"], Some(Sorted)),
            (vec!["b", "a"], vec!["b"], Some(Sorted)),
            (vec!["b", "a"], vec!["c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "b"], Some(Sorted)),
            (vec!["b", "a"], vec!["b", "c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "b", "c"], Some(Sorted)),
            (vec!["c", "b"], vec!["a"], Some(Linear)),
            (vec!["c", "b"], vec!["b"], Some(Linear)),
            (vec!["c", "b"], vec!["c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "b"], Some(Linear)),
            (vec!["c", "b"], vec!["b", "c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["c", "a"], vec!["a"], Some(PartiallySorted(vec![1]))),
            (vec!["c", "a"], vec!["b"], Some(PartiallySorted(vec![1]))),
            (vec!["c", "a"], vec!["c"], Some(PartiallySorted(vec![1]))),
            (
                vec!["c", "a"],
                vec!["a", "b"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["b", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["a", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["a", "b", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (vec!["c", "b", "a"], vec!["a"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["b"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "b"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["b", "c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "b", "c"], Some(Sorted)),
        ];
        for (case_idx, test_case) in test_cases.iter().enumerate() {
            let (partition_by_columns, order_by_params, expected) = &test_case;
            let mut partition_by_exprs = vec![];
            for col_name in partition_by_columns {
                partition_by_exprs.push(col(col_name, &test_schema)?);
            }

            let mut order_by_exprs = vec![];
            for col_name in order_by_params {
                let expr = col(col_name, &test_schema)?;
                // Give default ordering, this is same with input ordering direction
                // In this test we do check for reversibility.
                let options = SortOptions::default();
                order_by_exprs.push(PhysicalSortExpr { expr, options });
            }
            let res =
                get_window_mode(&partition_by_exprs, &order_by_exprs, &exec_unbounded);
            // Since reversibility is not important in this test. Convert Option<(bool, InputOrderMode)> to Option<InputOrderMode>
            let res = res.map(|(_, mode)| mode);
            assert_eq!(
                res, *expected,
                "Unexpected result for in unbounded test case#: {case_idx:?}, case: {test_case:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_window_mode() -> Result<()> {
        let test_schema = create_test_schema3()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let sort_exprs = vec![
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ];
        let exec_unbounded = streaming_table_exec(&test_schema, sort_exprs, true)?;

        // test cases consists of vector of tuples. Where each tuple represents a single test case.
        // First field in the tuple is Vec<str> where each element in the vector represents PARTITION BY columns
        // For instance `vec!["a", "b"]` corresponds to PARTITION BY a, b
        // Second field in the tuple is Vec<(str, bool, bool)> where each element in the vector represents ORDER BY columns
        // For instance, vec![("c", false, false)], corresponds to ORDER BY c ASC NULLS LAST,
        // similarly, vec![("c", true, true)], corresponds to ORDER BY c DESC NULLS FIRST,
        // Third field in the tuple is Option<(bool, InputOrderMode)>, which corresponds to expected result.
        // None represents that existing ordering is not sufficient to run executor with any one of the algorithms
        // (We need to add SortExec to be able to run it).
        // Some((bool, InputOrderMode)) represents, we can run algorithm with existing ordering. Algorithm should work in
        // InputOrderMode, bool field represents whether we should reverse window expressions to run executor with existing ordering.
        // For instance, `Some((false, InputOrderMode::Sorted))`, represents that we shouldn't reverse window expressions. And algorithm
        // should work in Sorted mode to work with existing ordering.
        let test_cases = vec![
            // PARTITION BY a, b ORDER BY c ASC NULLS LAST
            (vec!["a", "b"], vec![("c", false, false)], None),
            // ORDER BY c ASC NULLS FIRST
            (vec![], vec![("c", false, true)], None),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (vec!["b"], vec![("c", false, true)], None),
            // PARTITION BY a, ORDER BY c ASC NULLS FIRST
            (vec!["a"], vec![("c", false, true)], None),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true), ("e", false, true)],
                None,
            ),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (vec!["a"], vec![("b", false, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a ASC NULLS FIRST
            (vec!["a"], vec![("a", false, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a ASC NULLS LAST
            (vec!["a"], vec![("a", false, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a DESC NULLS FIRST
            (vec!["a"], vec![("a", true, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a DESC NULLS LAST
            (vec!["a"], vec![("a", true, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY b ASC NULLS LAST
            (vec!["a"], vec![("b", false, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY b DESC NULLS LAST
            (vec!["a"], vec![("b", true, false)], Some((true, Sorted))),
            // PARTITION BY a, b ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true)],
                Some((false, Sorted)),
            ),
            // PARTITION BY b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["b", "a"],
                vec![("c", false, true)],
                Some((false, Sorted)),
            ),
            // PARTITION BY a, b ORDER BY c DESC NULLS LAST
            (
                vec!["a", "b"],
                vec![("c", true, false)],
                Some((true, Sorted)),
            ),
            // PARTITION BY e ORDER BY a ASC NULLS FIRST
            (
                vec!["e"],
                vec![("a", false, true)],
                // For unbounded, expects to work in Linear mode. Shouldn't reverse window function.
                Some((false, Linear)),
            ),
            // PARTITION BY b, c ORDER BY a ASC NULLS FIRST, c ASC NULLS FIRST
            (
                vec!["b", "c"],
                vec![("a", false, true), ("c", false, true)],
                Some((false, Linear)),
            ),
            // PARTITION BY b ORDER BY a ASC NULLS FIRST
            (vec!["b"], vec![("a", false, true)], Some((false, Linear))),
            // PARTITION BY a, e ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "e"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![0]))),
            ),
            // PARTITION BY a, c ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "c"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![0]))),
            ),
            // PARTITION BY c, a ORDER BY b ASC NULLS FIRST
            (
                vec!["c", "a"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![1]))),
            ),
            // PARTITION BY d, b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["d", "b", "a"],
                vec![("c", false, true)],
                Some((false, PartiallySorted(vec![2, 1]))),
            ),
            // PARTITION BY e, b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["e", "b", "a"],
                vec![("c", false, true)],
                Some((false, PartiallySorted(vec![2, 1]))),
            ),
            // PARTITION BY d, a ORDER BY b ASC NULLS FIRST
            (
                vec!["d", "a"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![1]))),
            ),
            // PARTITION BY b, ORDER BY b, a ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", false, true), ("a", false, true)],
                Some((false, Sorted)),
            ),
            // ORDER BY b, a ASC NULLS FIRST
            (vec![], vec![("b", false, true), ("a", false, true)], None),
        ];
        for (case_idx, test_case) in test_cases.iter().enumerate() {
            let (partition_by_columns, order_by_params, expected) = &test_case;
            let mut partition_by_exprs = vec![];
            for col_name in partition_by_columns {
                partition_by_exprs.push(col(col_name, &test_schema)?);
            }

            let mut order_by_exprs = vec![];
            for (col_name, descending, nulls_first) in order_by_params {
                let expr = col(col_name, &test_schema)?;
                let options = SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                };
                order_by_exprs.push(PhysicalSortExpr { expr, options });
            }

            assert_eq!(
                get_window_mode(&partition_by_exprs, &order_by_exprs, &exec_unbounded),
                *expected,
                "Unexpected result for in unbounded test case#: {case_idx:?}, case: {test_case:?}"
            );
        }

        Ok(())
    }
}
