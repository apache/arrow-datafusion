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

//! Optimizer rule for type validation and coercion

use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{RewriteRecursion, TreeNodeRewriter};
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, plan_err, DFSchema, DFSchemaRef,
    DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{
    self, AggregateFunctionDefinition, Between, BinaryExpr, Case, Exists, InList,
    InSubquery, Like, ScalarFunction, WindowFunction,
};
use datafusion_expr::expr_rewriter::rewrite_preserving_name;
use datafusion_expr::expr_schema::cast_subquery;
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::type_coercion::binary::{
    comparison_coercion, get_input_types, like_coercion,
};
use datafusion_expr::type_coercion::functions::data_types;
use datafusion_expr::type_coercion::other::{
    get_coerce_type_for_case_expression, get_coerce_type_for_list,
};
use datafusion_expr::type_coercion::{is_datetime, is_utf8_or_large_utf8};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    is_false, is_not_false, is_not_true, is_not_unknown, is_true, is_unknown,
    type_coercion, window_function, AggregateFunction, BuiltinScalarFunction, Expr,
    ExprSchemable, LogicalPlan, Operator, Projection, ScalarFunctionDefinition,
    Signature, WindowFrame, WindowFrameBound, WindowFrameUnits,
};

use crate::analyzer::AnalyzerRule;

#[derive(Default)]
pub struct TypeCoercion {}

impl TypeCoercion {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        analyze_internal(&DFSchema::empty(), &plan)
    }
}

fn analyze_internal(
    // use the external schema to handle the correlated subqueries case
    external_schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    // optimize child plans first
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|p| analyze_internal(external_schema, p))
        .collect::<Result<Vec<_>>>()?;
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema = merge_schema(new_inputs.iter().collect());

    if let LogicalPlan::TableScan(ts) = plan {
        let source_schema =
            DFSchema::try_from_qualified_schema(&ts.table_name, &ts.source.schema())?;
        schema.merge(&source_schema);
    }

    // merge the outer schema for correlated subqueries
    // like case:
    // select t2.c2 from t1 where t1.c1 in (select t2.c1 from t2 where t2.c2=t1.c3)
    schema.merge(external_schema);

    let mut expr_rewrite = TypeCoercionRewriter {
        schema: Arc::new(schema),
    };

    let new_expr = plan
        .expressions()
        .into_iter()
        .map(|expr| {
            // ensure aggregate names don't change:
            // https://github.com/apache/arrow-datafusion/issues/3555
            rewrite_preserving_name(expr, &mut expr_rewrite)
        })
        .collect::<Result<Vec<_>>>()?;

    // TODO: with_new_exprs can't change the schema, so we need to do this here
    match &plan {
        LogicalPlan::Projection(_) => Ok(LogicalPlan::Projection(Projection::try_new(
            new_expr,
            Arc::new(new_inputs[0].clone()),
        )?)),
        _ => plan.with_new_exprs(new_expr, &new_inputs),
    }
}

pub(crate) struct TypeCoercionRewriter {
    pub(crate) schema: DFSchemaRef,
}

impl TreeNodeRewriter for TypeCoercionRewriter {
    type N = Expr;

    fn pre_visit(&mut self, _expr: &Expr) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::ScalarSubquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => {
                let new_plan = analyze_internal(&self.schema, &subquery)?;
                Ok(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns,
                }))
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let new_plan = analyze_internal(&self.schema, &subquery.subquery)?;
                Ok(Expr::Exists(Exists {
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                    },
                    negated,
                }))
            }
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => {
                let new_plan = analyze_internal(&self.schema, &subquery.subquery)?;
                let expr_type = expr.get_type(&self.schema)?;
                let subquery_type = new_plan.schema().field(0).data_type();
                let common_type = comparison_coercion(&expr_type, subquery_type).ok_or(plan_datafusion_err!(
                        "expr type {expr_type:?} can't cast to {subquery_type:?} in InSubquery"
                    ),
                )?;
                let new_subquery = Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns: subquery.outer_ref_columns,
                };
                Ok(Expr::InSubquery(InSubquery::new(
                    Box::new(expr.cast_to(&common_type, &self.schema)?),
                    cast_subquery(new_subquery, &common_type)?,
                    negated,
                )))
            }
            Expr::IsTrue(expr) => {
                let expr = is_true(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotTrue(expr) => {
                let expr = is_not_true(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsFalse(expr) => {
                let expr = is_false(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotFalse(expr) => {
                let expr =
                    is_not_false(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsUnknown(expr) => {
                let expr = is_unknown(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotUnknown(expr) => {
                let expr =
                    is_not_unknown(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                let left_type = expr.get_type(&self.schema)?;
                let right_type = pattern.get_type(&self.schema)?;
                let coerced_type = like_coercion(&left_type,  &right_type).ok_or_else(|| {
                    let op_name = if case_insensitive {
                        "ILIKE"
                    } else {
                        "LIKE"
                    };
                    plan_datafusion_err!(
                        "There isn't a common type to coerce {left_type} and {right_type} in {op_name} expression"
                    )
                })?;
                let expr = Box::new(expr.cast_to(&coerced_type, &self.schema)?);
                let pattern = Box::new(pattern.cast_to(&coerced_type, &self.schema)?);
                let expr = Expr::Like(Like::new(
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                ));
                Ok(expr)
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (left_type, right_type) = get_input_types(
                    &left.get_type(&self.schema)?,
                    &op,
                    &right.get_type(&self.schema)?,
                )?;

                Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left.cast_to(&left_type, &self.schema)?),
                    op,
                    Box::new(right.cast_to(&right_type, &self.schema)?),
                )))
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let expr_type = expr.get_type(&self.schema)?;
                let low_type = low.get_type(&self.schema)?;
                let low_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {expr_type} and {low_type} in BETWEEN expression"
                        ))
                    })?;
                let high_type = high.get_type(&self.schema)?;
                let high_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                        ))
                    })?;
                let coercion_type =
                    comparison_coercion(&low_coerced_type, &high_coerced_type)
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                            ))
                        })?;
                let expr = Expr::Between(Between::new(
                    Box::new(expr.cast_to(&coercion_type, &self.schema)?),
                    negated,
                    Box::new(low.cast_to(&coercion_type, &self.schema)?),
                    Box::new(high.cast_to(&coercion_type, &self.schema)?),
                ));
                Ok(expr)
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let expr_data_type = expr.get_type(&self.schema)?;
                let list_data_types = list
                    .iter()
                    .map(|list_expr| list_expr.get_type(&self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let result_type =
                    get_coerce_type_for_list(&expr_data_type, &list_data_types);
                match result_type {
                    None => plan_err!(
                        "Can not find compatible types to compare {expr_data_type:?} with {list_data_types:?}"
                    ),
                    Some(coerced_type) => {
                        // find the coerced type
                        let cast_expr = expr.cast_to(&coerced_type, &self.schema)?;
                        let cast_list_expr = list
                            .into_iter()
                            .map(|list_expr| {
                                list_expr.cast_to(&coerced_type, &self.schema)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let expr = Expr::InList(InList ::new(
                             Box::new(cast_expr),
                             cast_list_expr,
                            negated,
                        ));
                        Ok(expr)
                    }
                }
            }
            Expr::Case(case) => {
                let case = coerce_case_expression(case, &self.schema)?;
                Ok(Expr::Case(case))
            }
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => match func_def {
                ScalarFunctionDefinition::BuiltIn(fun) => {
                    let new_args = coerce_arguments_for_signature(
                        args.as_slice(),
                        &self.schema,
                        &fun.signature(),
                    )?;
                    let new_args = coerce_arguments_for_fun(
                        new_args.as_slice(),
                        &self.schema,
                        &fun,
                    )?;
                    Ok(Expr::ScalarFunction(ScalarFunction::new(fun, new_args)))
                }
                ScalarFunctionDefinition::UDF(fun) => {
                    let new_expr = coerce_arguments_for_signature(
                        args.as_slice(),
                        &self.schema,
                        fun.signature(),
                    )?;
                    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, new_expr)))
                }
                ScalarFunctionDefinition::Name(_) => {
                    internal_err!("Function `Expr` with name should be resolved.")
                }
            },
            Expr::AggregateFunction(expr::AggregateFunction {
                func_def,
                args,
                distinct,
                filter,
                order_by,
            }) => match func_def {
                AggregateFunctionDefinition::BuiltIn(fun) => {
                    let new_expr = coerce_agg_exprs_for_signature(
                        &fun,
                        &args,
                        &self.schema,
                        &fun.signature(),
                    )?;
                    let expr = Expr::AggregateFunction(expr::AggregateFunction::new(
                        fun, new_expr, distinct, filter, order_by,
                    ));
                    Ok(expr)
                }
                AggregateFunctionDefinition::UDF(fun) => {
                    let new_expr = coerce_arguments_for_signature(
                        args.as_slice(),
                        &self.schema,
                        fun.signature(),
                    )?;
                    let expr = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                        fun, new_expr, false, filter, order_by,
                    ));
                    Ok(expr)
                }
                AggregateFunctionDefinition::Name(_) => {
                    internal_err!("Function `Expr` with name should be resolved.")
                }
            },
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }) => {
                let window_frame =
                    coerce_window_frame(window_frame, &self.schema, &order_by)?;

                let args = match &fun {
                    window_function::WindowFunction::AggregateFunction(fun) => {
                        coerce_agg_exprs_for_signature(
                            fun,
                            &args,
                            &self.schema,
                            &fun.signature(),
                        )?
                    }
                    _ => args,
                };

                let expr = Expr::WindowFunction(WindowFunction::new(
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                ));
                Ok(expr)
            }
            expr => Ok(expr),
        }
    }
}

/// Casts the given `value` to `target_type`. Note that this function
/// only considers `Null` or `Utf8` values.
fn coerce_scalar(target_type: &DataType, value: &ScalarValue) -> Result<ScalarValue> {
    match value {
        // Coerce Utf8 values:
        ScalarValue::Utf8(Some(val)) => {
            ScalarValue::try_from_string(val.clone(), target_type)
        }
        s => {
            if s.is_null() {
                // Coerce `Null` values:
                ScalarValue::try_from(target_type)
            } else {
                // Values except `Utf8`/`Null` variants already have the right type
                // (casted before) since we convert `sqlparser` outputs to `Utf8`
                // for all possible cases. Therefore, we return a clone here.
                Ok(s.clone())
            }
        }
    }
}

/// This function coerces `value` to `target_type` in a range-aware fashion.
/// If the coercion is successful, we return an `Ok` value with the result.
/// If the coercion fails because `target_type` is not wide enough (i.e. we
/// can not coerce to `target_type`, but we can to a wider type in the same
/// family), we return a `Null` value of this type to signal this situation.
/// Downstream code uses this signal to treat these values as *unbounded*.
fn coerce_scalar_range_aware(
    target_type: &DataType,
    value: &ScalarValue,
) -> Result<ScalarValue> {
    coerce_scalar(target_type, value).or_else(|err| {
        // If type coercion fails, check if the largest type in family works:
        if let Some(largest_type) = get_widest_type_in_family(target_type) {
            coerce_scalar(largest_type, value).map_or_else(
                |_| exec_err!("Cannot cast {value:?} to {target_type:?}"),
                |_| ScalarValue::try_from(target_type),
            )
        } else {
            Err(err)
        }
    })
}

/// This function returns the widest type in the family of `given_type`.
/// If the given type is already the widest type, it returns `None`.
/// For example, if `given_type` is `Int8`, it returns `Int64`.
fn get_widest_type_in_family(given_type: &DataType) -> Option<&DataType> {
    match given_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Some(&DataType::UInt64),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(&DataType::Int64),
        DataType::Float16 | DataType::Float32 => Some(&DataType::Float64),
        _ => None,
    }
}

/// Coerces the given (window frame) `bound` to `target_type`.
fn coerce_frame_bound(
    target_type: &DataType,
    bound: &WindowFrameBound,
) -> Result<WindowFrameBound> {
    match bound {
        WindowFrameBound::Preceding(v) => {
            coerce_scalar_range_aware(target_type, v).map(WindowFrameBound::Preceding)
        }
        WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        WindowFrameBound::Following(v) => {
            coerce_scalar_range_aware(target_type, v).map(WindowFrameBound::Following)
        }
    }
}

// Coerces the given `window_frame` to use appropriate natural types.
// For example, ROWS and GROUPS frames use `UInt64` during calculations.
fn coerce_window_frame(
    window_frame: WindowFrame,
    schema: &DFSchemaRef,
    expressions: &[Expr],
) -> Result<WindowFrame> {
    let mut window_frame = window_frame;
    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let target_type = match window_frame.units {
        WindowFrameUnits::Range => {
            if let Some(col_type) = current_types.first() {
                if col_type.is_numeric() || is_utf8_or_large_utf8(col_type) {
                    col_type
                } else if is_datetime(col_type) {
                    &DataType::Interval(IntervalUnit::MonthDayNano)
                } else {
                    return internal_err!(
                        "Cannot run range queries on datatype: {col_type:?}"
                    );
                }
            } else {
                return internal_err!("ORDER BY column cannot be empty");
            }
        }
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => &DataType::UInt64,
    };
    window_frame.start_bound =
        coerce_frame_bound(target_type, &window_frame.start_bound)?;
    window_frame.end_bound = coerce_frame_bound(target_type, &window_frame.end_bound)?;
    Ok(window_frame)
}

// Support the `IsTrue` `IsNotTrue` `IsFalse` `IsNotFalse` type coercion.
// The above op will be rewrite to the binary op when creating the physical op.
fn get_casted_expr_for_bool_op(expr: &Expr, schema: &DFSchemaRef) -> Result<Expr> {
    let left_type = expr.get_type(schema)?;
    get_input_types(&left_type, &Operator::IsDistinctFrom, &DataType::Boolean)?;
    cast_expr(expr, &DataType::Boolean, schema)
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature(
    expressions: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(vec![]);
    }

    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types(&current_types, signature)?;

    expressions
        .iter()
        .enumerate()
        .map(|(i, expr)| cast_expr(expr, &new_types[i], schema))
        .collect::<Result<Vec<_>>>()
}

fn coerce_arguments_for_fun(
    expressions: &[Expr],
    schema: &DFSchema,
    fun: &BuiltinScalarFunction,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(vec![]);
    }

    let mut expressions: Vec<Expr> = expressions.to_vec();

    // Cast Fixedsizelist to List for array functions
    if *fun == BuiltinScalarFunction::MakeArray {
        expressions = expressions
            .into_iter()
            .map(|expr| {
                let data_type = expr.get_type(schema).unwrap();
                if let DataType::FixedSizeList(field, _) = data_type {
                    let field = field.as_ref().clone();
                    let to_type = DataType::List(Arc::new(field));
                    expr.cast_to(&to_type, schema)
                } else {
                    Ok(expr)
                }
            })
            .collect::<Result<Vec<_>>>()?;
    }

    if *fun == BuiltinScalarFunction::MakeArray {
        // Find the final data type for the function arguments
        let current_types = expressions
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<Result<Vec<_>>>()?;

        let new_type = current_types
            .iter()
            .skip(1)
            .fold(current_types.first().unwrap().clone(), |acc, x| {
                comparison_coercion(&acc, x).unwrap_or(acc)
            });

        return expressions
            .iter()
            .zip(current_types)
            .map(|(expr, from_type)| cast_array_expr(expr, &from_type, &new_type, schema))
            .collect();
    }
    Ok(expressions)
}

/// Cast `expr` to the specified type, if possible
fn cast_expr(expr: &Expr, to_type: &DataType, schema: &DFSchema) -> Result<Expr> {
    expr.clone().cast_to(to_type, schema)
}

/// Cast array `expr` to the specified type, if possible
fn cast_array_expr(
    expr: &Expr,
    from_type: &DataType,
    to_type: &DataType,
    schema: &DFSchema,
) -> Result<Expr> {
    if from_type.equals_datatype(&DataType::Null) {
        Ok(expr.clone())
    } else {
        cast_expr(expr, to_type, schema)
    }
}

/// Returns the coerced exprs for each `input_exprs`.
/// Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
/// data type of `input_exprs` need to be coerced.
fn coerce_agg_exprs_for_signature(
    agg_fun: &AggregateFunction,
    input_exprs: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if input_exprs.is_empty() {
        return Ok(vec![]);
    }
    let current_types = input_exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let coerced_types =
        type_coercion::aggregates::coerce_types(agg_fun, &current_types, signature)?;

    input_exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| cast_expr(expr, &coerced_types[i], schema))
        .collect::<Result<Vec<_>>>()
}

fn coerce_case_expression(case: Case, schema: &DFSchemaRef) -> Result<Case> {
    // Given expressions like:
    //
    // CASE a1
    //   WHEN a2 THEN b1
    //   WHEN a3 THEN b2
    //   ELSE b3
    // END
    //
    // or:
    //
    // CASE
    //   WHEN x1 THEN b1
    //   WHEN x2 THEN b2
    //   ELSE b3
    // END
    //
    // Then all aN (a1, a2, a3) must be converted to a common data type in the first example
    // (case-when expression coercion)
    //
    // All xN (x1, x2) must be converted to a boolean data type in the second example
    // (when-boolean expression coercion)
    //
    // And all bN (b1, b2, b3) must be converted to a common data type in both examples
    // (then-else expression coercion)
    //
    // If any fail to find and cast to a common/specific data type, will return error
    //
    // Note that case-when and when-boolean expression coercions are mutually exclusive
    // Only one or the other can occur for a case expression, whilst then-else expression coercion will always occur

    // prepare types
    let case_type = case
        .expr
        .as_ref()
        .map(|expr| expr.get_type(&schema))
        .transpose()?;
    let then_types = case
        .when_then_expr
        .iter()
        .map(|(_when, then)| then.get_type(&schema))
        .collect::<Result<Vec<_>>>()?;
    let else_type = case
        .else_expr
        .as_ref()
        .map(|expr| expr.get_type(&schema))
        .transpose()?;

    // find common coercible types
    let case_when_coerce_type = case_type
        .as_ref()
        .map(|case_type| {
            let when_types = case
                .when_then_expr
                .iter()
                .map(|(when, _then)| when.get_type(&schema))
                .collect::<Result<Vec<_>>>()?;
            let coerced_type =
                get_coerce_type_for_case_expression(&when_types, Some(case_type));
            coerced_type.ok_or_else(|| {
                plan_datafusion_err!(
                    "Failed to coerce case ({case_type:?}) and when ({when_types:?}) \
                     to common types in CASE WHEN expression"
                )
            })
        })
        .transpose()?;
    let then_else_coerce_type =
        get_coerce_type_for_case_expression(&then_types, else_type.as_ref()).ok_or_else(
            || {
                plan_datafusion_err!(
                    "Failed to coerce then ({then_types:?}) and else ({else_type:?}) \
                     to common types in CASE WHEN expression"
                )
            },
        )?;

    // do cast if found common coercible types
    let case_expr = case
        .expr
        .zip(case_when_coerce_type.as_ref())
        .map(|(case_expr, coercible_type)| case_expr.cast_to(coercible_type, &schema))
        .transpose()?
        .map(Box::new);
    let when_then = case
        .when_then_expr
        .into_iter()
        .map(|(when, then)| {
            let when_type = case_when_coerce_type.as_ref().unwrap_or(&DataType::Boolean);
            let when = when.cast_to(when_type, &schema).map_err(|e| {
                DataFusionError::Context(
                    format!(
                        "WHEN expressions in CASE couldn't be \
                         converted to common type ({when_type})"
                    ),
                    Box::new(e),
                )
            })?;
            let then = then.cast_to(&then_else_coerce_type, &schema)?;
            Ok((Box::new(when), Box::new(then)))
        })
        .collect::<Result<Vec<_>>>()?;
    let else_expr = case
        .else_expr
        .map(|expr| expr.cast_to(&then_else_coerce_type, &schema))
        .transpose()?
        .map(Box::new);

    Ok(Case::new(case_expr, when_then, else_expr))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{FixedSizeListArray, Int32Array};
    use arrow::datatypes::{DataType, TimeUnit};

    use arrow::datatypes::Field;
    use datafusion_common::tree_node::TreeNode;
    use datafusion_common::{DFField, DFSchema, DFSchemaRef, Result, ScalarValue};
    use datafusion_expr::expr::{self, InSubquery, Like, ScalarFunction};
    use datafusion_expr::{
        cast, col, concat, concat_ws, create_udaf, is_true, AccumulatorFactoryFunction,
        AggregateFunction, AggregateUDF, BinaryExpr, BuiltinScalarFunction, Case,
        ColumnarValue, ExprSchemable, Filter, Operator, StateTypeFunction, Subquery,
    };
    use datafusion_expr::{
        lit,
        logical_plan::{EmptyRelation, Projection},
        Expr, LogicalPlan, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF,
        Signature, Volatility,
    };
    use datafusion_physical_expr::expressions::AvgAccumulator;

    use crate::analyzer::type_coercion::{
        cast_expr, coerce_case_expression, TypeCoercion, TypeCoercionRewriter,
    };
    use crate::test::assert_analyzed_plan_eq;

    fn empty() -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }))
    }

    fn empty_with_type(data_type: DataType) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new_unqualified("a", data_type, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }))
    }

    #[test]
    fn simple_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = empty_with_type(DataType::Float64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a < CAST(UInt32(2) AS Float64)\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn nested_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = empty_with_type(DataType::Float64);

        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone().or(expr)],
            empty,
        )?);
        let expected = "Projection: a < CAST(UInt32(2) AS Float64) OR a < CAST(UInt32(2) AS Float64)\
            \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn scalar_udf() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
        let fun: ScalarFunctionImplementation =
            Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::new_utf8("a"))));
        let udf = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new(
                "TestScalarUDF",
                &Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
                &return_type,
                &fun,
            )),
            vec![lit(123_i32)],
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty)?);
        let expected =
            "Projection: TestScalarUDF(CAST(Int32(123) AS Float32))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn scalar_udf_invalid_input() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
        let fun: ScalarFunctionImplementation = Arc::new(move |_| unimplemented!());
        let udf = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new(
                "TestScalarUDF",
                &Signature::uniform(1, vec![DataType::Int32], Volatility::Stable),
                &return_type,
                &fun,
            )),
            vec![lit("Apple")],
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, "")
            .err()
            .unwrap();
        assert_eq!(
    "type_coercion\ncaused by\nError during planning: Coercion from [Utf8] to the signature Uniform(1, [Int32]) failed.",
    err.strip_backtrace()
    );
        Ok(())
    }

    #[test]
    fn scalar_function() -> Result<()> {
        // test that automatic argument type coercion for scalar functions work
        let empty = empty();
        let lit_expr = lit(10i64);
        let fun: BuiltinScalarFunction = BuiltinScalarFunction::Acos;
        let scalar_function_expr =
            Expr::ScalarFunction(ScalarFunction::new(fun, vec![lit_expr]));
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![scalar_function_expr],
            empty,
        )?);
        let expected = "Projection: acos(CAST(Int64(10) AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn agg_udaf() -> Result<()> {
        let empty = empty();
        let my_avg = create_udaf(
            "MY_AVG",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|_| Ok(Box::<AvgAccumulator>::default())),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        );
        let udaf = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            Arc::new(my_avg),
            vec![lit(10i64)],
            false,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);
        let expected = "Projection: MY_AVG(CAST(Int64(10) AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn agg_udaf_invalid_input() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
        let state_type: StateTypeFunction =
            Arc::new(move |_| Ok(Arc::new(vec![DataType::UInt64, DataType::Float64])));
        let accumulator: AccumulatorFactoryFunction =
            Arc::new(|_| Ok(Box::<AvgAccumulator>::default()));
        let my_avg = AggregateUDF::new(
            "MY_AVG",
            &Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
            &return_type,
            &accumulator,
            &state_type,
        );
        let udaf = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            Arc::new(my_avg),
            vec![lit("10")],
            false,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, "")
            .err()
            .unwrap();
        assert_eq!(
            "type_coercion\ncaused by\nError during planning: Coercion from [Utf8] to the signature Uniform(1, [Float64]) failed.",
            err.strip_backtrace()
        );
        Ok(())
    }

    #[test]
    fn agg_function_case() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![lit(12i64)],
            false,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);
        let expected = "Projection: AVG(CAST(Int64(12) AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let empty = empty_with_type(DataType::Int32);
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![col("a")],
            false,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);
        let expected = "Projection: AVG(CAST(a AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_function_invalid_input() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![lit("1")],
            false,
            None,
            None,
        ));
        let err = Projection::try_new(vec![agg_expr], empty)
            .err()
            .unwrap()
            .strip_backtrace();
        assert_eq!(
            "Error during planning: No function matches the given name and argument types 'AVG(Utf8)'. You might need to add explicit type casts.\n\tCandidate functions:\n\tAVG(Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64/Float32/Float64)",
            err
        );
        Ok(())
    }

    #[test]
    fn binary_op_date32_op_interval() -> Result<()> {
        //CAST(Utf8("1998-03-18") AS Date32) + IntervalDayTime("386547056640")
        let expr = cast(lit("1998-03-18"), DataType::Date32)
            + lit(ScalarValue::IntervalDayTime(Some(386547056640)));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Date32) + IntervalDayTime(\"386547056640\")\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn inlist_case() -> Result<()> {
        // a in (1,4,8), a is int64
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: a IN ([CAST(Int32(1) AS Int64), CAST(Int8(4) AS Int64), Int64(8)]) AS a IN (Map { iter: Iter([Literal(Int32(1)), Literal(Int8(4)), Literal(Int64(8))]) })\
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        // a in (1,4,8), a is decimal
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::new_with_metadata(
                vec![DFField::new_unqualified(
                    "a",
                    DataType::Decimal128(12, 4),
                    true,
                )],
                std::collections::HashMap::new(),
            )?),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: CAST(a AS Decimal128(24, 4)) IN ([CAST(Int32(1) AS Decimal128(24, 4)), CAST(Int8(4) AS Decimal128(24, 4)), CAST(Int64(8) AS Decimal128(24, 4))]) AS a IN (Map { iter: Iter([Literal(Int32(1)), Literal(Int8(4)), Literal(Int64(8))]) })\
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn between_case() -> Result<()> {
        let expr = col("a").between(
            lit("2002-05-08"),
            // (cast('2002-05-08' as date) + interval '1 months')
            cast(lit("2002-05-08"), DataType::Date32)
                + lit(ScalarValue::new_interval_ym(0, 1)),
        );
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty, None)?);
        let expected =
            "Filter: a BETWEEN Utf8(\"2002-05-08\") AND CAST(CAST(Utf8(\"2002-05-08\") AS Date32) + IntervalYearMonth(\"1\") AS Utf8)\
            \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn between_infer_cheap_type() -> Result<()> {
        let expr = col("a").between(
            // (cast('2002-05-08' as date) + interval '1 months')
            cast(lit("2002-05-08"), DataType::Date32)
                + lit(ScalarValue::new_interval_ym(0, 1)),
            lit("2002-12-08"),
        );
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty, None)?);
        // TODO: we should cast col(a).
        let expected =
            "Filter: CAST(a AS Date32) BETWEEN CAST(Utf8(\"2002-05-08\") AS Date32) + IntervalYearMonth(\"1\") AND CAST(Utf8(\"2002-12-08\") AS Date32)\
            \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn is_bool_for_type_coercion() -> Result<()> {
        // is true
        let expr = col("a").is_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);
        let expected = "Projection: a IS TRUE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let ret = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, "");
        let err = ret.unwrap_err().to_string();
        assert!(err.contains("Cannot infer common argument type for comparison operation Int64 IS DISTINCT FROM Boolean"), "{err}");

        // is not true
        let expr = col("a").is_not_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT TRUE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        // is false
        let expr = col("a").is_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS FALSE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        // is not false
        let expr = col("a").is_not_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT FALSE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        Ok(())
    }

    #[test]
    fn like_for_type_coercion() -> Result<()> {
        // like : utf8 like "abc"
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let expected = "Projection: a LIKE Utf8(\"abc\")\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let expected = "Projection: a LIKE CAST(NULL AS Utf8) AS a LIKE NULL \
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains(
            "There isn't a common type to coerce Int64 and Utf8 in LIKE expression"
        ));

        // ilike
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let expected = "Projection: a ILIKE Utf8(\"abc\")\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let expected = "Projection: a ILIKE CAST(NULL AS Utf8) AS a ILIKE NULL \
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains(
            "There isn't a common type to coerce Int64 and Utf8 in ILIKE expression"
        ));
        Ok(())
    }

    #[test]
    fn unknown_for_type_coercion() -> Result<()> {
        // unknown
        let expr = col("a").is_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);
        let expected = "Projection: a IS UNKNOWN\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let ret = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected);
        let err = ret.unwrap_err().to_string();
        assert!(err.contains("Cannot infer common argument type for comparison operation Utf8 IS DISTINCT FROM Boolean"), "{err}");

        // is not unknown
        let expr = col("a").is_not_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT UNKNOWN\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        Ok(())
    }

    #[test]
    fn concat_for_type_coercion() -> Result<()> {
        let empty = empty_with_type(DataType::Utf8);
        let args = [col("a"), lit("b"), lit(true), lit(false), lit(13)];

        // concat
        {
            let expr = concat(&args);

            let plan =
                LogicalPlan::Projection(Projection::try_new(vec![expr], empty.clone())?);
            let expected =
                "Projection: concat(a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation";
            assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        }

        // concat_ws
        {
            let expr = concat_ws(lit("-"), args.to_vec());

            let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
            let expected =
                "Projection: concat_ws(Utf8(\"-\"), a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation";
            assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        }

        Ok(())
    }

    #[test]
    fn test_casting_for_fixed_size_list() -> Result<()> {
        let val = lit(ScalarValue::FixedSizeList(Arc::new(
            FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                3,
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                None,
            ),
        )));
        let expr = Expr::ScalarFunction(ScalarFunction::new(
            BuiltinScalarFunction::MakeArray,
            vec![val.clone()],
        ));
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new_unqualified(
                "item",
                DataType::FixedSizeList(
                    Arc::new(Field::new("a", DataType::Int32, true)),
                    3,
                ),
                true,
            )],
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema };
        let result = expr.rewrite(&mut rewriter)?;

        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new_unqualified(
                "item",
                DataType::List(Arc::new(Field::new("a", DataType::Int32, true))),
                true,
            )],
            std::collections::HashMap::new(),
        )?);
        let expected_casted_expr = cast_expr(
            &val,
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            &schema,
        )?;

        let expected = Expr::ScalarFunction(ScalarFunction::new(
            BuiltinScalarFunction::MakeArray,
            vec![expected_casted_expr],
        ));

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_type_coercion_rewrite() -> Result<()> {
        // gt
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new_unqualified("a", DataType::Int64, true)],
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).gt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).gt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);

        // eq
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new_unqualified("a", DataType::Int64, true)],
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).eq(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).eq(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);

        // lt
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new_unqualified("a", DataType::Int64, true)],
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).lt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).lt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn binary_op_date32_eq_ts() -> Result<()> {
        let expr = cast(
            lit("1998-03-18"),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .eq(cast(lit("1998-03-18"), DataType::Date32));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        dbg!(&plan);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None)) = CAST(CAST(Utf8(\"1998-03-18\") AS Date32) AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    fn cast_if_not_same_type(
        expr: Box<Expr>,
        data_type: &DataType,
        schema: &DFSchemaRef,
    ) -> Box<Expr> {
        if &expr.get_type(schema).unwrap() != data_type {
            Box::new(cast(*expr, data_type.clone()))
        } else {
            expr
        }
    }

    fn cast_helper(
        case: Case,
        case_when_type: DataType,
        then_else_type: DataType,
        schema: &DFSchemaRef,
    ) -> Case {
        let expr = case
            .expr
            .map(|e| cast_if_not_same_type(e, &case_when_type, schema));
        let when_then_expr = case
            .when_then_expr
            .into_iter()
            .map(|(when, then)| {
                (
                    cast_if_not_same_type(when, &case_when_type, schema),
                    cast_if_not_same_type(then, &then_else_type, schema),
                )
            })
            .collect::<Vec<_>>();
        let else_expr = case
            .else_expr
            .map(|e| cast_if_not_same_type(e, &then_else_type, schema));

        Case {
            expr,
            when_then_expr,
            else_expr,
        }
    }

    #[test]
    fn test_case_expression_coercion() -> Result<()> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![
                DFField::new_unqualified("boolean", DataType::Boolean, true),
                DFField::new_unqualified("integer", DataType::Int32, true),
                DFField::new_unqualified("float", DataType::Float32, true),
                DFField::new_unqualified(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                DFField::new_unqualified("date", DataType::Date32, true),
                DFField::new_unqualified(
                    "interval",
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                    true,
                ),
                DFField::new_unqualified("binary", DataType::Binary, true),
                DFField::new_unqualified("string", DataType::Utf8, true),
                DFField::new_unqualified("decimal", DataType::Decimal128(10, 10), true),
            ],
            std::collections::HashMap::new(),
        )?);

        let case = Case {
            expr: None,
            when_then_expr: vec![
                (Box::new(col("boolean")), Box::new(col("integer"))),
                (Box::new(col("integer")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("string"))),
            ],
            else_expr: None,
        };
        let case_when_common_type = DataType::Boolean;
        let then_else_common_type = DataType::Utf8;
        let expected = cast_helper(
            case.clone(),
            case_when_common_type,
            then_else_common_type,
            &schema,
        );
        let actual = coerce_case_expression(case, &schema)?;
        assert_eq!(expected, actual);

        let case = Case {
            expr: Some(Box::new(col("string"))),
            when_then_expr: vec![
                (Box::new(col("float")), Box::new(col("integer"))),
                (Box::new(col("integer")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("string"))),
            ],
            else_expr: Some(Box::new(col("string"))),
        };
        let case_when_common_type = DataType::Utf8;
        let then_else_common_type = DataType::Utf8;
        let expected = cast_helper(
            case.clone(),
            case_when_common_type,
            then_else_common_type,
            &schema,
        );
        let actual = coerce_case_expression(case, &schema)?;
        assert_eq!(expected, actual);

        let case = Case {
            expr: Some(Box::new(col("interval"))),
            when_then_expr: vec![
                (Box::new(col("float")), Box::new(col("integer"))),
                (Box::new(col("binary")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("string"))),
            ],
            else_expr: Some(Box::new(col("string"))),
        };
        let err = coerce_case_expression(case, &schema).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: \
            Failed to coerce case (Interval(MonthDayNano)) and \
            when ([Float32, Binary, Utf8]) to common types in \
            CASE WHEN expression"
        );

        let case = Case {
            expr: Some(Box::new(col("string"))),
            when_then_expr: vec![
                (Box::new(col("float")), Box::new(col("date"))),
                (Box::new(col("string")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("binary"))),
            ],
            else_expr: Some(Box::new(col("timestamp"))),
        };
        let err = coerce_case_expression(case, &schema).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: \
            Failed to coerce then ([Date32, Float32, Binary]) and \
            else (Some(Timestamp(Nanosecond, None))) to common types \
            in CASE WHEN expression"
        );

        Ok(())
    }

    #[test]
    fn interval_plus_timestamp() -> Result<()> {
        // SELECT INTERVAL '1' YEAR + '2000-01-01T00:00:00'::timestamp;
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(ScalarValue::IntervalYearMonth(Some(12)))),
            Operator::Plus,
            Box::new(cast(
                lit("2000-01-01T00:00:00"),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
        ));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: IntervalYearMonth(\"12\") + CAST(Utf8(\"2000-01-01T00:00:00\") AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn timestamp_subtract_timestamp() -> Result<()> {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(cast(
                lit("1998-03-18"),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
            Operator::Minus,
            Box::new(cast(
                lit("1998-03-18"),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
        ));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        dbg!(&plan);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None)) - CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn in_subquery_cast_subquery() -> Result<()> {
        let empty_int32 = empty_with_type(DataType::Int32);
        let empty_int64 = empty_with_type(DataType::Int64);

        let in_subquery_expr = Expr::InSubquery(InSubquery::new(
            Box::new(col("a")),
            Subquery {
                subquery: empty_int32,
                outer_ref_columns: vec![],
            },
            false,
        ));
        let plan =
            LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_int64, None)?);
        // add cast for subquery
        let expected = "\
        Filter: a IN (<subquery>)\
        \n  Subquery:\
        \n    Projection: CAST(a AS Int64)\
        \n      EmptyRelation\
        \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn in_subquery_cast_expr() -> Result<()> {
        let empty_int32 = empty_with_type(DataType::Int32);
        let empty_int64 = empty_with_type(DataType::Int64);

        let in_subquery_expr = Expr::InSubquery(InSubquery::new(
            Box::new(col("a")),
            Subquery {
                subquery: empty_int64,
                outer_ref_columns: vec![],
            },
            false,
        ));
        let plan =
            LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_int32, None)?);
        // add cast for subquery
        let expected = "\
        Filter: CAST(a AS Int64) IN (<subquery>)\
        \n  Subquery:\
        \n    EmptyRelation\
        \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn in_subquery_cast_all() -> Result<()> {
        let empty_inside = empty_with_type(DataType::Decimal128(10, 5));
        let empty_outside = empty_with_type(DataType::Decimal128(8, 8));

        let in_subquery_expr = Expr::InSubquery(InSubquery::new(
            Box::new(col("a")),
            Subquery {
                subquery: empty_inside,
                outer_ref_columns: vec![],
            },
            false,
        ));
        let plan =
            LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_outside, None)?);
        // add cast for subquery
        let expected = "Filter: CAST(a AS Decimal128(13, 8)) IN (<subquery>)\
        \n  Subquery:\
        \n    Projection: CAST(a AS Decimal128(13, 8))\
        \n      EmptyRelation\
        \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }
}
