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

use crate::var_provider::is_system_variables;
use crate::{
    execution_props::ExecutionProps,
    expressions::{
        self, binary, like, Column, DateTimeIntervalExpr, GetIndexedFieldExpr, Literal,
    },
    functions, udf,
    var_provider::VarType,
    PhysicalExpr,
};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{
    unqualified_field_not_found, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::Cast;
use datafusion_expr::{
    binary_expr, Between, BinaryExpr, Expr, GetIndexedField, Like, Operator, TryCast,
};
use std::sync::Arc;

/// Create a physical expression from a logical expression ([Expr]).
///
/// # Arguments
///
/// * `e` - The logical expression
/// * `input_dfschema` - The DataFusion schema for the input, used to resolve `Column` references
///                      to qualified or unqualified fields by name.
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    if input_schema.fields.len() != input_dfschema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "create_physical_expr expected same number of fields, got \
                     Arrow schema with {}  and DataFusion schema with {}",
            input_schema.fields.len(),
            input_dfschema.fields().len()
        )));
    }
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
            if is_system_variables(variable_names) {
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
        Expr::IsTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsNotTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsNotFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsNotUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Create physical expressions for left and right operands
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
            // Match the data types and operator to determine the appropriate expression, if
            // they are supported temporal types and operations, create DateTimeIntervalExpr,
            // else create BinaryExpr.
            match (
                lhs.data_type(input_schema)?,
                op,
                rhs.data_type(input_schema)?,
            ) {
                (
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                    Operator::Plus | Operator::Minus,
                    DataType::Interval(_),
                ) => Ok(Arc::new(DateTimeIntervalExpr::try_new(
                    lhs,
                    *op,
                    rhs,
                    input_schema,
                )?)),
                (
                    DataType::Interval(_),
                    Operator::Plus | Operator::Minus,
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                ) => Ok(Arc::new(DateTimeIntervalExpr::try_new(
                    rhs,
                    *op,
                    lhs,
                    input_schema,
                )?)),
                (
                    DataType::Timestamp(_, _),
                    Operator::Minus,
                    DataType::Timestamp(_, _),
                ) => Ok(Arc::new(DateTimeIntervalExpr::try_new(
                    lhs,
                    *op,
                    rhs,
                    input_schema,
                )?)),
                (
                    DataType::Interval(_),
                    Operator::Plus | Operator::Minus,
                    DataType::Interval(_),
                ) => Ok(Arc::new(DateTimeIntervalExpr::try_new(
                    lhs,
                    *op,
                    rhs,
                    input_schema,
                )?)),
                _ => {
                    // Note that the logical planner is responsible
                    // for type coercion on the arguments (e.g. if one
                    // argument was originally Int32 and one was
                    // Int64 they will both be coerced to Int64).
                    //
                    // There should be no coercion during physical
                    // planning.
                    binary(lhs, *op, rhs, input_schema)
                }
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            if escape_char.is_some() {
                return Err(DataFusionError::Execution(
                    "LIKE does not support escape_char".to_string(),
                ));
            }
            let physical_expr = create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let physical_pattern = create_physical_expr(
                pattern,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            like(
                *negated,
                false,
                physical_expr,
                physical_pattern,
                input_schema,
            )
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            if escape_char.is_some() {
                return Err(DataFusionError::Execution(
                    "ILIKE does not support escape_char".to_string(),
                ));
            }
            let physical_expr = create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let physical_pattern = create_physical_expr(
                pattern,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            like(
                *negated,
                true,
                physical_expr,
                physical_pattern,
                input_schema,
            )
        }
        Expr::Case(case) => {
            let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = &case.expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            let when_expr = case
                .when_then_expr
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
            let then_expr = case
                .when_then_expr
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
            let else_expr: Option<Arc<dyn PhysicalExpr>> =
                if let Some(e) = &case.else_expr {
                    Some(create_physical_expr(
                        e.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )?)
                } else {
                    None
                };
            Ok(expressions::case(expr, when_then_expr, else_expr)?)
        }
        Expr::Cast(Cast { expr, data_type }) => expressions::cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::TryCast(TryCast { expr, data_type }) => expressions::try_cast(
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
        Expr::GetIndexedField(GetIndexedField { key, expr }) => {
            Ok(Arc::new(GetIndexedFieldExpr::new(
                create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?,
                key.clone(),
            )))
        }

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
            // udfs with zero params expect null array as input
            if args.is_empty() {
                physical_args.push(Arc::new(Literal::new(ScalarValue::Null)));
            }
            udf::create_physical_expr(fun.clone().as_ref(), &physical_args, input_schema)
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
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

                let list_exprs = list
                    .iter()
                    .map(|expr| {
                        create_physical_expr(
                            expr,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                expressions::in_list(value_expr, list_exprs, negated, input_schema)
            }
        },
        Expr::HiddenColumn(_, _) => {
            let hidden_col_name = e.display_name()?;
            let col_idx =
                input_dfschema.index_of_column_by_name(None, &hidden_col_name)?;
            if let Some(idx) = col_idx {
                Ok(Arc::new(Column::new(&hidden_col_name, idx)))
            } else {
                Err(unqualified_field_not_found(
                    &hidden_col_name,
                    input_dfschema,
                ))
            }
        }
        Expr::HiddenExpr(expr, _) => {
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Physical plan does not support logical expression {other:?}"
        ))),
    }
}
