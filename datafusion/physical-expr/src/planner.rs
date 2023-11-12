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

use crate::expressions::GetFieldAccessExpr;
use crate::var_provider::is_system_variables;
use crate::{
    execution_props::ExecutionProps,
    expressions::{self, binary, like, Column, GetIndexedFieldExpr, Literal},
    functions, udf,
    var_provider::VarType,
    PhysicalExpr,
};
use arrow::datatypes::Schema;
use datafusion_common::logical_type::ExtensionType;
use datafusion_common::{
    exec_err, internal_err, not_impl_err, plan_err, DFSchema, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::expr::{Alias, Cast, InList, ScalarFunction, ScalarUDF};
use datafusion_expr::{
    binary_expr, Between, BinaryExpr, Expr, GetFieldAccess, GetIndexedField, Like,
    Operator, TryCast,
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
        return internal_err!(
            "create_physical_expr expected same number of fields, got \
                     Arrow schema with {}  and DataFusion schema with {}",
            input_schema.fields.len(),
            input_dfschema.fields().len()
        );
    }
    match e {
        Expr::Alias(Alias { expr, .. }) => Ok(create_physical_expr(
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
                    _ => plan_err!("No system variable provider found"),
                }
            } else {
                match execution_props.get_var_provider(VarType::UserDefined) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => plan_err!("No user defined variable provider found"),
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
            // Note that the logical planner is responsible
            // for type coercion on the arguments (e.g. if one
            // argument was originally Int32 and one was
            // Int64 they will both be coerced to Int64).
            //
            // There should be no coercion during physical
            // planning.
            binary(lhs, *op, rhs, input_schema)
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if escape_char.is_some() {
                return exec_err!("LIKE does not support escape_char");
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
                *case_insensitive,
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
            data_type.physical_type(),
        ),
        Expr::TryCast(TryCast { expr, data_type }) => expressions::try_cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.physical_type(),
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
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let field = match field {
                GetFieldAccess::NamedStructField { name } => {
                    GetFieldAccessExpr::NamedStructField { name: name.clone() }
                }
                GetFieldAccess::ListIndex { key } => GetFieldAccessExpr::ListIndex {
                    key: create_physical_expr(
                        key,
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )?,
                },
                GetFieldAccess::ListRange { start, stop } => {
                    GetFieldAccessExpr::ListRange {
                        start: create_physical_expr(
                            start,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        )?,
                        stop: create_physical_expr(
                            stop,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        )?,
                    }
                }
            };
            Ok(Arc::new(GetIndexedFieldExpr::new(
                create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?,
                field,
            )))
        }

        Expr::ScalarFunction(ScalarFunction { fun, args }) => {
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
        Expr::ScalarUDF(ScalarUDF { fun, args }) => {
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
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => match expr.as_ref() {
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
        other => {
            not_impl_err!("Physical plan does not support logical expression {other:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{col, left, Literal};

    #[test]
    fn test_create_physical_expr_scalar_input_output() -> Result<()> {
        let expr = col("letter").eq(left("APACHE".lit(), 1i64.lit()));

        let schema = Schema::new(vec![Field::new("letter", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from_qualified_schema("data", &schema)?;
        let p = create_physical_expr(&expr, &df_schema, &schema, &ExecutionProps::new())?;

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from_iter_values(vec![
                "A", "B", "C", "D",
            ]))],
        )?;
        let result = p.evaluate(&batch)?;
        let result = result.into_array(4).expect("Failed to convert to array");

        assert_eq!(
            &result,
            &(Arc::new(BooleanArray::from(vec![true, false, false, false,])) as ArrayRef)
        );

        Ok(())
    }
}
