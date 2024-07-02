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

use std::sync::Arc;

use crate::scalar_function;
use crate::{
    expressions::{self, binary, like, Column, Literal},
    PhysicalExpr,
};

use arrow::datatypes::Schema;
use datafusion_common::{
    exec_err, not_impl_err, plan_err, DFSchema, Result, ScalarValue, ToDFSchema,
};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::{Alias, Cast, InList, ScalarFunction};
use datafusion_expr::var_provider::is_system_variables;
use datafusion_expr::var_provider::VarType;
use datafusion_expr::{binary_expr, Between, BinaryExpr, Expr, Like, Operator, TryCast};

/// [PhysicalExpr] evaluate DataFusion expressions such as `A + 1`, or `CAST(c1
/// AS int)`.
///
/// [PhysicalExpr] are the physical counterpart to [Expr] used in logical
/// planning, and can be evaluated directly on a [RecordBatch]. They are
/// normally created from [Expr] by a [PhysicalPlanner] and can be created
/// directly using [create_physical_expr].
///
/// A Physical expression knows its type, nullability and how to evaluate itself.
///
/// [PhysicalPlanner]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html
/// [RecordBatch]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
///
/// # Example: Create `PhysicalExpr` from `Expr`
/// ```
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::DFSchema;
/// # use datafusion_expr::{Expr, col, lit};
/// # use datafusion_physical_expr::create_physical_expr;
/// # use datafusion_expr::execution_props::ExecutionProps;
/// // For a logical expression `a = 1`, we can create a physical expression
/// let expr = col("a").eq(lit(1));
/// // To create a PhysicalExpr we need 1. a schema
/// let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
/// let df_schema = DFSchema::try_from(schema).unwrap();
/// // 2. ExecutionProps
/// let props = ExecutionProps::new();
/// // We can now create a PhysicalExpr:
/// let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
/// ```
///
/// # Example: Executing a PhysicalExpr to obtain [ColumnarValue]
/// ```
/// # use std::sync::Arc;
/// # use arrow::array::{cast::AsArray, BooleanArray, Int32Array, RecordBatch};
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::{assert_batches_eq, DFSchema};
/// # use datafusion_expr::{Expr, col, lit, ColumnarValue};
/// # use datafusion_physical_expr::create_physical_expr;
/// # use datafusion_expr::execution_props::ExecutionProps;
/// # let expr = col("a").eq(lit(1));
/// # let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
/// # let df_schema = DFSchema::try_from(schema.clone()).unwrap();
/// # let props = ExecutionProps::new();
/// // Given a PhysicalExpr, for `a = 1` we can evaluate it against a RecordBatch like this:
/// let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
/// // Input of [1,2,3]
/// let input_batch = RecordBatch::try_from_iter(vec![
///   ("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _)
/// ]).unwrap();
/// // The result is a ColumnarValue (either an Array or a Scalar)
/// let result = physical_expr.evaluate(&input_batch).unwrap();
/// // In this case, a BooleanArray with the result of the comparison
/// let ColumnarValue::Array(arr) = result else {
///  panic!("Expected an array")
/// };
/// assert_eq!(arr.as_boolean(), &BooleanArray::from(vec![true, false, false]));
/// ```
///
/// [ColumnarValue]: datafusion_expr::ColumnarValue
///
/// Create a physical expression from a logical expression ([Expr]).
///
/// # Arguments
///
/// * `e` - The logical expression
/// * `input_dfschema` - The DataFusion schema for the input, used to resolve `Column` references
///                      to qualified or unqualified fields by name.
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_schema: &Schema = &input_dfschema.into();

    match e {
        Expr::Alias(Alias { expr, .. }) => {
            Ok(create_physical_expr(expr, input_dfschema, execution_props)?)
        }
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
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsNotTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsNotFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsNotUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Create physical expressions for left and right operands
            let lhs = create_physical_expr(left, input_dfschema, execution_props)?;
            let rhs = create_physical_expr(right, input_dfschema, execution_props)?;
            // Note that the logical planner is responsible
            // for type coercion on the arguments (e.g. if one
            // argument was originally Int32 and one was
            // Int64 they will both be coerced to Int64).
            //
            // There should be no coercion during physical
            // planning.
            binary(lhs, op.clone(), rhs, input_schema)
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
            let physical_expr =
                create_physical_expr(expr, input_dfschema, execution_props)?;
            let physical_pattern =
                create_physical_expr(pattern, input_dfschema, execution_props)?;
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
                    execution_props,
                )?)
            } else {
                None
            };
            let (when_expr, then_expr): (Vec<&Expr>, Vec<&Expr>) = case
                .when_then_expr
                .iter()
                .map(|(w, t)| (w.as_ref(), t.as_ref()))
                .unzip();
            let when_expr =
                create_physical_exprs(when_expr, input_dfschema, execution_props)?;
            let then_expr =
                create_physical_exprs(then_expr, input_dfschema, execution_props)?;
            let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                when_expr
                    .iter()
                    .zip(then_expr.iter())
                    .map(|(w, t)| (Arc::clone(w), Arc::clone(t)))
                    .collect();
            let else_expr: Option<Arc<dyn PhysicalExpr>> =
                if let Some(e) = &case.else_expr {
                    Some(create_physical_expr(
                        e.as_ref(),
                        input_dfschema,
                        execution_props,
                    )?)
                } else {
                    None
                };
            Ok(expressions::case(expr, when_then_expr, else_expr)?)
        }
        Expr::Cast(Cast { expr, data_type }) => expressions::cast(
            create_physical_expr(expr, input_dfschema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::TryCast(TryCast { expr, data_type }) => expressions::try_cast(
            create_physical_expr(expr, input_dfschema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::Not(expr) => {
            expressions::not(create_physical_expr(expr, input_dfschema, execution_props)?)
        }
        Expr::Negative(expr) => expressions::negative(
            create_physical_expr(expr, input_dfschema, execution_props)?,
            input_schema,
        ),
        Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
            expr,
            input_dfschema,
            execution_props,
        )?),
        Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
            expr,
            input_dfschema,
            execution_props,
        )?),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let physical_args =
                create_physical_exprs(args, input_dfschema, execution_props)?;

            scalar_function::create_physical_expr(
                Arc::clone(func).as_ref(),
                &physical_args,
                input_schema,
                args,
                input_dfschema,
            )
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let value_expr = create_physical_expr(expr, input_dfschema, execution_props)?;
            let low_expr = create_physical_expr(low, input_dfschema, execution_props)?;
            let high_expr = create_physical_expr(high, input_dfschema, execution_props)?;

            // rewrite the between into the two binary operators
            let binary_expr = binary(
                binary(
                    Arc::clone(&value_expr),
                    Operator::GtEq,
                    low_expr,
                    input_schema,
                )?,
                Operator::And,
                binary(
                    Arc::clone(&value_expr),
                    Operator::LtEq,
                    high_expr,
                    input_schema,
                )?,
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
                let value_expr =
                    create_physical_expr(expr, input_dfschema, execution_props)?;

                let list_exprs =
                    create_physical_exprs(list, input_dfschema, execution_props)?;
                expressions::in_list(value_expr, list_exprs, negated, input_schema)
            }
        },
        other => {
            not_impl_err!("Physical plan does not support logical expression {other:?}")
        }
    }
}

/// Create vector of Physical Expression from a vector of logical expression
pub fn create_physical_exprs<'a, I>(
    exprs: I,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Vec<Arc<dyn PhysicalExpr>>>
where
    I: IntoIterator<Item = &'a Expr>,
{
    exprs
        .into_iter()
        .map(|expr| create_physical_expr(expr, input_dfschema, execution_props))
        .collect::<Result<Vec<_>>>()
}

/// Convert a logical expression to a physical expression (without any simplification, etc)
pub fn logical2physical(expr: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    let df_schema = schema.clone().to_dfschema().unwrap();
    let execution_props = ExecutionProps::new();
    create_physical_expr(expr, &df_schema, &execution_props).unwrap()
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field};

    use datafusion_expr::{col, lit};

    use super::*;

    #[test]
    fn test_create_physical_expr_scalar_input_output() -> Result<()> {
        let expr = col("letter").eq(lit("A"));

        let schema = Schema::new(vec![Field::new("letter", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from_qualified_schema("data", &schema)?;
        let p = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())?;

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
