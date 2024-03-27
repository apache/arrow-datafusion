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

use super::{Between, Expr, Like};
use crate::expr::{
    AggregateFunction, AggregateFunctionDefinition, Alias, BinaryExpr, Cast,
    GetFieldAccess, GetIndexedField, InList, InSubquery, Placeholder, ScalarFunction,
    ScalarFunctionDefinition, Sort, TryCast, Unnest, WindowFunction,
};
use crate::field_util::GetFieldAccessSchema;
use crate::type_coercion::binary::get_result_type;
use crate::type_coercion::functions::data_types;
use crate::{utils, LogicalPlan, Projection, Subquery};
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{
    internal_err, not_impl_err, plan_datafusion_err, plan_err, Column, ExprSchema,
    OwnedTableReference, Result,
};
use std::collections::HashMap;
use std::sync::Arc;

/// trait to allow expr to typable with respect to a schema
pub trait ExprSchemable {
    /// given a schema, return the type of the expr
    fn get_type(&self, schema: &dyn ExprSchema) -> Result<DataType>;

    /// given a schema, return the nullability of the expr
    fn nullable(&self, input_schema: &dyn ExprSchema) -> Result<bool>;

    /// given a schema, return the expr's optional metadata
    fn metadata(&self, schema: &dyn ExprSchema) -> Result<HashMap<String, String>>;

    /// convert to a field with respect to a schema
    fn to_field(
        &self,
        input_schema: &dyn ExprSchema,
    ) -> Result<(Option<OwnedTableReference>, Arc<Field>)>;

    /// cast to a type with respect to a schema
    fn cast_to(self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> Result<Expr>;

    /// given a schema, return the type and nullability of the expr
    fn data_type_and_nullable(&self, schema: &dyn ExprSchema)
        -> Result<(DataType, bool)>;
}

impl ExprSchemable for Expr {
    /// Returns the [arrow::datatypes::DataType] of the expression
    /// based on [ExprSchema]
    ///
    /// Note: [`DFSchema`] implements [ExprSchema].
    ///
    /// [`DFSchema`]: datafusion_common::DFSchema
    ///
    /// # Examples
    ///
    /// ## Get the type of an expression that adds 2 columns. Adding an Int32
    /// ## and Float32 results in Float32 type
    ///
    /// ```
    /// # use arrow::datatypes::{DataType, Field};
    /// # use datafusion_common::DFSchema;
    /// # use datafusion_expr::{col, ExprSchemable};
    /// # use std::collections::HashMap;
    ///
    /// fn main() {
    ///   let expr = col("c1") + col("c2");
    ///   let schema = DFSchema::from_unqualifed_fields(
    ///     vec![
    ///       Field::new("c1", DataType::Int32, true),
    ///       Field::new("c2", DataType::Float32, true),
    ///       ].into(),
    ///       HashMap::new(),
    ///   ).unwrap();
    ///   assert_eq!("Float32", format!("{}", expr.get_type(&schema).unwrap()));
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// [arrow::datatypes::DataType].  This happens when e.g. the
    /// expression refers to a column that does not exist in the
    /// schema, or when the expression is incorrectly typed
    /// (e.g. `[utf8] + [bool]`).
    fn get_type(&self, schema: &dyn ExprSchema) -> Result<DataType> {
        match self {
            Expr::Alias(Alias { expr, name, .. }) => match &**expr {
                Expr::Placeholder(Placeholder { data_type, .. }) => match &data_type {
                    None => schema.data_type(&Column::from_name(name)).cloned(),
                    Some(dt) => Ok(dt.clone()),
                },
                _ => expr.get_type(schema),
            },
            Expr::Sort(Sort { expr, .. }) | Expr::Negative(expr) => expr.get_type(schema),
            Expr::Column(c) => Ok(schema.data_type(c)?.clone()),
            Expr::OuterReferenceColumn(ty, _) => Ok(ty.clone()),
            Expr::ScalarVariable(ty, _) => Ok(ty.clone()),
            Expr::Literal(l) => Ok(l.data_type()),
            Expr::Case(case) => case.when_then_expr[0].1.get_type(schema),
            Expr::Cast(Cast { data_type, .. })
            | Expr::TryCast(TryCast { data_type, .. }) => Ok(data_type.clone()),
            Expr::Unnest(Unnest { exprs }) => {
                let arg_data_types = exprs
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                let arg_data_type = arg_data_types[0].clone();
                // Unnest's output type is the inner type of the list
                match arg_data_type{
                    DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) =>{
                        Ok(field.data_type().clone())
                    }
                    DataType::Struct(_) => {
                        not_impl_err!("unnest() does not support struct yet")
                    }
                    DataType::Null => {
                        not_impl_err!("unnest() does not support null yet")
                    }
                    _ => {
                        plan_err!("unnest() can only be applied to array, struct and null")
                    }
                }
            }
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => {
                let arg_data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                match func_def {
                    ScalarFunctionDefinition::BuiltIn(fun) => {
                        // verify that function is invoked with correct number and type of arguments as defined in `TypeSignature`
                        data_types(&arg_data_types, &fun.signature()).map_err(|_| {
                            plan_datafusion_err!(
                                "{}",
                                utils::generate_signature_error_msg(
                                    &format!("{fun}"),
                                    fun.signature(),
                                    &arg_data_types,
                                )
                            )
                        })?;

                        // perform additional function arguments validation (due to limited
                        // expressiveness of `TypeSignature`), then infer return type
                        fun.return_type(&arg_data_types)
                    }
                    ScalarFunctionDefinition::UDF(fun) => {
                        // verify that function is invoked with correct number and type of arguments as defined in `TypeSignature`
                        data_types(&arg_data_types, fun.signature()).map_err(|_| {
                            plan_datafusion_err!(
                                "{}",
                                utils::generate_signature_error_msg(
                                    fun.name(),
                                    fun.signature().clone(),
                                    &arg_data_types,
                                )
                            )
                        })?;

                        // perform additional function arguments validation (due to limited
                        // expressiveness of `TypeSignature`), then infer return type
                        Ok(fun.return_type_from_exprs(args, schema, &arg_data_types)?)
                    }
                    ScalarFunctionDefinition::Name(_) => {
                        internal_err!("Function `Expr` with name should be resolved.")
                    }
                }
            }
            Expr::WindowFunction(WindowFunction { fun, args, .. }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                fun.return_type(&data_types)
            }
            Expr::AggregateFunction(AggregateFunction { func_def, args, .. }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                match func_def {
                    AggregateFunctionDefinition::BuiltIn(fun) => {
                        fun.return_type(&data_types)
                    }
                    AggregateFunctionDefinition::UDF(fun) => {
                        Ok(fun.return_type(&data_types)?)
                    }
                    AggregateFunctionDefinition::Name(_) => {
                        internal_err!("Function `Expr` with name should be resolved.")
                    }
                }
            }
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_) => Ok(DataType::Boolean),
            Expr::ScalarSubquery(subquery) => {
                Ok(subquery.subquery.schema().field(0).data_type().clone())
            }
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ref op,
            }) => get_result_type(&left.get_type(schema)?, op, &right.get_type(schema)?),
            Expr::Like { .. } | Expr::SimilarTo { .. } => Ok(DataType::Boolean),
            Expr::Placeholder(Placeholder { data_type, .. }) => {
                data_type.clone().ok_or_else(|| {
                    plan_datafusion_err!("Placeholder type could not be resolved. Make sure that the placeholder is bound to a concrete type, e.g. by providing parameter values.")
                })
            }
            Expr::Wildcard { qualifier } => {
                // Wildcard do not really have a type and do not appear in projections
                match qualifier {
                    Some(_) => internal_err!("QualifiedWildcard expressions are not valid in a logical query plan"),
                    None => Ok(DataType::Null)
                }
            }
            Expr::GroupingSet(_) => {
                // grouping sets do not really have a type and do not appear in projections
                Ok(DataType::Null)
            }
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                field_for_index(expr, field, schema).map(|x| x.data_type().clone())
            }
        }
    }

    /// Returns the nullability of the expression based on [ExprSchema].
    ///
    /// Note: [`DFSchema`] implements [ExprSchema].
    ///
    /// [`DFSchema`]: datafusion_common::DFSchema
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// nullability.  This happens when the expression refers to a
    /// column that does not exist in the schema.
    fn nullable(&self, input_schema: &dyn ExprSchema) -> Result<bool> {
        match self {
            Expr::Alias(Alias { expr, .. })
            | Expr::Not(expr)
            | Expr::Negative(expr)
            | Expr::Sort(Sort { expr, .. }) => expr.nullable(input_schema),

            Expr::InList(InList { expr, list, .. }) => {
                // Avoid inspecting too many expressions.
                const MAX_INSPECT_LIMIT: usize = 6;
                // Stop if a nullable expression is found or an error occurs.
                let has_nullable = std::iter::once(expr.as_ref())
                    .chain(list)
                    .take(MAX_INSPECT_LIMIT)
                    .find_map(|e| {
                        e.nullable(input_schema)
                            .map(|nullable| if nullable { Some(()) } else { None })
                            .transpose()
                    })
                    .transpose()?;
                Ok(match has_nullable {
                    // If a nullable subexpression is found, the result may also be nullable.
                    Some(_) => true,
                    // If the list is too long, we assume it is nullable.
                    None if list.len() + 1 > MAX_INSPECT_LIMIT => true,
                    // All the subexpressions are non-nullable, so the result must be non-nullable.
                    _ => false,
                })
            }

            Expr::Between(Between {
                expr, low, high, ..
            }) => Ok(expr.nullable(input_schema)?
                || low.nullable(input_schema)?
                || high.nullable(input_schema)?),

            Expr::Column(c) => input_schema.nullable(c),
            Expr::OuterReferenceColumn(_, _) => Ok(true),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::Case(case) => {
                // this expression is nullable if any of the input expressions are nullable
                let then_nullable = case
                    .when_then_expr
                    .iter()
                    .map(|(_, t)| t.nullable(input_schema))
                    .collect::<Result<Vec<_>>>()?;
                if then_nullable.contains(&true) {
                    Ok(true)
                } else if let Some(e) = &case.else_expr {
                    e.nullable(input_schema)
                } else {
                    // CASE produces NULL if there is no `else` expr
                    // (aka when none of the `when_then_exprs` match)
                    Ok(true)
                }
            }
            Expr::Cast(Cast { expr, .. }) => expr.nullable(input_schema),
            Expr::ScalarVariable(_, _)
            | Expr::TryCast { .. }
            | Expr::ScalarFunction(..)
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::Unnest(_)
            | Expr::Placeholder(_) => Ok(true),
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Exists { .. } => Ok(false),
            Expr::InSubquery(InSubquery { expr, .. }) => expr.nullable(input_schema),
            Expr::ScalarSubquery(subquery) => {
                Ok(subquery.subquery.schema().field(0).is_nullable())
            }
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ..
            }) => Ok(left.nullable(input_schema)? || right.nullable(input_schema)?),
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                Ok(expr.nullable(input_schema)? || pattern.nullable(input_schema)?)
            }
            Expr::Wildcard { .. } => internal_err!(
                "Wildcard expressions are not valid in a logical query plan"
            ),
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                // If schema is nested, check if parent is nullable
                // if it is, return early
                if let Expr::Column(col) = expr.as_ref() {
                    if input_schema.nullable(col)? {
                        return Ok(true);
                    }
                }
                field_for_index(expr, field, input_schema).map(|x| x.is_nullable())
            }
            Expr::GroupingSet(_) => {
                // grouping sets do not really have the concept of nullable and do not appear
                // in projections
                Ok(true)
            }
        }
    }

    fn metadata(&self, schema: &dyn ExprSchema) -> Result<HashMap<String, String>> {
        match self {
            Expr::Column(c) => Ok(schema.metadata(c)?.clone()),
            Expr::Alias(Alias { expr, .. }) => expr.metadata(schema),
            _ => Ok(HashMap::new()),
        }
    }

    /// Returns the datatype and nullability of the expression based on [ExprSchema].
    ///
    /// Note: [`DFSchema`] implements [ExprSchema].
    ///
    /// [`DFSchema`]: datafusion_common::DFSchema
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// datatype or nullability.
    fn data_type_and_nullable(
        &self,
        schema: &dyn ExprSchema,
    ) -> Result<(DataType, bool)> {
        match self {
            Expr::Alias(Alias { expr, name, .. }) => match &**expr {
                Expr::Placeholder(Placeholder { data_type, .. }) => match &data_type {
                    None => schema
                        .data_type_and_nullable(&Column::from_name(name))
                        .map(|(d, n)| (d.clone(), n)),
                    Some(dt) => Ok((dt.clone(), expr.nullable(schema)?)),
                },
                _ => expr.data_type_and_nullable(schema),
            },
            Expr::Sort(Sort { expr, .. }) | Expr::Negative(expr) => {
                expr.data_type_and_nullable(schema)
            }
            Expr::Column(c) => schema
                .data_type_and_nullable(c)
                .map(|(d, n)| (d.clone(), n)),
            Expr::OuterReferenceColumn(ty, _) => Ok((ty.clone(), true)),
            Expr::ScalarVariable(ty, _) => Ok((ty.clone(), true)),
            Expr::Literal(l) => Ok((l.data_type(), l.is_null())),
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Exists { .. } => Ok((DataType::Boolean, false)),
            Expr::ScalarSubquery(subquery) => Ok((
                subquery.subquery.schema().field(0).data_type().clone(),
                subquery.subquery.schema().field(0).is_nullable(),
            )),
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ref op,
            }) => {
                let left = left.data_type_and_nullable(schema)?;
                let right = right.data_type_and_nullable(schema)?;
                Ok((get_result_type(&left.0, op, &right.0)?, left.1 || right.1))
            }
            _ => Ok((self.get_type(schema)?, self.nullable(schema)?)),
        }
    }

    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    ///
    /// So for example, a projected expression `col(c1) + col(c2)` is
    /// placed in an output field **named** col("c1 + c2")
    fn to_field(
        &self,
        input_schema: &dyn ExprSchema,
    ) -> Result<(Option<OwnedTableReference>, Arc<Field>)> {
        match self {
            Expr::Column(c) => {
                let (data_type, nullable) = self.data_type_and_nullable(input_schema)?;
                Ok((
                    c.relation.clone(),
                    Field::new(&c.name, data_type, nullable)
                        .with_metadata(self.metadata(input_schema)?)
                        .into(),
                ))
            }
            Expr::Alias(Alias { relation, name, .. }) => {
                let (data_type, nullable) = self.data_type_and_nullable(input_schema)?;
                Ok((
                    relation.clone(),
                    Field::new(name, data_type, nullable)
                        .with_metadata(self.metadata(input_schema)?)
                        .into(),
                ))
            }
            _ => {
                let (data_type, nullable) = self.data_type_and_nullable(input_schema)?;
                Ok((
                    None,
                    Field::new(self.display_name()?, data_type, nullable)
                        .with_metadata(self.metadata(input_schema)?)
                        .into(),
                ))
            }
        }
    }

    /// Wraps this expression in a cast to a target [arrow::datatypes::DataType].
    ///
    /// # Errors
    ///
    /// This function errors when it is impossible to cast the
    /// expression to the target [arrow::datatypes::DataType].
    fn cast_to(self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> Result<Expr> {
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }

        // TODO(kszucs): most of the operations do not validate the type correctness
        // like all of the binary expressions below. Perhaps Expr should track the
        // type of the expression?

        if can_cast_types(&this_type, cast_to_type) {
            match self {
                Expr::ScalarSubquery(subquery) => {
                    Ok(Expr::ScalarSubquery(cast_subquery(subquery, cast_to_type)?))
                }
                _ => Ok(Expr::Cast(Cast::new(Box::new(self), cast_to_type.clone()))),
            }
        } else {
            plan_err!("Cannot automatically convert {this_type:?} to {cast_to_type:?}")
        }
    }
}

/// return the schema [`Field`] for the type referenced by `get_indexed_field`
fn field_for_index(
    expr: &Expr,
    field: &GetFieldAccess,
    schema: &dyn ExprSchema,
) -> Result<Field> {
    let expr_dt = expr.get_type(schema)?;
    match field {
        GetFieldAccess::NamedStructField { name } => {
            GetFieldAccessSchema::NamedStructField { name: name.clone() }
        }
        GetFieldAccess::ListIndex { key } => GetFieldAccessSchema::ListIndex {
            key_dt: key.get_type(schema)?,
        },
        GetFieldAccess::ListRange {
            start,
            stop,
            stride,
        } => GetFieldAccessSchema::ListRange {
            start_dt: start.get_type(schema)?,
            stop_dt: stop.get_type(schema)?,
            stride_dt: stride.get_type(schema)?,
        },
    }
    .get_accessed_field(&expr_dt)
}

/// cast subquery in InSubquery/ScalarSubquery to a given type.
pub fn cast_subquery(subquery: Subquery, cast_to_type: &DataType) -> Result<Subquery> {
    if subquery.subquery.schema().field(0).data_type() == cast_to_type {
        return Ok(subquery);
    }

    let plan = subquery.subquery.as_ref();
    let new_plan = match plan {
        LogicalPlan::Projection(projection) => {
            let cast_expr = projection.expr[0]
                .clone()
                .cast_to(cast_to_type, projection.input.schema())?;
            LogicalPlan::Projection(Projection::try_new(
                vec![cast_expr],
                projection.input.clone(),
            )?)
        }
        _ => {
            let cast_expr = Expr::Column(Column::from(plan.schema().qualified_field(0)))
                .cast_to(cast_to_type, subquery.subquery.schema())?;
            LogicalPlan::Projection(Projection::try_new(
                vec![cast_expr],
                subquery.subquery,
            )?)
        }
    };
    Ok(Subquery {
        subquery: Arc::new(new_plan),
        outer_ref_columns: subquery.outer_ref_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, lit};
    use arrow::datatypes::{DataType, Fields, SchemaBuilder};
    use datafusion_common::{Column, DFSchema, ScalarValue};

    macro_rules! test_is_expr_nullable {
        ($EXPR_TYPE:ident) => {{
            let expr = lit(ScalarValue::Null).$EXPR_TYPE();
            assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        }};
    }

    #[test]
    fn expr_schema_nullability() {
        let expr = col("foo").eq(lit(1));
        assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        assert!(expr
            .nullable(&MockExprSchema::new().with_nullable(true))
            .unwrap());

        test_is_expr_nullable!(is_null);
        test_is_expr_nullable!(is_not_null);
        test_is_expr_nullable!(is_true);
        test_is_expr_nullable!(is_not_true);
        test_is_expr_nullable!(is_false);
        test_is_expr_nullable!(is_not_false);
        test_is_expr_nullable!(is_unknown);
        test_is_expr_nullable!(is_not_unknown);
    }

    #[test]
    fn test_between_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Int32)
                .with_nullable(nullable)
        };

        let expr = col("foo").between(lit(1), lit(2));
        assert!(!expr.nullable(&get_schema(false)).unwrap());
        assert!(expr.nullable(&get_schema(true)).unwrap());

        let null = lit(ScalarValue::Int32(None));

        let expr = col("foo").between(null.clone(), lit(2));
        assert!(expr.nullable(&get_schema(false)).unwrap());

        let expr = col("foo").between(lit(1), null.clone());
        assert!(expr.nullable(&get_schema(false)).unwrap());

        let expr = col("foo").between(null.clone(), null);
        assert!(expr.nullable(&get_schema(false)).unwrap());
    }

    #[test]
    fn test_inlist_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Int32)
                .with_nullable(nullable)
        };

        let expr = col("foo").in_list(vec![lit(1); 5], false);
        assert!(!expr.nullable(&get_schema(false)).unwrap());
        assert!(expr.nullable(&get_schema(true)).unwrap());
        // Testing nullable() returns an error.
        assert!(expr
            .nullable(&get_schema(false).with_error_on_nullable(true))
            .is_err());

        let null = lit(ScalarValue::Int32(None));
        let expr = col("foo").in_list(vec![null, lit(1)], false);
        assert!(expr.nullable(&get_schema(false)).unwrap());

        // Testing on long list
        let expr = col("foo").in_list(vec![lit(1); 6], false);
        assert!(expr.nullable(&get_schema(false)).unwrap());
    }

    #[test]
    fn test_like_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Utf8)
                .with_nullable(nullable)
        };

        let expr = col("foo").like(lit("bar"));
        assert!(!expr.nullable(&get_schema(false)).unwrap());
        assert!(expr.nullable(&get_schema(true)).unwrap());

        let expr = col("foo").like(lit(ScalarValue::Utf8(None)));
        assert!(expr.nullable(&get_schema(false)).unwrap());
    }

    #[test]
    fn expr_schema_data_type() {
        let expr = col("foo");
        assert_eq!(
            DataType::Utf8,
            expr.get_type(&MockExprSchema::new().with_data_type(DataType::Utf8))
                .unwrap()
        );
    }

    #[test]
    fn test_expr_metadata() {
        let mut meta = HashMap::new();
        meta.insert("bar".to_string(), "buzz".to_string());
        let expr = col("foo");
        let schema = MockExprSchema::new()
            .with_data_type(DataType::Int32)
            .with_metadata(meta.clone());

        // col and alias should be metadata-preserving
        assert_eq!(meta, expr.metadata(&schema).unwrap());
        assert_eq!(meta, expr.clone().alias("bar").metadata(&schema).unwrap());

        // cast should drop input metadata since the type has changed
        assert_eq!(
            HashMap::new(),
            expr.clone()
                .cast_to(&DataType::Int64, &schema)
                .unwrap()
                .metadata(&schema)
                .unwrap()
        );

        let schema = DFSchema::from_unqualifed_fields(
            vec![Field::new("foo", DataType::Int32, true).with_metadata(meta.clone())]
                .into(),
            HashMap::new(),
        )
        .unwrap();

        // verify to_field method populates metadata
        assert_eq!(&meta, expr.to_field(&schema).unwrap().1.metadata());
    }

    #[test]
    fn test_nested_schema_nullability() {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("foo", DataType::Int32, true));
        builder.push(Field::new(
            "parent",
            DataType::Struct(Fields::from(vec![Field::new(
                "child",
                DataType::Int64,
                false,
            )])),
            true,
        ));
        let schema = builder.finish();

        let dfschema = DFSchema::from_field_specific_qualified_schema(
            vec![Some("table_name"), None],
            &Arc::new(schema),
        )
        .unwrap();

        let expr = col("parent").field("child");
        assert!(expr.nullable(&dfschema).unwrap());
    }

    #[derive(Debug)]
    struct MockExprSchema {
        nullable: bool,
        data_type: DataType,
        error_on_nullable: bool,
        metadata: HashMap<String, String>,
    }

    impl MockExprSchema {
        fn new() -> Self {
            Self {
                nullable: false,
                data_type: DataType::Null,
                error_on_nullable: false,
                metadata: HashMap::new(),
            }
        }

        fn with_nullable(mut self, nullable: bool) -> Self {
            self.nullable = nullable;
            self
        }

        fn with_data_type(mut self, data_type: DataType) -> Self {
            self.data_type = data_type;
            self
        }

        fn with_error_on_nullable(mut self, error_on_nullable: bool) -> Self {
            self.error_on_nullable = error_on_nullable;
            self
        }

        fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
            self.metadata = metadata;
            self
        }
    }

    impl ExprSchema for MockExprSchema {
        fn nullable(&self, _col: &Column) -> Result<bool> {
            if self.error_on_nullable {
                internal_err!("nullable error")
            } else {
                Ok(self.nullable)
            }
        }

        fn data_type(&self, _col: &Column) -> Result<&DataType> {
            Ok(&self.data_type)
        }

        fn metadata(&self, _col: &Column) -> Result<&HashMap<String, String>> {
            Ok(&self.metadata)
        }

        fn data_type_and_nullable(&self, col: &Column) -> Result<(&DataType, bool)> {
            Ok((self.data_type(col)?, self.nullable(col)?))
        }
    }
}
