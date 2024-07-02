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

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::logical_type::signature::LogicalType;
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::planner::RawFieldAccessExpr;
use sqlparser::ast::{CastKind, Expr as SQLExpr, Subscript, TrimWhereField, Value};

use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, plan_err, DFSchema, Result,
    ScalarValue,
};
use datafusion_common::logical_type::{TypeRelation, ExtensionType};
use datafusion_expr::expr::InList;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    lit, Between, BinaryExpr, Cast, Expr, ExprSchemable, GetFieldAccess, Like, Literal,
    Operator, TryCast,
};

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

mod binary_op;
mod function;
mod grouping_set;
mod identifier;
mod order_by;
mod subquery;
mod substring;
mod unary_op;
mod value;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn sql_expr_to_logical_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        enum StackEntry {
            SQLExpr(Box<SQLExpr>),
            Operator(sqlparser::ast::BinaryOperator),
        }

        // Virtual stack machine to convert SQLExpr to Expr
        // This allows visiting the expr tree in a depth-first manner which
        // produces expressions in postfix notations, i.e. `a + b` => `a b +`.
        // See https://github.com/apache/datafusion/issues/1444
        let mut stack = vec![StackEntry::SQLExpr(Box::new(sql))];
        let mut eval_stack = vec![];

        while let Some(entry) = stack.pop() {
            match entry {
                StackEntry::SQLExpr(sql_expr) => {
                    match *sql_expr {
                        SQLExpr::BinaryOp { left, op, right } => {
                            // Note the order that we push the entries to the stack
                            // is important. We want to visit the left node first.
                            stack.push(StackEntry::Operator(op));
                            stack.push(StackEntry::SQLExpr(right));
                            stack.push(StackEntry::SQLExpr(left));
                        }
                        _ => {
                            let expr = self.sql_expr_to_logical_expr_internal(
                                *sql_expr,
                                schema,
                                planner_context,
                            )?;
                            eval_stack.push(expr);
                        }
                    }
                }
                StackEntry::Operator(op) => {
                    let right = eval_stack.pop().unwrap();
                    let left = eval_stack.pop().unwrap();
                    let expr = self.build_logical_expr(op, left, right, schema)?;
                    eval_stack.push(expr);
                }
            }
        }

        assert_eq!(1, eval_stack.len());
        let expr = eval_stack.pop().unwrap();
        Ok(expr)
    }

    fn build_logical_expr(
        &self,
        op: sqlparser::ast::BinaryOperator,
        left: Expr,
        right: Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        // try extension planers
        let mut binary_expr = datafusion_expr::planner::RawBinaryExpr { op, left, right };
        for planner in self.planners.iter() {
            match planner.plan_binary_op(binary_expr, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(expr) => {
                    binary_expr = expr;
                }
            }
        }

        let datafusion_expr::planner::RawBinaryExpr { op, left, right } = binary_expr;
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            self.parse_sql_binary_op(op)?,
            Box::new(right),
        )))
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr = self.sql_expr_to_logical_expr(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        let expr = expr.infer_placeholder_types(schema)?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema.iter().find(|(qualifier, field)| match qualifier {
                        Some(field_q) => {
                            field.name() == &col.name
                                && field_q.to_string().ends_with(&format!(".{q}"))
                        }
                        _ => false,
                    }) {
                        Some((qualifier, df_field)) => Expr::from((qualifier, df_field)),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    /// Internal implementation. Use
    /// [`Self::sql_expr_to_logical_expr`] to plan exprs.
    fn sql_expr_to_logical_expr_internal(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            SQLExpr::Value(value) => {
                self.parse_value(value, planner_context.prepare_param_data_types())
            }
            SQLExpr::Extract { field, expr } => {
                let date_part = self
                    .context_provider
                    .get_function_meta("date_part")
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Unable to find expected 'date_part' function"
                        )
                    })?;
                let args = vec![
                    Expr::Literal(ScalarValue::from(format!("{field}"))),
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
                ];
                Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                    date_part, args,
                )))
            }

            SQLExpr::Array(arr) => self.sql_array_literal(arr.elem, schema),
            SQLExpr::Interval(interval) => {
                self.sql_interval_to_expr(false, interval, schema, planner_context)
            }
            SQLExpr::Identifier(id) => {
                self.sql_identifier_to_expr(id, schema, planner_context)
            }

            SQLExpr::MapAccess { .. } => {
                not_impl_err!("Map Access")
            }

            // <expr>["foo"], <expr>[4] or <expr>[4:5]
            SQLExpr::Subscript { expr, subscript } => {
                let expr =
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;

                let field_access = match *subscript {
                    Subscript::Index { index } => {
                        // index can be a name, in which case it is a named field access
                        match index {
                            SQLExpr::Value(
                                Value::SingleQuotedString(s)
                                | Value::DoubleQuotedString(s),
                            ) => GetFieldAccess::NamedStructField {
                                name: ScalarValue::from(s),
                            },
                            SQLExpr::JsonAccess { .. } => {
                                return not_impl_err!("JsonAccess");
                            }
                            // otherwise treat like a list index
                            _ => GetFieldAccess::ListIndex {
                                key: Box::new(self.sql_expr_to_logical_expr(
                                    index,
                                    schema,
                                    planner_context,
                                )?),
                            },
                        }
                    }
                    Subscript::Slice {
                        lower_bound,
                        upper_bound,
                        stride,
                    } => {
                        // Means access like [:2]
                        let lower_bound = if let Some(lower_bound) = lower_bound {
                            self.sql_expr_to_logical_expr(
                                lower_bound,
                                schema,
                                planner_context,
                            )
                        } else {
                            not_impl_err!("Slice subscript requires a lower bound")
                        }?;

                        // means access like [2:]
                        let upper_bound = if let Some(upper_bound) = upper_bound {
                            self.sql_expr_to_logical_expr(
                                upper_bound,
                                schema,
                                planner_context,
                            )
                        } else {
                            not_impl_err!("Slice subscript requires an upper bound")
                        }?;

                        // stride, default to 1
                        let stride = if let Some(stride) = stride {
                            self.sql_expr_to_logical_expr(
                                stride,
                                schema,
                                planner_context,
                            )?
                        } else {
                            lit(1i64)
                        };

                        GetFieldAccess::ListRange {
                            start: Box::new(lower_bound),
                            stop: Box::new(upper_bound),
                            stride: Box::new(stride),
                        }
                    }
                };

                let mut field_access_expr = RawFieldAccessExpr { expr, field_access };
                for planner in self.planners.iter() {
                    match planner.plan_field_access(field_access_expr, schema)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => {
                            field_access_expr = expr;
                        }
                    }
                }

                not_impl_err!("GetFieldAccess not supported by UserDefinedExtensionPlanners: {field_access_expr:?}")
            }

            SQLExpr::CompoundIdentifier(ids) => {
                self.sql_compound_identifier_to_expr(ids, schema, planner_context)
            }

            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.sql_case_identifier_to_expr(
                operand,
                conditions,
                results,
                else_result,
                schema,
                planner_context,
            ),

            SQLExpr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                expr,
                data_type,
                format,
            } => {
                if let Some(format) = format {
                    return not_impl_err!("CAST with format is not supported: {format}");
                }

                let dt = self.convert_data_type(&data_type)?;
                let expr =
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;

                // numeric constants are treated as seconds (rather as nanoseconds)
                // to align with postgres / duckdb semantics
                let expr = match dt.logical() {
                    LogicalType::Timestamp(TimeUnit::Nanosecond, tz)
                        if expr.get_type(schema)?.logical() == &LogicalType::Int64 =>
                    {
                        Expr::Cast(Cast::new(
                            Box::new(expr),
                            TypeRelation::from(DataType::Timestamp(TimeUnit::Second, tz.clone())),
                        ))
                    }
                    _ => expr,
                };

                Ok(Expr::Cast(Cast::new(Box::new(expr), dt)))
            }

            SQLExpr::Cast {
                kind: CastKind::TryCast | CastKind::SafeCast,
                expr,
                data_type,
                format,
            } => {
                if let Some(format) = format {
                    return not_impl_err!("CAST with format is not supported: {format}");
                }

                Ok(Expr::TryCast(TryCast::new(
                    Box::new(self.sql_expr_to_logical_expr(
                        *expr,
                        schema,
                        planner_context,
                    )?),
                    self.convert_data_type(&data_type)?,
                )))
            }

            SQLExpr::TypedString { data_type, value } => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => {
                Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(self.sql_expr_to_logical_expr(
                        *left,
                        schema,
                        planner_context,
                    )?),
                    Operator::IsDistinctFrom,
                    Box::new(self.sql_expr_to_logical_expr(
                        *right,
                        schema,
                        planner_context,
                    )?),
                )))
            }

            SQLExpr::IsNotDistinctFrom(left, right) => {
                Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(self.sql_expr_to_logical_expr(
                        *left,
                        schema,
                        planner_context,
                    )?),
                    Operator::IsNotDistinctFrom,
                    Box::new(self.sql_expr_to_logical_expr(
                        *right,
                        schema,
                        planner_context,
                    )?),
                )))
            }

            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsUnknown(expr) => Ok(Expr::IsUnknown(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotUnknown(expr) => Ok(Expr::IsNotUnknown(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::UnaryOp { op, expr } => {
                self.parse_sql_unary_op(op, *expr, schema, planner_context)
            }

            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between::new(
                Box::new(self.sql_expr_to_logical_expr(
                    *expr,
                    schema,
                    planner_context,
                )?),
                negated,
                Box::new(self.sql_expr_to_logical_expr(*low, schema, planner_context)?),
                Box::new(self.sql_expr_to_logical_expr(
                    *high,
                    schema,
                    planner_context,
                )?),
            ))),

            SQLExpr::InList {
                expr,
                list,
                negated,
            } => self.sql_in_list_to_expr(*expr, list, negated, schema, planner_context),

            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.sql_like_to_expr(
                negated,
                *expr,
                *pattern,
                escape_char,
                schema,
                planner_context,
                false,
            ),

            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.sql_like_to_expr(
                negated,
                *expr,
                *pattern,
                escape_char,
                schema,
                planner_context,
                true,
            ),

            SQLExpr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.sql_similarto_to_expr(
                negated,
                *expr,
                *pattern,
                escape_char,
                schema,
                planner_context,
            ),

            SQLExpr::BinaryOp { .. } => {
                internal_err!("binary_op should be handled by sql_expr_to_logical_expr.")
            }

            #[cfg(feature = "unicode_expressions")]
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
            } => self.sql_substring_to_expr(
                expr,
                substring_from,
                substring_for,
                schema,
                planner_context,
            ),

            #[cfg(not(feature = "unicode_expressions"))]
            SQLExpr::Substring { .. } => {
                internal_err!(
                    "statement substring requires compilation with feature flag: unicode_expressions."
                )
            }

            SQLExpr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => self.sql_trim_to_expr(
                *expr,
                trim_where,
                trim_what,
                trim_characters,
                schema,
                planner_context,
            ),

            SQLExpr::Function(function) => {
                self.sql_function_to_expr(function, schema, planner_context)
            }

            SQLExpr::Rollup(exprs) => {
                self.sql_rollup_to_expr(exprs, schema, planner_context)
            }
            SQLExpr::Cube(exprs) => self.sql_cube_to_expr(exprs, schema, planner_context),
            SQLExpr::GroupingSets(exprs) => {
                self.sql_grouping_sets_to_expr(exprs, schema, planner_context)
            }

            SQLExpr::Floor {
                expr,
                field: _field,
            } => self.sql_fn_name_to_expr(*expr, "floor", schema, planner_context),
            SQLExpr::Ceil {
                expr,
                field: _field,
            } => self.sql_fn_name_to_expr(*expr, "ceil", schema, planner_context),
            SQLExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => self.sql_overlay_to_expr(
                *expr,
                *overlay_what,
                *overlay_from,
                overlay_for,
                schema,
                planner_context,
            ),
            SQLExpr::Nested(e) => {
                self.sql_expr_to_logical_expr(*e, schema, planner_context)
            }

            SQLExpr::Exists { subquery, negated } => {
                self.parse_exists_subquery(*subquery, negated, schema, planner_context)
            }
            SQLExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.parse_in_subquery(*expr, *subquery, negated, schema, planner_context)
            }
            SQLExpr::Subquery(subquery) => {
                self.parse_scalar_subquery(*subquery, schema, planner_context)
            }

            SQLExpr::Struct { values, fields } => {
                self.parse_struct(values, fields, schema, planner_context)
            }
            SQLExpr::Position { expr, r#in } => {
                self.sql_position_to_expr(*expr, *r#in, schema, planner_context)
            }
            SQLExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => Ok(Expr::Cast(Cast::new(
                Box::new(self.sql_expr_to_logical_expr_internal(
                    *timestamp,
                    schema,
                    planner_context,
                )?),
                match *time_zone {
                    SQLExpr::Value(Value::SingleQuotedString(s)) => {
                        TypeRelation::from(DataType::Timestamp(TimeUnit::Nanosecond, Some(s.into())))
                    }
                    _ => {
                        return not_impl_err!(
                            "Unsupported ast node in sqltorel: {time_zone:?}"
                        )
                    }
                },
            ))),
            _ => not_impl_err!("Unsupported ast node in sqltorel: {sql:?}"),
        }
    }

    /// Parses a struct(..) expression
    fn parse_struct(
        &self,
        values: Vec<SQLExpr>,
        fields: Vec<sqlparser::ast::StructField>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if !fields.is_empty() {
            return not_impl_err!("Struct fields are not supported yet");
        }

        if values
            .iter()
            .any(|value| matches!(value, SQLExpr::Named { .. }))
        {
            self.create_named_struct(values, input_schema, planner_context)
        } else {
            self.create_struct(values, input_schema, planner_context)
        }
    }

    // Handles a call to struct(...) where the arguments are named. For example
    // `struct (v as foo, v2 as bar)` by creating a call to the `named_struct` function
    fn create_named_struct(
        &self,
        values: Vec<SQLExpr>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args = values
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let args = if let SQLExpr::Named { expr, name } = value {
                    [
                        name.value.lit(),
                        self.sql_expr_to_logical_expr(
                            *expr,
                            input_schema,
                            planner_context,
                        )?,
                    ]
                } else {
                    [
                        format!("c{i}").lit(),
                        self.sql_expr_to_logical_expr(
                            value,
                            input_schema,
                            planner_context,
                        )?,
                    ]
                };

                Ok(args)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        let named_struct_func = self
            .context_provider
            .get_function_meta("named_struct")
            .ok_or_else(|| {
            internal_datafusion_err!("Unable to find expected 'named_struct' function")
        })?;

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            named_struct_func,
            args,
        )))
    }

    // Handles a call to struct(...) where the arguments are not named. For example
    // `struct (v, v2)` by creating a call to the `struct` function
    // which will create a struct with fields named `c0`, `c1`, etc.
    fn create_struct(
        &self,
        values: Vec<SQLExpr>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args = values
            .into_iter()
            .map(|value| {
                self.sql_expr_to_logical_expr(value, input_schema, planner_context)
            })
            .collect::<Result<Vec<_>>>()?;
        let struct_func = self
            .context_provider
            .get_function_meta("struct")
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected 'struct' function")
            })?;

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            struct_func,
            args,
        )))
    }

    fn sql_in_list_to_expr(
        &self,
        expr: SQLExpr,
        list: Vec<SQLExpr>,
        negated: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let list_expr = list
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::InList(InList::new(
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            list_expr,
            negated,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    fn sql_like_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<String>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        case_insensitive: bool,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let pattern_type = pattern.get_type(schema)?;
        if !matches!(pattern_type.logical(), LogicalType::Utf8 | LogicalType::Null) {
            return plan_err!("Invalid pattern in LIKE expression");
        }
        let escape_char = if let Some(char) = escape_char {
            if char.len() != 1 {
                return plan_err!("Invalid escape character in LIKE expression");
            }
            Some(char.chars().next().unwrap())
        } else {
            None
        };
        Ok(Expr::Like(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
            case_insensitive,
        )))
    }

    fn sql_similarto_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<String>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let pattern_type = pattern.get_type(schema)?;
        if !matches!(pattern_type.logical(), LogicalType::Utf8 | LogicalType::Null) {
            return plan_err!("Invalid pattern in SIMILAR TO expression");
        }
        let escape_char = if let Some(char) = escape_char {
            if char.len() != 1 {
                return plan_err!("Invalid escape character in SIMILAR TO expression");
            }
            Some(char.chars().next().unwrap())
        } else {
            None
        };
        Ok(Expr::SimilarTo(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
            false,
        )))
    }

    fn sql_trim_to_expr(
        &self,
        expr: SQLExpr,
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<SQLExpr>>,
        trim_characters: Option<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let args = match (trim_what, trim_characters) {
            (Some(to_trim), None) => {
                let to_trim =
                    self.sql_expr_to_logical_expr(*to_trim, schema, planner_context)?;
                Ok(vec![arg, to_trim])
            }
            (None, Some(trim_characters)) => {
                if let Some(first) = trim_characters.first() {
                    let to_trim = self.sql_expr_to_logical_expr(
                        first.clone(),
                        schema,
                        planner_context,
                    )?;
                    Ok(vec![arg, to_trim])
                } else {
                    plan_err!("TRIM CHARACTERS cannot be empty")
                }
            }
            (Some(_), Some(_)) => {
                plan_err!("Both TRIM and TRIM CHARACTERS cannot be specified")
            }
            (None, None) => Ok(vec![arg]),
        }?;

        let fun_name = match trim_where {
            Some(TrimWhereField::Leading) => "ltrim",
            Some(TrimWhereField::Trailing) => "rtrim",
            Some(TrimWhereField::Both) => "btrim",
            None => "trim",
        };
        let fun = self
            .context_provider
            .get_function_meta(fun_name)
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected '{fun_name}' function")
            })?;

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }

    fn sql_overlay_to_expr(
        &self,
        expr: SQLExpr,
        overlay_what: SQLExpr,
        overlay_from: SQLExpr,
        overlay_for: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let fun = self
            .context_provider
            .get_function_meta("overlay")
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected 'overlay' function")
            })?;
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let what_arg =
            self.sql_expr_to_logical_expr(overlay_what, schema, planner_context)?;
        let from_arg =
            self.sql_expr_to_logical_expr(overlay_from, schema, planner_context)?;
        let args = match overlay_for {
            Some(for_expr) => {
                let for_expr =
                    self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                vec![arg, what_arg, from_arg, for_expr]
            }
            None => vec![arg, what_arg, from_arg],
        };
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }
    fn sql_position_to_expr(
        &self,
        substr_expr: SQLExpr,
        str_expr: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let fun = self
            .context_provider
            .get_function_meta("strpos")
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected 'strpos' function")
            })?;
        let substr =
            self.sql_expr_to_logical_expr(substr_expr, schema, planner_context)?;
        let fullstr = self.sql_expr_to_logical_expr(str_expr, schema, planner_context)?;
        let args = vec![fullstr, substr];
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use arrow_schema::DataType;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};

    use crate::TableReference;

    use super::*;

    struct TestContextProvider {
        options: ConfigOptions,
        tables: HashMap<String, Arc<dyn TableSource>>,
    }

    impl TestContextProvider {
        pub fn new() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "table1".to_string(),
                create_table_source(vec![Field::new(
                    "column1".to_string(),
                    DataType::Utf8,
                    false,
                )]),
            );

            Self {
                options: Default::default(),
                tables,
            }
        }
    }

    impl ContextProvider for TestContextProvider {
        fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            match self.tables.get(name.table()) {
                Some(table) => Ok(table.clone()),
                _ => plan_err!("Table not found: {}", name.table()),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<TypeRelation> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
            None
        }

        fn udf_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn udaf_names(&self) -> Vec<String> {
            Vec::new()
        }

        fn udwf_names(&self) -> Vec<String> {
            Vec::new()
        }
    }

    fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(
            Schema::new_with_metadata(fields, HashMap::new()),
        )))
    }

    macro_rules! test_stack_overflow {
        ($num_expr:expr) => {
            paste::item! {
                #[test]
                fn [<test_stack_overflow_ $num_expr>]() {
                    let schema = DFSchema::empty();
                    let mut planner_context = PlannerContext::default();

                    let expr_str = (0..$num_expr)
                        .map(|i| format!("column1 = 'value{:?}'", i))
                        .collect::<Vec<String>>()
                        .join(" OR ");

                    let dialect = GenericDialect{};
                    let mut parser = Parser::new(&dialect)
                        .try_with_sql(expr_str.as_str())
                        .unwrap();
                    let sql_expr = parser.parse_expr().unwrap();

                    let context_provider = TestContextProvider::new();
                    let sql_to_rel = SqlToRel::new(&context_provider);

                    // Should not stack overflow
                    sql_to_rel.sql_expr_to_logical_expr(
                        sql_expr,
                        &schema,
                        &mut planner_context,
                    ).unwrap();
                }
            }
        };
    }

    test_stack_overflow!(64);
    test_stack_overflow!(128);
    test_stack_overflow!(256);
    test_stack_overflow!(512);
    test_stack_overflow!(1024);
    test_stack_overflow!(2048);
    test_stack_overflow!(4096);
    test_stack_overflow!(8192);
}
