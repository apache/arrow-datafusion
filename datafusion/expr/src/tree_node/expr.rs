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

//! Tree node implementation for logical expr

use crate::expr::{
    AggregateFunction, AggregateFunctionDefinition, Alias, Between, BinaryExpr, Case,
    Cast, GetIndexedField, GroupingSet, InList, InSubquery, Like, Placeholder,
    ScalarFunction, ScalarFunctionDefinition, Sort, TryCast, WindowFunction,
};
use crate::{Expr, GetFieldAccess};
use std::borrow::Cow;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, VisitRecursionIterator};
use datafusion_common::{internal_err, DataFusionError, Result};

impl TreeNode for Expr {
    fn children_nodes(&self) -> Vec<Cow<Self>> {
        match self {
            Expr::Alias(Alias { expr, .. })
            | Expr::Not(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNull(expr)
            | Expr::Negative(expr)
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::Sort(Sort { expr, .. })
            | Expr::InSubquery(InSubquery { expr, .. }) => vec![Cow::Borrowed(expr)],
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                let expr = Cow::Borrowed(expr.as_ref());
                match field {
                    GetFieldAccess::ListIndex { key } => {
                        vec![Cow::Borrowed(key.as_ref()), expr]
                    }
                    GetFieldAccess::ListRange { start, stop } => {
                        vec![Cow::Borrowed(start), Cow::Borrowed(stop), expr]
                    }
                    GetFieldAccess::NamedStructField { name: _name } => {
                        vec![expr]
                    }
                }
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs))
            | Expr::GroupingSet(GroupingSet::Cube(exprs)) => exprs.iter().map(Cow::Borrowed).collect(),
            Expr::ScalarFunction(ScalarFunction { args, .. }) => args.iter().map(Cow::Borrowed).collect(),
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                lists_of_exprs.iter().flatten().map(Cow::Borrowed).collect()
            }
            Expr::Column(_)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_) => vec![],
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                vec![Cow::Borrowed(left), Cow::Borrowed(right)]
            }
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                vec![Cow::Borrowed(expr), Cow::Borrowed(pattern)]
            }
            Expr::Between(Between {
                expr, low, high, ..
            }) => vec![
                Cow::Borrowed(expr),
                Cow::Borrowed(low),
                Cow::Borrowed(high),
            ],
            Expr::Case(case) => {
                let mut expr_vec = vec![];
                if let Some(expr) = case.expr.as_ref() {
                    expr_vec.push(Cow::Borrowed(expr.as_ref()));
                };
                for (when, then) in case.when_then_expr.iter() {
                    expr_vec.push(Cow::Borrowed(when));
                    expr_vec.push(Cow::Borrowed(then));
                }
                if let Some(else_expr) = case.else_expr.as_ref() {
                    expr_vec.push(Cow::Borrowed(else_expr));
                }
                expr_vec
            }
            Expr::AggregateFunction(AggregateFunction {
                args,
                filter,
                order_by,
                ..
            }) => {
                let mut expr_vec: Vec<_> = args.iter().map(Cow::Borrowed).collect();

                if let Some(f) = filter {
                    expr_vec.push(Cow::Borrowed(f));
                }
                if let Some(o) = order_by {
                    expr_vec.extend(o.iter().map(Cow::Borrowed).collect::<Vec<_>>());
                }

                expr_vec
            }
            Expr::WindowFunction(WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            }) => {
                let mut expr_vec: Vec<_> = args.iter().map(Cow::Borrowed).collect();
                expr_vec.extend(partition_by.iter().map(Cow::Borrowed).collect::<Vec<_>>());
                expr_vec.extend(order_by.iter().map(Cow::Borrowed).collect::<Vec<_>>());
                expr_vec
            }
            Expr::InList(InList { expr, list, .. }) => {
                let mut expr_vec = vec![Cow::Borrowed(expr.as_ref())];
                expr_vec.extend(list.iter().map(Cow::Borrowed).collect::<Vec<_>>());
                expr_vec
            }
        }
    }

    fn visit_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        match self {
            Expr::Alias(Alias{expr,..})
            | Expr::Not(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNull(expr)
            | Expr::Negative(expr)
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::Sort(Sort { expr, .. })
            | Expr::InSubquery(InSubquery{ expr, .. }) => f(expr),
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                f(expr)?.and_then_on_continue(|| match field {
                    GetFieldAccess::ListIndex {key} => {
                        f(key)
                    },
                    GetFieldAccess::ListRange { start, stop} => {
                        f(start)?.and_then_on_continue(|| f(stop))
                    }
                    GetFieldAccess::NamedStructField { name: _name } => Ok(TreeNodeRecursion::Continue)
                })
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs))
            | Expr::GroupingSet(GroupingSet::Cube(exprs)) => exprs.iter().for_each_till_continue(f),
            Expr::ScalarFunction (ScalarFunction{ args, .. } )  => args.iter().for_each_till_continue(f),
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                lists_of_exprs.iter().flatten().for_each_till_continue(f)
            }
            | Expr::Column(_)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard {..}
            | Expr::Placeholder (_) => Ok(TreeNodeRecursion::Continue),
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                f(left)?
                    .and_then_on_continue(|| f(right))
            }
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                f(expr)?
                    .and_then_on_continue(|| f(pattern))
            }
            Expr::Between(Between { expr, low, high, .. }) => {
                f(expr)?
                    .and_then_on_continue(|| f(low))?
                    .and_then_on_continue(|| f(high))
            },
            Expr::Case( Case { expr, when_then_expr, else_expr }) => {
                expr.as_deref().into_iter().for_each_till_continue(f)?
                    .and_then_on_continue(||
                        when_then_expr.iter().for_each_till_continue(&mut |(w, t)| f(w)?.and_then_on_continue(|| f(t))))?
                    .and_then_on_continue(|| else_expr.as_deref().into_iter().for_each_till_continue(f))
            }
            Expr::AggregateFunction(AggregateFunction { args, filter, order_by, .. })
             => {
                args.iter().for_each_till_continue(f)?
                    .and_then_on_continue(|| filter.as_deref().into_iter().for_each_till_continue(f))?
                    .and_then_on_continue(|| order_by.iter().flatten().for_each_till_continue(f))
            }
            Expr::WindowFunction(WindowFunction { args, partition_by, order_by, .. }) => {
                args.iter().for_each_till_continue(f)?
                    .and_then_on_continue(|| partition_by.iter().for_each_till_continue(f))?
                    .and_then_on_continue(|| order_by.iter().for_each_till_continue(f))
            }
            Expr::InList(InList { expr, list, .. }) => {
                f(expr)?
                    .and_then_on_continue(|| list.iter().for_each_till_continue(f))
            }
        }
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let mut transform = transform;

        Ok(match self {
            Expr::Alias(Alias {
                expr,
                relation,
                name,
            }) => Expr::Alias(Alias::new(transform(*expr)?, relation, name)),
            Expr::Column(_) => self,
            Expr::OuterReferenceColumn(_, _) => self,
            Expr::Exists { .. } => self,
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => Expr::InSubquery(InSubquery::new(
                transform_boxed(expr, &mut transform)?,
                subquery,
                negated,
            )),
            Expr::ScalarSubquery(_) => self,
            Expr::ScalarVariable(ty, names) => Expr::ScalarVariable(ty, names),
            Expr::Literal(value) => Expr::Literal(value),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                Expr::BinaryExpr(BinaryExpr::new(
                    transform_boxed(left, &mut transform)?,
                    op,
                    transform_boxed(right, &mut transform)?,
                ))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => Expr::Like(Like::new(
                negated,
                transform_boxed(expr, &mut transform)?,
                transform_boxed(pattern, &mut transform)?,
                escape_char,
                case_insensitive,
            )),
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => Expr::SimilarTo(Like::new(
                negated,
                transform_boxed(expr, &mut transform)?,
                transform_boxed(pattern, &mut transform)?,
                escape_char,
                case_insensitive,
            )),
            Expr::Not(expr) => Expr::Not(transform_boxed(expr, &mut transform)?),
            Expr::IsNotNull(expr) => {
                Expr::IsNotNull(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNull(expr) => Expr::IsNull(transform_boxed(expr, &mut transform)?),
            Expr::IsTrue(expr) => Expr::IsTrue(transform_boxed(expr, &mut transform)?),
            Expr::IsFalse(expr) => Expr::IsFalse(transform_boxed(expr, &mut transform)?),
            Expr::IsUnknown(expr) => {
                Expr::IsUnknown(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNotTrue(expr) => {
                Expr::IsNotTrue(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNotFalse(expr) => {
                Expr::IsNotFalse(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNotUnknown(expr) => {
                Expr::IsNotUnknown(transform_boxed(expr, &mut transform)?)
            }
            Expr::Negative(expr) => {
                Expr::Negative(transform_boxed(expr, &mut transform)?)
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Expr::Between(Between::new(
                transform_boxed(expr, &mut transform)?,
                negated,
                transform_boxed(low, &mut transform)?,
                transform_boxed(high, &mut transform)?,
            )),
            Expr::Case(case) => {
                let expr = transform_option_box(case.expr, &mut transform)?;
                let when_then_expr = case
                    .when_then_expr
                    .into_iter()
                    .map(|(when, then)| {
                        Ok((
                            transform_boxed(when, &mut transform)?,
                            transform_boxed(then, &mut transform)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_expr = transform_option_box(case.else_expr, &mut transform)?;

                Expr::Case(Case::new(expr, when_then_expr, else_expr))
            }
            Expr::Cast(Cast { expr, data_type }) => {
                Expr::Cast(Cast::new(transform_boxed(expr, &mut transform)?, data_type))
            }
            Expr::TryCast(TryCast { expr, data_type }) => Expr::TryCast(TryCast::new(
                transform_boxed(expr, &mut transform)?,
                data_type,
            )),
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Expr::Sort(Sort::new(
                transform_boxed(expr, &mut transform)?,
                asc,
                nulls_first,
            )),
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => match func_def {
                ScalarFunctionDefinition::BuiltIn(fun) => Expr::ScalarFunction(
                    ScalarFunction::new(fun, transform_vec(args, &mut transform)?),
                ),
                ScalarFunctionDefinition::UDF(fun) => Expr::ScalarFunction(
                    ScalarFunction::new_udf(fun, transform_vec(args, &mut transform)?),
                ),
                ScalarFunctionDefinition::Name(_) => {
                    return internal_err!(
                        "Function `Expr` with name should be resolved."
                    );
                }
            },
            Expr::WindowFunction(WindowFunction {
                args,
                fun,
                partition_by,
                order_by,
                window_frame,
            }) => Expr::WindowFunction(WindowFunction::new(
                fun,
                transform_vec(args, &mut transform)?,
                transform_vec(partition_by, &mut transform)?,
                transform_vec(order_by, &mut transform)?,
                window_frame,
            )),
            Expr::AggregateFunction(AggregateFunction {
                args,
                func_def,
                distinct,
                filter,
                order_by,
            }) => match func_def {
                AggregateFunctionDefinition::BuiltIn(fun) => {
                    Expr::AggregateFunction(AggregateFunction::new(
                        fun,
                        transform_vec(args, &mut transform)?,
                        distinct,
                        transform_option_box(filter, &mut transform)?,
                        transform_option_vec(order_by, &mut transform)?,
                    ))
                }
                AggregateFunctionDefinition::UDF(fun) => {
                    let order_by = if let Some(order_by) = order_by {
                        Some(transform_vec(order_by, &mut transform)?)
                    } else {
                        None
                    };
                    Expr::AggregateFunction(AggregateFunction::new_udf(
                        fun,
                        transform_vec(args, &mut transform)?,
                        false,
                        transform_option_box(filter, &mut transform)?,
                        transform_option_vec(order_by, &mut transform)?,
                    ))
                }
                AggregateFunctionDefinition::Name(_) => {
                    return internal_err!(
                        "Function `Expr` with name should be resolved."
                    );
                }
            },
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::Rollup(exprs) => Expr::GroupingSet(GroupingSet::Rollup(
                    transform_vec(exprs, &mut transform)?,
                )),
                GroupingSet::Cube(exprs) => Expr::GroupingSet(GroupingSet::Cube(
                    transform_vec(exprs, &mut transform)?,
                )),
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    Expr::GroupingSet(GroupingSet::GroupingSets(
                        lists_of_exprs
                            .iter()
                            .map(|exprs| transform_vec(exprs.clone(), &mut transform))
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
            },

            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => Expr::InList(InList::new(
                transform_boxed(expr, &mut transform)?,
                transform_vec(list, &mut transform)?,
                negated,
            )),
            Expr::Wildcard { qualifier } => Expr::Wildcard { qualifier },
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                Expr::GetIndexedField(GetIndexedField::new(
                    transform_boxed(expr, &mut transform)?,
                    field,
                ))
            }
            Expr::Placeholder(Placeholder { id, data_type }) => {
                Expr::Placeholder(Placeholder { id, data_type })
            }
        })
    }

    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        match self {
            Expr::Alias(Alias { expr,.. })
            | Expr::Not(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNull(expr)
            | Expr::Negative(expr)
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::Sort(Sort { expr, .. })
            | Expr::InSubquery(InSubquery{ expr, .. }) => {
                let x = expr;
                f(x)
            }
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                f(expr)?.and_then_on_continue(|| match field {
                    GetFieldAccess::ListIndex {key} => {
                        f(key)
                    },
                    GetFieldAccess::ListRange { start, stop} => {
                        f(start)?.and_then_on_continue(|| f(stop))
                    }
                    GetFieldAccess::NamedStructField { name: _name } => Ok(TreeNodeRecursion::Continue)
                })
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs))
            | Expr::GroupingSet(GroupingSet::Cube(exprs)) => exprs.iter_mut().for_each_till_continue(f),
            | Expr::ScalarFunction(ScalarFunction{ args, .. }) => args.iter_mut().for_each_till_continue(f),
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                lists_of_exprs.iter_mut().flatten().for_each_till_continue(f)
            }
            | Expr::Column(_)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard {..}
            | Expr::Placeholder (_) => Ok(TreeNodeRecursion::Continue),
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                f(left)?
                    .and_then_on_continue(|| f(right))
            }
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                f(expr)?
                    .and_then_on_continue(|| f(pattern))
            }
            Expr::Between(Between { expr, low, high, .. }) => {
                f(expr)?
                    .and_then_on_continue(|| f(low))?
                    .and_then_on_continue(|| f(high))
            },
            Expr::Case( Case { expr, when_then_expr, else_expr }) => {
                expr.as_deref_mut().into_iter().for_each_till_continue(f)?
                    .and_then_on_continue(||
                        when_then_expr.iter_mut().for_each_till_continue(&mut |(w, t)| f(w)?.and_then_on_continue(|| f(t))))?
                    .and_then_on_continue(|| else_expr.as_deref_mut().into_iter().for_each_till_continue(f))
            }
            Expr::AggregateFunction(AggregateFunction { args, filter, order_by, .. }) => {
                args.iter_mut().for_each_till_continue(f)?
                    .and_then_on_continue(|| filter.as_deref_mut().into_iter().for_each_till_continue(f))?
                    .and_then_on_continue(|| order_by.iter_mut().flatten().for_each_till_continue(f))
            }
            Expr::WindowFunction(WindowFunction { args, partition_by, order_by, .. }) => {
                args.iter_mut().for_each_till_continue(f)?
                    .and_then_on_continue(|| partition_by.iter_mut().for_each_till_continue(f))?
                    .and_then_on_continue(|| order_by.iter_mut().for_each_till_continue(f))
            }
            Expr::InList(InList { expr, list, .. }) => {
                f(expr)?
                    .and_then_on_continue(|| list.iter_mut().for_each_till_continue(f))
            }
        }
    }
}

fn transform_boxed<F>(boxed_expr: Box<Expr>, transform: &mut F) -> Result<Box<Expr>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    // TODO:
    // It might be possible to avoid an allocation (the Box::new) below by reusing the box.
    let expr: Expr = *boxed_expr;
    let rewritten_expr = transform(expr)?;
    Ok(Box::new(rewritten_expr))
}

fn transform_option_box<F>(
    option_box: Option<Box<Expr>>,
    transform: &mut F,
) -> Result<Option<Box<Expr>>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    option_box
        .map(|expr| transform_boxed(expr, transform))
        .transpose()
}

/// &mut transform a Option<`Vec` of `Expr`s>
fn transform_option_vec<F>(
    option_box: Option<Vec<Expr>>,
    transform: &mut F,
) -> Result<Option<Vec<Expr>>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    Ok(if let Some(exprs) = option_box {
        Some(transform_vec(exprs, transform)?)
    } else {
        None
    })
}

/// &mut transform a `Vec` of `Expr`s
fn transform_vec<F>(v: Vec<Expr>, transform: &mut F) -> Result<Vec<Expr>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    v.into_iter().map(transform).collect()
}
