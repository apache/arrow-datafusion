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

//! Expression visitor

use super::Expr;
use datafusion_common::Result;

/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: ExpressionVisitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Encode the traversal of an expression tree. When passed to
/// `Expr::accept`, `ExpressionVisitor::visit` is invoked
/// recursively on all nodes of an expression tree. See the comments
/// on `Expr::accept` for details on its use
pub trait ExpressionVisitor<E: ExprVisitable = Expr>: Sized {
    /// Invoked before any children of `expr` are visisted.
    fn pre_visit(self, expr: &E) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor;

    /// Invoked after all children of `expr` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _expr: &E) -> Result<Self> {
        Ok(self)
    }
}

/// trait for types that can be visited by [`ExpressionVisitor`]
pub trait ExprVisitable: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: ExpressionVisitor<Self>>(&self, visitor: V) -> Result<V>;
}

impl ExprVisitable for Expr {
    /// Performs a depth first walk of an expression and
    /// its children, calling [`ExpressionVisitor::pre_visit`] and
    /// `visitor.post_visit`.
    ///
    /// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
    /// separate expression algorithms from the structure of the
    /// `Expr` tree and make it easier to add new types of expressions
    /// and algorithms that walk the tree.
    ///
    /// For an expression tree such as
    /// ```text
    /// BinaryExpr (GT)
    ///    left: Column("foo")
    ///    right: Column("bar")
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(BinaryExpr(GT))
    /// pre_visit(Column("foo"))
    /// post_visit(Column("foo"))
    /// pre_visit(Column("bar"))
    /// post_visit(Column("bar"))
    /// post_visit(BinaryExpr(GT))
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If `Recursion::Stop` is returned on a call to pre_visit, no
    /// children of that expression are visited, nor is post_visit
    /// called on that expression
    ///
    fn accept<V: ExpressionVisitor>(&self, visitor: V) -> Result<V> {
        let visitor = match visitor.pre_visit(self)? {
            Recursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            Recursion::Stop(visitor) => return Ok(visitor),
        };

        // recurse (and cover all expression types)
        let visitor = match self {
            Expr::Alias(expr, _)
            | Expr::Not(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsNull(expr)
            | Expr::Negative(expr)
            | Expr::Cast { expr, .. }
            | Expr::TryCast { expr, .. }
            | Expr::Sort { expr, .. }
            | Expr::GetIndexedField { expr, .. } => expr.accept(visitor),
            Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => Ok(visitor),
            Expr::BinaryExpr { left, right, .. } => {
                let visitor = left.accept(visitor)?;
                right.accept(visitor)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let visitor = expr.accept(visitor)?;
                let visitor = low.accept(visitor)?;
                high.accept(visitor)
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let visitor = if let Some(expr) = expr.as_ref() {
                    expr.accept(visitor)
                } else {
                    Ok(visitor)
                }?;
                let visitor = when_then_expr.iter().try_fold(
                    visitor,
                    |visitor, (when, then)| {
                        let visitor = when.accept(visitor)?;
                        then.accept(visitor)
                    },
                )?;
                if let Some(else_expr) = else_expr.as_ref() {
                    else_expr.accept(visitor)
                } else {
                    Ok(visitor)
                }
            }
            Expr::ScalarFunction { args, .. }
            | Expr::ScalarUDF { args, .. }
            | Expr::TableUDF { args, .. }
            | Expr::AggregateFunction { args, .. }
            | Expr::AggregateUDF { args, .. } => args
                .iter()
                .try_fold(visitor, |visitor, arg| arg.accept(visitor)),
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                let visitor = args
                    .iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))?;
                let visitor = partition_by
                    .iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))?;
                let visitor = order_by
                    .iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))?;
                Ok(visitor)
            }
            Expr::InList { expr, list, .. } => {
                let visitor = expr.accept(visitor)?;
                list.iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))
            }
        }?;

        visitor.post_visit(self)
    }
}
