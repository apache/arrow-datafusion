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

//! Rewrites for using Array Functions

use crate::array_has::array_has_all;
use crate::concat::{array_append, array_concat, array_prepend};
use crate::extract::{array_element, array_slice};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::list_ndims;
use datafusion_common::Result;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{BinaryExpr, Expr, GetFieldAccess, GetIndexedField, Operator};
use datafusion_functions::expr_fn::get_field;

/// Rewrites expressions into function calls to array functions
pub(crate) struct ArrayFunctionRewriter {}

impl FunctionRewrite for ArrayFunctionRewriter {
    fn name(&self) -> &str {
        "FunctionRewrite"
    }

    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let transformed = match expr {
            // array1 @> array2 -> array_has_all(array1, array2)
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::AtArrow
                    && is_func(&left, "make_array")
                    && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_has_all(*left, *right))
            }

            // array1 <@ array2 -> array_has_all(array2, array1)
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::ArrowAt
                    && is_func(&left, "make_array")
                    && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_has_all(*right, *left))
            }

            // Column cases:
            // 1) array_prepend/append/concat || column
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat
                    && is_one_of_func(
                        &left,
                        &["array_append", "array_prepend", "array_concat"],
                    )
                    && as_col(&right).is_some() =>
            {
                let c = as_col(&right).unwrap();
                let d = schema.field_from_column(c)?.data_type();
                let ndim = list_ndims(d);
                match ndim {
                    0 => Transformed::yes(array_append(*left, *right)),
                    _ => Transformed::yes(array_concat(vec![*left, *right])),
                }
            }
            // 2) select column1 || column2
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat
                    && as_col(&left).is_some()
                    && as_col(&right).is_some() =>
            {
                let c1 = as_col(&left).unwrap();
                let c2 = as_col(&right).unwrap();
                let d1 = schema.field_from_column(c1)?.data_type();
                let d2 = schema.field_from_column(c2)?.data_type();
                let ndim1 = list_ndims(d1);
                let ndim2 = list_ndims(d2);
                match (ndim1, ndim2) {
                    (0, _) => Transformed::yes(array_prepend(*left, *right)),
                    (_, 0) => Transformed::yes(array_append(*left, *right)),
                    _ => Transformed::yes(array_concat(vec![*left, *right])),
                }
            }

            // Chain concat operator (a || b) || array,
            // (array_concat, array_append, array_prepend) || array -> array concat
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat
                    && is_one_of_func(
                        &left,
                        &["array_append", "array_prepend", "array_concat"],
                    )
                    && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_concat(vec![*left, *right]))
            }

            // Chain concat operator (a || b) || scalar,
            // (array_concat, array_append, array_prepend) || scalar -> array append
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat
                    && is_one_of_func(
                        &left,
                        &["array_append", "array_prepend", "array_concat"],
                    ) =>
            {
                Transformed::yes(array_append(*left, *right))
            }

            // array || array -> array concat
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat
                    && is_func(&left, "make_array")
                    && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_concat(vec![*left, *right]))
            }

            // array || scalar -> array append
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat && is_func(&left, "make_array") =>
            {
                Transformed::yes(array_append(*left, *right))
            }

            // scalar || array -> array prepend
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::StringConcat && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_prepend(*left, *right))
            }

            Expr::GetIndexedField(GetIndexedField {
                expr,
                field: GetFieldAccess::NamedStructField { name },
            }) => {
                let expr = *expr.clone();
                let name = Expr::Literal(name);
                Transformed::yes(get_field(expr, name.clone()))
            }

            // expr[idx] ==> array_element(expr, idx)
            Expr::GetIndexedField(GetIndexedField {
                expr,
                field: GetFieldAccess::ListIndex { key },
            }) => Transformed::yes(array_element(*expr, *key)),

            // expr[start, stop, stride] ==> array_slice(expr, start, stop, stride)
            Expr::GetIndexedField(GetIndexedField {
                expr,
                field:
                    GetFieldAccess::ListRange {
                        start,
                        stop,
                        stride,
                    },
            }) => Transformed::yes(array_slice(*expr, *start, *stop, *stride)),

            _ => Transformed::no(expr),
        };
        Ok(transformed)
    }
}

/// Returns true if expr is a function call to the specified named function.
/// Returns false otherwise.
fn is_func(expr: &Expr, func_name: &str) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func_def, args: _ }) = expr else {
        return false;
    };

    func_def.name() == func_name
}

/// Returns true if expr is a function call with one of the specified names
fn is_one_of_func(expr: &Expr, func_names: &[&str]) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func_def, args: _ }) = expr else {
        return false;
    };

    func_names.contains(&func_def.name())
}

/// returns Some(col) if this is Expr::Column
fn as_col(expr: &Expr) -> Option<&Column> {
    if let Expr::Column(c) = expr {
        Some(c)
    } else {
        None
    }
}
