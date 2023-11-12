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

//! Utility functions for expression simplification

use crate::simplify_expressions::SimplifyInfo;
use datafusion_common::logical_type::ExtensionType;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    expr::{Between, BinaryExpr, InList},
    expr_fn::{and, bitwise_and, bitwise_or, concat_ws, or},
    lit, BuiltinScalarFunction, Expr, Like, Operator,
};

pub static POWS_OF_TEN: [i128; 38] = [
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000,
    10000000000000000000,
    100000000000000000000,
    1000000000000000000000,
    10000000000000000000000,
    100000000000000000000000,
    1000000000000000000000000,
    10000000000000000000000000,
    100000000000000000000000000,
    1000000000000000000000000000,
    10000000000000000000000000000,
    100000000000000000000000000000,
    1000000000000000000000000000000,
    10000000000000000000000000000000,
    100000000000000000000000000000000,
    1000000000000000000000000000000000,
    10000000000000000000000000000000000,
    100000000000000000000000000000000000,
    1000000000000000000000000000000000000,
    10000000000000000000000000000000000000,
];

/// returns true if `needle` is found in a chain of search_op
/// expressions. Such as: (A AND B) AND C
pub fn expr_contains(expr: &Expr, needle: &Expr, search_op: Operator) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == search_op => {
            expr_contains(left, needle, search_op)
                || expr_contains(right, needle, search_op)
        }
        _ => expr == needle,
    }
}

/// Deletes all 'needles' or remains one 'needle' that are found in a chain of xor
/// expressions. Such as: A ^ (A ^ (B ^ A))
pub fn delete_xor_in_complex_expr(expr: &Expr, needle: &Expr, is_left: bool) -> Expr {
    /// Deletes recursively 'needles' in a chain of xor expressions
    fn recursive_delete_xor_in_expr(
        expr: &Expr,
        needle: &Expr,
        xor_counter: &mut i32,
    ) -> Expr {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if *op == Operator::BitwiseXor =>
            {
                let left_expr = recursive_delete_xor_in_expr(left, needle, xor_counter);
                let right_expr = recursive_delete_xor_in_expr(right, needle, xor_counter);
                if left_expr == *needle {
                    *xor_counter += 1;
                    return right_expr;
                } else if right_expr == *needle {
                    *xor_counter += 1;
                    return left_expr;
                }

                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left_expr),
                    *op,
                    Box::new(right_expr),
                ))
            }
            _ => expr.clone(),
        }
    }

    let mut xor_counter: i32 = 0;
    let result_expr = recursive_delete_xor_in_expr(expr, needle, &mut xor_counter);
    if result_expr == *needle {
        return needle.clone();
    } else if xor_counter % 2 == 0 {
        if is_left {
            return Expr::BinaryExpr(BinaryExpr::new(
                Box::new(needle.clone()),
                Operator::BitwiseXor,
                Box::new(result_expr),
            ));
        } else {
            return Expr::BinaryExpr(BinaryExpr::new(
                Box::new(result_expr),
                Operator::BitwiseXor,
                Box::new(needle.clone()),
            ));
        }
    }
    result_expr
}

pub fn is_zero(s: &Expr) -> bool {
    match s {
        Expr::Literal(ScalarValue::Int8(Some(0)))
        | Expr::Literal(ScalarValue::Int16(Some(0)))
        | Expr::Literal(ScalarValue::Int32(Some(0)))
        | Expr::Literal(ScalarValue::Int64(Some(0)))
        | Expr::Literal(ScalarValue::UInt8(Some(0)))
        | Expr::Literal(ScalarValue::UInt16(Some(0)))
        | Expr::Literal(ScalarValue::UInt32(Some(0)))
        | Expr::Literal(ScalarValue::UInt64(Some(0))) => true,
        Expr::Literal(ScalarValue::Float32(Some(v))) if *v == 0. => true,
        Expr::Literal(ScalarValue::Float64(Some(v))) if *v == 0. => true,
        Expr::Literal(ScalarValue::Decimal128(Some(v), _p, _s)) if *v == 0 => true,
        _ => false,
    }
}

pub fn is_one(s: &Expr) -> bool {
    match s {
        Expr::Literal(ScalarValue::Int8(Some(1)))
        | Expr::Literal(ScalarValue::Int16(Some(1)))
        | Expr::Literal(ScalarValue::Int32(Some(1)))
        | Expr::Literal(ScalarValue::Int64(Some(1)))
        | Expr::Literal(ScalarValue::UInt8(Some(1)))
        | Expr::Literal(ScalarValue::UInt16(Some(1)))
        | Expr::Literal(ScalarValue::UInt32(Some(1)))
        | Expr::Literal(ScalarValue::UInt64(Some(1))) => true,
        Expr::Literal(ScalarValue::Float32(Some(v))) if *v == 1. => true,
        Expr::Literal(ScalarValue::Float64(Some(v))) if *v == 1. => true,
        Expr::Literal(ScalarValue::Decimal128(Some(v), _p, s)) => {
            *s >= 0
                && POWS_OF_TEN
                    .get(*s as usize)
                    .map(|x| x == v)
                    .unwrap_or_default()
        }
        _ => false,
    }
}

pub fn is_true(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => *v,
        _ => false,
    }
}

/// returns true if expr is a
/// `Expr::Literal(ScalarValue::Boolean(v))` , false otherwise
pub fn is_bool_lit(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Boolean(_)))
}

/// Return a literal NULL value of Boolean data type
pub fn lit_bool_null() -> Expr {
    Expr::Literal(ScalarValue::Boolean(None))
}

pub fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v) => v.is_null(),
        _ => false,
    }
}

pub fn is_false(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => !(*v),
        _ => false,
    }
}

/// returns true if `haystack` looks like (needle OP X) or (X OP needle)
pub fn is_op_with(target_op: Operator, haystack: &Expr, needle: &Expr) -> bool {
    matches!(haystack, Expr::BinaryExpr(BinaryExpr { left, op, right }) if op == &target_op && (needle == left.as_ref() || needle == right.as_ref()))
}

/// returns true if `not_expr` is !`expr` (not)
pub fn is_not_of(not_expr: &Expr, expr: &Expr) -> bool {
    matches!(not_expr, Expr::Not(inner) if expr == inner.as_ref())
}

/// returns true if `not_expr` is !`expr` (bitwise not)
pub fn is_negative_of(not_expr: &Expr, expr: &Expr) -> bool {
    matches!(not_expr, Expr::Negative(inner) if expr == inner.as_ref())
}

/// returns the contained boolean value in `expr` as
/// `Expr::Literal(ScalarValue::Boolean(v))`.
pub fn as_bool_lit(expr: Expr) -> Result<Option<bool>> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(v)) => Ok(v),
        _ => internal_err!("Expected boolean literal, got {expr:?}"),
    }
}

/// negate a Not clause
/// input is the clause to be negated.(args of Not clause)
/// For BinaryExpr, use the negation of op instead.
///    not ( A > B) ===> (A <= B)
/// For BoolExpr, not (A and B) ===> (not A) or (not B)
///     not (A or B) ===> (not A) and (not B)
///     not (not A) ===> A
/// For NullExpr, not (A is not null) ===> A is null
///     not (A is null) ===> A is not null
/// For InList, not (A not in (..)) ===> A in (..)
///     not (A in (..)) ===> A not in (..)
/// For Between, not (A between B and C) ===> (A not between B and C)
///     not (A not between B and C) ===> (A between B and C)
/// For others, use Not clause
pub fn negate_clause(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Some(negated_op) = op.negate() {
                return Expr::BinaryExpr(BinaryExpr::new(left, negated_op, right));
            }
            match op {
                // not (A and B) ===> (not A) or (not B)
                Operator::And => {
                    let left = negate_clause(*left);
                    let right = negate_clause(*right);

                    or(left, right)
                }
                // not (A or B) ===> (not A) and (not B)
                Operator::Or => {
                    let left = negate_clause(*left);
                    let right = negate_clause(*right);

                    and(left, right)
                }
                // use not clause
                _ => Expr::Not(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    left, op, right,
                )))),
            }
        }
        // not (not A) ===> A
        Expr::Not(expr) => *expr,
        // not (A is not null) ===> A is null
        Expr::IsNotNull(expr) => expr.is_null(),
        // not (A is null) ===> A is not null
        Expr::IsNull(expr) => expr.is_not_null(),
        // not (A not in (..)) ===> A in (..)
        // not (A in (..)) ===> A not in (..)
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => expr.in_list(list, !negated),
        // not (A between B and C) ===> (A not between B and C)
        // not (A not between B and C) ===> (A between B and C)
        Expr::Between(between) => Expr::Between(Between::new(
            between.expr,
            !between.negated,
            between.low,
            between.high,
        )),
        // not (A like B) ===> A not like B
        Expr::Like(like) => Expr::Like(Like::new(
            !like.negated,
            like.expr,
            like.pattern,
            like.escape_char,
            like.case_insensitive,
        )),
        // use not clause
        _ => Expr::Not(Box::new(expr)),
    }
}

/// bitwise negate a Negative clause
/// input is the clause to be bitwise negated.(args for Negative clause)
/// For BinaryExpr:
///    ~(A & B) ===> ~A | ~B
///    ~(A | B) ===> ~A & ~B
/// For Negative:
///    ~(~A) ===> A
/// For others, use Negative clause
pub fn distribute_negation(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            match op {
                // ~(A & B) ===> ~A | ~B
                Operator::BitwiseAnd => {
                    let left = distribute_negation(*left);
                    let right = distribute_negation(*right);

                    bitwise_or(left, right)
                }
                // ~(A | B) ===> ~A & ~B
                Operator::BitwiseOr => {
                    let left = distribute_negation(*left);
                    let right = distribute_negation(*right);

                    bitwise_and(left, right)
                }
                // use negative clause
                _ => Expr::Negative(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    left, op, right,
                )))),
            }
        }
        // ~(~A) ===> A
        Expr::Negative(expr) => *expr,
        // use negative clause
        _ => Expr::Negative(Box::new(expr)),
    }
}

/// Simplify the `log` function by the relevant rules:
/// 1. Log(a, 1) ===> 0
/// 2. Log(a, a) ===> 1
/// 3. Log(a, Power(a, b)) ===> b
pub fn simpl_log(current_args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<Expr> {
    let mut number = &current_args[0];
    let mut base = &Expr::Literal(ScalarValue::new_ten(
        &info.get_data_type(number)?.physical_type(),
    )?);
    if current_args.len() == 2 {
        base = &current_args[0];
        number = &current_args[1];
    }

    match number {
        Expr::Literal(value)
            if value
                == &ScalarValue::new_one(
                    &info.get_data_type(number)?.physical_type(),
                )? =>
        {
            Ok(Expr::Literal(ScalarValue::new_zero(
                &info.get_data_type(base)?.physical_type(),
            )?))
        }
        Expr::ScalarFunction(ScalarFunction {
            fun: BuiltinScalarFunction::Power,
            args,
        }) if base == &args[0] => Ok(args[1].clone()),
        _ => {
            if number == base {
                Ok(Expr::Literal(ScalarValue::new_one(
                    &info.get_data_type(number)?.physical_type(),
                )?))
            } else {
                Ok(Expr::ScalarFunction(ScalarFunction::new(
                    BuiltinScalarFunction::Log,
                    vec![base.clone(), number.clone()],
                )))
            }
        }
    }
}

/// Simplify the `power` function by the relevant rules:
/// 1. Power(a, 0) ===> 0
/// 2. Power(a, 1) ===> a
/// 3. Power(a, Log(a, b)) ===> b
pub fn simpl_power(current_args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<Expr> {
    let base = &current_args[0];
    let exponent = &current_args[1];

    match exponent {
        Expr::Literal(value)
            if value
                == &ScalarValue::new_zero(
                    &info.get_data_type(exponent)?.physical_type(),
                )? =>
        {
            Ok(Expr::Literal(ScalarValue::new_one(
                &info.get_data_type(base)?.physical_type(),
            )?))
        }
        Expr::Literal(value)
            if value
                == &ScalarValue::new_one(
                    &info.get_data_type(exponent)?.physical_type(),
                )? =>
        {
            Ok(base.clone())
        }
        Expr::ScalarFunction(ScalarFunction {
            fun: BuiltinScalarFunction::Log,
            args,
        }) if base == &args[0] => Ok(args[1].clone()),
        _ => Ok(Expr::ScalarFunction(ScalarFunction::new(
            BuiltinScalarFunction::Power,
            current_args,
        ))),
    }
}

/// Simplify the `concat` function by
/// 1. filtering out all `null` literals
/// 2. concatenating contiguous literal arguments
///
/// For example:
/// `concat(col(a), 'hello ', 'world', col(b), null)`
/// will be optimized to
/// `concat(col(a), 'hello world', col(b))`
pub fn simpl_concat(args: Vec<Expr>) -> Result<Expr> {
    let mut new_args = Vec::with_capacity(args.len());
    let mut contiguous_scalar = "".to_string();
    for arg in args {
        match arg {
            // filter out `null` args
            Expr::Literal(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None)) => {}
            // All literals have been converted to Utf8 or LargeUtf8 in type_coercion.
            // Concatenate it with the `contiguous_scalar`.
            Expr::Literal(
                ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)),
            ) => contiguous_scalar += &v,
            Expr::Literal(x) => {
                return internal_err!(
                "The scalar {x} should be casted to string type during the type coercion."
            )
            }
            // If the arg is not a literal, we should first push the current `contiguous_scalar`
            // to the `new_args` (if it is not empty) and reset it to empty string.
            // Then pushing this arg to the `new_args`.
            arg => {
                if !contiguous_scalar.is_empty() {
                    new_args.push(lit(contiguous_scalar));
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg);
            }
        }
    }
    if !contiguous_scalar.is_empty() {
        new_args.push(lit(contiguous_scalar));
    }

    Ok(Expr::ScalarFunction(ScalarFunction::new(
        BuiltinScalarFunction::Concat,
        new_args,
    )))
}

/// Simply the `concat_ws` function by
/// 1. folding to `null` if the delimiter is null
/// 2. filtering out `null` arguments
/// 3. using `concat` to replace `concat_ws` if the delimiter is an empty string
/// 4. concatenating contiguous literals if the delimiter is a literal.
pub fn simpl_concat_ws(delimiter: &Expr, args: &[Expr]) -> Result<Expr> {
    match delimiter {
        Expr::Literal(
            ScalarValue::Utf8(delimiter) | ScalarValue::LargeUtf8(delimiter),
        ) => {
            match delimiter {
                // when the delimiter is an empty string,
                // we can use `concat` to replace `concat_ws`
                Some(delimiter) if delimiter.is_empty() => simpl_concat(args.to_vec()),
                Some(delimiter) => {
                    let mut new_args = Vec::with_capacity(args.len());
                    new_args.push(lit(delimiter));
                    let mut contiguous_scalar = None;
                    for arg in args {
                        match arg {
                            // filter out null args
                            Expr::Literal(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None)) => {}
                            Expr::Literal(ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v))) => {
                                match contiguous_scalar {
                                    None => contiguous_scalar = Some(v.to_string()),
                                    Some(mut pre) => {
                                        pre += delimiter;
                                        pre += v;
                                        contiguous_scalar = Some(pre)
                                    }
                                }
                            }
                            Expr::Literal(s) => return internal_err!("The scalar {s} should be casted to string type during the type coercion."),
                            // If the arg is not a literal, we should first push the current `contiguous_scalar`
                            // to the `new_args` and reset it to None.
                            // Then pushing this arg to the `new_args`.
                            arg => {
                                if let Some(val) = contiguous_scalar {
                                    new_args.push(lit(val));
                                }
                                new_args.push(arg.clone());
                                contiguous_scalar = None;
                            }
                        }
                    }
                    if let Some(val) = contiguous_scalar {
                        new_args.push(lit(val));
                    }
                    Ok(Expr::ScalarFunction(ScalarFunction::new(
                        BuiltinScalarFunction::ConcatWithSeparator,
                        new_args,
                    )))
                }
                // if the delimiter is null, then the value of the whole expression is null.
                None => Ok(Expr::Literal(ScalarValue::Utf8(None))),
            }
        }
        Expr::Literal(d) => internal_err!(
            "The scalar {d} should be casted to string type during the type coercion."
        ),
        d => Ok(concat_ws(
            d.clone(),
            args.iter()
                .filter(|&x| !is_null(x))
                .cloned()
                .collect::<Vec<Expr>>(),
        )),
    }
}

#[cfg(test)]
pub mod for_test {
    use arrow::datatypes::DataType;
    use datafusion_expr::{call_fn, lit, Cast, Expr};

    pub fn now_expr() -> Expr {
        call_fn("now", vec![]).unwrap()
    }

    pub fn cast_to_int64_expr(expr: Expr) -> Expr {
        Expr::Cast(Cast::new(expr.into(), DataType::Int64))
    }

    pub fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
        call_fn("to_timestamp", vec![lit(arg.into())]).unwrap()
    }
}
