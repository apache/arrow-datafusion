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

//! Expr fn module contains the functional definitions for expressions.

use crate::{aggregate_function, built_in_function, lit, Expr, Operator};

/// Create a column expression based on a qualified or unqualified column name
pub fn col(ident: &str) -> Expr {
    Expr::Column(ident.into())
}

/// Return a new expression l <op> r
pub fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// Return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::And,
        right: Box::new(right),
    }
}

/// Return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::Or,
        right: Box::new(right),
    }
}

/// Create an expression to represent the min() aggregate function
pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Min,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Max,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Sum,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Avg,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Count,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the count(distinct) aggregate function
pub fn count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::Count,
        distinct: true,
        args: vec![expr],
    }
}

/// Create an in_list expression
pub fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList {
        expr: Box::new(expr),
        list,
        negated,
    }
}

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
pub fn concat(args: &[Expr]) -> Expr {
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::Concat,
        args: args.to_vec(),
    }
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator string, and should not be NULL.
/// Other NULL arguments are ignored.
pub fn concat_ws(sep: impl Into<String>, values: &[Expr]) -> Expr {
    let mut args = vec![lit(sep.into())];
    args.extend_from_slice(values);
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::ConcatWithSeparator,
        args,
    }
}

/// Returns a random value in the range 0.0 <= x < 1.0
pub fn random() -> Expr {
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::Random,
        args: vec![],
    }
}

/// Returns the approximate number of distinct input values.
/// This function provides an approximation of count(DISTINCT x).
/// Zero is returned if all input values are null.
/// This function should produce a standard error of 0.81%,
/// which is the standard deviation of the (approximately normal)
/// error distribution over all possible sets.
/// It does not guarantee an upper bound on the error for any specific input set.
pub fn approx_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::ApproxDistinct,
        distinct: false,
        args: vec![expr],
    }
}

/// Calculate an approximation of the specified `percentile` for `expr`.
pub fn approx_percentile_cont(expr: Expr, percentile: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::ApproxPercentileCont,
        distinct: false,
        args: vec![expr, percentile],
    }
}

/// Returns the precise number of distinct input values using bitmap.
pub fn bitmap_count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregate_function::AggregateFunction::BitMapCountDistinct,
        distinct: false,
        args: vec![expr],
    }
}

// TODO(kszucs): this seems buggy, unary_scalar_expr! is used for many
// varying arity functions
/// Create an convenience function representing a unary scalar function
macro_rules! unary_scalar_expr {
    ($ENUM:ident, $FUNC:ident) => {
        #[doc = concat!("Unary scalar function definition for ", stringify!($FUNC) ) ]
        pub fn $FUNC(e: Expr) -> Expr {
            Expr::ScalarFunction {
                fun: built_in_function::BuiltinScalarFunction::$ENUM,
                args: vec![e],
            }
        }
    };
}

macro_rules! scalar_expr {
    ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
        #[doc = concat!("Scalar function definition for ", stringify!($FUNC) ) ]
        pub fn $FUNC($($arg: Expr),*) -> Expr {
            Expr::ScalarFunction {
                fun: built_in_function::BuiltinScalarFunction::$ENUM,
                args: vec![$($arg),*],
            }
        }
    };
}

macro_rules! nary_scalar_expr {
    ($ENUM:ident, $FUNC:ident) => {
        #[doc = concat!("Scalar function definition for ", stringify!($FUNC) ) ]
        pub fn $FUNC(args: Vec<Expr>) -> Expr {
            Expr::ScalarFunction {
                fun: built_in_function::BuiltinScalarFunction::$ENUM,
                args,
            }
        }
    };
}

// generate methods for creating the supported unary/binary expressions

// math functions
unary_scalar_expr!(Sqrt, sqrt);
unary_scalar_expr!(Sin, sin);
unary_scalar_expr!(Cos, cos);
unary_scalar_expr!(Tan, tan);
unary_scalar_expr!(Asin, asin);
unary_scalar_expr!(Acos, acos);
unary_scalar_expr!(Atan, atan);
unary_scalar_expr!(Floor, floor);
unary_scalar_expr!(Ceil, ceil);
unary_scalar_expr!(Now, now);
unary_scalar_expr!(Round, round);
unary_scalar_expr!(Trunc, trunc);
unary_scalar_expr!(Abs, abs);
unary_scalar_expr!(Signum, signum);
unary_scalar_expr!(Exp, exp);
unary_scalar_expr!(Log2, log2);
unary_scalar_expr!(Log10, log10);
unary_scalar_expr!(Ln, ln);
unary_scalar_expr!(NullIf, nullif);

// string functions
scalar_expr!(Ascii, ascii, string);
scalar_expr!(BitLength, bit_length, string);
scalar_expr!(CharacterLength, character_length, string);
scalar_expr!(CharacterLength, length, string);
scalar_expr!(Chr, chr, string);
scalar_expr!(Digest, digest, string, algorithm);
scalar_expr!(InitCap, initcap, string);
scalar_expr!(Left, left, string, count);
scalar_expr!(Lower, lower, string);
scalar_expr!(Ltrim, ltrim, string);
scalar_expr!(MD5, md5, string);
scalar_expr!(OctetLength, octet_length, string);
scalar_expr!(Replace, replace, string, from, to);
scalar_expr!(Repeat, repeat, string, count);
scalar_expr!(Reverse, reverse, string);
scalar_expr!(Right, right, string, count);
scalar_expr!(Rtrim, rtrim, string);
scalar_expr!(SHA224, sha224, string);
scalar_expr!(SHA256, sha256, string);
scalar_expr!(SHA384, sha384, string);
scalar_expr!(SHA512, sha512, string);
scalar_expr!(SplitPart, split_part, expr, delimiter, index);
scalar_expr!(StartsWith, starts_with, string, characters);
scalar_expr!(Strpos, strpos, string, substring);
scalar_expr!(Substr, substr, string, position);
scalar_expr!(ToHex, to_hex, string);
scalar_expr!(Translate, translate, string, from, to);
scalar_expr!(Trim, trim, string);
scalar_expr!(Upper, upper, string);
//use vec as parameter
nary_scalar_expr!(Lpad, lpad);
nary_scalar_expr!(Rpad, rpad);
nary_scalar_expr!(RegexpReplace, regexp_replace);
nary_scalar_expr!(RegexpMatch, regexp_match);
nary_scalar_expr!(Btrim, btrim);
//there is a func concat_ws before, so use concat_ws_expr as name.c
nary_scalar_expr!(ConcatWithSeparator, concat_ws_expr);
nary_scalar_expr!(Concat, concat_expr);
nary_scalar_expr!(Now, now_expr);

// date functions
scalar_expr!(DatePart, date_part, part, date);
scalar_expr!(DateTrunc, date_trunc, part, date);
scalar_expr!(ToTimestampMillis, to_timestamp_millis, date);
scalar_expr!(ToTimestampMicros, to_timestamp_micros, date);
scalar_expr!(ToTimestampSeconds, to_timestamp_seconds, date);

/// Returns an array of fixed size with each argument on it.
pub fn array(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction {
        fun: built_in_function::BuiltinScalarFunction::Array,
        args,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn filter_is_null_and_is_not_null() {
        let col_null = col("col1");
        let col_not_null = col("col2");
        assert_eq!(format!("{:?}", col_null.is_null()), "#col1 IS NULL");
        assert_eq!(
            format!("{:?}", col_not_null.is_not_null()),
            "#col2 IS NOT NULL"
        );
    }

    macro_rules! test_unary_scalar_expr {
        ($ENUM:ident, $FUNC:ident) => {{
            if let Expr::ScalarFunction { fun, args } = $FUNC(col("tableA.a")) {
                let name = built_in_function::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(1, args.len());
            } else {
                assert!(false, "unexpected");
            }
        }};
    }

    macro_rules! test_scalar_expr {
        ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
            let expected = vec![$(stringify!($arg)),*];
            let result = $FUNC(
                $(
                    col(stringify!($arg.to_string()))
                ),*
            );
            if let Expr::ScalarFunction { fun, args } = result {
                let name = built_in_function::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(expected.len(), args.len());
            } else {
                assert!(false, "unexpected: {:?}", result);
            }
        };
    }

    macro_rules! test_nary_scalar_expr {
        ($ENUM:ident, $FUNC:ident, $($arg:ident),*) => {
            let expected = vec![$(stringify!($arg)),*];
            let result = $FUNC(
                vec![
                    $(
                        col(stringify!($arg.to_string()))
                    ),*
                ]
            );
            if let Expr::ScalarFunction { fun, args } = result {
                let name = built_in_function::BuiltinScalarFunction::$ENUM;
                assert_eq!(name, fun);
                assert_eq!(expected.len(), args.len());
            } else {
                assert!(false, "unexpected: {:?}", result);
            }
        };
    }

    #[test]
    fn scalar_function_definitions() {
        test_unary_scalar_expr!(Sqrt, sqrt);
        test_unary_scalar_expr!(Sin, sin);
        test_unary_scalar_expr!(Cos, cos);
        test_unary_scalar_expr!(Tan, tan);
        test_unary_scalar_expr!(Asin, asin);
        test_unary_scalar_expr!(Acos, acos);
        test_unary_scalar_expr!(Atan, atan);
        test_unary_scalar_expr!(Floor, floor);
        test_unary_scalar_expr!(Ceil, ceil);
        test_unary_scalar_expr!(Now, now);
        test_unary_scalar_expr!(Round, round);
        test_unary_scalar_expr!(Trunc, trunc);
        test_unary_scalar_expr!(Abs, abs);
        test_unary_scalar_expr!(Signum, signum);
        test_unary_scalar_expr!(Exp, exp);
        test_unary_scalar_expr!(Log2, log2);
        test_unary_scalar_expr!(Log10, log10);
        test_unary_scalar_expr!(Ln, ln);

        test_scalar_expr!(Ascii, ascii, input);
        test_scalar_expr!(BitLength, bit_length, string);
        test_nary_scalar_expr!(Btrim, btrim, string);
        test_nary_scalar_expr!(Btrim, btrim, string, characters);
        test_scalar_expr!(CharacterLength, character_length, string);
        test_scalar_expr!(CharacterLength, length, string);
        test_scalar_expr!(Chr, chr, string);
        test_scalar_expr!(Digest, digest, string, algorithm);
        test_scalar_expr!(InitCap, initcap, string);
        test_scalar_expr!(Left, left, string, count);
        test_scalar_expr!(Lower, lower, string);
        test_nary_scalar_expr!(Lpad, lpad, string, count);
        test_nary_scalar_expr!(Lpad, lpad, string, count, characters);
        test_scalar_expr!(Ltrim, ltrim, string);
        test_scalar_expr!(MD5, md5, string);
        test_scalar_expr!(OctetLength, octet_length, string);
        test_nary_scalar_expr!(RegexpMatch, regexp_match, string, pattern);
        test_nary_scalar_expr!(RegexpMatch, regexp_match, string, pattern, flags);
        test_nary_scalar_expr!(
            RegexpReplace,
            regexp_replace,
            string,
            pattern,
            replacement
        );
        test_nary_scalar_expr!(
            RegexpReplace,
            regexp_replace,
            string,
            pattern,
            replacement,
            flags
        );
        test_scalar_expr!(Replace, replace, string, from, to);
        test_scalar_expr!(Repeat, repeat, string, count);
        test_scalar_expr!(Reverse, reverse, string);
        test_scalar_expr!(Right, right, string, count);
        test_nary_scalar_expr!(Rpad, rpad, string, count);
        test_nary_scalar_expr!(Rpad, rpad, string, count, characters);
        test_scalar_expr!(Rtrim, rtrim, string);
        test_scalar_expr!(SHA224, sha224, string);
        test_scalar_expr!(SHA256, sha256, string);
        test_scalar_expr!(SHA384, sha384, string);
        test_scalar_expr!(SHA512, sha512, string);
        test_scalar_expr!(SplitPart, split_part, expr, delimiter, index);
        test_scalar_expr!(StartsWith, starts_with, string, characters);
        test_scalar_expr!(Strpos, strpos, string, substring);
        test_scalar_expr!(Substr, substr, string, position);
        test_scalar_expr!(ToHex, to_hex, string);
        test_scalar_expr!(Translate, translate, string, from, to);
        test_scalar_expr!(Trim, trim, string);
        test_scalar_expr!(Upper, upper, string);

        test_scalar_expr!(DatePart, date_part, part, date);
        test_scalar_expr!(DateTrunc, date_trunc, part, date);
    }
}
