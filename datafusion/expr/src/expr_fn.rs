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

//! Functions for creating logical expressions

use crate::expr::{
    AggregateFunction, BinaryExpr, Cast, Exists, GroupingSet, InList, InSubquery,
    Placeholder, ScalarFunction, TryCast,
};
use crate::function::PartitionEvaluatorFactory;
use crate::WindowUDF;
use crate::{
    aggregate_function, built_in_function, conditional_expressions::CaseBuilder,
    logical_plan::Subquery, AccumulatorFactoryFunction, AggregateUDF,
    BuiltinScalarFunction, Expr, LogicalPlan, Operator, ReturnTypeFunction,
    ScalarFunctionImplementation, ScalarUDF, Signature, StateTypeFunction, Volatility,
};
use arrow::datatypes::DataType;
use datafusion_common::logical_type::LogicalType;
use datafusion_common::{Column, Result};
use std::ops::Not;
use std::sync::Arc;

/// Create a column expression based on a qualified or unqualified column name. Will
/// normalize unquoted identifiers according to SQL rules (identifiers will become lowercase).
///
/// For example:
///
/// ```rust
/// # use datafusion_expr::col;
/// let c1 = col("a");
/// let c2 = col("A");
/// assert_eq!(c1, c2);
///
/// // note how quoting with double quotes preserves the case
/// let c3 = col(r#""A""#);
/// assert_ne!(c1, c3);
/// ```
pub fn col(ident: impl Into<Column>) -> Expr {
    Expr::Column(ident.into())
}

/// Create an out reference column which hold a reference that has been resolved to a field
/// outside of the current plan.
pub fn out_ref_col(dt: LogicalType, ident: impl Into<Column>) -> Expr {
    Expr::OuterReferenceColumn(dt, ident.into())
}

/// Create an unqualified column expression from the provided name, without normalizing
/// the column.
///
/// For example:
///
/// ```rust
/// # use datafusion_expr::{col, ident};
/// let c1 = ident("A"); // not normalized staying as column 'A'
/// let c2 = col("A"); // normalized via SQL rules becoming column 'a'
/// assert_ne!(c1, c2);
///
/// let c3 = col(r#""A""#);
/// assert_eq!(c1, c3);
///
/// let c4 = col("t1.a"); // parses as relation 't1' column 'a'
/// let c5 = ident("t1.a"); // parses as column 't1.a'
/// assert_ne!(c4, c5);
/// ```
pub fn ident(name: impl Into<String>) -> Expr {
    Expr::Column(Column::from_name(name))
}

/// Create placeholder value that will be filled in (such as `$1`)
///
/// Note the parameter type can be inferred using [`Expr::infer_placeholder_types`]
///
/// # Example
///
/// ```rust
/// # use datafusion_expr::{placeholder};
/// let p = placeholder("$0"); // $0, refers to parameter 1
/// assert_eq!(p.to_string(), "$0")
/// ```
pub fn placeholder(id: impl Into<String>) -> Expr {
    Expr::Placeholder(Placeholder {
        id: id.into(),
        data_type: None,
    })
}

/// Return a new expression `left <op> right`
pub fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

/// Return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::And,
        Box::new(right),
    ))
}

/// Return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Or,
        Box::new(right),
    ))
}

/// Return a new expression with a logical NOT
pub fn not(expr: Expr) -> Expr {
    expr.not()
}

/// Create an expression to represent the min() aggregate function
pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Min,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Max,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Sum,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Create an expression to represent the array_agg() aggregate function
pub fn array_agg(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::ArrayAgg,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Avg,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Count,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Return a new expression with bitwise AND
pub fn bitwise_and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseAnd,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise OR
pub fn bitwise_or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseOr,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise XOR
pub fn bitwise_xor(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseXor,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise SHIFT RIGHT
pub fn bitwise_shift_right(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseShiftRight,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise SHIFT LEFT
pub fn bitwise_shift_left(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseShiftLeft,
        Box::new(right),
    ))
}

/// Create an expression to represent the count(distinct) aggregate function
pub fn count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Count,
        vec![expr],
        true,
        None,
        None,
    ))
}

/// Create an in_list expression
pub fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList(InList::new(Box::new(expr), list, negated))
}

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
pub fn concat(args: &[Expr]) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new(
        BuiltinScalarFunction::Concat,
        args.to_vec(),
    ))
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator.
/// NULL arguments in `values` are ignored.
pub fn concat_ws(sep: Expr, values: Vec<Expr>) -> Expr {
    let mut args = values;
    args.insert(0, sep);
    Expr::ScalarFunction(ScalarFunction::new(
        BuiltinScalarFunction::ConcatWithSeparator,
        args,
    ))
}

/// Returns an approximate value of π
pub fn pi() -> Expr {
    Expr::ScalarFunction(ScalarFunction::new(BuiltinScalarFunction::Pi, vec![]))
}

/// Returns a random value in the range 0.0 <= x < 1.0
pub fn random() -> Expr {
    Expr::ScalarFunction(ScalarFunction::new(BuiltinScalarFunction::Random, vec![]))
}

/// Returns the approximate number of distinct input values.
/// This function provides an approximation of count(DISTINCT x).
/// Zero is returned if all input values are null.
/// This function should produce a standard error of 0.81%,
/// which is the standard deviation of the (approximately normal)
/// error distribution over all possible sets.
/// It does not guarantee an upper bound on the error for any specific input set.
pub fn approx_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::ApproxDistinct,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Calculate the median for `expr`.
pub fn median(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Median,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Calculate an approximation of the median for `expr`.
pub fn approx_median(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::ApproxMedian,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Calculate an approximation of the specified `percentile` for `expr`.
pub fn approx_percentile_cont(expr: Expr, percentile: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::ApproxPercentileCont,
        vec![expr, percentile],
        false,
        None,
        None,
    ))
}

/// Calculate an approximation of the specified `percentile` for `expr` and `weight_expr`.
pub fn approx_percentile_cont_with_weight(
    expr: Expr,
    weight_expr: Expr,
    percentile: Expr,
) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::ApproxPercentileContWithWeight,
        vec![expr, weight_expr, percentile],
        false,
        None,
        None,
    ))
}

/// Create an EXISTS subquery expression
pub fn exists(subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::Exists(Exists {
        subquery: Subquery {
            subquery,
            outer_ref_columns,
        },
        negated: false,
    })
}

/// Create a NOT EXISTS subquery expression
pub fn not_exists(subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::Exists(Exists {
        subquery: Subquery {
            subquery,
            outer_ref_columns,
        },
        negated: true,
    })
}

/// Create an IN subquery expression
pub fn in_subquery(expr: Expr, subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::InSubquery(InSubquery::new(
        Box::new(expr),
        Subquery {
            subquery,
            outer_ref_columns,
        },
        false,
    ))
}

/// Create a NOT IN subquery expression
pub fn not_in_subquery(expr: Expr, subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::InSubquery(InSubquery::new(
        Box::new(expr),
        Subquery {
            subquery,
            outer_ref_columns,
        },
        true,
    ))
}

/// Create a scalar subquery expression
pub fn scalar_subquery(subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::ScalarSubquery(Subquery {
        subquery,
        outer_ref_columns,
    })
}

/// Create an expression to represent the stddev() aggregate function
pub fn stddev(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Stddev,
        vec![expr],
        false,
        None,
        None,
    ))
}

/// Create a grouping set
pub fn grouping_set(exprs: Vec<Vec<Expr>>) -> Expr {
    Expr::GroupingSet(GroupingSet::GroupingSets(exprs))
}

/// Create a grouping set for all combination of `exprs`
pub fn cube(exprs: Vec<Expr>) -> Expr {
    Expr::GroupingSet(GroupingSet::Cube(exprs))
}

/// Create a grouping set for rollup
pub fn rollup(exprs: Vec<Expr>) -> Expr {
    Expr::GroupingSet(GroupingSet::Rollup(exprs))
}

/// Create a cast expression
pub fn cast(expr: Expr, data_type: LogicalType) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), data_type))
}

/// Create a try cast expression
pub fn try_cast(expr: Expr, data_type: LogicalType) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(expr), data_type))
}

/// Create is null expression
pub fn is_null(expr: Expr) -> Expr {
    Expr::IsNull(Box::new(expr))
}

/// Create is true expression
pub fn is_true(expr: Expr) -> Expr {
    Expr::IsTrue(Box::new(expr))
}

/// Create is not true expression
pub fn is_not_true(expr: Expr) -> Expr {
    Expr::IsNotTrue(Box::new(expr))
}

/// Create is false expression
pub fn is_false(expr: Expr) -> Expr {
    Expr::IsFalse(Box::new(expr))
}

/// Create is not false expression
pub fn is_not_false(expr: Expr) -> Expr {
    Expr::IsNotFalse(Box::new(expr))
}

/// Create is unknown expression
pub fn is_unknown(expr: Expr) -> Expr {
    Expr::IsUnknown(Box::new(expr))
}

/// Create is not unknown expression
pub fn is_not_unknown(expr: Expr) -> Expr {
    Expr::IsNotUnknown(Box::new(expr))
}

macro_rules! scalar_expr {
    ($ENUM:ident, $FUNC:ident, $($arg:ident)*, $DOC:expr) => {
        #[doc = $DOC ]
        pub fn $FUNC($($arg: Expr),*) -> Expr {
            Expr::ScalarFunction(ScalarFunction::new(
                built_in_function::BuiltinScalarFunction::$ENUM,
                vec![$($arg),*],
            ))
        }
    };
}

macro_rules! nary_scalar_expr {
    ($ENUM:ident, $FUNC:ident, $DOC:expr) => {
        #[doc = $DOC ]
        pub fn $FUNC(args: Vec<Expr>) -> Expr {
            Expr::ScalarFunction(ScalarFunction::new(
                built_in_function::BuiltinScalarFunction::$ENUM,
                args,
            ))
        }
    };
}

// generate methods for creating the supported unary/binary expressions

// math functions
scalar_expr!(Sqrt, sqrt, num, "square root of a number");
scalar_expr!(Cbrt, cbrt, num, "cube root of a number");
scalar_expr!(Sin, sin, num, "sine");
scalar_expr!(Cos, cos, num, "cosine");
scalar_expr!(Tan, tan, num, "tangent");
scalar_expr!(Cot, cot, num, "cotangent");
scalar_expr!(Sinh, sinh, num, "hyperbolic sine");
scalar_expr!(Cosh, cosh, num, "hyperbolic cosine");
scalar_expr!(Tanh, tanh, num, "hyperbolic tangent");
scalar_expr!(Asin, asin, num, "inverse sine");
scalar_expr!(Acos, acos, num, "inverse cosine");
scalar_expr!(Atan, atan, num, "inverse tangent");
scalar_expr!(Asinh, asinh, num, "inverse hyperbolic sine");
scalar_expr!(Acosh, acosh, num, "inverse hyperbolic cosine");
scalar_expr!(Atanh, atanh, num, "inverse hyperbolic tangent");
scalar_expr!(Factorial, factorial, num, "factorial");
scalar_expr!(
    Floor,
    floor,
    num,
    "nearest integer less than or equal to argument"
);
scalar_expr!(
    Ceil,
    ceil,
    num,
    "nearest integer greater than or equal to argument"
);
scalar_expr!(Degrees, degrees, num, "converts radians to degrees");
scalar_expr!(Radians, radians, num, "converts degrees to radians");
nary_scalar_expr!(Round, round, "round to nearest integer");
nary_scalar_expr!(
    Trunc,
    trunc,
    "truncate toward zero, with optional precision"
);
scalar_expr!(Abs, abs, num, "absolute value");
scalar_expr!(Signum, signum, num, "sign of the argument (-1, 0, +1) ");
scalar_expr!(Exp, exp, num, "exponential");
scalar_expr!(Gcd, gcd, arg_1 arg_2, "greatest common divisor");
scalar_expr!(Lcm, lcm, arg_1 arg_2, "least common multiple");
scalar_expr!(Log2, log2, num, "base 2 logarithm");
scalar_expr!(Log10, log10, num, "base 10 logarithm");
scalar_expr!(Ln, ln, num, "natural logarithm");
scalar_expr!(NullIf, nullif, arg_1 arg_2, "returns NULL if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the COALESCE expression.");
scalar_expr!(Power, power, base exponent, "`base` raised to the power of `exponent`");
scalar_expr!(Atan2, atan2, y x, "inverse tangent of a division given in the argument");
scalar_expr!(
    ToHex,
    to_hex,
    num,
    "returns the hexdecimal representation of an integer"
);
scalar_expr!(Uuid, uuid, , "returns uuid v4 as a string value");
scalar_expr!(Log, log, base x, "logarithm of a `x` for a particular `base`");

// array functions
scalar_expr!(
    ArrayAppend,
    array_append,
    array element,
    "appends an element to the end of an array."
);

scalar_expr!(
    ArrayPopBack,
    array_pop_back,
    array,
    "returns the array without the last element."
);

nary_scalar_expr!(ArrayConcat, array_concat, "concatenates arrays.");
scalar_expr!(
    ArrayHas,
    array_has,
    first_array second_array,
    "returns true, if the element appears in the first array, otherwise false."
);
scalar_expr!(
    ArrayEmpty,
    array_empty,
    array,
    "returns 1 for an empty array or 0 for a non-empty array."
);
scalar_expr!(
    ArrayHasAll,
    array_has_all,
    first_array second_array,
    "returns true if each element of the second array appears in the first array; otherwise, it returns false."
);
scalar_expr!(
    ArrayHasAny,
    array_has_any,
    first_array second_array,
    "returns true if at least one element of the second array appears in the first array; otherwise, it returns false."
);
scalar_expr!(
    Flatten,
    flatten,
    array,
    "flattens an array of arrays into a single array."
);
scalar_expr!(
    ArrayDims,
    array_dims,
    array,
    "returns an array of the array's dimensions."
);
scalar_expr!(
    ArrayElement,
    array_element,
    array element,
    "extracts the element with the index n from the array."
);
scalar_expr!(
    ArrayLength,
    array_length,
    array dimension,
    "returns the length of the array dimension."
);
scalar_expr!(
    ArrayNdims,
    array_ndims,
    array,
    "returns the number of dimensions of the array."
);
scalar_expr!(
    ArrayPosition,
    array_position,
    array element index,
    "searches for an element in the array, returns first occurrence."
);
scalar_expr!(
    ArrayPositions,
    array_positions,
    array element,
    "searches for an element in the array, returns all occurrences."
);
scalar_expr!(
    ArrayPrepend,
    array_prepend,
    array element,
    "prepends an element to the beginning of an array."
);
scalar_expr!(
    ArrayRepeat,
    array_repeat,
    element count,
    "returns an array containing element `count` times."
);
scalar_expr!(
    ArrayRemove,
    array_remove,
    array element,
    "removes the first element from the array equal to the given value."
);
scalar_expr!(
    ArrayRemoveN,
    array_remove_n,
    array element max,
    "removes the first `max` elements from the array equal to the given value."
);
scalar_expr!(
    ArrayRemoveAll,
    array_remove_all,
    array element,
    "removes all elements from the array equal to the given value."
);
scalar_expr!(
    ArrayReplace,
    array_replace,
    array from to,
    "replaces the first occurrence of the specified element with another specified element."
);
scalar_expr!(
    ArrayReplaceN,
    array_replace_n,
    array from to max,
    "replaces the first `max` occurrences of the specified element with another specified element."
);
scalar_expr!(
    ArrayReplaceAll,
    array_replace_all,
    array from to,
    "replaces all occurrences of the specified element with another specified element."
);
scalar_expr!(
    ArraySlice,
    array_slice,
    array offset length,
    "returns a slice of the array."
);
scalar_expr!(
    ArrayToString,
    array_to_string,
    array delimiter,
    "converts each element to its text representation."
);
scalar_expr!(
    Cardinality,
    cardinality,
    array,
    "returns the total number of elements in the array."
);
nary_scalar_expr!(
    MakeArray,
    array,
    "returns an Arrow array using the specified input expressions."
);
scalar_expr!(
    ArrayIntersect,
    array_intersect,
    first_array second_array,
    "Returns an array of the elements in the intersection of array1 and array2."
);

// string functions
scalar_expr!(Ascii, ascii, chr, "ASCII code value of the character");
scalar_expr!(
    BitLength,
    bit_length,
    string,
    "the number of bits in the `string`"
);
scalar_expr!(
    CharacterLength,
    character_length,
    string,
    "the number of characters in the `string`"
);
scalar_expr!(
    Chr,
    chr,
    code_point,
    "converts the Unicode code point to a UTF8 character"
);
scalar_expr!(Digest, digest, input algorithm, "compute the binary hash of `input`, using the `algorithm`");
scalar_expr!(Encode, encode, input encoding, "encode the `input`, using the `encoding`. encoding can be base64 or hex");
scalar_expr!(Decode, decode, input encoding, "decode the`input`, using the `encoding`. encoding can be base64 or hex");
scalar_expr!(InitCap, initcap, string, "converts the first letter of each word in `string` in uppercase and the remaining characters in lowercase");
scalar_expr!(Left, left, string n, "returns the first `n` characters in the `string`");
scalar_expr!(Lower, lower, string, "convert the string to lower case");
scalar_expr!(
    Ltrim,
    ltrim,
    string,
    "removes all characters, spaces by default, from the beginning of a string"
);
scalar_expr!(MD5, md5, string, "returns the MD5 hash of a string");
scalar_expr!(
    OctetLength,
    octet_length,
    string,
    "returns the number of bytes of a string"
);
scalar_expr!(Replace, replace, string from to, "replaces all occurrences of `from` with `to` in the `string`");
scalar_expr!(Repeat, repeat, string n, "repeats the `string` to `n` times");
scalar_expr!(Reverse, reverse, string, "reverses the `string`");
scalar_expr!(Right, right, string n, "returns the last `n` characters in the `string`");
scalar_expr!(
    Rtrim,
    rtrim,
    string,
    "removes all characters, spaces by default, from the end of a string"
);
scalar_expr!(SHA224, sha224, string, "SHA-224 hash");
scalar_expr!(SHA256, sha256, string, "SHA-256 hash");
scalar_expr!(SHA384, sha384, string, "SHA-384 hash");
scalar_expr!(SHA512, sha512, string, "SHA-512 hash");
scalar_expr!(SplitPart, split_part, string delimiter index, "splits a string based on a delimiter and picks out the desired field based on the index.");
scalar_expr!(StringToArray, string_to_array, string delimiter null_string, "splits a `string` based on a `delimiter` and returns an array of parts. Any parts matching the optional `null_string` will be replaced with `NULL`");
scalar_expr!(StartsWith, starts_with, string prefix, "whether the `string` starts with the `prefix`");
scalar_expr!(Strpos, strpos, string substring, "finds the position from where the `substring` matches the `string`");
scalar_expr!(Substr, substr, string position, "substring from the `position` to the end");
scalar_expr!(Substr, substring, string position length, "substring from the `position` with `length` characters");
scalar_expr!(Translate, translate, string from to, "replaces the characters in `from` with the counterpart in `to`");
scalar_expr!(
    Trim,
    trim,
    string,
    "removes all characters, space by default from the string"
);
scalar_expr!(Upper, upper, string, "converts the string to upper case");
//use vec as parameter
nary_scalar_expr!(
    Lpad,
    lpad,
    "fill up a string to the length by prepending the characters"
);
nary_scalar_expr!(
    Rpad,
    rpad,
    "fill up a string to the length by appending the characters"
);
nary_scalar_expr!(
    RegexpReplace,
    regexp_replace,
    "replace strings that match a regular expression"
);
nary_scalar_expr!(
    RegexpMatch,
    regexp_match,
    "matches a regular expression against a string and returns matched substrings."
);
nary_scalar_expr!(
    Btrim,
    btrim,
    "removes all characters, spaces by default, from both sides of a string"
);
nary_scalar_expr!(Coalesce, coalesce, "returns `coalesce(args...)`, which evaluates to the value of the first [Expr] which is not NULL");
//there is a func concat_ws before, so use concat_ws_expr as name.c
nary_scalar_expr!(
    ConcatWithSeparator,
    concat_ws_expr,
    "concatenates several strings, placing a seperator between each one"
);
nary_scalar_expr!(Concat, concat_expr, "concatenates several strings");

// date functions
scalar_expr!(DatePart, date_part, part date, "extracts a subfield from the date");
scalar_expr!(DateTrunc, date_trunc, part date, "truncates the date to a specified level of precision");
scalar_expr!(DateBin, date_bin, stride source origin, "coerces an arbitrary timestamp to the start of the nearest specified interval");
scalar_expr!(
    ToTimestampMillis,
    to_timestamp_millis,
    date,
    "converts a string to a `Timestamp(Milliseconds, None)`"
);
scalar_expr!(
    ToTimestampMicros,
    to_timestamp_micros,
    date,
    "converts a string to a `Timestamp(Microseconds, None)`"
);
scalar_expr!(
    ToTimestampNanos,
    to_timestamp_nanos,
    date,
    "converts a string to a `Timestamp(Nanoseconds, None)`"
);
scalar_expr!(
    ToTimestampSeconds,
    to_timestamp_seconds,
    date,
    "converts a string to a `Timestamp(Seconds, None)`"
);
scalar_expr!(
    FromUnixtime,
    from_unixtime,
    unixtime,
    "returns the unix time in format"
);
scalar_expr!(CurrentDate, current_date, ,"returns current UTC date as a [`DataType::Date32`] value");
scalar_expr!(Now, now, ,"returns current timestamp in nanoseconds, using the same value for all instances of now() in same statement");
scalar_expr!(CurrentTime, current_time, , "returns current UTC time as a [`DataType::Time64`] value");
scalar_expr!(Nanvl, nanvl, x y, "returns x if x is not NaN otherwise returns y");
scalar_expr!(
    Isnan,
    isnan,
    num,
    "returns true if a given number is +NaN or -NaN otherwise returns false"
);
scalar_expr!(
    Iszero,
    iszero,
    num,
    "returns true if a given number is +0.0 or -0.0 otherwise returns false"
);

scalar_expr!(ArrowTypeof, arrow_typeof, val, "data type");

scalar_expr!(
    Struct,
    struct_fun,
    val,
    "returns a vector of fields from the struct"
);

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
pub fn case(expr: Expr) -> CaseBuilder {
    CaseBuilder::new(Some(Box::new(expr)), vec![], vec![], None)
}

/// Create a CASE WHEN statement with boolean WHEN expressions and no base expression.
pub fn when(when: Expr, then: Expr) -> CaseBuilder {
    CaseBuilder::new(None, vec![when], vec![then], None)
}

/// Creates a new UDF with a specific signature and specific return type.
/// This is a helper function to create a new UDF.
/// The function `create_udf` returns a subset of all possible `ScalarFunction`:
/// * the UDF has a fixed return type
/// * the UDF has a fixed signature (e.g. [f64, f64])
pub fn create_udf(
    name: &str,
    input_types: Vec<DataType>,
    return_type: Arc<DataType>,
    volatility: Volatility,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(
        name,
        &Signature::exact(input_types, volatility),
        &return_type,
        &fun,
    )
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Accumulator's implementation`.
pub fn create_udaf(
    name: &str,
    input_type: Vec<DataType>,
    return_type: Arc<DataType>,
    volatility: Volatility,
    accumulator: AccumulatorFactoryFunction,
    state_type: Arc<Vec<DataType>>,
) -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));
    AggregateUDF::new(
        name,
        &Signature::exact(input_type, volatility),
        &return_type,
        &accumulator,
        &state_type,
    )
}

/// Creates a new UDWF with a specific signature, state type and return type.
///
/// The signature and state type must match the [`PartitionEvaluator`]'s implementation`.
///
/// [`PartitionEvaluator`]: crate::PartitionEvaluator
pub fn create_udwf(
    name: &str,
    input_type: DataType,
    return_type: Arc<DataType>,
    volatility: Volatility,
    partition_evaluator_factory: PartitionEvaluatorFactory,
) -> WindowUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    WindowUDF::new(
        name,
        &Signature::exact(vec![input_type], volatility),
        &return_type,
        &partition_evaluator_factory,
    )
}

/// Calls a named built in function
/// ```
/// use datafusion_expr::{col, lit, call_fn};
///
/// // create the expression sin(x) < 0.2
/// let expr = call_fn("sin", vec![col("x")]).unwrap().lt(lit(0.2));
/// ```
pub fn call_fn(name: impl AsRef<str>, args: Vec<Expr>) -> Result<Expr> {
    match name.as_ref().parse::<BuiltinScalarFunction>() {
        Ok(fun) => Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args))),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::lit;

    #[test]
    fn filter_is_null_and_is_not_null() {
        let col_null = col("col1");
        let col_not_null = ident("col2");
        assert_eq!(format!("{}", col_null.is_null()), "col1 IS NULL");
        assert_eq!(
            format!("{}", col_not_null.is_not_null()),
            "col2 IS NOT NULL"
        );
    }

    macro_rules! test_unary_scalar_expr {
        ($ENUM:ident, $FUNC:ident) => {{
            if let Expr::ScalarFunction(ScalarFunction { fun, args }) =
                $FUNC(col("tableA.a"))
            {
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
            let expected = [$(stringify!($arg)),*];
            let result = $FUNC(
                $(
                    col(stringify!($arg.to_string()))
                ),*
            );
            if let Expr::ScalarFunction(ScalarFunction { fun, args }) = result {
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
            let expected = [$(stringify!($arg)),*];
            let result = $FUNC(
                vec![
                    $(
                        col(stringify!($arg.to_string()))
                    ),*
                ]
            );
            if let Expr::ScalarFunction(ScalarFunction { fun, args }) = result {
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
        test_unary_scalar_expr!(Cbrt, cbrt);
        test_unary_scalar_expr!(Sin, sin);
        test_unary_scalar_expr!(Cos, cos);
        test_unary_scalar_expr!(Tan, tan);
        test_unary_scalar_expr!(Cot, cot);
        test_unary_scalar_expr!(Sinh, sinh);
        test_unary_scalar_expr!(Cosh, cosh);
        test_unary_scalar_expr!(Tanh, tanh);
        test_unary_scalar_expr!(Asin, asin);
        test_unary_scalar_expr!(Acos, acos);
        test_unary_scalar_expr!(Atan, atan);
        test_unary_scalar_expr!(Asinh, asinh);
        test_unary_scalar_expr!(Acosh, acosh);
        test_unary_scalar_expr!(Atanh, atanh);
        test_unary_scalar_expr!(Factorial, factorial);
        test_unary_scalar_expr!(Floor, floor);
        test_unary_scalar_expr!(Ceil, ceil);
        test_unary_scalar_expr!(Degrees, degrees);
        test_unary_scalar_expr!(Radians, radians);
        test_nary_scalar_expr!(Round, round, input);
        test_nary_scalar_expr!(Round, round, input, decimal_places);
        test_nary_scalar_expr!(Trunc, trunc, num);
        test_nary_scalar_expr!(Trunc, trunc, num, precision);
        test_unary_scalar_expr!(Abs, abs);
        test_unary_scalar_expr!(Signum, signum);
        test_unary_scalar_expr!(Exp, exp);
        test_unary_scalar_expr!(Log2, log2);
        test_unary_scalar_expr!(Log10, log10);
        test_unary_scalar_expr!(Ln, ln);
        test_scalar_expr!(Atan2, atan2, y, x);
        test_scalar_expr!(Nanvl, nanvl, x, y);
        test_scalar_expr!(Isnan, isnan, input);
        test_scalar_expr!(Iszero, iszero, input);

        test_scalar_expr!(Ascii, ascii, input);
        test_scalar_expr!(BitLength, bit_length, string);
        test_nary_scalar_expr!(Btrim, btrim, string);
        test_nary_scalar_expr!(Btrim, btrim, string, characters);
        test_scalar_expr!(CharacterLength, character_length, string);
        test_scalar_expr!(Chr, chr, string);
        test_scalar_expr!(Digest, digest, string, algorithm);
        test_scalar_expr!(Encode, encode, string, encoding);
        test_scalar_expr!(Decode, decode, string, encoding);
        test_scalar_expr!(Gcd, gcd, arg_1, arg_2);
        test_scalar_expr!(Lcm, lcm, arg_1, arg_2);
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
        test_scalar_expr!(StringToArray, string_to_array, expr, delimiter, null_value);
        test_scalar_expr!(StartsWith, starts_with, string, characters);
        test_scalar_expr!(Strpos, strpos, string, substring);
        test_scalar_expr!(Substr, substr, string, position);
        test_scalar_expr!(Substr, substring, string, position, count);
        test_scalar_expr!(ToHex, to_hex, string);
        test_scalar_expr!(Translate, translate, string, from, to);
        test_scalar_expr!(Trim, trim, string);
        test_scalar_expr!(Upper, upper, string);

        test_scalar_expr!(DatePart, date_part, part, date);
        test_scalar_expr!(DateTrunc, date_trunc, part, date);
        test_scalar_expr!(DateBin, date_bin, stride, source, origin);
        test_scalar_expr!(FromUnixtime, from_unixtime, unixtime);

        test_scalar_expr!(ArrayAppend, array_append, array, element);
        test_scalar_expr!(ArrayPopBack, array_pop_back, array);
        test_unary_scalar_expr!(ArrayDims, array_dims);
        test_scalar_expr!(ArrayLength, array_length, array, dimension);
        test_unary_scalar_expr!(ArrayNdims, array_ndims);
        test_scalar_expr!(ArrayPosition, array_position, array, element, index);
        test_scalar_expr!(ArrayPositions, array_positions, array, element);
        test_scalar_expr!(ArrayPrepend, array_prepend, array, element);
        test_scalar_expr!(ArrayRepeat, array_repeat, element, count);
        test_scalar_expr!(ArrayRemove, array_remove, array, element);
        test_scalar_expr!(ArrayRemoveN, array_remove_n, array, element, max);
        test_scalar_expr!(ArrayRemoveAll, array_remove_all, array, element);
        test_scalar_expr!(ArrayReplace, array_replace, array, from, to);
        test_scalar_expr!(ArrayReplaceN, array_replace_n, array, from, to, max);
        test_scalar_expr!(ArrayReplaceAll, array_replace_all, array, from, to);
        test_scalar_expr!(ArrayToString, array_to_string, array, delimiter);
        test_unary_scalar_expr!(Cardinality, cardinality);
        test_nary_scalar_expr!(MakeArray, array, input);

        test_unary_scalar_expr!(ArrowTypeof, arrow_typeof);
    }

    #[test]
    fn uuid_function_definitions() {
        if let Expr::ScalarFunction(ScalarFunction { fun, args }) = uuid() {
            let name = BuiltinScalarFunction::Uuid;
            assert_eq!(name, fun);
            assert_eq!(0, args.len());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn digest_function_definitions() {
        if let Expr::ScalarFunction(ScalarFunction { fun, args }) =
            digest(col("tableA.a"), lit("md5"))
        {
            let name = BuiltinScalarFunction::Digest;
            assert_eq!(name, fun);
            assert_eq!(2, args.len());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn encode_function_definitions() {
        if let Expr::ScalarFunction(ScalarFunction { fun, args }) =
            encode(col("tableA.a"), lit("base64"))
        {
            let name = BuiltinScalarFunction::Encode;
            assert_eq!(name, fun);
            assert_eq!(2, args.len());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn decode_function_definitions() {
        if let Expr::ScalarFunction(ScalarFunction { fun, args }) =
            decode(col("tableA.a"), lit("hex"))
        {
            let name = BuiltinScalarFunction::Decode;
            assert_eq!(name, fun);
            assert_eq!(2, args.len());
        } else {
            unreachable!();
        }
    }
}
