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

//! Signature module contains foundational types that are used to represent signatures, types,
//! and return types of functions in DataFusion.

use arrow::datatypes::DataType;

/// Constant that is used as a placeholder for any valid timezone.
/// This is used where a function can accept a timestamp type with any
/// valid timezone, it exists to avoid the need to enumerate all possible
/// timezones. See [`TypeSignature`] for more details.
///
/// Type coercion always ensures that functions will be executed using
/// timestamp arrays that have a valid time zone. Functions must never
/// return results with this timezone.
pub const TIMEZONE_WILDCARD: &str = "+TZ";

///A function's volatility, which defines the functions eligibility for certain optimizations
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum Volatility {
    /// An immutable function will always return the same output when given the same
    /// input. An example of this is [super::BuiltinScalarFunction::Cos]. DataFusion
    /// will attempt to inline immutable functions during planning.
    Immutable,
    /// A stable function may return different values given the same input across different
    /// queries but must return the same value for a given input within a query. An example of
    /// this is [super::BuiltinScalarFunction::Now]. DataFusion
    /// will attempt to inline `Stable` functions during planning, when possible.
    /// For query `select col1, now() from t1`, it might take a while to execute but
    /// `now()` column will be the same for each output row, which is evaluated
    /// during planning.
    Stable,
    /// A volatile function may change the return value from evaluation to evaluation.
    /// Multiple invocations of a volatile function may return different results when used in the
    /// same query. An example of this is [super::BuiltinScalarFunction::Random]. DataFusion
    /// can not evaluate such functions during planning.
    /// In the query `select col1, random() from t1`, `random()` function will be evaluated
    /// for each output row, resulting in a unique random value for each row.
    Volatile,
}

/// A function's type signature defines the types of arguments the function supports.
///
/// Functions typically support only a few different types of arguments compared to the
/// different datatypes in Arrow. To make functions easy to use, when possible DataFusion
/// automatically coerces (add casts to) function arguments so they match the type signature.
///
/// For example, a function like `cos` may only be implemented for `Float64` arguments. To support a query
/// that calles `cos` with a different argument type, such as `cos(int_column)`, type coercion automatically
/// adds a cast such as `cos(CAST int_column AS DOUBLE)` during planning.
///
/// # Data Types
/// Types to match are represented using Arrow's [`DataType`].  [`DataType::Timestamp`] has an optional variable
/// timezone specification. To specify a function can handle a timestamp with *ANY* timezone, use
/// the [`TIMEZONE_WILDCARD`]. For example:
///
/// ```
/// # use arrow::datatypes::{DataType, TimeUnit};
/// # use datafusion_expr::{TIMEZONE_WILDCARD, TypeSignature};
/// let type_signature = TypeSignature::Exact(vec![
///   // A nanosecond precision timestamp with ANY timezone
///   // matches  Timestamp(Nanosecond, Some("+0:00"))
///   // matches  Timestamp(Nanosecond, Some("+5:00"))
///   // does not match  Timestamp(Nanosecond, None)
///   DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeSignature {
    /// One or more arguments of an common type out of a list of valid types.
    ///
    /// # Examples
    /// A function such as `concat` is `Variadic(vec![DataType::Utf8, DataType::LargeUtf8])`
    Variadic(Vec<DataType>),
    /// One or more arguments of an arbitrary but equal type.
    /// DataFusion attempts to coerce all argument types to match the first argument's type
    ///
    /// # Examples
    /// Given types in signature should be coericible to the same final type.
    /// A function such as `make_array` is `VariadicEqual`.
    ///
    /// `make_array(i32, i64) -> make_array(i64, i64)`
    VariadicEqual,
    /// One or more arguments with arbitrary types
    VariadicAny,
    /// Fixed number of arguments of an arbitrary but equal type out of a list of valid types.
    ///
    /// # Examples
    /// 1. A function of one argument of f64 is `Uniform(1, vec![DataType::Float64])`
    /// 2. A function of one argument of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`
    Uniform(usize, Vec<DataType>),
    /// Exact number of arguments of an exact type
    Exact(Vec<DataType>),
    /// Fixed number of arguments of arbitrary types
    /// If a function takes 0 argument, its `TypeSignature` should be `Any(0)`
    Any(usize),
    /// Matches exactly one of a list of [`TypeSignature`]s. Coercion is attempted to match
    /// the signatures in order, and stops after the first success, if any.
    ///
    /// # Examples
    /// Function `make_array` takes 0 or more arguments with arbitrary types, its `TypeSignature`
    /// is `OneOf(vec![Any(0), VariadicAny])`.
    OneOf(Vec<TypeSignature>),
    /// Specialized Signature for ArrayAppend and similar functions
    /// The first argument should be List/LargeList, and the second argument should be non-list or list.
    /// The second argument's list dimension should be one dimension less than the first argument's list dimension.
    /// List dimension of the List/LargeList is equivalent to the number of List.
    /// List dimension of the non-list is 0.
    ArrayAndElement,
    /// Specialized Signature for ArrayConcat
    /// Accept arbitrary arguments but they SHOULD be List/LargeList or Null, and the list dimension MAY NOT be the same.
    ArrayConcat,
}

impl TypeSignature {
    pub(crate) fn to_string_repr(&self) -> Vec<String> {
        match self {
            TypeSignature::Variadic(types) => {
                vec![format!("{}, ..", Self::join_types(types, "/"))]
            }
            TypeSignature::Uniform(arg_count, valid_types) => {
                vec![std::iter::repeat(Self::join_types(valid_types, "/"))
                    .take(*arg_count)
                    .collect::<Vec<String>>()
                    .join(", ")]
            }
            TypeSignature::Exact(types) => {
                vec![Self::join_types(types, ", ")]
            }
            TypeSignature::Any(arg_count) => {
                vec![std::iter::repeat("Any")
                    .take(*arg_count)
                    .collect::<Vec<&str>>()
                    .join(", ")]
            }
            TypeSignature::VariadicEqual => {
                vec!["CoercibleT, .., CoercibleT".to_string()]
            }
            TypeSignature::VariadicAny => vec!["Any, .., Any".to_string()],
            TypeSignature::OneOf(sigs) => {
                sigs.iter().flat_map(|s| s.to_string_repr()).collect()
            }
            TypeSignature::ArrayAndElement => {
                vec!["ArrayAndElement(List<T>, T)".to_string()]
            }
            TypeSignature::ArrayConcat => {
                vec!["ArrayConcat(List<T> / NULL, .., List<T> / NULL)".to_string()]
            }
        }
    }

    /// Helper function to join types with specified delimiter.
    pub(crate) fn join_types<T: std::fmt::Display>(
        types: &[T],
        delimiter: &str,
    ) -> String {
        types
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<String>>()
            .join(delimiter)
    }

    /// Check whether 0 input argument is valid for given `TypeSignature`
    pub fn supports_zero_argument(&self) -> bool {
        match &self {
            TypeSignature::Exact(vec) => vec.is_empty(),
            TypeSignature::Uniform(0, _) | TypeSignature::Any(0) => true,
            TypeSignature::OneOf(types) => types
                .iter()
                .any(|type_sig| type_sig.supports_zero_argument()),
            _ => false,
        }
    }
}

/// Defines the supported argument types ([`TypeSignature`]) and [`Volatility`] for a function.
///
/// DataFusion will automatically coerce (cast) argument types to one of the supported
/// function signatures, if possible.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Signature {
    /// The data types that the function accepts. See [TypeSignature] for more information.
    pub type_signature: TypeSignature,
    /// The volatility of the function. See [Volatility] for more information.
    pub volatility: Volatility,
}

impl Signature {
    /// Creates a new Signature from a given type signature and volatility.
    pub fn new(type_signature: TypeSignature, volatility: Volatility) -> Self {
        Signature {
            type_signature,
            volatility,
        }
    }
    /// An arbitrary number of arguments with the same type, from those listed in `common_types`.
    pub fn variadic(common_types: Vec<DataType>, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Variadic(common_types),
            volatility,
        }
    }
    /// An arbitrary number of arguments of the same type.
    pub fn variadic_equal(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::VariadicEqual,
            volatility,
        }
    }
    /// An arbitrary number of arguments of any type.
    pub fn variadic_any(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::VariadicAny,
            volatility,
        }
    }
    /// A fixed number of arguments of the same type, from those listed in `valid_types`.
    pub fn uniform(
        arg_count: usize,
        valid_types: Vec<DataType>,
        volatility: Volatility,
    ) -> Self {
        Self {
            type_signature: TypeSignature::Uniform(arg_count, valid_types),
            volatility,
        }
    }
    /// Exactly matches the types in `exact_types`, in order.
    pub fn exact(exact_types: Vec<DataType>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Exact(exact_types),
            volatility,
        }
    }
    /// A specified number of arguments of any type
    pub fn any(arg_count: usize, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Any(arg_count),
            volatility,
        }
    }
    /// Any one of a list of [TypeSignature]s.
    pub fn one_of(type_signatures: Vec<TypeSignature>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::OneOf(type_signatures),
            volatility,
        }
    }
}

/// Monotonicity of the `ScalarFunctionExpr` with respect to its arguments.
/// Each element of this vector corresponds to an argument and indicates whether
/// the function's behavior is monotonic, or non-monotonic/unknown for that argument, namely:
/// - `None` signifies unknown monotonicity or non-monotonicity.
/// - `Some(true)` indicates that the function is monotonically increasing w.r.t. the argument in question.
/// - Some(false) indicates that the function is monotonically decreasing w.r.t. the argument in question.
pub type FuncMonotonicity = Vec<Option<bool>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_zero_argument_tests() {
        // Testing `TypeSignature`s which supports 0 arg
        let positive_cases = vec![
            TypeSignature::Exact(vec![]),
            TypeSignature::Uniform(0, vec![DataType::Float64]),
            TypeSignature::Any(0),
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int8]),
                TypeSignature::Any(0),
                TypeSignature::Uniform(1, vec![DataType::Int8]),
            ]),
        ];

        for case in positive_cases {
            assert!(
                case.supports_zero_argument(),
                "Expected {:?} to support zero arguments",
                case
            );
        }

        // Testing `TypeSignature`s which doesn't support 0 arg
        let negative_cases = vec![
            TypeSignature::Exact(vec![DataType::Utf8]),
            TypeSignature::Uniform(1, vec![DataType::Float64]),
            TypeSignature::Any(1),
            TypeSignature::VariadicAny,
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int8]),
                TypeSignature::Uniform(1, vec![DataType::Int8]),
            ]),
        ];

        for case in negative_cases {
            assert!(
                !case.supports_zero_argument(),
                "Expected {:?} not to support zero arguments",
                case
            );
        }
    }
}
