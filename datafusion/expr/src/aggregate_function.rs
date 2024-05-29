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

//! Aggregate function module contains all built-in aggregate functions definitions

use std::sync::Arc;
use std::{fmt, str::FromStr};

use crate::utils;
use crate::{type_coercion::aggregates::*, Signature, TypeSignature, Volatility};

use arrow::datatypes::{DataType, Field};
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};

use strum_macros::EnumIter;

/// Enum of all built-in aggregate functions
// Contributor's guide for adding new aggregate functions
// https://datafusion.apache.org/contributor-guide/index.html#how-to-add-a-new-aggregate-function
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, EnumIter)]
pub enum AggregateFunction {
    /// Count
    Count,
    /// Sum
    Sum,
    /// Minimum
    Min,
    /// Maximum
    Max,
    /// Average
    Avg,
    /// Approximate distinct function
    ApproxDistinct,
    /// Aggregation into an array
    ArrayAgg,
    /// N'th value in a group according to some ordering
    NthValue,
    /// Variance (Sample)
    Variance,
    /// Variance (Population)
    VariancePop,
    /// Standard Deviation (Sample)
    Stddev,
    /// Standard Deviation (Population)
    StddevPop,
    /// Correlation
    Correlation,
    /// Slope from linear regression
    RegrSlope,
    /// Intercept from linear regression
    RegrIntercept,
    /// Number of input rows in which both expressions are not null
    RegrCount,
    /// R-squared value from linear regression
    RegrR2,
    /// Average of the independent variable
    RegrAvgx,
    /// Average of the dependent variable
    RegrAvgy,
    /// Sum of squares of the independent variable
    RegrSXX,
    /// Sum of squares of the dependent variable
    RegrSYY,
    /// Sum of products of pairs of numbers
    RegrSXY,
    /// Approximate continuous percentile function
    ApproxPercentileCont,
    /// Approximate continuous percentile function with weight
    ApproxPercentileContWithWeight,
    /// ApproxMedian
    ApproxMedian,
    /// Grouping
    Grouping,
    /// Bit And
    BitAnd,
    /// Bit Or
    BitOr,
    /// Bit Xor
    BitXor,
    /// Bool And
    BoolAnd,
    /// Bool Or
    BoolOr,
    /// String aggregation
    StringAgg,
}

impl AggregateFunction {
    pub fn name(&self) -> &str {
        use AggregateFunction::*;
        match self {
            Count => "COUNT",
            Sum => "SUM",
            Min => "MIN",
            Max => "MAX",
            Avg => "AVG",
            ApproxDistinct => "APPROX_DISTINCT",
            ArrayAgg => "ARRAY_AGG",
            NthValue => "NTH_VALUE",
            Variance => "VAR",
            VariancePop => "VAR_POP",
            Stddev => "STDDEV",
            StddevPop => "STDDEV_POP",
            Correlation => "CORR",
            RegrSlope => "REGR_SLOPE",
            RegrIntercept => "REGR_INTERCEPT",
            RegrCount => "REGR_COUNT",
            RegrR2 => "REGR_R2",
            RegrAvgx => "REGR_AVGX",
            RegrAvgy => "REGR_AVGY",
            RegrSXX => "REGR_SXX",
            RegrSYY => "REGR_SYY",
            RegrSXY => "REGR_SXY",
            ApproxPercentileCont => "APPROX_PERCENTILE_CONT",
            ApproxPercentileContWithWeight => "APPROX_PERCENTILE_CONT_WITH_WEIGHT",
            ApproxMedian => "APPROX_MEDIAN",
            Grouping => "GROUPING",
            BitAnd => "BIT_AND",
            BitOr => "BIT_OR",
            BitXor => "BIT_XOR",
            BoolAnd => "BOOL_AND",
            BoolOr => "BOOL_OR",
            StringAgg => "STRING_AGG",
        }
    }
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for AggregateFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<AggregateFunction> {
        Ok(match name {
            // general
            "avg" => AggregateFunction::Avg,
            "bit_and" => AggregateFunction::BitAnd,
            "bit_or" => AggregateFunction::BitOr,
            "bit_xor" => AggregateFunction::BitXor,
            "bool_and" => AggregateFunction::BoolAnd,
            "bool_or" => AggregateFunction::BoolOr,
            "count" => AggregateFunction::Count,
            "max" => AggregateFunction::Max,
            "mean" => AggregateFunction::Avg,
            "min" => AggregateFunction::Min,
            "sum" => AggregateFunction::Sum,
            "array_agg" => AggregateFunction::ArrayAgg,
            "nth_value" => AggregateFunction::NthValue,
            "string_agg" => AggregateFunction::StringAgg,
            // statistical
            "corr" => AggregateFunction::Correlation,
            "stddev" => AggregateFunction::Stddev,
            "stddev_pop" => AggregateFunction::StddevPop,
            "stddev_samp" => AggregateFunction::Stddev,
            "var" => AggregateFunction::Variance,
            "var_pop" => AggregateFunction::VariancePop,
            "var_samp" => AggregateFunction::Variance,
            "regr_slope" => AggregateFunction::RegrSlope,
            "regr_intercept" => AggregateFunction::RegrIntercept,
            "regr_count" => AggregateFunction::RegrCount,
            "regr_r2" => AggregateFunction::RegrR2,
            "regr_avgx" => AggregateFunction::RegrAvgx,
            "regr_avgy" => AggregateFunction::RegrAvgy,
            "regr_sxx" => AggregateFunction::RegrSXX,
            "regr_syy" => AggregateFunction::RegrSYY,
            "regr_sxy" => AggregateFunction::RegrSXY,
            // approximate
            "approx_distinct" => AggregateFunction::ApproxDistinct,
            "approx_median" => AggregateFunction::ApproxMedian,
            "approx_percentile_cont" => AggregateFunction::ApproxPercentileCont,
            "approx_percentile_cont_with_weight" => {
                AggregateFunction::ApproxPercentileContWithWeight
            }
            // other
            "grouping" => AggregateFunction::Grouping,
            _ => {
                return plan_err!("There is no built-in function named {name}");
            }
        })
    }
}

impl AggregateFunction {
    /// Returns the datatype of the aggregate function given its argument types
    ///
    /// This is used to get the returned data type for aggregate expr.
    pub fn return_type(&self, input_expr_types: &[DataType]) -> Result<DataType> {
        // Note that this function *must* return the same type that the respective physical expression returns
        // or the execution panics.

        let coerced_data_types = coerce_types(self, input_expr_types, &self.signature())
            // original errors are all related to wrong function signature
            // aggregate them for better error message
            .map_err(|_| {
                plan_datafusion_err!(
                    "{}",
                    utils::generate_signature_error_msg(
                        &format!("{self}"),
                        self.signature(),
                        input_expr_types,
                    )
                )
            })?;

        match self {
            AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
                Ok(DataType::Int64)
            }
            AggregateFunction::Max | AggregateFunction::Min => {
                // For min and max agg function, the returned type is same as input type.
                // The coerced_data_types is same with input_types.
                Ok(coerced_data_types[0].clone())
            }
            AggregateFunction::Sum => sum_return_type(&coerced_data_types[0]),
            AggregateFunction::BitAnd
            | AggregateFunction::BitOr
            | AggregateFunction::BitXor => Ok(coerced_data_types[0].clone()),
            AggregateFunction::BoolAnd | AggregateFunction::BoolOr => {
                Ok(DataType::Boolean)
            }
            AggregateFunction::Variance => variance_return_type(&coerced_data_types[0]),
            AggregateFunction::VariancePop => {
                variance_return_type(&coerced_data_types[0])
            }
            AggregateFunction::Correlation => {
                correlation_return_type(&coerced_data_types[0])
            }
            AggregateFunction::Stddev => stddev_return_type(&coerced_data_types[0]),
            AggregateFunction::StddevPop => stddev_return_type(&coerced_data_types[0]),
            AggregateFunction::RegrSlope
            | AggregateFunction::RegrIntercept
            | AggregateFunction::RegrCount
            | AggregateFunction::RegrR2
            | AggregateFunction::RegrAvgx
            | AggregateFunction::RegrAvgy
            | AggregateFunction::RegrSXX
            | AggregateFunction::RegrSYY
            | AggregateFunction::RegrSXY => Ok(DataType::Float64),
            AggregateFunction::Avg => avg_return_type(&coerced_data_types[0]),
            AggregateFunction::ArrayAgg => Ok(DataType::List(Arc::new(Field::new(
                "item",
                coerced_data_types[0].clone(),
                true,
            )))),
            AggregateFunction::ApproxPercentileCont => Ok(coerced_data_types[0].clone()),
            AggregateFunction::ApproxPercentileContWithWeight => {
                Ok(coerced_data_types[0].clone())
            }
            AggregateFunction::ApproxMedian => Ok(coerced_data_types[0].clone()),
            AggregateFunction::Grouping => Ok(DataType::Int32),
            AggregateFunction::NthValue => Ok(coerced_data_types[0].clone()),
            AggregateFunction::StringAgg => Ok(DataType::LargeUtf8),
        }
    }
}

/// Returns the internal sum datatype of the avg aggregate function.
pub fn sum_type_of_avg(input_expr_types: &[DataType]) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.
    let fun = AggregateFunction::Avg;
    let coerced_data_types = crate::type_coercion::aggregates::coerce_types(
        &fun,
        input_expr_types,
        &fun.signature(),
    )?;
    avg_sum_type(&coerced_data_types[0])
}

impl AggregateFunction {
    /// the signatures supported by the function `fun`.
    pub fn signature(&self) -> Signature {
        // note: the physical expression must accept the type returned by this function or the execution panics.
        match self {
            AggregateFunction::Count => Signature::variadic_any(Volatility::Immutable),
            AggregateFunction::ApproxDistinct
            | AggregateFunction::Grouping
            | AggregateFunction::ArrayAgg => Signature::any(1, Volatility::Immutable),
            AggregateFunction::Min | AggregateFunction::Max => {
                let valid = STRINGS
                    .iter()
                    .chain(NUMERICS.iter())
                    .chain(TIMESTAMPS.iter())
                    .chain(DATES.iter())
                    .chain(TIMES.iter())
                    .chain(BINARYS.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                Signature::uniform(1, valid, Volatility::Immutable)
            }
            AggregateFunction::BitAnd
            | AggregateFunction::BitOr
            | AggregateFunction::BitXor => {
                Signature::uniform_interger(1, Volatility::Immutable)
            }
            AggregateFunction::BoolAnd | AggregateFunction::BoolOr => {
                Signature::exact(vec![DataType::Boolean], Volatility::Immutable)
            }
            AggregateFunction::Avg
            | AggregateFunction::Sum
            | AggregateFunction::Variance
            | AggregateFunction::VariancePop
            | AggregateFunction::Stddev
            | AggregateFunction::StddevPop
            | AggregateFunction::ApproxMedian => {
                Signature::uniform_numeric(1, Volatility::Immutable)
            }
            AggregateFunction::NthValue => Signature::any(2, Volatility::Immutable),
            AggregateFunction::Correlation
            | AggregateFunction::RegrSlope
            | AggregateFunction::RegrIntercept
            | AggregateFunction::RegrCount
            | AggregateFunction::RegrR2
            | AggregateFunction::RegrAvgx
            | AggregateFunction::RegrAvgy
            | AggregateFunction::RegrSXX
            | AggregateFunction::RegrSYY
            | AggregateFunction::RegrSXY => {
                Signature::uniform_numeric(2, Volatility::Immutable)
            }
            AggregateFunction::ApproxPercentileCont => {
                let mut variants =
                    Vec::with_capacity(NUMERICS.len() * (INTEGERS.len() + 1));
                // Accept any numeric value paired with a float64 percentile
                for num in NUMERICS {
                    variants
                        .push(TypeSignature::Exact(vec![num.clone(), DataType::Float64]));
                    // Additionally accept an integer number of centroids for T-Digest
                    for int in INTEGERS {
                        variants.push(TypeSignature::Exact(vec![
                            num.clone(),
                            DataType::Float64,
                            int.clone(),
                        ]))
                    }
                }

                Signature::one_of(variants, Volatility::Immutable)
            }
            AggregateFunction::ApproxPercentileContWithWeight => Signature::one_of(
                // Accept any numeric value paired with a float64 percentile
                NUMERICS
                    .iter()
                    .map(|t| {
                        TypeSignature::Exact(vec![
                            t.clone(),
                            t.clone(),
                            DataType::Float64,
                        ])
                    })
                    .collect(),
                Volatility::Immutable,
            ),
            AggregateFunction::StringAgg => {
                Signature::uniform_string(2, Volatility::Immutable)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use strum::IntoEnumIterator;

    #[test]
    // Test for AggregateFuncion's Display and from_str() implementations.
    // For each variant in AggregateFuncion, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same. This assertion is also necessary for
    // function suggestion. See https://github.com/apache/datafusion/issues/8082
    fn test_display_and_from_str() {
        for func_original in AggregateFunction::iter() {
            let func_name = func_original.to_string();
            let func_from_str =
                AggregateFunction::from_str(func_name.to_lowercase().as_str()).unwrap();
            assert_eq!(func_from_str, func_original);
        }
    }
}
