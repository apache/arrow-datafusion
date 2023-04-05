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

use crate::{type_coercion::aggregates::*, Signature, TypeSignature, Volatility};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result};
use std::{fmt, str::FromStr};

/// Enum of all built-in aggregate functions
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
    /// median
    Median,
    /// Approximate aggregate function
    ApproxDistinct,
    /// array_agg
    ArrayAgg,
    /// Variance (Sample)
    Variance,
    /// Variance (Population)
    VariancePop,
    /// Standard Deviation (Sample)
    Stddev,
    /// Standard Deviation (Population)
    StddevPop,
    /// Covariance (Sample)
    Covariance,
    /// Covariance (Population)
    CovariancePop,
    /// Correlation
    Correlation,
    /// Approximate continuous percentile function
    ApproxPercentileCont,
    /// Approximate continuous percentile function with weight
    ApproxPercentileContWithWeight,
    /// ApproxMedian
    ApproxMedian,
    /// Grouping
    Grouping,
    /// GroupingID
    GroupingId,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // uppercase of the debug.
        match self {
            AggregateFunction::GroupingId => {
                write!(f, "GROUPING_ID")
            }
            _ => write!(f, "{}", format!("{self:?}").to_uppercase()),
        }
    }
}

impl FromStr for AggregateFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<AggregateFunction> {
        Ok(match name {
            "min" => AggregateFunction::Min,
            "max" => AggregateFunction::Max,
            "count" => AggregateFunction::Count,
            "avg" => AggregateFunction::Avg,
            "mean" => AggregateFunction::Avg,
            "sum" => AggregateFunction::Sum,
            "median" => AggregateFunction::Median,
            "approx_distinct" => AggregateFunction::ApproxDistinct,
            "array_agg" => AggregateFunction::ArrayAgg,
            "var" => AggregateFunction::Variance,
            "var_samp" => AggregateFunction::Variance,
            "var_pop" => AggregateFunction::VariancePop,
            "stddev" => AggregateFunction::Stddev,
            "stddev_samp" => AggregateFunction::Stddev,
            "stddev_pop" => AggregateFunction::StddevPop,
            "covar" => AggregateFunction::Covariance,
            "covar_samp" => AggregateFunction::Covariance,
            "covar_pop" => AggregateFunction::CovariancePop,
            "corr" => AggregateFunction::Correlation,
            "approx_percentile_cont" => AggregateFunction::ApproxPercentileCont,
            "approx_percentile_cont_with_weight" => {
                AggregateFunction::ApproxPercentileContWithWeight
            }
            "approx_median" => AggregateFunction::ApproxMedian,
            "grouping" => AggregateFunction::Grouping,
            "grouping_id" => AggregateFunction::GroupingId,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {name}"
                )));
            }
        })
    }
}

/// Returns the datatype of the aggregate function.
/// This is used to get the returned data type for aggregate expr.
pub fn return_type(
    fun: &AggregateFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    let coerced_data_types = crate::type_coercion::aggregates::coerce_types(
        fun,
        input_expr_types,
        &signature(fun),
    )?;

    match fun {
        AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
            Ok(DataType::Int64)
        }
        AggregateFunction::Max | AggregateFunction::Min => {
            // For min and max agg function, the returned type is same as input type.
            // The coerced_data_types is same with input_types.
            Ok(coerced_data_types[0].clone())
        }
        AggregateFunction::Sum => sum_return_type(&coerced_data_types[0]),
        AggregateFunction::Variance => variance_return_type(&coerced_data_types[0]),
        AggregateFunction::VariancePop => variance_return_type(&coerced_data_types[0]),
        AggregateFunction::Covariance => covariance_return_type(&coerced_data_types[0]),
        AggregateFunction::CovariancePop => {
            covariance_return_type(&coerced_data_types[0])
        }
        AggregateFunction::Correlation => correlation_return_type(&coerced_data_types[0]),
        AggregateFunction::Stddev => stddev_return_type(&coerced_data_types[0]),
        AggregateFunction::StddevPop => stddev_return_type(&coerced_data_types[0]),
        AggregateFunction::Avg => avg_return_type(&coerced_data_types[0]),
        AggregateFunction::ArrayAgg => Ok(DataType::List(Box::new(Field::new(
            "item",
            coerced_data_types[0].clone(),
            true,
        )))),
        AggregateFunction::ApproxPercentileCont => Ok(coerced_data_types[0].clone()),
        AggregateFunction::ApproxPercentileContWithWeight => {
            Ok(coerced_data_types[0].clone())
        }
        AggregateFunction::ApproxMedian | AggregateFunction::Median => {
            Ok(coerced_data_types[0].clone())
        }
        AggregateFunction::Grouping => Ok(DataType::UInt8),
        AggregateFunction::GroupingId => Ok(DataType::UInt32),
    }
}

/// the signatures supported by the function `fun`.
pub fn signature(fun: &AggregateFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        AggregateFunction::Count
        | AggregateFunction::ApproxDistinct
        | AggregateFunction::Grouping
        | AggregateFunction::ArrayAgg => Signature::any(1, Volatility::Immutable),
        AggregateFunction::Min | AggregateFunction::Max => {
            let valid = STRINGS
                .iter()
                .chain(NUMERICS.iter())
                .chain(TIMESTAMPS.iter())
                .chain(DATES.iter())
                .chain(TIMES.iter())
                .cloned()
                .collect::<Vec<_>>();
            Signature::uniform(1, valid, Volatility::Immutable)
        }
        AggregateFunction::Avg
        | AggregateFunction::Sum
        | AggregateFunction::Variance
        | AggregateFunction::VariancePop
        | AggregateFunction::Stddev
        | AggregateFunction::StddevPop
        | AggregateFunction::Median
        | AggregateFunction::ApproxMedian => {
            Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable)
        }
        AggregateFunction::Covariance | AggregateFunction::CovariancePop => {
            Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable)
        }
        AggregateFunction::Correlation => {
            Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable)
        }
        AggregateFunction::ApproxPercentileCont => {
            // Accept any numeric value paired with a float64 percentile
            let with_tdigest_size = NUMERICS.iter().map(|t| {
                TypeSignature::Exact(vec![t.clone(), DataType::Float64, t.clone()])
            });
            Signature::one_of(
                NUMERICS
                    .iter()
                    .map(|t| TypeSignature::Exact(vec![t.clone(), DataType::Float64]))
                    .chain(with_tdigest_size)
                    .collect(),
                Volatility::Immutable,
            )
        }
        AggregateFunction::ApproxPercentileContWithWeight => Signature::one_of(
            // Accept any numeric value paired with a float64 percentile
            NUMERICS
                .iter()
                .map(|t| {
                    TypeSignature::Exact(vec![t.clone(), t.clone(), DataType::Float64])
                })
                .collect(),
            Volatility::Immutable,
        ),
        AggregateFunction::GroupingId => Signature {
            type_signature: TypeSignature::Arbitrary,
            volatility: Volatility::Immutable,
        },
    }
}
