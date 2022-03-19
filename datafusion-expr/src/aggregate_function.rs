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
    /// ApproxMedian
    ApproxMedian,
    /// BitMap count distinct function
    BitMapCountDistinct,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // uppercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_uppercase())
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
            "sum" => AggregateFunction::Sum,
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
            "approx_median" => AggregateFunction::ApproxMedian,
            "bitmap_distinct" => AggregateFunction::BitMapCountDistinct,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {}",
                    name
                )));
            }
        })
    }
}
