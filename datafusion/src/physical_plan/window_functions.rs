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

//! Window functions provide the ability to perform calculations across
//! sets of rows that are related to the current query row.
//!
//! see also https://www.postgresql.org/docs/current/functions-window.html

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    aggregates, aggregates::AggregateFunction, functions::Signature,
    type_coercion::data_types,
};
use arrow::datatypes::DataType;
use std::{fmt, str::FromStr};

/// WindowFunction
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFunction {
    /// window function that leverages an aggregate function
    AggregateFunction(AggregateFunction),
    /// window function that leverages a built-in window function
    BuiltInWindowFunction(BuiltInWindowFunction),
}

impl FromStr for WindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<WindowFunction> {
        if let Ok(aggregate) = AggregateFunction::from_str(name) {
            Ok(WindowFunction::AggregateFunction(aggregate))
        } else if let Ok(built_in_function) = BuiltInWindowFunction::from_str(name) {
            Ok(WindowFunction::BuiltInWindowFunction(built_in_function))
        } else {
            Err(DataFusionError::Plan(format!(
                "There is no built-in function named {}",
                name
            )))
        }
    }
}

impl fmt::Display for BuiltInWindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // uppercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}

impl fmt::Display for WindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFunction::AggregateFunction(fun) => fun.fmt(f),
            WindowFunction::BuiltInWindowFunction(fun) => fun.fmt(f),
        }
    }
}

/// An aggregate function that is part of a built-in window function
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltInWindowFunction {
    /// number of the current row within its partition, counting from 1
    RowNumber,
    /// rank of the current row with gaps; same as row_number of its first peer
    Rank,
    /// ank of the current row without gaps; this function counts peer groups
    DenseRank,
    /// relative rank of the current row: (rank - 1) / (total rows - 1)
    PercentRank,
    /// relative rank of the current row: (number of rows preceding or peer with current row) / (total rows)
    CumeDist,
    /// integer ranging from 1 to the argument value, dividing the partition as equally as possible
    Ntile,
    /// returns value evaluated at the row that is offset rows before the current row within the partition;
    /// if there is no such row, instead return default (which must be of the same type as value).
    /// Both offset and default are evaluated with respect to the current row.
    /// If omitted, offset defaults to 1 and default to null
    Lag,
    /// returns value evaluated at the row that is offset rows after the current row within the partition;
    /// if there is no such row, instead return default (which must be of the same type as value).
    /// Both offset and default are evaluated with respect to the current row.
    /// If omitted, offset defaults to 1 and default to null
    Lead,
    /// returns value evaluated at the row that is the first row of the window frame
    FirstValue,
    /// returns value evaluated at the row that is the last row of the window frame
    LastValue,
    /// returns value evaluated at the row that is the nth row of the window frame (counting from 1); null if no such row
    NthValue,
}

impl FromStr for BuiltInWindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltInWindowFunction> {
        Ok(match name.to_lowercase().as_str() {
            "row_number" => BuiltInWindowFunction::RowNumber,
            "rank" => BuiltInWindowFunction::Rank,
            "dense_rank" => BuiltInWindowFunction::DenseRank,
            "percent_rank" => BuiltInWindowFunction::PercentRank,
            "cume_dist" => BuiltInWindowFunction::CumeDist,
            "ntile" => BuiltInWindowFunction::Ntile,
            "lag" => BuiltInWindowFunction::Lag,
            "lead" => BuiltInWindowFunction::Lead,
            "first_value" => BuiltInWindowFunction::FirstValue,
            "last_value" => BuiltInWindowFunction::LastValue,
            "nth_value" => BuiltInWindowFunction::NthValue,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in window function named {}",
                    name
                )))
            }
        })
    }
}

/// Returns the datatype of the window function
pub fn return_type(fun: &WindowFunction, arg_types: &[DataType]) -> Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    // verify that this is a valid set of data types for this function
    data_types(arg_types, &signature(fun))?;

    match fun {
        WindowFunction::AggregateFunction(fun) => aggregates::return_type(fun, arg_types),
        WindowFunction::BuiltInWindowFunction(fun) => match fun {
            BuiltInWindowFunction::RowNumber
            | BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank => Ok(DataType::UInt64),
            BuiltInWindowFunction::PercentRank | BuiltInWindowFunction::CumeDist => {
                Ok(DataType::Float64)
            }
            BuiltInWindowFunction::Ntile => Ok(DataType::UInt32),
            BuiltInWindowFunction::Lag
            | BuiltInWindowFunction::Lead
            | BuiltInWindowFunction::FirstValue
            | BuiltInWindowFunction::LastValue
            | BuiltInWindowFunction::NthValue => Ok(arg_types[0].clone()),
        },
    }
}

/// the signatures supported by the function `fun`.
fn signature(fun: &WindowFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        WindowFunction::AggregateFunction(fun) => aggregates::signature(fun),
        WindowFunction::BuiltInWindowFunction(fun) => match fun {
            BuiltInWindowFunction::RowNumber
            | BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank
            | BuiltInWindowFunction::PercentRank
            | BuiltInWindowFunction::CumeDist => Signature::Any(0),
            BuiltInWindowFunction::Lag
            | BuiltInWindowFunction::Lead
            | BuiltInWindowFunction::FirstValue
            | BuiltInWindowFunction::LastValue => Signature::Any(1),
            BuiltInWindowFunction::Ntile => Signature::Exact(vec![DataType::UInt64]),
            BuiltInWindowFunction::NthValue => Signature::Any(2),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_window_function_from_str() -> Result<()> {
        assert_eq!(
            WindowFunction::from_str("max")?,
            WindowFunction::AggregateFunction(AggregateFunction::Max)
        );
        assert_eq!(
            WindowFunction::from_str("min")?,
            WindowFunction::AggregateFunction(AggregateFunction::Min)
        );
        assert_eq!(
            WindowFunction::from_str("avg")?,
            WindowFunction::AggregateFunction(AggregateFunction::Avg)
        );
        assert_eq!(
            WindowFunction::from_str("cum_dist")?,
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::CumeDist)
        );
        Ok(())
    }
}
