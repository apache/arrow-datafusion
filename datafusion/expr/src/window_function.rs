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
//! see also <https://www.postgresql.org/docs/current/functions-window.html>

use crate::aggregate_function::AggregateFunction;
use crate::function::ConstantArg;
use crate::type_coercion::functions::data_types;
use crate::utils;
use crate::{AggregateUDF, Signature, TypeSignature, Volatility, WindowUDF};
use arrow::datatypes::DataType;
use datafusion_common::{plan_datafusion_err, plan_err, DataFusionError, Result};
use std::sync::Arc;
use std::{fmt, str::FromStr};
use strum_macros::EnumIter;

/// WindowFunction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFunction {
    /// A built in aggregate function that leverages an aggregate function
    AggregateFunction(AggregateFunction),
    /// A a built-in window function
    BuiltInWindowFunction(BuiltInWindowFunction),
    /// A user defined aggregate function
    AggregateUDF(Arc<AggregateUDF>),
    /// A user defined aggregate function
    WindowUDF(Arc<WindowUDF>),
}

/// Find DataFusion's built-in window function by name.
pub fn find_df_window_func(name: &str) -> Option<WindowFunction> {
    let name = name.to_lowercase();
    // Code paths for window functions leveraging ordinary aggregators and
    // built-in window functions are quite different, and the same function
    // may have different implementations for these cases. If the sought
    // function is not found among built-in window functions, we search for
    // it among aggregate functions.
    if let Ok(built_in_function) = BuiltInWindowFunction::from_str(name.as_str()) {
        Some(WindowFunction::BuiltInWindowFunction(built_in_function))
    } else if let Ok(aggregate) = AggregateFunction::from_str(name.as_str()) {
        Some(WindowFunction::AggregateFunction(aggregate))
    } else {
        None
    }
}

impl fmt::Display for BuiltInWindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl fmt::Display for WindowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFunction::AggregateFunction(fun) => fun.fmt(f),
            WindowFunction::BuiltInWindowFunction(fun) => fun.fmt(f),
            WindowFunction::AggregateUDF(fun) => std::fmt::Debug::fmt(fun, f),
            WindowFunction::WindowUDF(fun) => fun.fmt(f),
        }
    }
}

/// A [window function] built in to DataFusion
///
/// [window function]: https://en.wikipedia.org/wiki/Window_function_(SQL)
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumIter)]
pub enum BuiltInWindowFunction {
    /// number of the current row within its partition, counting from 1
    RowNumber,
    /// rank of the current row with gaps; same as row_number of its first peer
    Rank,
    /// rank of the current row without gaps; this function counts peer groups
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

impl BuiltInWindowFunction {
    fn name(&self) -> &str {
        use BuiltInWindowFunction::*;
        match self {
            RowNumber => "ROW_NUMBER",
            Rank => "RANK",
            DenseRank => "DENSE_RANK",
            PercentRank => "PERCENT_RANK",
            CumeDist => "CUME_DIST",
            Ntile => "NTILE",
            Lag => "LAG",
            Lead => "LEAD",
            FirstValue => "FIRST_VALUE",
            LastValue => "LAST_VALUE",
            NthValue => "NTH_VALUE",
        }
    }
}

impl FromStr for BuiltInWindowFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltInWindowFunction> {
        Ok(match name.to_uppercase().as_str() {
            "ROW_NUMBER" => BuiltInWindowFunction::RowNumber,
            "RANK" => BuiltInWindowFunction::Rank,
            "DENSE_RANK" => BuiltInWindowFunction::DenseRank,
            "PERCENT_RANK" => BuiltInWindowFunction::PercentRank,
            "CUME_DIST" => BuiltInWindowFunction::CumeDist,
            "NTILE" => BuiltInWindowFunction::Ntile,
            "LAG" => BuiltInWindowFunction::Lag,
            "LEAD" => BuiltInWindowFunction::Lead,
            "FIRST_VALUE" => BuiltInWindowFunction::FirstValue,
            "LAST_VALUE" => BuiltInWindowFunction::LastValue,
            "NTH_VALUE" => BuiltInWindowFunction::NthValue,
            _ => return plan_err!("There is no built-in window function named {name}"),
        })
    }
}

/// Returns the datatype of the window function
#[deprecated(
    since = "27.0.0",
    note = "please use `WindowFunction::return_type` instead"
)]
pub fn return_type(
    fun: &WindowFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    fun.return_type(input_expr_types, &[])
}

impl WindowFunction {
    /// Returns the datatype of the window function
    pub fn return_type(
        &self,
        input_expr_types: &[DataType],
        constant_args: &[ConstantArg],
    ) -> Result<DataType> {
        match self {
            WindowFunction::AggregateFunction(fun) => fun.return_type(input_expr_types),
            WindowFunction::BuiltInWindowFunction(fun) => {
                fun.return_type(input_expr_types)
            }
            WindowFunction::AggregateUDF(fun) => Ok(fun
                .return_type
                .infer(input_expr_types, constant_args)?
                .as_ref()
                .clone()),
            WindowFunction::WindowUDF(fun) => Ok(fun
                .return_type
                .infer(input_expr_types, constant_args)?
                .as_ref()
                .clone()),
        }
    }
}

/// Returns the datatype of the built-in window function
impl BuiltInWindowFunction {
    pub fn return_type(&self, input_expr_types: &[DataType]) -> Result<DataType> {
        // Note that this function *must* return the same type that the respective physical expression returns
        // or the execution panics.

        // verify that this is a valid set of data types for this function
        data_types(input_expr_types, &self.signature())
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
            | BuiltInWindowFunction::NthValue => Ok(input_expr_types[0].clone()),
        }
    }
}

/// the signatures supported by the function `fun`.
#[deprecated(
    since = "27.0.0",
    note = "please use `WindowFunction::signature` instead"
)]
pub fn signature(fun: &WindowFunction) -> Signature {
    fun.signature()
}

impl WindowFunction {
    /// the signatures supported by the function `fun`.
    pub fn signature(&self) -> Signature {
        match self {
            WindowFunction::AggregateFunction(fun) => fun.signature(),
            WindowFunction::BuiltInWindowFunction(fun) => fun.signature(),
            WindowFunction::AggregateUDF(fun) => fun.signature.clone(),
            WindowFunction::WindowUDF(fun) => fun.signature.clone(),
        }
    }
}

/// the signatures supported by the built-in window function `fun`.
#[deprecated(
    since = "27.0.0",
    note = "please use `BuiltInWindowFunction::signature` instead"
)]
pub fn signature_for_built_in(fun: &BuiltInWindowFunction) -> Signature {
    fun.signature()
}

impl BuiltInWindowFunction {
    /// the signatures supported by the built-in window function `fun`.
    pub fn signature(&self) -> Signature {
        // note: the physical expression must accept the type returned by this function or the execution panics.
        match self {
            BuiltInWindowFunction::RowNumber
            | BuiltInWindowFunction::Rank
            | BuiltInWindowFunction::DenseRank
            | BuiltInWindowFunction::PercentRank
            | BuiltInWindowFunction::CumeDist => Signature::any(0, Volatility::Immutable),
            BuiltInWindowFunction::Lag | BuiltInWindowFunction::Lead => {
                Signature::one_of(
                    vec![
                        TypeSignature::Any(1),
                        TypeSignature::Any(2),
                        TypeSignature::Any(3),
                    ],
                    Volatility::Immutable,
                )
            }
            BuiltInWindowFunction::FirstValue | BuiltInWindowFunction::LastValue => {
                Signature::any(1, Volatility::Immutable)
            }
            BuiltInWindowFunction::Ntile => Signature::any(1, Volatility::Immutable),
            BuiltInWindowFunction::NthValue => Signature::any(2, Volatility::Immutable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_count_return_type() -> Result<()> {
        let fun = find_df_window_func("count").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[])?;
        assert_eq!(DataType::Int64, observed);

        let observed = fun.return_type(&[DataType::UInt64], &[])?;
        assert_eq!(DataType::Int64, observed);

        Ok(())
    }

    #[test]
    fn test_first_value_return_type() -> Result<()> {
        let fun = find_df_window_func("first_value").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::UInt64], &[])?;
        assert_eq!(DataType::UInt64, observed);

        Ok(())
    }

    #[test]
    fn test_last_value_return_type() -> Result<()> {
        let fun = find_df_window_func("last_value").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64], &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_lead_return_type() -> Result<()> {
        let fun = find_df_window_func("lead").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64], &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_lag_return_type() -> Result<()> {
        let fun = find_df_window_func("lag").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64], &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_nth_value_return_type() -> Result<()> {
        let fun = find_df_window_func("nth_value").unwrap();
        let observed = fun.return_type(&[DataType::Utf8, DataType::UInt64], &[])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64, DataType::UInt64], &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_percent_rank_return_type() -> Result<()> {
        let fun = find_df_window_func("percent_rank").unwrap();
        let observed = fun.return_type(&[], &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_cume_dist_return_type() -> Result<()> {
        let fun = find_df_window_func("cume_dist").unwrap();
        let observed = fun.return_type(&[], &[])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_window_function_case_insensitive() -> Result<()> {
        let names = vec![
            "row_number",
            "rank",
            "dense_rank",
            "percent_rank",
            "cume_dist",
            "ntile",
            "lag",
            "lead",
            "first_value",
            "last_value",
            "nth_value",
            "min",
            "max",
            "count",
            "avg",
            "sum",
        ];
        for name in names {
            let fun = find_df_window_func(name).unwrap();
            let fun2 = find_df_window_func(name.to_uppercase().as_str()).unwrap();
            assert_eq!(fun, fun2);
            assert_eq!(fun.to_string(), name.to_uppercase());
        }
        Ok(())
    }

    #[test]
    fn test_find_df_window_function() {
        assert_eq!(
            find_df_window_func("max"),
            Some(WindowFunction::AggregateFunction(AggregateFunction::Max))
        );
        assert_eq!(
            find_df_window_func("min"),
            Some(WindowFunction::AggregateFunction(AggregateFunction::Min))
        );
        assert_eq!(
            find_df_window_func("avg"),
            Some(WindowFunction::AggregateFunction(AggregateFunction::Avg))
        );
        assert_eq!(
            find_df_window_func("cume_dist"),
            Some(WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::CumeDist
            ))
        );
        assert_eq!(
            find_df_window_func("first_value"),
            Some(WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::FirstValue
            ))
        );
        assert_eq!(
            find_df_window_func("LAST_value"),
            Some(WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::LastValue
            ))
        );
        assert_eq!(
            find_df_window_func("LAG"),
            Some(WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::Lag
            ))
        );
        assert_eq!(
            find_df_window_func("LEAD"),
            Some(WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::Lead
            ))
        );
        assert_eq!(find_df_window_func("not_exist"), None)
    }

    #[test]
    // Test for BuiltInWindowFunction's Display and from_str() implementations.
    // For each variant in BuiltInWindowFunction, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same. This assertion is also necessary for
    // function suggestion. See https://github.com/apache/arrow-datafusion/issues/8082
    fn test_display_and_from_str() {
        for func_original in BuiltInWindowFunction::iter() {
            let func_name = func_original.to_string();
            let func_from_str = BuiltInWindowFunction::from_str(&func_name).unwrap();
            assert_eq!(func_from_str, func_original);
        }
    }
}
