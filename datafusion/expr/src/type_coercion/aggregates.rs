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

use arrow::datatypes::{
    DataType, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use datafusion_common::{DataFusionError, Result};
use std::ops::Deref;

use crate::{AggregateFunction, Signature, TypeSignature};

use super::functions::can_coerce_from;

pub static STRINGS: &[DataType] = &[DataType::Utf8, DataType::LargeUtf8];

pub static NUMERICS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float32,
    DataType::Float64,
];

pub static TIMESTAMPS: &[DataType] = &[
    DataType::Timestamp(TimeUnit::Second, None),
    DataType::Timestamp(TimeUnit::Millisecond, None),
    DataType::Timestamp(TimeUnit::Microsecond, None),
    DataType::Timestamp(TimeUnit::Nanosecond, None),
];

pub static DATES: &[DataType] = &[DataType::Date32, DataType::Date64];

pub static TIMES: &[DataType] = &[
    DataType::Time32(TimeUnit::Second),
    DataType::Time32(TimeUnit::Millisecond),
    DataType::Time64(TimeUnit::Microsecond),
    DataType::Time64(TimeUnit::Nanosecond),
];

/// Returns the coerced data type for each `input_types`.
/// Different aggregate function with different input data type will get corresponding coerced data type.
pub fn coerce_types(
    agg_fun: &AggregateFunction,
    input_types: &[DataType],
    signature: &Signature,
) -> Result<Vec<DataType>> {
    // Validate input_types matches (at least one of) the func signature.
    check_arg_count(agg_fun, input_types, &signature.type_signature)?;

    match agg_fun {
        AggregateFunction::Count | AggregateFunction::ApproxDistinct => {
            Ok(input_types.to_vec())
        }
        AggregateFunction::ArrayAgg => Ok(input_types.to_vec()),
        AggregateFunction::Min | AggregateFunction::Max => {
            // min and max support the dictionary data type
            // unpack the dictionary to get the value
            get_min_max_result_type(input_types)
        }
        AggregateFunction::Sum => {
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            // smallint, int, bigint, real, double precision, decimal, or interval.
            if !is_sum_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Avg => {
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            // smallint, int, bigint, real, double precision, decimal, or interval
            if !is_avg_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Variance => {
            if !is_variance_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::VariancePop => {
            if !is_variance_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Covariance => {
            if !is_covariance_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::CovariancePop => {
            if !is_covariance_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Stddev => {
            if !is_stddev_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::StddevPop => {
            if !is_stddev_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Correlation => {
            if !is_correlation_support_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::ApproxPercentileCont => {
            if !is_approx_percentile_cont_supported_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            if input_types.len() == 3 && !is_integer_arg_type(&input_types[2]) {
                return Err(DataFusionError::Plan(format!(
                        "The percentile sample points count for {:?} must be integer, not {:?}.",
                        agg_fun, input_types[2]
                    )));
            }
            let mut result = input_types.to_vec();
            if can_coerce_from(&DataType::Float64, &input_types[1]) {
                result[1] = DataType::Float64;
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Could not coerce the percent argument for {:?} to Float64. Was  {:?}.",
                    agg_fun, input_types[1]
                )));
            }
            Ok(result)
        }
        AggregateFunction::ApproxPercentileContWithWeight => {
            if !is_approx_percentile_cont_supported_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            if !is_approx_percentile_cont_supported_arg_type(&input_types[1]) {
                return Err(DataFusionError::Plan(format!(
                    "The weight argument for {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[1]
                )));
            }
            if !matches!(input_types[2], DataType::Float64) {
                return Err(DataFusionError::Plan(format!(
                    "The percentile argument for {:?} must be Float64, not {:?}.",
                    agg_fun, input_types[2]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::ApproxMedian => {
            if !is_approx_percentile_cont_supported_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        AggregateFunction::Median => Ok(input_types.to_vec()),
        AggregateFunction::Grouping => Ok(vec![input_types[0].clone()]),
        AggregateFunction::GroupingId => Ok(input_types.to_vec()),
    }
}

/// Validate the length of `input_types` matches the `signature` for `agg_fun`.
///
/// This method DOES NOT validate the argument types - only that (at least one,
/// in the case of [`TypeSignature::OneOf`]) signature matches the desired
/// number of input types.
fn check_arg_count(
    agg_fun: &AggregateFunction,
    input_types: &[DataType],
    signature: &TypeSignature,
) -> Result<()> {
    match signature {
        TypeSignature::Uniform(agg_count, _) | TypeSignature::Any(agg_count) => {
            if input_types.len() != *agg_count {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} expects {:?} arguments, but {:?} were provided",
                    agg_fun,
                    agg_count,
                    input_types.len()
                )));
            }
        }
        TypeSignature::Exact(types) => {
            if types.len() != input_types.len() {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} expects {:?} arguments, but {:?} were provided",
                    agg_fun,
                    types.len(),
                    input_types.len()
                )));
            }
        }
        TypeSignature::OneOf(variants) => {
            let ok = variants
                .iter()
                .any(|v| check_arg_count(agg_fun, input_types, v).is_ok());
            if !ok {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not accept {:?} function arguments.",
                    agg_fun,
                    input_types.len()
                )));
            }
        }
        TypeSignature::Arbitrary => return Ok(()),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Aggregate functions do not support this {signature:?}"
            )));
        }
    }
    Ok(())
}

fn get_min_max_result_type(input_types: &[DataType]) -> Result<Vec<DataType>> {
    // make sure that the input types only has one element.
    assert_eq!(input_types.len(), 1);
    // min and max support the dictionary data type
    // unpack the dictionary to get the value
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        // TODO add checker for datatype which min and max supported
        // For example, the `Struct` and `Map` type are not supported in the MIN and MAX function
        _ => Ok(input_types.to_vec()),
    }
}

/// function return type of a sum
pub fn sum_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Ok(DataType::Int64)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(DataType::UInt64)
        }
        // In the https://www.postgresql.org/docs/current/functions-aggregate.html doc,
        // the result type of floating-point is FLOAT64 with the double precision.
        DataType::Float64 | DataType::Float32 => Ok(DataType::Float64),
        DataType::Decimal128(precision, scale) => {
            // in the spark, the result type is DECIMAL(min(38,precision+10), s)
            // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
            let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 10);
            Ok(DataType::Decimal128(new_precision, *scale))
        }
        other => Err(DataFusionError::Plan(format!(
            "SUM does not support type \"{other:?}\""
        ))),
    }
}

/// function return type of variance
pub fn variance_return_type(arg_type: &DataType) -> Result<DataType> {
    if NUMERICS.contains(arg_type) {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Plan(format!(
            "VAR does not support {arg_type:?}"
        )))
    }
}

/// function return type of covariance
pub fn covariance_return_type(arg_type: &DataType) -> Result<DataType> {
    if NUMERICS.contains(arg_type) {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Plan(format!(
            "COVAR does not support {arg_type:?}"
        )))
    }
}

/// function return type of correlation
pub fn correlation_return_type(arg_type: &DataType) -> Result<DataType> {
    if NUMERICS.contains(arg_type) {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Plan(format!(
            "CORR does not support {arg_type:?}"
        )))
    }
}

/// function return type of standard deviation
pub fn stddev_return_type(arg_type: &DataType) -> Result<DataType> {
    if NUMERICS.contains(arg_type) {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Plan(format!(
            "STDDEV does not support {arg_type:?}"
        )))
    }
}

/// function return type of an average
pub fn avg_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Decimal128(precision, scale) => {
            // in the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
            // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
            let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 4);
            let new_scale = DECIMAL128_MAX_SCALE.min(*scale + 4);
            Ok(DataType::Decimal128(new_precision, new_scale))
        }
        arg_type if NUMERICS.contains(arg_type) => Ok(DataType::Float64),
        other => Err(DataFusionError::Plan(format!(
            "AVG does not support {other:?}"
        ))),
    }
}

pub fn is_sum_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
        || matches!(arg_type, DataType::Decimal128(_, _))
    )
}

pub fn is_avg_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
            || matches!(arg_type, DataType::Decimal128(_, _))
    )
}

pub fn is_variance_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
    )
}

pub fn is_covariance_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
    )
}

pub fn is_stddev_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
    )
}

pub fn is_correlation_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
    )
}

pub fn is_integer_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
    )
}

/// Return `true` if `arg_type` is of a [`DataType`] that the
/// [`AggregateFunction::ApproxPercentileCont`] aggregation can operate on.
pub fn is_approx_percentile_cont_supported_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        arg_type if NUMERICS.contains(arg_type)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_function;
    use arrow::datatypes::DataType;

    #[test]
    fn test_aggregate_coerce_types() {
        // test input args with error number input types
        let fun = AggregateFunction::Min;
        let input_types = vec![DataType::Int64, DataType::Int32];
        let signature = aggregate_function::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!("Error during planning: The function Min expects 1 arguments, but 2 were provided", result.unwrap_err().to_string());

        // test input args is invalid data type for sum or avg
        let fun = AggregateFunction::Sum;
        let input_types = vec![DataType::Utf8];
        let signature = aggregate_function::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!(
            "Error during planning: The function Sum does not support inputs of type Utf8.",
            result.unwrap_err().to_string()
        );
        let fun = AggregateFunction::Avg;
        let signature = aggregate_function::signature(&fun);
        let result = coerce_types(&fun, &input_types, &signature);
        assert_eq!(
            "Error during planning: The function Avg does not support inputs of type Utf8.",
            result.unwrap_err().to_string()
        );

        // test count, array_agg, approx_distinct, min, max.
        // the coerced types is same with input types
        let funs = vec![
            AggregateFunction::Count,
            AggregateFunction::ArrayAgg,
            AggregateFunction::ApproxDistinct,
            AggregateFunction::Min,
            AggregateFunction::Max,
        ];
        let input_types = vec![
            vec![DataType::Int32],
            vec![DataType::Decimal128(10, 2)],
            vec![DataType::Utf8],
        ];
        for fun in funs {
            for input_type in &input_types {
                let signature = aggregate_function::signature(&fun);
                let result = coerce_types(&fun, input_type, &signature);
                assert_eq!(*input_type, result.unwrap());
            }
        }
        // test sum, avg
        let funs = vec![AggregateFunction::Sum, AggregateFunction::Avg];
        let input_types = vec![
            vec![DataType::Int32],
            vec![DataType::Float32],
            vec![DataType::Decimal128(20, 3)],
        ];
        for fun in funs {
            for input_type in &input_types {
                let signature = aggregate_function::signature(&fun);
                let result = coerce_types(&fun, input_type, &signature);
                assert_eq!(*input_type, result.unwrap());
            }
        }

        // ApproxPercentileCont input types
        let input_types = vec![
            vec![DataType::Int8, DataType::Float64],
            vec![DataType::Int16, DataType::Float64],
            vec![DataType::Int32, DataType::Float64],
            vec![DataType::Int64, DataType::Float64],
            vec![DataType::UInt8, DataType::Float64],
            vec![DataType::UInt16, DataType::Float64],
            vec![DataType::UInt32, DataType::Float64],
            vec![DataType::UInt64, DataType::Float64],
            vec![DataType::Float32, DataType::Float64],
            vec![DataType::Float64, DataType::Float64],
        ];
        for input_type in &input_types {
            let signature =
                aggregate_function::signature(&AggregateFunction::ApproxPercentileCont);
            let result = coerce_types(
                &AggregateFunction::ApproxPercentileCont,
                input_type,
                &signature,
            );
            assert_eq!(*input_type, result.unwrap());
        }
    }

    #[test]
    fn test_avg_return_data_type() -> Result<()> {
        let data_type = DataType::Decimal128(10, 5);
        let result_type = avg_return_type(&data_type)?;
        assert_eq!(DataType::Decimal128(14, 9), result_type);

        let data_type = DataType::Decimal128(36, 10);
        let result_type = avg_return_type(&data_type)?;
        assert_eq!(DataType::Decimal128(38, 14), result_type);
        Ok(())
    }

    #[test]
    fn test_variance_return_data_type() -> Result<()> {
        let data_type = DataType::Float64;
        let result_type = variance_return_type(&data_type)?;
        assert_eq!(DataType::Float64, result_type);

        let data_type = DataType::Decimal128(36, 10);
        assert!(variance_return_type(&data_type).is_err());
        Ok(())
    }

    #[test]
    fn test_sum_return_data_type() -> Result<()> {
        let data_type = DataType::Decimal128(10, 5);
        let result_type = sum_return_type(&data_type)?;
        assert_eq!(DataType::Decimal128(20, 5), result_type);

        let data_type = DataType::Decimal128(36, 10);
        let result_type = sum_return_type(&data_type)?;
        assert_eq!(DataType::Decimal128(38, 10), result_type);
        Ok(())
    }

    #[test]
    fn test_stddev_return_data_type() -> Result<()> {
        let data_type = DataType::Float64;
        let result_type = stddev_return_type(&data_type)?;
        assert_eq!(DataType::Float64, result_type);

        let data_type = DataType::Decimal128(36, 10);
        assert!(stddev_return_type(&data_type).is_err());
        Ok(())
    }

    #[test]
    fn test_covariance_return_data_type() -> Result<()> {
        let data_type = DataType::Float64;
        let result_type = covariance_return_type(&data_type)?;
        assert_eq!(DataType::Float64, result_type);

        let data_type = DataType::Decimal128(36, 10);
        assert!(covariance_return_type(&data_type).is_err());
        Ok(())
    }

    #[test]
    fn test_correlation_return_data_type() -> Result<()> {
        let data_type = DataType::Float64;
        let result_type = correlation_return_type(&data_type)?;
        assert_eq!(DataType::Float64, result_type);

        let data_type = DataType::Decimal128(36, 10);
        assert!(correlation_return_type(&data_type).is_err());
        Ok(())
    }
}
