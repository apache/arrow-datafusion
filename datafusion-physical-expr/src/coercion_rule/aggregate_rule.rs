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

//! Support the coercion rule for aggregate function.

#[cfg(feature = "roaring_bitmap")]
use crate::expressions::is_bitmap_count_distinct_supported_arg_type;
use crate::expressions::{
    is_approx_percentile_cont_supported_arg_type, is_avg_support_arg_type,
    is_correlation_support_arg_type, is_covariance_support_arg_type,
    is_stddev_support_arg_type, is_sum_support_arg_type, is_variance_support_arg_type,
    try_cast,
};
use crate::PhysicalExpr;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::AggregateFunction;
use datafusion_expr::{Signature, TypeSignature};
use std::ops::Deref;
use std::sync::Arc;

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
            if !matches!(input_types[1], DataType::Float64) {
                return Err(DataFusionError::Plan(format!(
                    "The percentile argument for {:?} must be Float64, not {:?}.",
                    agg_fun, input_types[1]
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
        #[cfg(feature = "roaring_bitmap")]
        AggregateFunction::BitMapCountDistinct => {
            if !is_bitmap_count_distinct_supported_arg_type(&input_types[0]) {
                return Err(DataFusionError::Plan(format!(
                    "The function {:?} does not support inputs of type {:?}.",
                    agg_fun, input_types[0]
                )));
            }
            Ok(input_types.to_vec())
        }
        #[cfg(not(feature = "roaring_bitmap"))]
        AggregateFunction::BitMapCountDistinct => {
            return Err(DataFusionError::Plan(format!(
                "The function {:?} does not support inputs of type {:?}.",
                agg_fun, input_types[0]
            )));
        }
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
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Aggregate functions do not support this {:?}",
                signature
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

/// Returns the coerced exprs for each `input_exprs`.
/// Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
/// data type of `input_exprs` need to be coerced.
pub fn coerce_exprs(
    agg_fun: &AggregateFunction,
    input_exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    signature: &Signature,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    if input_exprs.is_empty() {
        return Ok(vec![]);
    }
    let input_types = input_exprs
        .iter()
        .map(|e| e.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    // get the coerced data types
    let coerced_types = coerce_types(agg_fun, &input_types, signature)?;

    // try cast if need
    input_exprs
        .iter()
        .zip(coerced_types.into_iter())
        .map(|(expr, coerced_type)| try_cast(expr.clone(), schema, coerced_type))
        .collect::<Result<Vec<_>>>()
}
