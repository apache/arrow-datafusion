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

//! Function module contains typing and signature for built-in and user defined functions.

use crate::{Accumulator, BuiltinScalarFunction, PartitionEvaluator, Signature};
use crate::{AggregateFunction, BuiltInWindowFunction, ColumnarValue};
use arrow::datatypes::DataType;
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::{Result, ScalarValue};
use std::sync::Arc;
use strum::IntoEnumIterator;

/// Scalar function
///
/// The Fn param is the wrapped function but be aware that the function will
/// be passed with the slice / vec of columnar values (either scalar or array)
/// with the exception of zero param function, where a singular element vec
/// will be passed. In that case the single element is a null array to indicate
/// the batch's row count (so that the generative zero-argument function can know
/// the result array size).
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

/// Constant argument, (arg index, constant value).
pub type ConstantArg = (usize, ScalarValue);

/// Factory that returns the functions's return type given the input argument types and constant arguments
pub trait ReturnTypeFactory: Send + Sync {
    fn infer(
        &self,
        input_types: &[DataType],
        constant_args: &[ConstantArg],
    ) -> Result<Arc<DataType>>;
}

/// Factory that returns the functions's return type given the input argument types
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;

impl ReturnTypeFactory for ReturnTypeFunction {
    fn infer(
        &self,
        input_types: &[DataType],
        _constant_args: &[ConstantArg],
    ) -> Result<Arc<DataType>> {
        self(input_types)
    }
}

/// Factory that returns an accumulator for the given aggregate, given
/// its return datatype.
pub type AccumulatorFactoryFunction =
    Arc<dyn Fn(&DataType) -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// Factory that creates a PartitionEvaluator for the given window
/// function
pub type PartitionEvaluatorFactory =
    Arc<dyn Fn() -> Result<Box<dyn PartitionEvaluator>> + Send + Sync>;

/// Factory that returns the types used by an aggregator to serialize
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;

/// Returns the datatype of the scalar function
#[deprecated(
    since = "27.0.0",
    note = "please use `BuiltinScalarFunction::return_type` instead"
)]
pub fn return_type(
    fun: &BuiltinScalarFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    fun.return_type(input_expr_types)
}

/// Return the [`Signature`] supported by the function `fun`.
#[deprecated(
    since = "27.0.0",
    note = "please use `BuiltinScalarFunction::signature` instead"
)]
pub fn signature(fun: &BuiltinScalarFunction) -> Signature {
    fun.signature()
}

/// Suggest a valid function based on an invalid input function name
pub fn suggest_valid_function(input_function_name: &str, is_window_func: bool) -> String {
    let valid_funcs = if is_window_func {
        // All aggregate functions and builtin window functions
        AggregateFunction::iter()
            .map(|func| func.to_string())
            .chain(BuiltInWindowFunction::iter().map(|func| func.to_string()))
            .collect()
    } else {
        // All scalar functions and aggregate functions
        BuiltinScalarFunction::iter()
            .map(|func| func.to_string())
            .chain(AggregateFunction::iter().map(|func| func.to_string()))
            .collect()
    };
    find_closest_match(valid_funcs, input_function_name)
}

/// Find the closest matching string to the target string in the candidates list, using edit distance(case insensitve)
/// Input `candidates` must not be empty otherwise it will panic
fn find_closest_match(candidates: Vec<String>, target: &str) -> String {
    let target = target.to_lowercase();
    candidates
        .into_iter()
        .min_by_key(|candidate| {
            datafusion_strsim::levenshtein(&candidate.to_lowercase(), &target)
        })
        .expect("No candidates provided.") // Panic if `candidates` argument is empty
}
