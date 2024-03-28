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

use crate::ColumnarValue;
use crate::{Accumulator, Expr, PartitionEvaluator};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::Result;
use std::sync::Arc;

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

/// Factory that returns the functions's return type given the input argument types
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;

/// Factory that returns an accumulator for the given aggregate, given
/// its return datatype, the sorting expressions and the schema for ordering.
pub type AccumulatorFactoryFunction = Arc<
    dyn Fn(&DataType, &[Expr], &Schema) -> Result<Box<dyn Accumulator>> + Send + Sync,
>;

/// Factory that creates a PartitionEvaluator for the given window
/// function
pub type PartitionEvaluatorFactory =
    Arc<dyn Fn() -> Result<Box<dyn PartitionEvaluator>> + Send + Sync>;

/// Factory that returns the types used by an aggregator to serialize
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;
