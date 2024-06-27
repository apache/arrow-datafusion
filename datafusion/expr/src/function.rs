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
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum Hint {
    /// Indicates the argument needs to be padded if it is scalar
    Pad,
    /// Indicates the argument can be converted to an array of length 1
    AcceptsSingular,
}

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

/// [`AccumulatorArgs`] contains information about how an aggregate
/// function was called, including the types of its arguments and any optional
/// ordering expressions.
#[derive(Debug)]
pub struct AccumulatorArgs<'a> {
    /// The return type of the aggregate function.
    pub data_type: &'a DataType,

    /// The schema of the input arguments
    pub schema: &'a Schema,

    /// Whether to ignore nulls.
    ///
    /// SQL allows the user to specify `IGNORE NULLS`, for example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1) IGNORE NULLS FROM t;
    /// ```
    pub ignore_nulls: bool,

    /// The expressions in the `ORDER BY` clause passed to this aggregator.
    ///
    /// SQL allows the user to specify the ordering of arguments to the
    /// aggregate using an `ORDER BY`. For example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1 ORDER BY column2) FROM t;
    /// ```
    ///
    /// If no `ORDER BY` is specified, `sort_exprs`` will be empty.
    pub sort_exprs: &'a [Expr],

    /// The name of the aggregate expression
    pub name: &'a str,

    /// Whether the aggregate function is distinct.
    ///
    /// ```sql
    /// SELECT COUNT(DISTINCT column1) FROM t;
    /// ```
    pub is_distinct: bool,

    /// The input type of the aggregate function.
    pub input_type: &'a DataType,

    /// If the input type is nullable.
    pub input_nullable: bool,

    /// The logical expression of arguments the aggregate function takes.
    pub input_exprs: &'a [Expr],
}

/// [`StateFieldsArgs`] contains information about the fields that an
/// aggregate function's accumulator should have. Used for [`AggregateUDFImpl::state_fields`].
///
/// [`AggregateUDFImpl::state_fields`]: crate::udaf::AggregateUDFImpl::state_fields
pub struct StateFieldsArgs<'a> {
    /// The name of the aggregate function.
    pub name: &'a str,

    /// The input type of the aggregate function.
    pub input_type: &'a DataType,

    /// If the input type is nullable.
    pub input_nullable: bool,

    /// The return type of the aggregate function.
    pub return_type: &'a DataType,

    /// The ordering fields of the aggregate function.
    pub ordering_fields: &'a [Field],

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
}

/// Factory that returns an accumulator for the given aggregate function.
pub type AccumulatorFactoryFunction =
    Arc<dyn Fn(AccumulatorArgs) -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// Factory that creates a PartitionEvaluator for the given window
/// function
pub type PartitionEvaluatorFactory =
    Arc<dyn Fn() -> Result<Box<dyn PartitionEvaluator>> + Send + Sync>;

/// Factory that returns the types used by an aggregator to serialize
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;

/// [crate::udaf::AggregateUDFImpl::simplify] simplifier closure
/// A closure with two arguments:
/// * 'aggregate_function': [crate::expr::AggregateFunction] for which simplified has been invoked
/// * 'info': [crate::simplify::SimplifyInfo]
///
/// closure returns simplified [Expr] or an error.
pub type AggregateFunctionSimplification = Box<
    dyn Fn(
        crate::expr::AggregateFunction,
        &dyn crate::simplify::SimplifyInfo,
    ) -> Result<Expr>,
>;

/// [crate::udwf::WindowUDFImpl::simplify] simplifier closure
/// A closure with two arguments:
/// * 'window_function': [crate::expr::WindowFunction] for which simplified has been invoked
/// * 'info': [crate::simplify::SimplifyInfo]
///
/// closure returns simplified [Expr] or an error.
pub type WindowFunctionSimplification = Box<
    dyn Fn(
        crate::expr::WindowFunction,
        &dyn crate::simplify::SimplifyInfo,
    ) -> Result<Expr>,
>;
