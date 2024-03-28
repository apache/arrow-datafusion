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

//! [`AggregateUDF`]: User Defined Aggregate Functions

use crate::groups_accumulator::GroupsAccumulator;
use crate::{Accumulator, Expr};
use crate::{
    AccumulatorFactoryFunction, ReturnTypeFunction, Signature, StateTypeFunction,
};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{not_impl_err, Result};
use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Logical representation of a user-defined [aggregate function] (UDAF).
///
/// An aggregate function combines the values from multiple input rows
/// into a single output "aggregate" (summary) row. It is different
/// from a scalar function because it is stateful across batches. User
/// defined aggregate functions can be used as normal SQL aggregate
/// functions (`GROUP BY` clause) as well as window functions (`OVER`
/// clause).
///
/// `AggregateUDF` provides DataFusion the information needed to plan and call
/// aggregate functions, including name, type information, and a factory
/// function to create an [`Accumulator`] instance, to perform the actual
/// aggregation.
///
/// For more information, please see [the examples]:
///
/// 1. For simple use cases, use [`create_udaf`] (examples in [`simple_udaf.rs`]).
///
/// 2. For advanced use cases, use [`AggregateUDFImpl`] which provides full API
/// access (examples in [`advanced_udaf.rs`]).
///
/// # API Note
/// This is a separate struct from `AggregateUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [the examples]: https://github.com/apache/arrow-datafusion/tree/main/datafusion-examples#single-process
/// [aggregate function]: https://en.wikipedia.org/wiki/Aggregate_function
/// [`Accumulator`]: crate::Accumulator
/// [`create_udaf`]: crate::expr_fn::create_udaf
/// [`simple_udaf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udaf.rs
/// [`advanced_udaf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
#[derive(Debug, Clone)]
pub struct AggregateUDF {
    inner: Arc<dyn AggregateUDFImpl>,
}

impl PartialEq for AggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }
}

impl Eq for AggregateUDF {}

impl std::hash::Hash for AggregateUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.signature().hash(state);
    }
}

impl AggregateUDF {
    /// Create a new AggregateUDF
    ///
    /// See  [`AggregateUDFImpl`] for a more convenient way to create a
    /// `AggregateUDF` using trait objects
    #[deprecated(since = "34.0.0", note = "please implement AggregateUDFImpl instead")]
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        accumulator: &AccumulatorFactoryFunction,
        state_type: &StateTypeFunction,
    ) -> Self {
        Self::new_from_impl(AggregateUDFLegacyWrapper {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            accumulator: accumulator.clone(),
            state_type: state_type.clone(),
        })
    }

    /// Create a new `AggregateUDF` from a `[AggregateUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`AggregateUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> AggregateUDF
    where
        F: AggregateUDFImpl + 'static,
    {
        Self {
            inner: Arc::new(fun),
        }
    }

    /// Return the underlying [`AggregateUDFImpl`] trait object for this function
    pub fn inner(&self) -> Arc<dyn AggregateUDFImpl> {
        self.inner.clone()
    }

    /// Adds additional names that can be used to invoke this function, in
    /// addition to `name`
    ///
    /// If you implement [`AggregateUDFImpl`] directly you should return aliases directly.
    pub fn with_aliases(self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        Self::new_from_impl(AliasedAggregateUDFImpl::new(self.inner.clone(), aliases))
    }

    /// creates an [`Expr`] that calls the aggregate function.
    ///
    /// This utility allows using the UDAF without requiring access to
    /// the registry, such as with the DataFrame API.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::AggregateFunction(crate::expr::AggregateFunction::new_udf(
            Arc::new(self.clone()),
            args,
            false,
            None,
            None,
        ))
    }

    /// Returns this function's name
    ///
    /// See [`AggregateUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the aliases for this function.
    pub fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    /// Returns this function's signature (what input types are accepted)
    ///
    /// See [`AggregateUDFImpl::signature`] for more details.
    pub fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Return the type of the function given its input types
    ///
    /// See [`AggregateUDFImpl::return_type`] for more details.
    pub fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.inner.return_type(args)
    }

    /// Return an accumulator the given aggregate, given its return datatype
    pub fn accumulator(
        &self,
        return_type: &DataType,
        sort_exprs: &[Expr],
        schema: &Schema,
    ) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(return_type, sort_exprs, schema)
    }

    /// Return the type of the intermediate state used by this aggregator, given
    /// its return datatype. Supports multi-phase aggregations
    pub fn state_type(&self, return_type: &DataType) -> Result<Vec<DataType>> {
        self.inner.state_type(return_type)
    }

    /// See [`AggregateUDFImpl::groups_accumulator_supported`] for more details.
    pub fn groups_accumulator_supported(&self) -> bool {
        self.inner.groups_accumulator_supported()
    }

    /// See [`AggregateUDFImpl::create_groups_accumulator`] for more details.
    pub fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator()
    }
}

impl<F> From<F> for AggregateUDF
where
    F: AggregateUDFImpl + Send + Sync + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Trait for implementing [`AggregateUDF`].
///
/// This trait exposes the full API for implementing user defined aggregate functions and
/// can be used to implement any function.
///
/// See [`advanced_udaf.rs`] for a full example with complete implementation and
/// [`AggregateUDF`] for other available options.
///
///
/// [`advanced_udaf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Signature, Volatility, Expr};
/// # use datafusion_expr::{AggregateUDFImpl, AggregateUDF, Accumulator};
/// # use arrow::datatypes::Schema;
/// #[derive(Debug, Clone)]
/// struct GeoMeanUdf {
///   signature: Signature
/// };
///
/// impl GeoMeanUdf {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable)
///      }
///   }
/// }
///
/// /// Implement the AggregateUDFImpl trait for GeoMeanUdf
/// impl AggregateUDFImpl for GeoMeanUdf {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "geo_mean" }
///    fn signature(&self) -> &Signature { &self.signature }
///    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
///      if !matches!(args.get(0), Some(&DataType::Float64)) {
///        return plan_err!("add_one only accepts Float64 arguments");
///      }
///      Ok(DataType::Float64)
///    }
///    // This is the accumulator factory; DataFusion uses it to create new accumulators.
///    fn accumulator(&self, _arg: &DataType, _sort_exprs: &[Expr], _schema: &Schema) -> Result<Box<dyn Accumulator>> { unimplemented!() }
///    fn state_type(&self, _return_type: &DataType) -> Result<Vec<DataType>> {
///        Ok(vec![DataType::Float64, DataType::UInt32])
///    }
/// }
///
/// // Create a new AggregateUDF from the implementation
/// let geometric_mean = AggregateUDF::from(GeoMeanUdf::new());
///
/// // Call the function `geo_mean(col)`
/// let expr = geometric_mean.call(vec![col("a")]);
/// ```
pub trait AggregateUDFImpl: Debug + Send + Sync {
    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns the function's [`Signature`] for information about what input
    /// types are accepted and the function's Volatility.
    fn signature(&self) -> &Signature;

    /// What [`DataType`] will be returned by this function, given the types of
    /// the arguments
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Return a new [`Accumulator`] that aggregates values for a specific
    /// group during query execution.
    ///
    /// `arg`: the type of the argument to this accumulator
    ///
    /// `sort_exprs`: contains a list of `Expr::SortExpr`s if the
    /// aggregate is called with an explicit `ORDER BY`. For example,
    /// `ARRAY_AGG(x ORDER BY y ASC)`. In this case, `sort_exprs` would contain `[y ASC]`
    ///
    /// `schema` is the input schema to the udaf
    fn accumulator(
        &self,
        arg: &DataType,
        sort_exprs: &[Expr],
        schema: &Schema,
    ) -> Result<Box<dyn Accumulator>>;

    /// Return the type used to serialize the  [`Accumulator`]'s intermediate state.
    /// See [`Accumulator::state()`] for more details
    fn state_type(&self, return_type: &DataType) -> Result<Vec<DataType>>;

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator]` will be called.
    fn groups_accumulator_supported(&self) -> bool {
        false
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("GroupsAccumulator hasn't been implemented for {self:?} yet")
    }

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }
}

/// AggregateUDF that adds an alias to the underlying function. It is better to
/// implement [`AggregateUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug)]
struct AliasedAggregateUDFImpl {
    inner: Arc<dyn AggregateUDFImpl>,
    aliases: Vec<String>,
}

impl AliasedAggregateUDFImpl {
    pub fn new(
        inner: Arc<dyn AggregateUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));

        Self { inner, aliases }
    }
}

impl AggregateUDFImpl for AliasedAggregateUDFImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(
        &self,
        arg: &DataType,
        sort_exprs: &[Expr],
        schema: &Schema,
    ) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(arg, sort_exprs, schema)
    }

    fn state_type(&self, return_type: &DataType) -> Result<Vec<DataType>> {
        self.inner.state_type(return_type)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Implementation of [`AggregateUDFImpl`] that wraps the function style pointers
/// of the older API
pub struct AggregateUDFLegacyWrapper {
    /// name
    name: String,
    /// Signature (input arguments)
    signature: Signature,
    /// Return type
    return_type: ReturnTypeFunction,
    /// actual implementation
    accumulator: AccumulatorFactoryFunction,
    /// the accumulator's state's description as a function of the return type
    state_type: StateTypeFunction,
}

impl Debug for AggregateUDFLegacyWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl AggregateUDFImpl for AggregateUDFLegacyWrapper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Old API returns an Arc of the datatype for some reason
        let res = (self.return_type)(arg_types)?;
        Ok(res.as_ref().clone())
    }

    fn accumulator(
        &self,
        arg: &DataType,
        sort_exprs: &[Expr],
        schema: &Schema,
    ) -> Result<Box<dyn Accumulator>> {
        (self.accumulator)(arg, sort_exprs, schema)
    }

    fn state_type(&self, return_type: &DataType) -> Result<Vec<DataType>> {
        let res = (self.state_type)(return_type)?;
        Ok(res.as_ref().clone())
    }
}
