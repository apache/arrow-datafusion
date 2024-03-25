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

//! [`ScalarUDF`]: Scalar User Defined Functions

use crate::simplify::{ExprSimplifyResult, SimplifyInfo};
use crate::{
    ColumnarValue, Expr, FuncMonotonicity, ReturnTypeFunction,
    ScalarFunctionImplementation, Signature,
};
use arrow::datatypes::DataType;
use datafusion_common::{ExprSchema, Result};
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

/// Logical representation of a Scalar User Defined Function.
///
/// A scalar function produces a single row output for each row of input. This
/// struct contains the information DataFusion needs to plan and invoke
/// functions you supply such name, type signature, return type, and actual
/// implementation.
///
/// 1. For simple use cases, use [`create_udf`] (examples in [`simple_udf.rs`]).
///
/// 2. For advanced use cases, use [`ScalarUDFImpl`] which provides full API
/// access (examples in  [`advanced_udf.rs`]).
///
/// # API Note
///
/// This is a separate struct from `ScalarUDFImpl` to maintain backwards
/// compatibility with the older API.
///
/// [`create_udf`]: crate::expr_fn::create_udf
/// [`simple_udf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udf.rs
/// [`advanced_udf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
#[derive(Debug, Clone)]
pub struct ScalarUDF {
    inner: Arc<dyn ScalarUDFImpl>,
}

impl PartialEq for ScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.signature() == other.signature()
    }
}

impl Eq for ScalarUDF {}

impl std::hash::Hash for ScalarUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.signature().hash(state);
    }
}

impl ScalarUDF {
    /// Create a new ScalarUDF from low level details.
    ///
    /// See  [`ScalarUDFImpl`] for a more convenient way to create a
    /// `ScalarUDF` using trait objects
    #[deprecated(since = "34.0.0", note = "please implement ScalarUDFImpl instead")]
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        fun: &ScalarFunctionImplementation,
    ) -> Self {
        Self::new_from_impl(ScalarUdfLegacyWrapper {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            fun: fun.clone(),
        })
    }

    /// Create a new `ScalarUDF` from a `[ScalarUDFImpl]` trait object
    ///
    /// Note this is the same as using the `From` impl (`ScalarUDF::from`)
    pub fn new_from_impl<F>(fun: F) -> ScalarUDF
    where
        F: ScalarUDFImpl + 'static,
    {
        Self {
            inner: Arc::new(fun),
        }
    }

    /// Return the underlying [`ScalarUDFImpl`] trait object for this function
    pub fn inner(&self) -> Arc<dyn ScalarUDFImpl> {
        self.inner.clone()
    }

    /// Adds additional names that can be used to invoke this function, in
    /// addition to `name`
    ///
    /// If you implement [`ScalarUDFImpl`] directly you should return aliases directly.
    pub fn with_aliases(self, aliases: impl IntoIterator<Item = &'static str>) -> Self {
        Self::new_from_impl(AliasedScalarUDFImpl::new(self.inner.clone(), aliases))
    }

    /// Returns a [`Expr`] logical expression to call this UDF with specified
    /// arguments.
    ///
    /// This utility allows using the UDF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(crate::expr::ScalarFunction::new_udf(
            Arc::new(self.clone()),
            args,
        ))
    }

    /// Returns this function's name.
    ///
    /// See [`ScalarUDFImpl::name`] for more details.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the aliases for this function.
    ///
    /// See [`ScalarUDF::with_aliases`] for more details
    pub fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    /// Returns this function's [`Signature`] (what input types are accepted).
    ///
    /// See [`ScalarUDFImpl::signature`] for more details.
    pub fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// The datatype this function returns given the input argument input types.
    /// This function is used when the input arguments are [`Expr`]s.
    ///
    ///
    /// See [`ScalarUDFImpl::return_type_from_exprs`] for more details.
    pub fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        // If the implementation provides a return_type_from_exprs, use it
        self.inner.return_type_from_exprs(args, schema, arg_types)
    }

    /// Do the function rewrite
    ///
    /// See [`ScalarUDFImpl::simplify`] for more details.
    pub fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    /// Invoke the function on `args`, returning the appropriate result.
    ///
    /// See [`ScalarUDFImpl::invoke`] for more details.
    pub fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        self.inner.invoke(args)
    }

    /// Returns a `ScalarFunctionImplementation` that can invoke the function
    /// during execution
    pub fn fun(&self) -> ScalarFunctionImplementation {
        let captured = self.inner.clone();
        Arc::new(move |args| captured.invoke(args))
    }

    /// This function specifies monotonicity behaviors for User defined scalar functions.
    ///
    /// See [`ScalarUDFImpl::monotonicity`] for more details.
    pub fn monotonicity(&self) -> Result<Option<FuncMonotonicity>> {
        self.inner.monotonicity()
    }
}

impl<F> From<F> for ScalarUDF
where
    F: ScalarUDFImpl + Send + Sync + 'static,
{
    fn from(fun: F) -> Self {
        Self::new_from_impl(fun)
    }
}

/// Trait for implementing [`ScalarUDF`].
///
/// This trait exposes the full API for implementing user defined functions and
/// can be used to implement any function.
///
/// See [`advanced_udf.rs`] for a full example with complete implementation and
/// [`ScalarUDF`] for other available options.
///
///
/// [`advanced_udf.rs`]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/advanced_udf.rs
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Signature, Volatility};
/// # use datafusion_expr::{ScalarUDFImpl, ScalarUDF};
/// #[derive(Debug)]
/// struct AddOne {
///   signature: Signature
/// };
///
/// impl AddOne {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable)
///      }
///   }
/// }
///
/// /// Implement the ScalarUDFImpl trait for AddOne
/// impl ScalarUDFImpl for AddOne {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "add_one" }
///    fn signature(&self) -> &Signature { &self.signature }
///    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
///      if !matches!(args.get(0), Some(&DataType::Int32)) {
///        return plan_err!("add_one only accepts Int32 arguments");
///      }
///      Ok(DataType::Int32)
///    }
///    // The actual implementation would add one to the argument
///    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> { unimplemented!() }
/// }
///
/// // Create a new ScalarUDF from the implementation
/// let add_one = ScalarUDF::from(AddOne::new());
///
/// // Call the function `add_one(col)`
/// let expr = add_one.call(vec![col("a")]);
/// ```
pub trait ScalarUDFImpl: Debug + Send + Sync {
    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns the function's [`Signature`] for information about what input
    /// types are accepted and the function's Volatility.
    fn signature(&self) -> &Signature;

    /// What [`DataType`] will be returned by this function, given the types of
    /// the arguments.
    ///
    /// # Notes
    ///
    /// If you provide an implementation for [`Self::return_type_from_exprs`],
    /// DataFusion will not call `return_type` (this function). In this case it
    /// is recommended to return [`DataFusionError::Internal`].
    ///
    /// [`DataFusionError::Internal`]: datafusion_common::DataFusionError::Internal
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// What [`DataType`] will be returned by this function, given the
    /// arguments?
    ///
    /// Note most UDFs should implement [`Self::return_type`] and not this
    /// function. The output type for most functions only depends on the types
    /// of their inputs (e.g. `sqrt(f32)` is always `f32`).
    ///
    /// By default, this function calls [`Self::return_type`] with the
    /// types of each argument.
    ///
    /// This method can be overridden for functions that return different
    /// *types* based on the *values* of their arguments.
    ///
    /// For example, the following two function calls get the same argument
    /// types (something and a `Utf8` string) but return different types based
    /// on the value of the second argument:
    ///
    /// * `arrow_cast(x, 'Int16')` --> `Int16`
    /// * `arrow_cast(x, 'Float32')` --> `Float32`
    ///
    /// # Notes:
    ///
    /// This function must consistently return the same type for the same
    /// logical input even if the input is simplified (e.g. it must return the same
    /// value for `('foo' | 'bar')` as it does for ('foobar').
    fn return_type_from_exprs(
        &self,
        _args: &[Expr],
        _schema: &dyn ExprSchema,
        arg_types: &[DataType],
    ) -> Result<DataType> {
        self.return_type(arg_types)
    }

    /// Invoke the function on `args`, returning the appropriate result
    ///
    /// The function will be invoked passed with the slice of [`ColumnarValue`]
    /// (either scalar or array).
    ///
    /// # Zero Argument Functions
    /// If the function has zero parameters (e.g. `now()`) it will be passed a
    /// single element slice which is a a null array to indicate the batch's row
    /// count (so the function can know the resulting array size).
    ///
    /// # Performance
    ///
    /// For the best performance, the implementations of `invoke` should handle
    /// the common case when one or more of their arguments are constant values
    /// (aka  [`ColumnarValue::Scalar`]).
    ///
    /// [`ColumnarValue::values_to_arrays`] can be used to convert the arguments
    /// to arrays, which will likely be simpler code, but be slower.
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue>;

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Aliases can be used to invoke the same function using different names.
    /// For example in some databases `now()` and `current_timestamp()` are
    /// aliases for the same function. This behavior can be obtained by
    /// returning `current_timestamp` as an alias for the `now` function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }

    /// This function specifies monotonicity behaviors for User defined scalar functions.
    fn monotonicity(&self) -> Result<Option<FuncMonotonicity>> {
        Ok(None)
    }

    /// Optionally apply per-UDF simplification / rewrite rules.
    ///
    /// This can be used to apply function specific simplification rules during
    /// optimization (e.g. `arrow_cast` --> `Expr::Cast`). The default
    /// implementation does nothing.
    ///
    /// Note that DataFusion handles simplifying arguments and  "constant
    /// folding" (replacing a function call with constant arguments such as
    /// `my_add(1,2) --> 3` ). Thus, there is no need to implement such
    /// optimizations manually for specific UDFs.
    ///
    /// # Arguments
    /// * 'args': The arguments of the function
    /// * 'schema': The schema of the function
    ///
    /// # Returns
    /// [`ExprSimplifyResult`] indicating the result of the simplification NOTE
    /// if the function cannot be simplified, the arguments *MUST* be returned
    /// unmodified
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }
}

/// ScalarUDF that adds an alias to the underlying function. It is better to
/// implement [`ScalarUDFImpl`], which supports aliases, directly if possible.
#[derive(Debug)]
struct AliasedScalarUDFImpl {
    inner: Arc<dyn ScalarUDFImpl>,
    aliases: Vec<String>,
}

impl AliasedScalarUDFImpl {
    pub fn new(
        inner: Arc<dyn ScalarUDFImpl>,
        new_aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let mut aliases = inner.aliases().to_vec();
        aliases.extend(new_aliases.into_iter().map(|s| s.to_string()));

        Self { inner, aliases }
    }
}

impl ScalarUDFImpl for AliasedScalarUDFImpl {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        self.inner.invoke(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Implementation of [`ScalarUDFImpl`] that wraps the function style pointers
/// of the older API (see <https://github.com/apache/arrow-datafusion/pull/8578>
/// for more details)
struct ScalarUdfLegacyWrapper {
    /// The name of the function
    name: String,
    /// The signature (the types of arguments that are supported)
    signature: Signature,
    /// Function that returns the return type given the argument types
    return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    fun: ScalarFunctionImplementation,
}

impl Debug for ScalarUdfLegacyWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarUDFImpl for ScalarUdfLegacyWrapper {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        (self.fun)(args)
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}
