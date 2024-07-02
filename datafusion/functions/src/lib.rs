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
#![deny(clippy::clone_on_ref_ptr)]

//! Function packages for [DataFusion].
//!
//! This crate contains a collection of various function packages for DataFusion,
//! implemented using the extension API. Users may wish to control which functions
//! are available to control the binary size of their application as well as
//! use dialect specific implementations of functions (e.g. Spark vs Postgres)
//!
//! Each package is implemented as a separate
//! module, activated by a feature flag.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
//! # Available Packages
//! See the list of [modules](#modules) in this crate for available packages.
//!
//! # Using A Package
//! You can register all functions in all packages using the [`register_all`] function.
//!
//! To access and use only the functions in a certain package, use the
//! `functions()` method in each module.
//!
//! ```
//! # fn main() -> datafusion_common::Result<()> {
//! # let mut registry = datafusion_execution::registry::MemoryFunctionRegistry::new();
//! # use datafusion_execution::FunctionRegistry;
//! // get the encoding functions
//! use datafusion_functions::encoding;
//! for udf in encoding::functions() {
//!   registry.register_udf(udf)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Each package also exports an `expr_fn` submodule to help create [`Expr`]s that invoke
//! functions using a fluent style. For example:
//!
//! ```
//! // create an Expr that will invoke the encode function
//! use datafusion_expr::{col, lit};
//! use datafusion_functions::expr_fn;
//! // Equivalent to "encode(my_data, 'hex')" in SQL:
//! let expr = expr_fn::encode(col("my_data"), lit("hex"));
//! ```
//!
//![`Expr`]: datafusion_expr::Expr
//!
//! # Implementing A New Package
//!
//! To add a new package to this crate, you should follow the model of existing
//! packages. The high level steps are:
//!
//! 1. Create a new module with the appropriate [`ScalarUDF`] implementations.
//!
//! 2. Use the macros in [`macros`] to create standard entry points.
//!
//! 3. Add a new feature to `Cargo.toml`, with any optional dependencies
//!
//! 4. Use the `make_package!` macro to expose the module when the
//! feature is enabled.
//!
//! [`ScalarUDF`]: datafusion_expr::ScalarUDF
use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

#[macro_use]
pub mod macros;

#[cfg(feature = "string_expressions")]
pub mod string;
make_stub_package!(string, "string_expressions");

/// Core datafusion expressions
/// Enabled via feature flag `core_expressions`
#[cfg(feature = "core_expressions")]
pub mod core;
make_stub_package!(core, "core_expressions");

/// Date and time expressions.
/// Contains functions such as to_timestamp
/// Enabled via feature flag `datetime_expressions`
#[cfg(feature = "datetime_expressions")]
pub mod datetime;
make_stub_package!(datetime, "datetime_expressions");

/// Encoding expressions.
/// Contains Hex and binary `encode` and `decode` functions.
/// Enabled via feature flag `encoding_expressions`
#[cfg(feature = "encoding_expressions")]
pub mod encoding;
make_stub_package!(encoding, "encoding_expressions");

/// Mathematical functions.
/// Enabled via feature flag `math_expressions`
#[cfg(feature = "math_expressions")]
pub mod math;
make_stub_package!(math, "math_expressions");

/// Regular expression functions.
/// Enabled via feature flag `regex_expressions`
#[cfg(feature = "regex_expressions")]
pub mod regex;
make_stub_package!(regex, "regex_expressions");

#[cfg(feature = "crypto_expressions")]
pub mod crypto;
make_stub_package!(crypto, "crypto_expressions");

#[cfg(feature = "unicode_expressions")]
pub mod unicode;
make_stub_package!(unicode, "unicode_expressions");

mod utils;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    #[cfg(feature = "core_expressions")]
    pub use super::core::expr_fn::*;
    #[cfg(feature = "crypto_expressions")]
    pub use super::crypto::expr_fn::*;
    #[cfg(feature = "datetime_expressions")]
    pub use super::datetime::expr_fn::*;
    #[cfg(feature = "encoding_expressions")]
    pub use super::encoding::expr_fn::*;
    #[cfg(feature = "math_expressions")]
    pub use super::math::expr_fn::*;
    #[cfg(feature = "regex_expressions")]
    pub use super::regex::expr_fn::*;
    #[cfg(feature = "string_expressions")]
    pub use super::string::expr_fn::*;
    #[cfg(feature = "unicode_expressions")]
    pub use super::unicode::expr_fn::*;
}

/// Return all default functions
pub fn all_default_functions() -> Vec<Arc<ScalarUDF>> {
    core::functions()
        .into_iter()
        .chain(datetime::functions())
        .chain(encoding::functions())
        .chain(math::functions())
        .chain(regex::functions())
        .chain(crypto::functions())
        .chain(unicode::functions())
        .chain(string::functions())
        .collect::<Vec<_>>()
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let all_functions = all_default_functions();

    all_functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::all_default_functions;
    use datafusion_common::Result;
    use std::collections::HashSet;

    #[test]
    fn test_no_duplicate_name() -> Result<()> {
        let mut names = HashSet::new();
        for func in all_default_functions() {
            assert!(
                names.insert(func.name().to_string().to_lowercase()),
                "duplicate function name: {}",
                func.name()
            );
            for alias in func.aliases() {
                assert!(
                    names.insert(alias.to_string().to_lowercase()),
                    "duplicate function name: {}",
                    alias
                );
            }
        }
        Ok(())
    }
}
