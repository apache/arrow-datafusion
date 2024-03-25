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

//! Array Functions for [DataFusion].
//!
//! This crate contains a collection of array functions implemented using the
//! extension API.
//!
//! [DataFusion]: https://crates.io/crates/datafusion
//!
//! You can register the functions in this crate using the [`register_all`] function.
//!

#[macro_use]
pub mod macros;

mod array_has;
mod cardinality;
mod concat;
mod dimension;
mod empty;
mod except;
mod extract;
mod flatten;
mod length;
mod make_array;
mod position;
mod range;
mod remove;
mod repeat;
mod replace;
mod resize;
mod reverse;
mod rewrite;
mod set_ops;
mod sort;
mod string;
mod utils;

use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

/// Fluent-style API for creating `Expr`s
pub mod expr_fn {
    pub use super::array_has::array_has;
    pub use super::array_has::array_has_all;
    pub use super::array_has::array_has_any;
    pub use super::cardinality::cardinality;
    pub use super::concat::array_append;
    pub use super::concat::array_concat;
    pub use super::concat::array_prepend;
    pub use super::dimension::array_dims;
    pub use super::dimension::array_ndims;
    pub use super::empty::array_empty;
    pub use super::except::array_except;
    pub use super::extract::array_element;
    pub use super::extract::array_pop_back;
    pub use super::extract::array_pop_front;
    pub use super::extract::array_slice;
    pub use super::flatten::flatten;
    pub use super::length::array_length;
    pub use super::make_array::make_array;
    pub use super::position::array_position;
    pub use super::position::array_positions;
    pub use super::range::gen_series;
    pub use super::range::range;
    pub use super::remove::array_remove;
    pub use super::remove::array_remove_all;
    pub use super::remove::array_remove_n;
    pub use super::repeat::array_repeat;
    pub use super::replace::array_replace;
    pub use super::replace::array_replace_all;
    pub use super::replace::array_replace_n;
    pub use super::resize::array_resize;
    pub use super::reverse::array_reverse;
    pub use super::set_ops::array_distinct;
    pub use super::set_ops::array_intersect;
    pub use super::set_ops::array_union;
    pub use super::sort::array_sort;
    pub use super::string::array_to_string;
    pub use super::string::string_to_array;
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        string::array_to_string_udf(),
        string::string_to_array_udf(),
        range::range_udf(),
        range::gen_series_udf(),
        dimension::array_dims_udf(),
        cardinality::cardinality_udf(),
        dimension::array_ndims_udf(),
        concat::array_append_udf(),
        concat::array_prepend_udf(),
        concat::array_concat_udf(),
        except::array_except_udf(),
        extract::array_element_udf(),
        extract::array_pop_back_udf(),
        extract::array_pop_front_udf(),
        extract::array_slice_udf(),
        make_array::make_array_udf(),
        array_has::array_has_udf(),
        array_has::array_has_all_udf(),
        array_has::array_has_any_udf(),
        empty::array_empty_udf(),
        length::array_length_udf(),
        flatten::flatten_udf(),
        sort::array_sort_udf(),
        repeat::array_repeat_udf(),
        resize::array_resize_udf(),
        reverse::array_reverse_udf(),
        set_ops::array_distinct_udf(),
        set_ops::array_intersect_udf(),
        set_ops::array_union_udf(),
        position::array_position_udf(),
        position::array_positions_udf(),
        remove::array_remove_udf(),
        remove::array_remove_all_udf(),
        remove::array_remove_n_udf(),
        replace::array_replace_n_udf(),
        replace::array_replace_all_udf(),
        replace::array_replace_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    registry.register_function_rewrite(Arc::new(rewrite::ArrayFunctionRewriter {}))?;

    Ok(())
}
