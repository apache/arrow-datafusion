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

//! [DataFusion](https://github.com/apache/datafusion)
//! is an extensible query execution framework that uses
//! [Apache Arrow](https://arrow.apache.org) as its in-memory format.
//!
//! This crate is a submodule of DataFusion that provides types representing
//! logical query plans ([LogicalPlan]) and logical expressions ([Expr]) as well as utilities for
//! working with these types.
//!
//! The [expr_fn] module contains functions for creating expressions.

mod accumulator;
mod built_in_window_function;
mod columnar_value;
mod literal;
mod operator;
mod partition_evaluator;
mod signature;
mod table_source;
mod udaf;
mod udf;
mod udwf;

pub mod aggregate_function;
pub mod conditional_expressions;
pub mod execution_props;
pub mod expr;
pub mod expr_fn;
pub mod expr_rewriter;
pub mod expr_schema;
pub mod function;
pub mod groups_accumulator;
pub mod interval_arithmetic;
pub mod logical_plan;
pub mod simplify;
pub mod sort_properties;
pub mod tree_node;
pub mod type_coercion;
pub mod utils;
pub mod var_provider;
pub mod window_frame;
pub mod window_state;

pub use accumulator::Accumulator;
pub use aggregate_function::AggregateFunction;
pub use built_in_window_function::BuiltInWindowFunction;
pub use columnar_value::ColumnarValue;
pub use expr::{
    Between, BinaryExpr, Case, Cast, Expr, GetFieldAccess, GetIndexedField, GroupingSet,
    Like, TryCast, WindowFunctionDefinition,
};
pub use expr_fn::*;
pub use expr_schema::ExprSchemable;
pub use function::{
    AccumulatorFactoryFunction, PartitionEvaluatorFactory, ReturnTypeFunction,
    ScalarFunctionImplementation, StateTypeFunction,
};
pub use groups_accumulator::{EmitTo, GroupsAccumulator};
pub use literal::{lit, lit_timestamp_nano, Literal, TimestampLiteral};
pub use logical_plan::*;
pub use operator::Operator;
pub use partition_evaluator::PartitionEvaluator;
pub use signature::{
    ArrayFunctionSignature, Signature, TypeSignature, ValidType, Volatility,
    TIMEZONE_WILDCARD,
};
pub use table_source::{TableProviderFilterPushDown, TableSource, TableType};
pub use udaf::{AggregateUDF, AggregateUDFImpl, ReversedUDAF};
pub use udf::{ScalarUDF, ScalarUDFImpl};
pub use udwf::{WindowUDF, WindowUDFImpl};
pub use window_frame::{WindowFrame, WindowFrameBound, WindowFrameUnits};

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
