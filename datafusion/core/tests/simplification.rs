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

//! This program demonstrates the DataFusion expression simplification API.

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::{error::Result, execution::context::ExecutionProps, prelude::*};
use datafusion_common::logical_type::LogicalType;
use datafusion_expr::{Expr, ExprSchemable};
use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyInfo};

/// In order to simplify expressions, DataFusion must have information
/// about the expressions.
///
/// You can provide that information using DataFusion [DFSchema]
/// objects or from some other implementation
struct MyInfo {
    /// The input schema
    schema: DFSchema,

    /// Execution specific details needed for constant evaluation such
    /// as the current time for `now()` and [VariableProviders]
    execution_props: ExecutionProps,
}

impl SimplifyInfo for MyInfo {
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        Ok(matches!(expr.get_type(&self.schema)?, LogicalType::Boolean))
    }

    fn nullable(&self, expr: &Expr) -> Result<bool> {
        expr.nullable(&self.schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    fn get_data_type(&self, expr: &Expr) -> Result<LogicalType> {
        expr.get_type(&self.schema)
    }
}

impl From<DFSchema> for MyInfo {
    fn from(schema: DFSchema) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }
}

/// A schema like:
///
/// a: Int32 (possibly with nulls)
/// b: Int32
/// s: Utf8
fn schema() -> DFSchema {
    Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
    ])
    .try_into()
    .unwrap()
}

#[test]
fn basic() {
    let info: MyInfo = schema().into();

    // The `Expr` is a core concept in DataFusion, and DataFusion can
    // help simplify it.

    // For example 'a < (2 + 3)' can be rewritten into the easier to
    // optimize form `a < 5` automatically
    let expr = col("a").lt(lit(2i32) + lit(3i32));

    let simplifier = ExprSimplifier::new(info);
    let simplified = simplifier.simplify(expr).unwrap();
    assert_eq!(simplified, col("a").lt(lit(5i32)));
}

#[test]
fn fold_and_simplify() {
    let info: MyInfo = schema().into();

    // What will it do with the expression `concat('foo', 'bar') == 'foobar')`?
    let expr = concat(&[lit("foo"), lit("bar")]).eq(lit("foobar"));

    // Since datafusion applies both simplification *and* rewriting
    // some expressions can be entirely simplified
    let simplifier = ExprSimplifier::new(info);
    let simplified = simplifier.simplify(expr).unwrap();
    assert_eq!(simplified, lit(true))
}
