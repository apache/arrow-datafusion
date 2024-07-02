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

//! Structs and traits to provide the information needed for expression simplification.

use datafusion_common::logical_type::signature::LogicalType;
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_common::logical_type::{TypeRelation, ExtensionType};
use crate::{execution_props::ExecutionProps, Expr, ExprSchemable};

/// Provides the information necessary to apply algebraic simplification to an
/// [Expr]. See [SimplifyContext] for one concrete implementation.
///
/// This trait exists so that other systems can plug schema
/// information in without having to create `DFSchema` objects. If you
/// have a [`DFSchemaRef`] you can use [`SimplifyContext`]
pub trait SimplifyInfo {
    /// returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool>;

    /// returns true of this expr is nullable (could possibly be NULL)
    fn nullable(&self, expr: &Expr) -> Result<bool>;

    /// Returns details needed for partial expression evaluation
    fn execution_props(&self) -> &ExecutionProps;

    /// Returns data type of this expr needed for determining optimized int type of a value
    fn get_data_type(&self, expr: &Expr) -> Result<TypeRelation>;
}

/// Provides simplification information based on DFSchema and
/// [`ExecutionProps`]. This is the default implementation used by DataFusion
///
/// # Example
/// See the `simplify_demo` in the [`expr_api` example]
///
/// [`expr_api` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs
#[derive(Debug, Clone)]
pub struct SimplifyContext<'a> {
    schema: Option<DFSchemaRef>,
    props: &'a ExecutionProps,
}

impl<'a> SimplifyContext<'a> {
    /// Create a new SimplifyContext
    pub fn new(props: &'a ExecutionProps) -> Self {
        Self {
            schema: None,
            props,
        }
    }

    /// Register a [`DFSchemaRef`] with this context
    pub fn with_schema(mut self, schema: DFSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl<'a> SimplifyInfo for SimplifyContext<'a> {
    /// returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        for schema in &self.schema {
            if let Ok(LogicalType::Boolean) = expr.get_type(schema).map(|t| t.logical().clone()) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns true if expr is nullable
    fn nullable(&self, expr: &Expr) -> Result<bool> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            DataFusionError::Internal(
                "attempt to get nullability without schema".to_string(),
            )
        })?;
        expr.nullable(schema.as_ref())
    }

    /// Returns data type of this expr needed for determining optimized int type of a value
    fn get_data_type(&self, expr: &Expr) -> Result<TypeRelation> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            DataFusionError::Internal(
                "attempt to get data type without schema".to_string(),
            )
        })?;
        expr.get_type(schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.props
    }
}

/// Was the expression simplified?
#[derive(Debug)]
pub enum ExprSimplifyResult {
    /// The function call was simplified to an entirely new Expr
    Simplified(Expr),
    /// the function call could not be simplified, and the arguments
    /// are return unmodified.
    Original(Vec<Expr>),
}
