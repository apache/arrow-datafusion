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

//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::functions::{create_physical_fun, out_ordering};
use crate::physical_expr::{down_cast_any_ref, physical_exprs_equal};
use crate::sort_properties::SortProperties;
use crate::PhysicalExpr;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, Result};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{
    expr_vec_fmt, BuiltinScalarFunction, ColumnarValue, FuncMonotonicity,
    ScalarFunctionDefinition, ScalarFunctionImplementation,
};

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: ScalarFunctionDefinition,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
    // Keeps monotonicity information of the function.
    // FuncMonotonicity vector is one to one mapped to `args`,
    // and it specifies the effect of an increase or decrease in
    // the corresponding `arg` to the function value.
    monotonicity: Option<FuncMonotonicity>,
    // Whether this function can be invoked with zero arguments
    supports_zero_argument: bool,
    // Execution properties
    execution_props: ExecutionProps,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_type", &self.return_type)
            .field("monotonicity", &self.monotonicity)
            .field("supports_zero_argument", &self.supports_zero_argument)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: ScalarFunctionDefinition,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
        monotonicity: Option<FuncMonotonicity>,
        supports_zero_argument: bool,
        execution_props: &ExecutionProps,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_type,
            monotonicity,
            supports_zero_argument,
            execution_props: execution_props.clone(),
        }
    }

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarFunctionDefinition {
        &self.fun
    }

    /// The name for this expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Input arguments
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }

    /// Monotonicity information of the function
    pub fn monotonicity(&self) -> &Option<FuncMonotonicity> {
        &self.monotonicity
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // evaluate the arguments, if there are no arguments we'll instead pass in a null array
        // indicating the batch size (as a convention)
        let inputs = match (
            self.args.is_empty(),
            self.name.parse::<BuiltinScalarFunction>(),
        ) {
            // MakeArray support zero argument but has the different behavior from the array with one null.
            (true, Ok(scalar_fun))
                if scalar_fun
                    .signature()
                    .type_signature
                    .supports_zero_argument() =>
            {
                vec![ColumnarValue::create_null_array(batch.num_rows())]
            }
            // If the function supports zero argument, we pass in a null array indicating the batch size.
            // This is for user-defined functions.
            (true, Err(_))
                if self.supports_zero_argument && self.name != "make_array" =>
            {
                vec![ColumnarValue::create_null_array(batch.num_rows())]
            }
            _ => self
                .args
                .iter()
                .map(|e| e.evaluate(batch))
                .collect::<Result<Vec<_>>>()?,
        };

        let fun_implementation = match self.fun {
            ScalarFunctionDefinition::BuiltIn(ref fun) => {
                create_physical_fun(fun, &self.execution_props)?
            }
            _ => {
                todo!("User-defined functions are not supported yet")
            }
        };
        // evaluate the function
        let fun = fun_implementation.as_ref();
        (fun)(&inputs)
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::new(
            &self.name,
            self.fun.clone(),
            children,
            self.return_type().clone(),
            self.monotonicity.clone(),
            self.supports_zero_argument,
            &self.execution_props,
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.name.hash(&mut s);
        self.args.hash(&mut s);
        self.return_type.hash(&mut s);
        // Add `self.fun` when hash is available
    }

    fn get_ordering(&self, children: &[SortProperties]) -> SortProperties {
        self.monotonicity
            .as_ref()
            .map(|monotonicity| out_ordering(monotonicity, children))
            .unwrap_or(SortProperties::Unordered)
    }
}

impl PartialEq<dyn Any> for ScalarFunctionExpr {
    /// Comparing name, args and return_type
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && physical_exprs_equal(&self.args, &x.args)
                    && self.return_type == x.return_type
            })
            .unwrap_or(false)
    }
}
