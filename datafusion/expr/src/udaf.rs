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

//! Udaf module contains functions and structs supporting user-defined aggregate functions.

use crate::function::ReturnTypeFactory;
use crate::Expr;
use crate::{
    AccumulatorFactoryFunction, ReturnTypeFunction, Signature, StateTypeFunction,
};
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
/// `AggregateUDF` provides DataFusion the information needed to plan
/// and call aggregate functions, including name, type information,
/// and a factory function to create [`Accumulator`], which peform the
/// actual aggregation.
///
/// For more information, please see [the examples].
///
/// [the examples]: https://github.com/apache/arrow-datafusion/tree/main/datafusion-examples#single-process
/// [aggregate function]: https://en.wikipedia.org/wiki/Aggregate_function
/// [`Accumulator`]: crate::Accumulator
#[derive(Clone)]
pub struct AggregateUDF {
    /// name
    pub name: String,
    /// Signature (input arguments)
    pub signature: Signature,
    /// Return type
    pub return_type: Arc<dyn ReturnTypeFactory>,
    /// actual implementation
    pub accumulator: AccumulatorFactoryFunction,
    /// the accumulator's state's description as a function of the return type
    pub state_type: StateTypeFunction,
}

impl Debug for AggregateUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for AggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for AggregateUDF {}

impl std::hash::Hash for AggregateUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
    }
}

impl AggregateUDF {
    /// Create a new AggregateUDF
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        accumulator: &AccumulatorFactoryFunction,
        state_type: &StateTypeFunction,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: Arc::new(return_type.clone()),
            accumulator: accumulator.clone(),
            state_type: state_type.clone(),
        }
    }

    /// creates an [`Expr`] that calls the aggregate function.
    ///
    /// This utility allows using the UDAF without requiring access to
    /// the registry, such as with the DataFrame API.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::AggregateUDF(crate::expr::AggregateUDF {
            fun: Arc::new(self.clone()),
            args,
            filter: None,
            order_by: None,
        })
    }
}
