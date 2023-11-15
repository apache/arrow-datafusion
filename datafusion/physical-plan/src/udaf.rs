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

//! This module contains functions and structs supporting user-defined aggregate functions.

use fmt::Debug;
use std::any::Any;
use std::fmt;

use arrow::{
    datatypes::Field,
    datatypes::{DataType, Schema},
};

use super::{expressions::format_state_name, Accumulator, AggregateExpr};
use datafusion_common::{not_impl_err, DataFusionError, Result};
pub use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::PhysicalExpr;

use datafusion_physical_expr::aggregate::utils::down_cast_any_ref;
use datafusion_physical_expr::utils::extract_constant_args;
use std::sync::Arc;

/// Creates a physical expression of the UDAF, that includes all necessary type coercion.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDAF.
pub fn create_aggregate_expr(
    fun: &AggregateUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    let input_exprs_types = input_phy_exprs
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let constant_args = extract_constant_args(input_phy_exprs);

    Ok(Arc::new(AggregateFunctionExpr {
        fun: fun.clone(),
        args: input_phy_exprs.to_vec(),
        data_type: fun
            .return_type
            .infer(&input_exprs_types, &constant_args)?
            .as_ref()
            .clone(),
        name: name.into(),
    }))
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Output / return type of this aggregate
    data_type: DataType,
    name: String,
}

impl AggregateFunctionExpr {
    /// Return the `AggregateUDF` used by this `AggregateFunctionExpr`
    pub fn fun(&self) -> &AggregateUDF {
        &self.fun
    }
}

impl AggregateExpr for AggregateFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let fields = (self.fun.state_type)(&self.data_type)?
            .iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(
                    format_state_name(&self.name, &format!("{i}")),
                    data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<Field>>();

        Ok(fields)
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        (self.fun.accumulator)(&self.data_type)
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator = (self.fun.accumulator)(&self.data_type)?;

        // Accumulators that have window frame startings different
        // than `UNBOUNDED PRECEDING`, such as `1 PRECEEDING`, need to
        // implement retract_batch method in order to run correctly
        // currently in DataFusion.
        //
        // If this `retract_batches` is not present, there is no way
        // to calculate result correctly. For example, the query
        //
        // ```sql
        // SELECT
        //  SUM(a) OVER(ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum_a
        // FROM
        //  t
        // ```
        //
        // 1. First sum value will be the sum of rows between `[0, 1)`,
        //
        // 2. Second sum value will be the sum of rows between `[0, 2)`
        //
        // 3. Third sum value will be the sum of rows between `[1, 3)`, etc.
        //
        // Since the accumulator keeps the running sum:
        //
        // 1. First sum we add to the state sum value between `[0, 1)`
        //
        // 2. Second sum we add to the state sum value between `[1, 2)`
        // (`[0, 1)` is already in the state sum, hence running sum will
        // cover `[0, 2)` range)
        //
        // 3. Third sum we add to the state sum value between `[2, 3)`
        // (`[0, 2)` is already in the state sum).  Also we need to
        // retract values between `[0, 1)` by this way we can obtain sum
        // between [1, 3) which is indeed the apropriate range.
        //
        // When we use `UNBOUNDED PRECEDING` in the query starting
        // index will always be 0 for the desired range, and hence the
        // `retract_batch` method will not be called. In this case
        // having retract_batch is not a requirement.
        //
        // This approach is a a bit different than window function
        // approach. In window function (when they use a window frame)
        // they get all the desired range during evaluation.
        if !accumulator.supports_retract_batch() {
            return not_impl_err!(
                "Aggregate can not be used as a sliding accumulator because \
                     `retract_batch` is not implemented: {}",
                self.name
            );
        }
        Ok(accumulator)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for AggregateFunctionExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.fun == x.fun
                    && self.args.len() == x.args.len()
                    && self
                        .args
                        .iter()
                        .zip(x.args.iter())
                        .all(|(this_arg, other_arg)| this_arg.eq(other_arg))
            })
            .unwrap_or(false)
    }
}
