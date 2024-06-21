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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;

use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::compute::bool_and as compute_bool_and;
use arrow::compute::bool_or as compute_bool_or;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;

use datafusion_common::internal_err;
use datafusion_common::{downcast_value, not_impl_err};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature, Volatility,
};

use datafusion_physical_expr_common::aggregate::groups_accumulator::bool_op::BooleanGroupsAccumulator;

// returns the new value after bool_and/bool_or with the new values, taking nullability into account
macro_rules! typed_bool_and_or_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let delta = $OP(array);
        Ok(ScalarValue::$SCALAR(delta))
    }};
}

// bool_and/bool_or the array and returns a ScalarValue of its corresponding type.
macro_rules! bool_and_or_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            DataType::Boolean => {
                typed_bool_and_or_batch!($VALUES, BooleanArray, Boolean, $OP)
            }
            e => {
                return internal_err!(
                    "Bool and/Bool or is not expected to receive the type {e:?}"
                );
            }
        }
    }};
}

/// dynamically-typed bool_and(array) -> ScalarValue
fn bool_and_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bool_and_or_batch!(values, compute_bool_and)
}

/// dynamically-typed bool_or(array) -> ScalarValue
fn bool_or_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bool_and_or_batch!(values, compute_bool_or)
}

make_udaf_expr_and_func!(
    BoolAnd,
    bool_and,
    expression,
    "The values to combine with `AND`",
    bool_and_udaf
);

make_udaf_expr_and_func!(
    BoolOr,
    bool_or,
    expression,
    "The values to combine with `OR`",
    bool_or_udaf
);

/// BOOL_AND aggregate expression
#[derive(Debug)]
pub struct BoolAnd {
    signature: Signature,
}

impl BoolAnd {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Boolean],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for BoolAnd {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for BoolAnd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bool_and"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::<BoolAndAccumulator>::default())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(args.name, self.name()),
            DataType::Boolean,
            true,
        )])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        match args.data_type {
            DataType::Boolean => {
                Ok(Box::new(BooleanGroupsAccumulator::new(|x, y| x && y)))
            }
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {} with {}",
                args.name,
                args.data_type
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn create_sliding_accumulator(
        &self,
        _: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::<BoolAndAccumulator>::default())
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

#[derive(Debug, Default)]
struct BoolAndAccumulator {
    acc: Option<bool>,
}

impl Accumulator for BoolAndAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.acc = match (self.acc, bool_and_batch(values)?) {
            (None, ScalarValue::Boolean(v)) => v,
            (Some(v), ScalarValue::Boolean(None)) => Some(v),
            (Some(a), ScalarValue::Boolean(Some(b))) => Some(a && b),
            _ => unreachable!(),
        };
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Boolean(self.acc))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Boolean(self.acc)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

/// BOOL_OR aggregate expression
#[derive(Debug, Clone)]
pub struct BoolOr {
    signature: Signature,
}

impl BoolOr {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Boolean],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for BoolOr {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for BoolOr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bool_or"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::<BoolOrAccumulator>::default())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(args.name, self.name()),
            DataType::Boolean,
            true,
        )])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        match args.data_type {
            DataType::Boolean => {
                Ok(Box::new(BooleanGroupsAccumulator::new(|x, y| x || y)))
            }
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {} with {}",
                args.name,
                args.data_type
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn create_sliding_accumulator(
        &self,
        _: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::<BoolOrAccumulator>::default())
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

#[derive(Debug, Default)]
struct BoolOrAccumulator {
    acc: Option<bool>,
}

impl Accumulator for BoolOrAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.acc = match (self.acc, bool_or_batch(values)?) {
            (None, ScalarValue::Boolean(v)) => v,
            (Some(v), ScalarValue::Boolean(None)) => Some(v),
            (Some(a), ScalarValue::Boolean(Some(b))) => Some(a || b),
            _ => unreachable!(),
        };
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Boolean(self.acc))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Boolean(self.acc)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}
