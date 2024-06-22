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

//! Aggregate function stubs for test in expr / optimizer.
//!
//! These are used to avoid a dependence on `datafusion-functions-aggregate` which live in a different crate

use std::any::Any;

use crate::{
    expr::AggregateFunction,
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::AggregateOrderSensitivity,
    Accumulator, AggregateUDFImpl, Expr, GroupsAccumulator, ReversedUDAF, Signature,
    Volatility,
};
use arrow::datatypes::{
    DataType, Field, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
use datafusion_common::{exec_err, not_impl_err, Result};

macro_rules! create_func {
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident) => {
        paste::paste! {
            /// Singleton instance of [$UDAF], ensures the UDAF is only created once
            /// named STATIC_$(UDAF). For example `STATIC_FirstValue`
            #[allow(non_upper_case_globals)]
            static [< STATIC_ $UDAF >]: std::sync::OnceLock<std::sync::Arc<crate::AggregateUDF>> =
            std::sync::OnceLock::new();

            /// AggregateFunction that returns a [AggregateUDF] for [$UDAF]
            ///
            /// [AggregateUDF]: crate::AggregateUDF
            pub fn $AGGREGATE_UDF_FN() -> std::sync::Arc<crate::AggregateUDF> {
                [< STATIC_ $UDAF >]
                    .get_or_init(|| {
                        std::sync::Arc::new(crate::AggregateUDF::from(<$UDAF>::default()))
                    })
                    .clone()
            }
        }
    }
}

create_func!(Sum, sum_udaf);

pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        sum_udaf(),
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

create_func!(Count, count_udaf);

pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        count_udaf(),
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

/// Stub `sum` used for optimizer testing
#[derive(Debug)]
pub struct Sum {
    signature: Signature,
}

impl Sum {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Sum {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Sum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!("SUM expects exactly one argument");
        }

        // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
        // smallint, int, bigint, real, double precision, decimal, or interval.

        fn coerced_type(data_type: &DataType) -> Result<DataType> {
            match data_type {
                DataType::Dictionary(_, v) => coerced_type(v),
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                    Ok(data_type.clone())
                }
                dt if dt.is_signed_integer() => Ok(DataType::Int64),
                dt if dt.is_unsigned_integer() => Ok(DataType::UInt64),
                dt if dt.is_floating() => Ok(DataType::Float64),
                _ => exec_err!("Sum not supported for {}", data_type),
            }
        }

        Ok(vec![coerced_type(&arg_types[0])?])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Int64 => Ok(DataType::Int64),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Decimal128(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal128(new_precision, *scale))
            }
            DataType::Decimal256(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL256_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal256(new_precision, *scale))
            }
            other => {
                exec_err!("[return_type] SUM not supported for {}", other)
            }
        }
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        unreachable!("stub should not have accumulate()")
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        unreachable!("stub should not have state_fields()")
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        unreachable!("stub should not have accumulate()")
    }

    fn create_sliding_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        unreachable!("stub should not have accumulate()")
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

/// Testing stub implementation of COUNT aggregate
pub struct Count {
    signature: Signature,
    aliases: Vec<String>,
}

impl std::fmt::Debug for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Count")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Count {
    pub fn new() -> Self {
        Self {
            aliases: vec!["count".to_string()],
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Count {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "COUNT"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        not_impl_err!("no impl for stub")
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("no impl for stub")
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("no impl for stub")
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

create_func!(Min, min_udaf);

pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        min_udaf(),
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

/// Testing stub implementation of Min aggregate
pub struct Min {
    signature: Signature,
    aliases: Vec<String>,
}

impl std::fmt::Debug for Min {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Min")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl Min {
    pub fn new() -> Self {
        Self {
            aliases: vec!["count".to_string()],
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Min {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "MIN"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        not_impl_err!("no impl for stub")
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("no impl for stub")
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("no impl for stub")
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

create_func!(Max, max_udaf);

pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        max_udaf(),
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

/// Testing stub implementation of MAX aggregate
pub struct Max {
    signature: Signature,
    aliases: Vec<String>,
}

impl std::fmt::Debug for Max {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Min")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl Max {
    pub fn new() -> Self {
        Self {
            aliases: vec!["count".to_string()],
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Max {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "MIN"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        not_impl_err!("no impl for stub")
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("no impl for stub")
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("no impl for stub")
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}
